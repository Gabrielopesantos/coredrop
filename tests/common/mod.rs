//! Fixtures shared by the integration test binaries.
//!
//! Each test binary compiles its own copy of this module, which is what the
//! `SPAWN_LOCK` below relies on: the hazard it guards is per-process.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::fmt;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_compression::tokio::bufread::ZstdDecoder;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tokio::io::AsyncReadExt;

use coredrop::config::HandlerConfig;

/// Serializes tests that write executable scripts and spawn subprocesses.
/// Without it, a concurrent test's fork can inherit another test's open
/// write-fd for a script, making that script's exec fail with ETXTBSY. The
/// race is between threads of one process, so a per-binary lock is enough.
pub static SPAWN_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The fixture `/proc` root inside a test's temp dir.
pub fn proc_root(root: &Path) -> PathBuf {
    root.join("proc")
}

/// Build a minimal fixture `/proc/<pid>` tree with a k8s cgroup and an env
/// var planted for redaction checks (`SECRET_KEY`).
pub fn write_fixture_proc(root: &Path, pid: i32, pod_uid: &str, container_id: &str) {
    let pid_dir = proc_root(root).join(pid.to_string());
    std::fs::create_dir_all(&pid_dir).unwrap();
    // cgroupfs v2: `0::/kubepods/<qos>/pod<uid>/<cid>`
    std::fs::write(
        pid_dir.join("cgroup"),
        format!("0::/kubepods/besteffort/pod{pod_uid}/{container_id}\n"),
    )
    .unwrap();
    std::fs::write(pid_dir.join("status"), b"Name:\tcrash-test\n").unwrap();
    std::fs::write(
        pid_dir.join("environ"),
        b"SECRET_KEY=hunter2\0LANG=en_US.UTF-8\0",
    )
    .unwrap();
}

/// Handler config rooted at a test's temp dir. crictl is `/bin/false`, so
/// enrichment fails and identity stays cgroup-only unless a test overrides it
/// with [`write_fake_crictl`]. Events are off; a test that wants them sets
/// `event_socket_path` after binding a listener.
pub fn base_config(root: &Path) -> HandlerConfig {
    HandlerConfig {
        cluster: "test".into(),
        no_redact: false,
        proc_root: proc_root(root).to_string_lossy().into_owned(),
        store_url: None,
        store_options: vec![],
        crictl_path: "/bin/false".into(),
        cri_runtime_endpoint: None,
        max_core_bytes: 0,
        max_cores_per_hour: 0,
        upload_deadline_secs: 0,
        crictl_timeout_secs: 0,
        rate_state_path: root.join("recent.json").to_string_lossy().into_owned(),
        event_socket_path: None,
    }
}

/// Write an executable `crictl` stand-in and return its path. Callers must
/// hold [`SPAWN_LOCK`].
pub fn write_fake_crictl(root: &Path, script: &str) -> PathBuf {
    let path = root.join("crictl");
    std::fs::write(&path, script).unwrap();
    let mut perms = std::fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&path, perms).unwrap();
    path
}

/// A `crictl inspect` response with the fields `extract` reads, as a shell
/// script that prints it.
pub fn fake_crictl_script(namespace: &str, pod: &str, container: &str) -> String {
    let json = format!(
        r#"{{"status":{{"id":"abc123def456abc123","metadata":{{"name":"{container}","attempt":2}},"image":{{"image":"docker.io/library/nginx:1.25"}},"imageRef":"docker.io/library/nginx@sha256:cafebabe1234","labels":{{"io.kubernetes.pod.namespace":"{namespace}","io.kubernetes.pod.name":"{pod}","io.kubernetes.container.name":"{container}"}}}}}}"#
    );
    format!("#!/bin/sh\nprintf '%s' '{json}'\n")
}

pub async fn unzstd(bytes: &[u8]) -> Vec<u8> {
    let mut dec = ZstdDecoder::new(io::Cursor::new(bytes.to_vec()));
    let mut out = Vec::new();
    dec.read_to_end(&mut out).await.unwrap();
    out
}

pub async fn get_object(store: &Arc<dyn ObjectStore>, key: &str) -> Vec<u8> {
    store
        .get(&ObjectPath::from(key))
        .await
        .unwrap_or_else(|_| panic!("object missing: {key}"))
        .bytes()
        .await
        .unwrap()
        .to_vec()
}

/// Whether `key` is absent from the store.
pub async fn is_absent(store: &Arc<dyn ObjectStore>, key: &str) -> bool {
    store.get(&ObjectPath::from(key)).await.is_err()
}

/// A core stream that never yields data nor EOF - stands in for a hung store
/// or kernel pipe when exercising the upload deadline.
pub struct StallReader;

impl tokio::io::AsyncRead for StallReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Pending
    }
}

/// An `ObjectStore` that rejects writes to keys ending in `fail_suffix` and
/// delegates everything else to an `InMemory`, so a test can fail one stage of
/// the capture pipeline and inspect what the earlier stages left behind.
pub struct FailingStore {
    inner: Arc<InMemory>,
    fail_suffix: &'static str,
    /// Also reject the multipart path. `BufWriter` picks single-put vs
    /// multipart by payload size, so a core (which streams) needs both.
    fail_multipart: bool,
}

impl FailingStore {
    /// Rejects the manifest PUT; the core and proc snapshot still land.
    pub fn manifest(inner: Arc<InMemory>) -> Self {
        Self {
            inner,
            fail_suffix: "-manifest.json",
            fail_multipart: false,
        }
    }

    /// Rejects the core upload on both the single-put and multipart paths.
    pub fn core(inner: Arc<InMemory>) -> Self {
        Self {
            inner,
            fail_suffix: "-core.zst",
            fail_multipart: true,
        }
    }

    fn rejects(&self, location: &ObjectPath) -> bool {
        location.as_ref().ends_with(self.fail_suffix)
    }

    fn injected_failure(&self) -> object_store::Error {
        object_store::Error::Generic {
            store: "FailingStore",
            source: Box::new(io::Error::other(format!(
                "injected failure for *{}",
                self.fail_suffix
            ))),
        }
    }
}

impl fmt::Display for FailingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailingStore(*{})", self.fail_suffix)
    }
}

impl fmt::Debug for FailingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailingStore(*{})", self.fail_suffix)
    }
}

#[async_trait]
impl ObjectStore for FailingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.rejects(location) {
            return Err(self.injected_failure());
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        if self.fail_multipart && self.rejects(location) {
            return Err(self.injected_failure());
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
