use std::fmt;
use std::io;
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
use coredrop::handler::{CaptureArgs, run};
use coredrop::manifest::Manifest;
use coredrop::upload;

// ── Fixtures ─────────────────────────────────────────────────────────────────

fn unique_tmp(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::path::PathBuf::from(format!(
        "/tmp/coredrop-handler-test-{}-{tag}-{nanos}",
        std::process::id()
    ))
}

/// Build a minimal fixture `/proc/<pid>` tree with a k8s cgroup and an env
/// var planted for redaction checks (`SECRET_KEY`).
fn write_fixture_proc(proc_dir: &std::path::Path, pid: i32, pod_uid: &str, container_id: &str) {
    let pid_dir = proc_dir.join(pid.to_string());
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

fn base_config(proc_dir: &std::path::Path) -> HandlerConfig {
    HandlerConfig {
        cluster: "test".into(),
        backend: "standalone".into(),
        no_redact: false,
        proc_root: proc_dir.to_str().unwrap().to_string(),
        systemd_coredump_path: None,
        store_url: None,
        store_options: vec![],
        crictl_path: "/bin/false".into(), // degraded -- cgroup-only identity
        cri_runtime_endpoint: None,
    }
}

async fn unzstd(bytes: &[u8]) -> Vec<u8> {
    let mut dec = ZstdDecoder::new(io::Cursor::new(bytes.to_vec()));
    let mut out = Vec::new();
    dec.read_to_end(&mut out).await.unwrap();
    out
}

async fn get_object(store: &Arc<dyn object_store::ObjectStore>, key: &str) -> Vec<u8> {
    store
        .get(&ObjectPath::from(key))
        .await
        .unwrap_or_else(|_| panic!("object missing: {key}"))
        .bytes()
        .await
        .unwrap()
        .to_vec()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// 2a -- happy path: core, proc-snapshot, and manifest all land in the store.
/// Verifies the load-bearing blob-first invariant: the manifest's
/// `core.object_key` points at an object that actually exists (no dangling
/// manifests).
#[tokio::test]
async fn handler_run_uploads_core_snapshot_and_writes_manifest() {
    let pod_uid = "ed1e9c81-9a92-4f7e-be2c-8b26b56d3b98";
    let container_id = "abc123def456abc123def456"; // 24 hex chars -- valid
    let ts: i64 = 1_749_600_000;
    let pid = 4242;
    let core_payload: &[u8] = b"fake core payload for testing - not a real ELF";

    let tmp = unique_tmp("e2e");
    let proc_dir = tmp.join("proc");
    write_fixture_proc(&proc_dir, pid, pod_uid, container_id);

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let config = base_config(&proc_dir);
    let args = CaptureArgs {
        host_pid: pid,
        signal: 11,
        timestamp: ts,
        exe: "!usr!bin!crasher".into(),
    };

    let mut core_in: &[u8] = core_payload;
    run(args, &config, &mut core_in, Some(store.clone()))
        .await
        .unwrap();

    // Core object: present and decompresses to the input bytes.
    let core_key = upload::core_object_key("test", pod_uid, container_id, ts);
    let stored_core = get_object(&store, &core_key).await;
    assert_eq!(unzstd(&stored_core).await, core_payload);

    // Proc-snapshot tar: present.
    let snap_key = upload::proc_snapshot_object_key("test", pod_uid, container_id, ts);
    get_object(&store, &snap_key).await;

    // Manifest: present, parses, and its core.object_key exists in the store.
    let manifest_key = upload::manifest_object_key("test", pod_uid, container_id, ts);
    let manifest_bytes = get_object(&store, &manifest_key).await;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).unwrap();

    assert!(manifest.core.present, "core.present must be true");
    assert_eq!(manifest.signal, 11);
    assert_eq!(manifest.signal_name.as_deref(), Some("SIGSEGV"));
    assert_eq!(manifest.cluster, "test");
    assert_eq!(manifest.identity.pod_uid, pod_uid);
    assert_eq!(manifest.identity.container_id, container_id);
    assert!(manifest.core.sha256.is_some(), "sha256 populated");
    assert!(manifest.core.size_bytes.unwrap_or(0) > 0, "size_bytes > 0");
    assert!(
        manifest.core.stored_bytes.unwrap_or(0) > 0,
        "stored_bytes > 0"
    );
    assert!(!manifest.core.truncated);
    assert_eq!(manifest.core.codec, "zstd");

    // The blob-first invariant: manifest.core.object_key must exist in the
    // store. No manifest may point at a missing core.
    let manifest_core_key = manifest
        .core
        .object_key
        .as_deref()
        .expect("core key in manifest");
    assert_eq!(manifest_core_key, core_key);
    store
        .get(&ObjectPath::from(manifest_core_key))
        .await
        .expect("manifest.core.object_key must exist in the store");

    // Proc-snapshot ref in manifest.
    let snap_ref = manifest
        .proc_snapshot
        .as_ref()
        .expect("proc_snapshot in manifest");
    assert_eq!(snap_ref.object_key, snap_key);
    assert!(snap_ref.file_count > 0);

    std::fs::remove_dir_all(&tmp).ok();
}

/// 2a -- no store: run completes Ok, core discarded, no manifest written.
#[tokio::test]
async fn handler_run_without_store_discards_silently() {
    let pod_uid = "ed1e9c81-9a92-4f7e-be2c-8b26b56d3b98";
    let container_id = "abc123def456abc123def456";
    let pid = 100;

    let tmp = unique_tmp("nostore");
    let proc_dir = tmp.join("proc");
    write_fixture_proc(&proc_dir, pid, pod_uid, container_id);

    let config = base_config(&proc_dir);
    let args = CaptureArgs {
        host_pid: pid,
        signal: 6,
        timestamp: 1_000_000,
        exe: "!usr!bin!app".into(),
    };

    let mut core_in: &[u8] = b"some core bytes";
    // No store_override, config.store_url = None -> DiscardBackend, no manifest.
    run(args, &config, &mut core_in, None).await.unwrap();

    std::fs::remove_dir_all(&tmp).ok();
}

/// 2a -- non-k8s cgroup: run completes Ok, core discarded (no key derivable).
#[tokio::test]
async fn handler_run_non_kubernetes_cgroup_skips_uploads() {
    let pid = 200;

    let tmp = unique_tmp("nokube");
    let proc_dir = tmp.join("proc");
    let pid_dir = proc_dir.join(pid.to_string());
    std::fs::create_dir_all(&pid_dir).unwrap();
    // Non-k8s cgroup -> parse_cgroup returns None.
    std::fs::write(pid_dir.join("cgroup"), "0::/system.slice/sshd.service\n").unwrap();

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let config = base_config(&proc_dir);
    let args = CaptureArgs {
        host_pid: pid,
        signal: 9,
        timestamp: 2_000_000,
        exe: "!usr!sbin!sshd".into(),
    };

    let mut core_in: &[u8] = b"core bytes";
    run(args, &config, &mut core_in, Some(store.clone()))
        .await
        .unwrap();

    // No identity -> no objects written.
    let result = store.list_with_delimiter(None).await.unwrap();
    assert!(
        result.objects.is_empty(),
        "no objects should be written without a k8s cgroup"
    );

    std::fs::remove_dir_all(&tmp).ok();
}

// ── FailManifestStore ─────────────────────────────────────────────────────────
//
// Thin ObjectStore wrapper that fails every put whose key ends with
// `-manifest.json`. Used in the 2c blob-first ordering test.

struct FailManifestStore {
    inner: Arc<InMemory>,
}

impl fmt::Display for FailManifestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailManifestStore")
    }
}

impl fmt::Debug for FailManifestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailManifestStore")
    }
}

#[async_trait]
impl ObjectStore for FailManifestStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().ends_with("-manifest.json") {
            return Err(object_store::Error::Generic {
                store: "FailManifestStore",
                source: Box::new(std::io::Error::other("injected manifest write failure")),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
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

/// 2c -- blob-first write ordering: when the manifest PUT fails, the core and
/// proc-snapshot are still present. An orphan blob is acceptable; a dangling
/// manifest (pointing at a missing core) is not.
#[tokio::test]
async fn handler_run_blob_first_core_survives_manifest_failure() {
    let pod_uid = "ed1e9c81-9a92-4f7e-be2c-8b26b56d3b98";
    let container_id = "abc123def456abc123def456";
    let ts: i64 = 1_749_600_000;
    let pid = 4242;

    let tmp = unique_tmp("blobfirst");
    let proc_dir = tmp.join("proc");
    write_fixture_proc(&proc_dir, pid, pod_uid, container_id);

    let inner = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = Arc::new(FailManifestStore {
        inner: inner.clone(),
    });
    let config = base_config(&proc_dir);
    let args = CaptureArgs {
        host_pid: pid,
        signal: 11,
        timestamp: ts,
        exe: "!usr!bin!crasher".into(),
    };

    let mut core_in: &[u8] = b"core payload for blob-first test";
    // run() must complete Ok even when manifest write fails (handler warns + continues).
    run(args, &config, &mut core_in, Some(store)).await.unwrap();

    // Core and proc-snapshot are present (written before the manifest attempt).
    let core_key = upload::core_object_key("test", pod_uid, container_id, ts);
    let snap_key = upload::proc_snapshot_object_key("test", pod_uid, container_id, ts);
    inner
        .get(&ObjectPath::from(core_key.as_str()))
        .await
        .expect("core present");
    inner
        .get(&ObjectPath::from(snap_key.as_str()))
        .await
        .expect("proc-snapshot present");

    // Manifest was NOT written (the FailManifestStore rejected it).
    let manifest_key = upload::manifest_object_key("test", pod_uid, container_id, ts);
    assert!(
        inner
            .get(&ObjectPath::from(manifest_key.as_str()))
            .await
            .is_err(),
        "manifest must not exist when write failed"
    );

    std::fs::remove_dir_all(&tmp).ok();
}
