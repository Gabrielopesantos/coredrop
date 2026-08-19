//! Streaming core upload: core fd -> zstd -> sha256 -> object store.
//!
//! Cores can be multi-GB; the standalone backend streams, never buffers. The
//! kernel hands the core on the handler's stdin; we pipe it straight through a
//! zstd encoder into an `object_store` multipart upload, computing the stored
//! object's size and sha256 *in the pipe* - the uncompressed core never lands
//! on disk or in memory. Only the small `/proc` snapshot is buffered (in
//! [`crate::snapshot`]).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use async_compression::tokio::write::ZstdEncoder;
use async_trait::async_trait;
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Instant, timeout_at};
use tracing::warn;

use crate::backend::{CaptureBackend, CoreStats};

// Core chunks of 256 KBs
const CORE_READ_CHUNK: usize = 256 * 1024;

/// Build the core's object key:
/// `{cluster}/{podUID}/{containerID}/{timestamp}-{hostPid}-core.zst`.
/// Handler-derivable from the cgroup - no UUIDs; the manifest carries the
/// human-readable identity from crictl. The host PID disambiguates two
/// crashes in the same container within the same second (possible before the
/// rate limiter engages), which would otherwise overwrite each other.
#[must_use]
pub fn core_object_key(
    cluster: &str,
    pod_uid: &str,
    container_id: &str,
    timestamp: i64,
    host_pid: i32,
) -> String {
    format!("{cluster}/{pod_uid}/{container_id}/{timestamp}-{host_pid}-core.zst")
}

/// Build the `/proc` snapshot's object key, mirroring `core_object_key`'s
/// scheme - same prefix, distinct suffix.
#[must_use]
pub fn proc_snapshot_object_key(
    cluster: &str,
    pod_uid: &str,
    container_id: &str,
    timestamp: i64,
    host_pid: i32,
) -> String {
    format!("{cluster}/{pod_uid}/{container_id}/{timestamp}-{host_pid}-procsnapshot.tar")
}

/// Build the JSON manifest's object key, sibling to the core.
#[must_use]
pub fn manifest_object_key(
    cluster: &str,
    pod_uid: &str,
    container_id: &str,
    timestamp: i64,
    host_pid: i32,
) -> String {
    format!("{cluster}/{pod_uid}/{container_id}/{timestamp}-{host_pid}-manifest.json")
}

/// Buffered single-shot PUT of a small object (the proc snapshot tar or the
/// manifest JSON). Unlike the multi-GB core (which *streams*), these are
/// bounded and already in memory.
///
/// # Errors
///
/// Fails when the object store PUT fails.
pub async fn put_object(store: &Arc<dyn ObjectStore>, key: &str, bytes: Vec<u8>) -> Result<()> {
    store
        .put(&ObjectPath::from(key), PutPayload::from(bytes))
        .await
        .with_context(|| format!("putting object {key}"))?;
    Ok(())
}

/// The standalone capture backend: stream the core to the S3-compatible object
/// store. The destination key is fixed at construction (handler-derived from
/// the cgroup).
pub struct StandaloneBackend {
    store: Arc<dyn ObjectStore>,
    key: ObjectPath,
    /// Max uncompressed bytes stored per core; `0` = unlimited. The stream
    /// past the cap is still drained (the kernel blocks until EOF) but not
    /// stored.
    max_core_bytes: u64,
}

impl StandaloneBackend {
    pub fn new(store: Arc<dyn ObjectStore>, key: &str, max_core_bytes: u64) -> Self {
        Self {
            store,
            key: ObjectPath::from(key),
            max_core_bytes,
        }
    }
}

#[async_trait]
impl CaptureBackend for StandaloneBackend {
    async fn drain_core(
        &self,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        deadline: Option<Duration>,
    ) -> Result<CoreStats> {
        let sink = BufWriter::new(self.store.clone(), self.key.clone());
        let (bytes, stored_bytes, sha256, truncated_reason) =
            stream_core_through_zstd(reader, sink, self.max_core_bytes, deadline).await?;
        Ok(CoreStats {
            bytes,
            stored_bytes,
            sha256: Some(sha256),
            truncated: truncated_reason.is_some(),
            truncated_reason,
        })
    }
}

// BufWriter has no Drop impl (object_store 0.13.1).
// MultipartUpload docs say S3/GCS "cannot perform cleanup on drop" -
// targets configured. So racing the whole drain against a timeout
// and letting the loser get dropped (the old approach) always leaked
// the in-progress multipart on timeout: abort() must be called explicitly,
// which means whatever cancels the read/write has to still own the BufWriter
// afterward. `race()` below races individual awaits, not the whole
// function, so `encoder`/`sink` stay in scope to call `abort_multipart` on
// timeout instead of being dropped with it.
//
// Caveat: `BufWriter::poll_shutdown` consumes the multipart handle into its
// own finish future on the first poll of `shutdown()`, so `abort()`
// panics ("Already shut down") if shutdown has been polled even once.
// `abort_multipart` is therefore only called from the read/write loop,
// never after `shutdown()` is invoked - a deadline landing during that
// final call has no safe abort path and relies on the bucket lifecycle
// rule in the chart README's Retention section instead.
async fn race<T>(
    fut: impl std::future::Future<Output = T>,
    deadline_at: Option<Instant>,
) -> Result<T, tokio::time::error::Elapsed> {
    match deadline_at {
        Some(dl) => timeout_at(dl, fut).await,
        None => Ok(fut.await),
    }
}

/// Abort the in-progress multipart upload. Only safe to call before
/// `shutdown()`/finalize has ever been polled - see the module comment
/// above `race`.
async fn abort_multipart(encoder: ZstdEncoder<HashingWriter<BufWriter>>) {
    let HashingWriter {
        inner: mut sink, ..
    } = encoder.into_inner();
    if let Err(e) = sink.abort().await {
        warn!(
            error = %e,
            "aborting multipart upload failed; orphaned parts may remain in the object store"
        );
    }
}

async fn stream_core_through_zstd<R>(
    core: &mut R,
    sink: BufWriter,
    max_core_bytes: u64,
    deadline: Option<Duration>,
) -> Result<(u64, u64, String, Option<String>)>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let deadline_at = deadline.map(|d| Instant::now() + d);
    let cap = if max_core_bytes == 0 {
        u64::MAX
    } else {
        max_core_bytes
    };
    let hashing = HashingWriter {
        inner: sink,
        hasher: Sha256::new(),
        bytes: 0,
    };
    let mut encoder = ZstdEncoder::new(hashing);

    let mut buf = vec![0u8; CORE_READ_CHUNK];
    let mut drained = 0u64;
    let mut written = 0u64;
    let mut truncated_reason: Option<String> = None;
    loop {
        match race(core.read(&mut buf), deadline_at).await {
            Err(_elapsed) => {
                warn!(
                    drained,
                    "core drain exceeded the upload deadline while reading; aborting multipart upload"
                );
                abort_multipart(encoder).await;
                return Err(anyhow!("core drain exceeded the upload deadline"));
            }
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                drained += n as u64;
                // min() keeps the value <= n, so the conversion back to usize cannot fail.
                let take =
                    usize::try_from((n as u64).min(cap.saturating_sub(written))).unwrap_or(n);
                if take > 0 {
                    match race(encoder.write_all(&buf[..take]), deadline_at).await {
                        Err(_elapsed) => {
                            warn!(
                                drained,
                                "core upload exceeded the upload deadline while writing; aborting multipart upload"
                            );
                            abort_multipart(encoder).await;
                            return Err(anyhow!("core drain exceeded the upload deadline"));
                        }
                        Ok(Err(e)) => {
                            abort_multipart(encoder).await;
                            return Err(e).context("writing core into zstd encoder");
                        }
                        Ok(Ok(())) => written += take as u64,
                    }
                }
                if take < n && truncated_reason.is_none() {
                    warn!(
                        cap,
                        "core exceeds size cap - storing first {cap} bytes, draining the rest"
                    );
                    truncated_reason = Some("size_cap".to_string());
                }
            }
            Ok(Err(e)) => {
                warn!(error = %e, drained, "core stream read error - finalizing partial object as truncated");
                truncated_reason = Some("stream_error".to_string());
                break;
            }
        }
    }

    // Past this point `shutdown()` has been polled at least once even if it
    // times out below, so the multipart handle is gone - no more `abort()`
    // calls are possible (see the module comment above `race`).
    match race(encoder.shutdown(), deadline_at).await {
        Err(_elapsed) => {
            warn!(
                "core upload exceeded the upload deadline while finalizing; \
                 multipart upload may be left incomplete in the object store"
            );
            return Err(anyhow!("core drain exceeded the upload deadline"));
        }
        Ok(Err(e)) => return Err(e).context("finalizing zstd stream + completing upload"),
        Ok(Ok(())) => {}
    }

    let HashingWriter {
        hasher,
        bytes: stored_bytes,
        ..
    } = encoder.into_inner();
    Ok((
        drained,
        stored_bytes,
        hex_lower(&hasher.finalize()),
        truncated_reason,
    ))
}

struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    bytes: u64,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for HashingWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                this.hasher.update(&buf[..n]);
                this.bytes += n as u64;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// The `AWS_*` and GCP/Azure keys `object_store` recognizes. Forwarding only
/// these (an allowlist) keeps an unknown env key from making `parse_url_opts`
/// error out and needlessly disabling capture.
// These are the exact env-var-shaped keys each cloud's workload-identity
// webhook injects into the pod, and each is confirmed wired to a real credential
// provider, not just stored and ignored:
//
// - IRSA (EKS): the pod-identity webhook injects `AWS_ROLE_ARN` +
//   `AWS_WEB_IDENTITY_TOKEN_FILE` (+ `AWS_DEFAULT_REGION`).
//   `AmazonS3Builder` reads both and feeds them to `WebIdentityProvider`
//   (assume-role-with-web-identity).
// - AKS workload identity: the webhook injects `AZURE_CLIENT_ID`,
//   `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_AUTHORITY_HOST`.
//   `MicrosoftAzureBuilder` feeds client id + tenant id + federated token
//   file to `WorkloadIdentityOAuthProvider`.
// - GKE workload identity: `GoogleCloudStorageBuilder` falls back to the GCE
//   metadata server (`InstanceCredentialProvider`) with no env vars at all
//   when no service-account key/path is configured - nothing to add
//   to this allowlist for GCP WI to work.
pub const ALLOWED_STORE_OPTS: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT",
    "AWS_ALLOW_HTTP",
    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST",
    "AWS_ROLE_ARN",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "GOOGLE_SERVICE_ACCOUNT",
    "GOOGLE_SERVICE_ACCOUNT_KEY",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "AZURE_STORAGE_ACCESS_KEY",
    "AZURE_STORAGE_CLIENT_ID",
    "AZURE_STORAGE_CLIENT_SECRET",
    "AZURE_STORAGE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_TENANT_ID",
    "AZURE_FEDERATED_TOKEN_FILE",
    "AZURE_AUTHORITY_HOST",
];

/// Collect the `object_store` config options ([`ALLOWED_STORE_OPTS`]) present
/// in the process environment. Shared by the daemon (building the config it
/// writes for the handler) and the handler (its own `from_env` fallback when
/// the daemon-written config is unreadable).
#[must_use]
pub fn store_options_from_env() -> Vec<(String, String)> {
    std::env::vars()
        .filter(|(k, _)| ALLOWED_STORE_OPTS.contains(&k.as_str()))
        .collect()
}

/// Whether the forwarded store options point at a plaintext endpoint. Cores
/// are secret-bearing, so an `http://` endpoint (or a blanket `AWS_ALLOW_HTTP`)
/// puts them on the network in the clear - acceptable for an in-cluster dev
/// store, worth a startup warning anywhere else.
#[must_use]
pub fn is_plaintext_endpoint(opts: &[(String, String)]) -> bool {
    let get = |key: &str| {
        opts.iter()
            .find(|(k, _)| k == key)
            .map_or("", |(_, v)| v.as_str())
    };
    get("AWS_ENDPOINT").to_lowercase().starts_with("http://")
        || matches!(
            get("AWS_ALLOW_HTTP").to_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
}

/// Retry policy for cloud uploads. `object_store` defaults to 10 retries over
/// 3 minutes; the handler may be holding one of the node's `core_pipe_limit`
/// concurrency slots mid-multipart, and a slow store keeps that slot occupied
/// longer, so bound the worst case tighter.
fn retry_config() -> object_store::RetryConfig {
    object_store::RetryConfig {
        max_retries: 3,
        retry_timeout: std::time::Duration::from_mins(1),
        ..Default::default()
    }
}

/// Build an object store from a store URL (e.g. `s3://crash-artifacts`) plus
/// `object_store` config options. `None` when the URL is invalid or the store
/// can't be built - the handler then discards the core.
///
/// Dispatches on scheme rather than `parse_url_opts` because retry config is
/// builder-only; opts are folded the same way `parse_url_opts` does (lowercase
/// key -> config key, unknown keys skipped).
pub fn object_store_from_url_opts(
    raw_url: &str,
    opts: Vec<(String, String)>,
) -> Option<Arc<dyn ObjectStore>> {
    let url = match url::Url::parse(raw_url) {
        Ok(u) => u,
        Err(e) => {
            warn!(error = %e, url = %raw_url, "store url is not a valid URL; core upload disabled");
            return None;
        }
    };
    let scheme = match object_store::ObjectStoreScheme::parse(&url) {
        Ok((scheme, _path)) => scheme,
        Err(e) => {
            warn!(error = %e, url = %raw_url, "unrecognized store url scheme; core upload disabled");
            return None;
        }
    };
    let built: object_store::Result<Arc<dyn ObjectStore>> = match scheme {
        object_store::ObjectStoreScheme::AmazonS3 => {
            let mut builder = object_store::aws::AmazonS3Builder::new()
                .with_url(url.to_string())
                .with_retry(retry_config());
            for (k, v) in opts {
                if let Ok(key) = k.to_ascii_lowercase().parse() {
                    builder = builder.with_config(key, v);
                }
            }
            builder.build().map(|s| Arc::new(s) as _)
        }
        object_store::ObjectStoreScheme::GoogleCloudStorage => {
            let mut builder = object_store::gcp::GoogleCloudStorageBuilder::new()
                .with_url(url.to_string())
                .with_retry(retry_config());
            for (k, v) in opts {
                if let Ok(key) = k.to_ascii_lowercase().parse() {
                    builder = builder.with_config(key, v);
                }
            }
            builder.build().map(|s| Arc::new(s) as _)
        }
        object_store::ObjectStoreScheme::MicrosoftAzure => {
            let mut builder = object_store::azure::MicrosoftAzureBuilder::new()
                .with_url(url.to_string())
                .with_retry(retry_config());
            for (k, v) in opts {
                if let Ok(key) = k.to_ascii_lowercase().parse() {
                    builder = builder.with_config(key, v);
                }
            }
            builder.build().map(|s| Arc::new(s) as _)
        }
        // Local backends: no HTTP, no retry policy to apply.
        object_store::ObjectStoreScheme::Memory => {
            Ok(Arc::new(object_store::memory::InMemory::new()) as _)
        }
        object_store::ObjectStoreScheme::Local => {
            object_store::local::LocalFileSystem::new_with_prefix(url.path())
                .map(|s| Arc::new(s) as _)
        }
        other => {
            warn!(scheme = ?other, url = %raw_url, "unsupported store url scheme; core upload disabled");
            return None;
        }
    };
    match built {
        Ok(store) => Some(store),
        Err(e) => {
            warn!(error = %e, url = %raw_url, "building object store failed; core upload disabled");
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use async_compression::tokio::bufread::ZstdDecoder;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use std::io;

    /// The Helm chart hard-codes a copy of [`ALLOWED_STORE_OPTS`] so it can
    /// `fail` on a typo'd key at render time. Nothing links the two lists, so
    /// this test is what keeps the copy honest: without it, adding a key here
    /// makes the chart reject a legitimate value.
    #[test]
    fn helpers_store_opts_match_allowlist() {
        const HELPERS: &str = include_str!("../charts/coredrop/templates/_helpers.tpl");

        let block = HELPERS
            .split_once(r#"{{- define "coredrop.allowedStoreOpts" -}}"#)
            .expect("_helpers.tpl defines coredrop.allowedStoreOpts")
            .1
            .split_once("{{- end -}}")
            .expect("the allowedStoreOpts define is terminated")
            .0;

        let chart: std::collections::BTreeSet<&str> = block
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect();
        let rust: std::collections::BTreeSet<&str> = ALLOWED_STORE_OPTS.iter().copied().collect();

        assert_eq!(
            chart, rust,
            "coredrop.allowedStoreOpts in _helpers.tpl drifted from ALLOWED_STORE_OPTS"
        );
    }

    #[test]
    fn builds_object_keys() {
        assert_eq!(
            core_object_key("prod", "pod-uid-123", "abc123def", 1_749_600_000, 4242),
            "prod/pod-uid-123/abc123def/1749600000-4242-core.zst"
        );
        assert_eq!(
            proc_snapshot_object_key("prod", "pod-uid-123", "abc123def", 1_749_600_000, 4242),
            "prod/pod-uid-123/abc123def/1749600000-4242-procsnapshot.tar"
        );
        assert_eq!(
            manifest_object_key("prod", "pod-uid-123", "abc123def", 1_749_600_000, 4242),
            "prod/pod-uid-123/abc123def/1749600000-4242-manifest.json"
        );
    }

    #[test]
    fn distinct_pids_yield_distinct_keys_within_the_same_second() {
        let a = core_object_key("prod", "pod-uid-123", "abc123def", 1_749_600_000, 111);
        let b = core_object_key("prod", "pod-uid-123", "abc123def", 1_749_600_000, 222);
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn put_object_round_trips_a_buffered_blob() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let key = proc_snapshot_object_key("local", "pod-a", "cid-b", 7, 4242);
        let tar = b"a-small-tar-bundle".to_vec();
        put_object(&store, &key, tar.clone()).await.unwrap();

        let stored = store
            .get(&ObjectPath::from(key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(stored.as_ref(), tar.as_slice());
    }

    async fn unzstd(bytes: &[u8]) -> Vec<u8> {
        let mut dec = ZstdDecoder::new(io::Cursor::new(bytes.to_vec()));
        let mut out = Vec::new();
        dec.read_to_end(&mut out).await.unwrap();
        out
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        hex_lower(&Sha256::digest(bytes))
    }

    #[tokio::test]
    async fn streams_zstd_with_integrity_and_round_trips() {
        let core: Vec<u8> = (0..200_000u32).map(|i| (i % 7) as u8).collect();
        let store = Arc::new(InMemory::new());
        let key = core_object_key("local", "pod-aaa", "cid-bbb", 42, 4242);

        let backend = StandaloneBackend::new(store.clone(), &key, 0);
        let mut reader: &[u8] = &core;
        let stats = backend.drain_core(&mut reader, None).await.unwrap();

        let stored = store
            .get(&ObjectPath::from(key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert!(!stats.truncated);
        assert_eq!(stats.bytes, core.len() as u64);
        assert_eq!(stats.stored_bytes, stored.len() as u64);
        assert!(
            stats.stored_bytes < stats.bytes,
            "zstd should compress this"
        );
        assert_eq!(stats.sha256.as_deref(), Some(sha256_hex(&stored).as_str()));
        assert_eq!(unzstd(&stored).await, core);
    }

    /// Yields one chunk, then stalls forever - stands in for a hung kernel
    /// pipe or wedged store mid-multipart.
    struct StallAfterOneChunk {
        chunk: Option<Vec<u8>>,
    }

    impl AsyncRead for StallAfterOneChunk {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            match self.chunk.as_mut() {
                Some(chunk) => {
                    let n = chunk.len().min(buf.remaining());
                    buf.put_slice(&chunk[..n]);
                    chunk.drain(..n);
                    if chunk.is_empty() {
                        self.chunk = None;
                    }
                    Poll::Ready(Ok(()))
                }
                None => Poll::Pending,
            }
        }
    }

    /// `InMemory` doesn't model billable orphaned parts the way S3/GCS do,
    /// so this can only prove the abort path runs without panicking and
    /// stores nothing - not that a real bucket gets cleaned up server-side.
    #[tokio::test(start_paused = true)]
    async fn drain_aborts_multipart_and_errors_on_deadline() {
        let store = Arc::new(InMemory::new());
        let key = ObjectPath::from("abort-test/core.zst");
        // Tiny capacity + a large incompressible first chunk forces the
        // BufWriter into multipart (Write) state before the second read
        // stalls and the deadline fires.
        let sink = BufWriter::with_capacity(store.clone(), key.clone(), 16);
        let incompressible: Vec<u8> = (0..512_000u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
            .collect();
        let mut reader = StallAfterOneChunk {
            chunk: Some(incompressible),
        };

        let result =
            stream_core_through_zstd(&mut reader, sink, 0, Some(Duration::from_secs(1))).await;

        assert!(
            result.is_err(),
            "a stalled drain must error at the deadline"
        );
        assert!(
            store.get(&key).await.is_err(),
            "no completed object may appear after an aborted multipart"
        );
    }

    struct FlakyReader {
        chunk: Vec<u8>,
        sent: bool,
    }

    impl AsyncRead for FlakyReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.sent {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "boom")));
            }
            self.sent = true;
            let n = self.chunk.len().min(buf.remaining());
            buf.put_slice(&self.chunk[..n]);
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn marks_truncated_on_a_short_read_but_finalizes_the_partial() {
        let chunk = vec![0x5Au8; 50_000];
        let store = Arc::new(InMemory::new());
        let key = core_object_key("local", "pod-x", "cid-y", 7, 4242);

        let backend = StandaloneBackend::new(store.clone(), &key, 0);
        let mut reader = FlakyReader {
            chunk: chunk.clone(),
            sent: false,
        };
        let stats = backend.drain_core(&mut reader, None).await.unwrap();

        assert!(stats.truncated);
        assert_eq!(stats.truncated_reason.as_deref(), Some("stream_error"));
        assert_eq!(stats.bytes, chunk.len() as u64);
        assert!(
            stats.stored_bytes > 0,
            "the partial object is still finalized"
        );

        let stored = store
            .get(&ObjectPath::from(key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(unzstd(&stored).await, chunk);
    }

    #[tokio::test]
    async fn caps_stored_core_but_drains_and_counts_everything() {
        let core: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
        let store = Arc::new(InMemory::new());
        let key = core_object_key("local", "pod-cap", "cid-cap", 9, 4242);

        let backend = StandaloneBackend::new(store.clone(), &key, 10_000);
        let mut reader: &[u8] = &core;
        let stats = backend.drain_core(&mut reader, None).await.unwrap();

        assert_eq!(stats.bytes, 50_000, "full stream drained and counted");
        assert!(stats.truncated);
        assert_eq!(stats.truncated_reason.as_deref(), Some("size_cap"));

        let stored = store
            .get(&ObjectPath::from(key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            unzstd(&stored).await,
            &core[..10_000],
            "stored object holds exactly the first cap bytes"
        );
    }

    #[test]
    fn builds_s3_store_with_opts() {
        let opts = vec![
            ("AWS_ACCESS_KEY_ID".to_string(), "ak".to_string()),
            ("AWS_SECRET_ACCESS_KEY".to_string(), "sk".to_string()),
            ("AWS_REGION".to_string(), "us-east-1".to_string()),
            ("NOT_A_REAL_KEY".to_string(), "ignored".to_string()),
        ];
        assert!(object_store_from_url_opts("s3://some-bucket", opts).is_some());
    }

    #[test]
    fn builds_memory_store() {
        assert!(object_store_from_url_opts("memory:///", vec![]).is_some());
    }

    #[test]
    fn invalid_url_yields_none() {
        assert!(object_store_from_url_opts("not a url", vec![]).is_none());
        assert!(object_store_from_url_opts("bogus://x", vec![]).is_none());
    }

    // `std::env::vars` is process-global; serialize the env-mutating test(s)
    // in this module so a parallel `cargo test` run can't interleave sets.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn store_options_from_env_forwards_workload_identity_keys() {
        let _guard = ENV_LOCK.lock().unwrap();
        let wi_keys = [
            "AWS_ROLE_ARN",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_DEFAULT_REGION",
            "AZURE_CLIENT_ID",
            "AZURE_TENANT_ID",
            "AZURE_FEDERATED_TOKEN_FILE",
            "AZURE_AUTHORITY_HOST",
        ];
        // SAFETY: serialized by ENV_LOCK above; no other test in this binary
        // mutates these specific keys.
        unsafe {
            for key in wi_keys {
                std::env::set_var(key, "test-value");
            }
            std::env::set_var("NOT_AN_ALLOWED_KEY", "leak-me-not");
        }

        let opts: std::collections::HashMap<_, _> = store_options_from_env().into_iter().collect();

        // SAFETY: serialized by ENV_LOCK.
        unsafe {
            for key in wi_keys {
                assert_eq!(opts.get(key).map(String::as_str), Some("test-value"));
                std::env::remove_var(key);
            }
            std::env::remove_var("NOT_AN_ALLOWED_KEY");
        }
        assert!(!opts.contains_key("NOT_AN_ALLOWED_KEY"));
    }

    #[test]
    fn plaintext_endpoint_detection() {
        let opts = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect()
        };

        assert!(is_plaintext_endpoint(&opts(&[(
            "AWS_ENDPOINT",
            "http://minio:9000"
        )])));
        assert!(is_plaintext_endpoint(&opts(&[("AWS_ALLOW_HTTP", "true")])));
        assert!(!is_plaintext_endpoint(&opts(&[(
            "AWS_ENDPOINT",
            "https://s3.example.com"
        )])));
        // Explicitly disabled is not a plaintext endpoint.
        assert!(!is_plaintext_endpoint(&opts(&[(
            "AWS_ALLOW_HTTP",
            "false"
        )])));
        assert!(!is_plaintext_endpoint(&[]));
    }
}
