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

use anyhow::{Context, Result};
use async_compression::tokio::write::ZstdEncoder;
use async_trait::async_trait;
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::warn;

use crate::backend::{CaptureBackend, CoreStats};

// Core chunks of 256 KBs
const CORE_READ_CHUNK: usize = 256 * 1024;

/// Build the core's object key: `{cluster}/{podUID}/{containerID}/{timestamp}-core.zst`.
/// Handler-derivable from the cgroup - no UUIDs; the manifest carries the
/// human-readable identity from crictl.
pub fn core_object_key(cluster: &str, pod_uid: &str, container_id: &str, timestamp: i64) -> String {
    format!("{cluster}/{pod_uid}/{container_id}/{timestamp}-core.zst")
}

/// Build the `/proc` snapshot's object key, mirroring `core_object_key`'s
/// scheme - same prefix, distinct suffix.
pub fn proc_snapshot_object_key(
    cluster: &str,
    pod_uid: &str,
    container_id: &str,
    timestamp: i64,
) -> String {
    format!("{cluster}/{pod_uid}/{container_id}/{timestamp}-procsnapshot.tar")
}

/// Build the JSON manifest's object key, sibling to the core.
pub fn manifest_object_key(
    cluster: &str,
    pod_uid: &str,
    container_id: &str,
    timestamp: i64,
) -> String {
    format!("{cluster}/{pod_uid}/{container_id}/{timestamp}-manifest.json")
}

/// Buffered single-shot PUT of a small object (the proc snapshot tar or the
/// manifest JSON). Unlike the multi-GB core (which *streams*), these are
/// bounded and already in memory.
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
    async fn drain_core(&self, reader: &mut (dyn AsyncRead + Unpin + Send)) -> Result<CoreStats> {
        let sink = BufWriter::new(self.store.clone(), self.key.clone());
        let (bytes, stored_bytes, sha256, truncated_reason) =
            stream_core_through_zstd(reader, sink, self.max_core_bytes).await?;
        Ok(CoreStats {
            bytes,
            stored_bytes,
            sha256: Some(sha256),
            truncated: truncated_reason.is_some(),
            truncated_reason,
        })
    }
}

async fn stream_core_through_zstd<R, W>(
    core: &mut R,
    sink: W,
    max_core_bytes: u64,
) -> Result<(u64, u64, String, Option<String>)>
where
    R: AsyncRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin,
{
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
        match core.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                drained += n as u64;
                let take = (n as u64).min(cap.saturating_sub(written)) as usize;
                if take > 0 {
                    encoder
                        .write_all(&buf[..take])
                        .await
                        .context("writing core into zstd encoder")?;
                    written += take as u64;
                }
                if take < n && truncated_reason.is_none() {
                    warn!(
                        cap,
                        "core exceeds size cap - storing first {cap} bytes, draining the rest"
                    );
                    truncated_reason = Some("size_cap".to_string());
                }
            }
            Err(e) => {
                warn!(error = %e, drained, "core stream read error - finalizing partial object as truncated");
                truncated_reason = Some("stream_error".to_string());
                break;
            }
        }
    }

    encoder
        .shutdown()
        .await
        .context("finalizing zstd stream + completing upload")?;

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
pub const ALLOWED_STORE_OPTS: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_REGION",
    "AWS_ENDPOINT",
    "AWS_ALLOW_HTTP",
    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST",
    "GOOGLE_SERVICE_ACCOUNT",
    "GOOGLE_SERVICE_ACCOUNT_KEY",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "AZURE_STORAGE_ACCESS_KEY",
    "AZURE_STORAGE_CLIENT_ID",
    "AZURE_STORAGE_CLIENT_SECRET",
    "AZURE_STORAGE_TENANT_ID",
];

/// Retry policy for cloud uploads. `object_store` defaults to 10 retries over
/// 3 minutes; the handler may be holding the kernel's core pipe mid-multipart,
/// so bound the worst case tighter.
fn retry_config() -> object_store::RetryConfig {
    object_store::RetryConfig {
        max_retries: 3,
        retry_timeout: std::time::Duration::from_secs(60),
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
mod tests {
    use super::*;
    use async_compression::tokio::bufread::ZstdDecoder;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use std::io;

    #[test]
    fn builds_object_keys() {
        assert_eq!(
            core_object_key("prod", "pod-uid-123", "abc123def", 1_749_600_000),
            "prod/pod-uid-123/abc123def/1749600000-core.zst"
        );
        assert_eq!(
            proc_snapshot_object_key("prod", "pod-uid-123", "abc123def", 1_749_600_000),
            "prod/pod-uid-123/abc123def/1749600000-procsnapshot.tar"
        );
        assert_eq!(
            manifest_object_key("prod", "pod-uid-123", "abc123def", 1_749_600_000),
            "prod/pod-uid-123/abc123def/1749600000-manifest.json"
        );
    }

    #[tokio::test]
    async fn put_object_round_trips_a_buffered_blob() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let key = proc_snapshot_object_key("local", "pod-a", "cid-b", 7);
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
        let key = core_object_key("local", "pod-aaa", "cid-bbb", 42);

        let backend = StandaloneBackend::new(store.clone(), &key, 0);
        let mut reader: &[u8] = &core;
        let stats = backend.drain_core(&mut reader).await.unwrap();

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
        let key = core_object_key("local", "pod-x", "cid-y", 7);

        let backend = StandaloneBackend::new(store.clone(), &key, 0);
        let mut reader = FlakyReader {
            chunk: chunk.clone(),
            sent: false,
        };
        let stats = backend.drain_core(&mut reader).await.unwrap();

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
        let key = core_object_key("local", "pod-cap", "cid-cap", 9);

        let backend = StandaloneBackend::new(store.clone(), &key, 10_000);
        let mut reader: &[u8] = &core;
        let stats = backend.drain_core(&mut reader).await.unwrap();

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
}
