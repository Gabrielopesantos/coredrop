//! The capture backend seam: where the kernel's core stream goes.
//!
//! The pre-reap `/proc` snapshot is backend-independent; only the core's
//! destination differs - that is this interface boundary. The
//! [`StandaloneBackend`](crate::upload::StandaloneBackend) (zstd-in-stream
//! multipart upload to the object store) is the primary backend;
//! [`DiscardBackend`] is the fallback when no store is configured: it drains
//! the pipe so the kernel completes the dump but stores nothing.

use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::time::{Instant, timeout_at};
use tracing::warn;

/// Integrity stats for a drained core. The byte count and truncation flag come
/// from every backend; the streaming `StandaloneBackend` also fills the stored
/// (compressed) size and the sha256 of the stored object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreStats {
    /// Uncompressed bytes drained from the kernel's core pipe.
    pub bytes: u64,
    /// Bytes actually stored in the object store (zstd-compressed). `0` for
    /// `DiscardBackend`, which stores nothing.
    pub stored_bytes: u64,
    /// sha256 (hex) of the *stored* object - the zstd-compressed bytes.
    /// `None` when nothing was stored (`DiscardBackend`).
    pub sha256: Option<String>,
    /// The stored core is incomplete (stream error or size cap).
    pub truncated: bool,
    /// Why the core is truncated: `size_cap` | `stream_error`. `None` when
    /// not truncated.
    pub truncated_reason: Option<String>,
}

/// Sink for the kernel's core stream. Implementations must consume `reader`
/// to completion - the kernel blocks on the core pipe until fully drained.
///
/// `deadline`, when set, bounds the entire drain: the handler holds one of
/// the node's `core_pipe_limit` slots for as long as this call runs, so no
/// implementation may wait on it unboundedly (see
/// [`StandaloneBackend`](crate::upload::StandaloneBackend) for why a naive
/// `tokio::time::timeout` around the whole call is not enough once object
/// storage is involved).
#[async_trait]
pub trait CaptureBackend: Send + Sync {
    async fn drain_core(
        &self,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        deadline: Option<Duration>,
    ) -> Result<CoreStats>;
}

/// Fallback core sink: count and discard the core so the kernel's pipe still
/// completes when no object store is configured or identity is unresolved.
pub struct DiscardBackend;

#[async_trait]
impl CaptureBackend for DiscardBackend {
    async fn drain_core(
        &self,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        deadline: Option<Duration>,
    ) -> Result<CoreStats> {
        // Nothing is stored here, so there is nothing to clean up on
        // timeout - a plain per-read race is enough (contrast with
        // StandaloneBackend, which owns an object-store multipart upload
        // that needs an explicit abort).
        let deadline_at = deadline.map(|d| Instant::now() + d);
        let mut buf = vec![0u8; 64 * 1024];
        let mut bytes = 0u64;
        loop {
            let read = match deadline_at {
                Some(dl) => timeout_at(dl, reader.read(&mut buf)).await,
                None => Ok(reader.read(&mut buf).await),
            };
            match read {
                Err(_elapsed) => {
                    warn!(bytes, "core stream discard exceeded the upload deadline");
                    return Err(anyhow!("core drain exceeded the upload deadline"));
                }
                Ok(Ok(0)) => {
                    return Ok(CoreStats {
                        bytes,
                        stored_bytes: 0,
                        sha256: None,
                        truncated: false,
                        truncated_reason: None,
                    });
                }
                Ok(Ok(n)) => bytes += n as u64,
                Ok(Err(e)) => {
                    warn!(error = %e, bytes, "core stream read error - marking truncated");
                    return Ok(CoreStats {
                        bytes,
                        stored_bytes: 0,
                        sha256: None,
                        truncated: true,
                        truncated_reason: Some("stream_error".to_string()),
                    });
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn discard_counts_every_byte() {
        let data = vec![0xABu8; 200_000];
        let mut reader: &[u8] = &data;
        let stats = DiscardBackend.drain_core(&mut reader, None).await.unwrap();
        assert_eq!(stats.bytes, 200_000);
        assert_eq!(stats.stored_bytes, 0);
        assert_eq!(stats.sha256, None);
        assert!(!stats.truncated);
    }

    #[tokio::test]
    async fn discard_handles_an_empty_core() {
        let mut reader: &[u8] = &[];
        let stats = DiscardBackend.drain_core(&mut reader, None).await.unwrap();
        assert_eq!(stats.bytes, 0);
        assert_eq!(stats.stored_bytes, 0);
        assert!(!stats.truncated);
    }

    /// A reader that never yields data or EOF - stands in for a hung kernel
    /// pipe.
    struct StallReader;

    impl AsyncRead for StallReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    #[tokio::test(start_paused = true)]
    async fn discard_errors_when_the_deadline_elapses() {
        let mut reader = StallReader;
        let result = DiscardBackend
            .drain_core(&mut reader, Some(Duration::from_secs(1)))
            .await;
        assert!(result.is_err());
    }
}
