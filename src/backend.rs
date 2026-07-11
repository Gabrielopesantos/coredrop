//! The capture backend seam: where the kernel's core stream goes.
//!
//! The pre-reap `/proc` snapshot is backend-independent; only the core's
//! destination differs - that is this interface boundary. The
//! [`StandaloneBackend`](crate::upload::StandaloneBackend) (zstd-in-stream
//! multipart upload to the object store) is the primary backend;
//! [`DiscardBackend`] is the fallback when no store is configured: it drains
//! the pipe so the kernel completes the dump but stores nothing.

use anyhow::Result;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt};
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
#[async_trait]
pub trait CaptureBackend: Send + Sync {
    async fn drain_core(&self, reader: &mut (dyn AsyncRead + Unpin + Send)) -> Result<CoreStats>;
}

/// Fallback core sink: count and discard the core so the kernel's pipe still
/// completes when no object store is configured or identity is unresolved.
pub struct DiscardBackend;

#[async_trait]
impl CaptureBackend for DiscardBackend {
    async fn drain_core(&self, reader: &mut (dyn AsyncRead + Unpin + Send)) -> Result<CoreStats> {
        let mut buf = vec![0u8; 64 * 1024];
        let mut bytes = 0u64;
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => {
                    return Ok(CoreStats {
                        bytes,
                        stored_bytes: 0,
                        sha256: None,
                        truncated: false,
                        truncated_reason: None,
                    });
                }
                Ok(n) => bytes += n as u64,
                Err(e) => {
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
        let stats = DiscardBackend.drain_core(&mut reader).await.unwrap();
        assert_eq!(stats.bytes, 200_000);
        assert_eq!(stats.stored_bytes, 0);
        assert_eq!(stats.sha256, None);
        assert!(!stats.truncated);
    }

    #[tokio::test]
    async fn discard_handles_an_empty_core() {
        let mut reader: &[u8] = &[];
        let stats = DiscardBackend.drain_core(&mut reader).await.unwrap();
        assert_eq!(stats.bytes, 0);
        assert_eq!(stats.stored_bytes, 0);
        assert!(!stats.truncated);
    }
}
