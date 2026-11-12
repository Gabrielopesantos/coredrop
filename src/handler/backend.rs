//! The capture backend seam: where the kernel's core stream goes.
//!
//! The pre-reap `/proc` snapshot is backend-independent; only the core's
//! destination differs - that is this interface boundary. The
//! [`SpoolBackend`](crate::handler::drain::SpoolBackend) (zstd-in-stream write to the
//! node-local spool) is the backend. A core that is not kept is never read:
//! the handler closes the pipe, and the kernel stops the dump.

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::io::AsyncRead;

/// Integrity stats for a drained core. The default value describes a core
/// that was not read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CoreStats {
    /// Uncompressed bytes drained from the kernel's core pipe.
    pub bytes: u64,
    /// Bytes stored (zstd-compressed).
    pub stored_bytes: u64,
    /// sha256 (hex) of the *stored* bytes - the zstd-compressed core.
    /// `None` when nothing was stored.
    pub sha256: Option<String>,
    /// The stored core is incomplete (stream error or size cap).
    pub truncated: bool,
    /// Why the core is truncated: `size_cap` | `spool_full` | `stream_error`.
    /// `None` when not truncated.
    pub truncated_reason: Option<String>,
}

/// The drain passed its deadline before the core stream ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainDeadline {
    /// Bytes drained before the deadline.
    pub bytes: u64,
}

impl std::fmt::Display for DrainDeadline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("core drain exceeded the deadline")
    }
}

impl std::error::Error for DrainDeadline {}

/// Sink for the kernel's core stream. The kernel writes the core until the
/// stream ends or the handler closes the pipe.
///
/// `deadline`, when set, bounds the entire drain: the handler holds a
/// `core_pipe_limit` slot while this call runs.
#[async_trait]
pub trait CaptureBackend: Send + Sync {
    /// Drain `reader` to completion and store the core. Errors with
    /// [`DrainDeadline`] when `deadline` passes, and otherwise only on an
    /// unrecoverable backend failure.
    async fn drain_core(
        &self,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        deadline: Option<Duration>,
    ) -> Result<CoreStats>;
}
