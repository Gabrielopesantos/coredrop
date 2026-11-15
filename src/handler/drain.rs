//! The core drain: the kernel's core stream goes through zstd and sha256
//! into the bundle's core file in one pass, so the uncompressed core never
//! lands on disk or in memory. The drain stops storing (but keeps reading)
//! at the size cap or when the spool budget is used up.

use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use anyhow::{Context, Result};
use async_compression::tokio::write::ZstdEncoder;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Instant, timeout_at};
use tracing::warn;

use crate::handler::buildid::hex;
use crate::manifest::TruncatedReason;
use crate::spool::SpoolBudget;

// Read the core in 256 KiB chunks.
const CORE_READ_CHUNK: usize = 256 * 1024;

// With a spool budget, flush the encoder after this much input. zstd holds
// input back until it has a full window, so without a flush the compressed
// size the budget check reads can trail the input by megabytes. A flush ends
// the current block but keeps the window, so it costs almost no ratio.
const BUDGET_FLUSH_INTERVAL: u64 = 1024 * 1024;

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
    /// Why the stored core is incomplete; `None` when it is complete.
    pub truncated: Option<TruncatedReason>,
}

/// The drain passed its deadline before the core stream ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoreWriteTimeout {
    /// Bytes drained before the deadline.
    pub bytes: u64,
}

impl std::fmt::Display for CoreWriteTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("core drain exceeded the deadline")
    }
}

impl std::error::Error for CoreWriteTimeout {}

/// Where and how much of a core is stored: the core goes through zstd into
/// the bundle's [`CORE_FILE`](crate::spool::CORE_FILE). A core that is not
/// kept is never read: the handler closes the pipe, and the kernel stops the
/// dump.
pub struct CoreDrain {
    path: PathBuf,
    /// Max uncompressed bytes stored per core; `0` = unlimited.
    max_core_bytes: u64,
    /// Max compressed bytes stored, from [`crate::spool::Spool::core_budget`].
    max_stored_bytes: u64,
    /// The spool cap, checked again during the write.
    budget: Option<SpoolBudget>,
}

impl CoreDrain {
    /// A drain that writes the core to `path`, capped at
    /// `max_core_bytes` uncompressed (`0` = unlimited) and about
    /// `max_stored_bytes` compressed. With a `budget`, it also stops storing
    /// once the spool as a whole is full.
    #[must_use]
    pub fn new(
        path: PathBuf,
        max_core_bytes: u64,
        max_stored_bytes: u64,
        budget: Option<SpoolBudget>,
    ) -> Self {
        Self {
            path,
            max_core_bytes,
            max_stored_bytes,
            budget,
        }
    }

    /// Drain `reader` to its end and store the core. `deadline`, when set,
    /// bounds the whole drain: the handler holds a `core_pipe_limit` slot
    /// while this call runs.
    ///
    /// # Errors
    ///
    /// [`CoreWriteTimeout`] when `deadline` passes before a core is truncated,
    /// and otherwise only when the core file cannot be created or written.
    pub async fn drain_core<R>(
        &self,
        reader: &mut R,
        deadline: Option<Duration>,
    ) -> Result<CoreStats>
    where
        R: AsyncRead + Unpin + ?Sized,
    {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&self.path)
            .await
            .with_context(|| format!("creating {}", self.path.display()))?;
        // The caller syncs the file once it has released the core pipe: a
        // slow fsync must not hold the kernel's `core_pipe_limit` slot.
        let (stats, _file) = stream_core_through_zstd(
            reader,
            file,
            self.max_core_bytes,
            self.max_stored_bytes,
            self.budget.as_ref(),
            deadline,
        )
        .await?;
        Ok(stats)
    }
}

/// Time to finish a truncated core when the deadline passes while the rest
/// of the stream is read only to count it.
const FINALIZE_GRACE: Duration = Duration::from_secs(5);

/// Put `deadline_at` on one await.
async fn race<T>(
    fut: impl std::future::Future<Output = T>,
    deadline_at: Option<Instant>,
) -> Result<T, tokio::time::error::Elapsed> {
    match deadline_at {
        Some(dl) => timeout_at(dl, fut).await,
        None => Ok(fut.await),
    }
}

/// Run one encoder step under the deadline. `what` names the step in the
/// log line and the error.
async fn encoder_step(
    fut: impl std::future::Future<Output = std::io::Result<()>>,
    deadline_at: Option<Instant>,
    drained: u64,
    what: &str,
) -> Result<()> {
    match race(fut, deadline_at).await {
        Err(_elapsed) => {
            warn!(drained, "core drain exceeded the deadline while {what}");
            Err(CoreWriteTimeout { bytes: drained }.into())
        }
        Ok(result) => result.with_context(|| format!("{what} the zstd stream")),
    }
}

/// Whether `budget` reports the spool full, checked once `stored` has grown
/// by its recheck interval since `checked_at`.
fn spool_filled(budget: Option<&SpoolBudget>, stored: u64, checked_at: &mut u64) -> bool {
    let Some(budget) = budget else {
        return false;
    };
    if stored - *checked_at < budget.recheck_every {
        return false;
    }
    *checked_at = stored;
    let full = !budget.has_room();
    if full {
        warn!(
            stored,
            "spool is full - keeping the core so far, draining the rest"
        );
    }
    full
}

/// Stream `core` through zstd into `sink` until EOF, hashing what is stored.
/// Past `max_core_bytes` uncompressed or `max_stored_bytes` compressed the
/// rest of the stream is drained but not stored. The compressed check runs
/// after each [`BUDGET_FLUSH_INTERVAL`] of input, so the stored size can pass
/// `max_stored_bytes` by up to that much. With a `budget`, the whole spool is
/// checked again after each `recheck_every` stored bytes, and storing stops
/// when it is full. Returns the stats and the sink.
pub(crate) async fn stream_core_through_zstd<R, W>(
    core: &mut R,
    sink: W,
    max_core_bytes: u64,
    max_stored_bytes: u64,
    budget: Option<&SpoolBudget>,
    deadline: Option<Duration>,
) -> Result<(CoreStats, W)>
where
    R: AsyncRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin,
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
    let mut unflushed = 0u64;
    let mut checked_at = 0u64;
    // Set when storing stops; the rest of the stream is only counted.
    let mut truncated_reason: Option<TruncatedReason> = None;
    let mut finalize_by = deadline_at;
    loop {
        match race(core.read(&mut buf), deadline_at).await {
            Err(_elapsed) => {
                if truncated_reason.is_none() {
                    warn!(drained, "core drain exceeded the deadline while reading");
                    return Err(CoreWriteTimeout { bytes: drained }.into());
                }
                // Past the size cap or the spool budget, the rest is only read
                // to count it. The stored prefix is complete: keep it, and
                // give the encoder a short time to finish it.
                warn!(
                    drained,
                    "core write timeout passed after truncation; keeping the stored part"
                );
                finalize_by = Some(Instant::now() + FINALIZE_GRACE);
                break;
            }
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                drained += n as u64;
                if truncated_reason.is_none() && encoder.get_ref().bytes >= max_stored_bytes {
                    warn!(
                        max_stored_bytes,
                        "spool budget used up - keeping the core so far, draining the rest"
                    );
                    truncated_reason = Some(TruncatedReason::SpoolFull);
                }
                // min() keeps the value <= n, so the conversion back to usize cannot fail.
                let take = if truncated_reason.is_none() {
                    usize::try_from((n as u64).min(cap.saturating_sub(written))).unwrap_or(n)
                } else {
                    0
                };
                if take > 0 {
                    encoder_step(
                        encoder.write_all(&buf[..take]),
                        deadline_at,
                        drained,
                        "writing",
                    )
                    .await?;
                    written += take as u64;
                    unflushed += take as u64;
                    if max_stored_bytes != u64::MAX && unflushed >= BUDGET_FLUSH_INTERVAL {
                        unflushed = 0;
                        encoder_step(encoder.flush(), deadline_at, drained, "flushing").await?;
                        // The flush put the stored bytes on disk, so the
                        // spool's usage includes them.
                        if spool_filled(budget, encoder.get_ref().bytes, &mut checked_at) {
                            truncated_reason = Some(TruncatedReason::SpoolFull);
                        }
                    }
                }
                if truncated_reason.is_none() && take < n {
                    warn!(
                        cap,
                        "core exceeds size cap - storing first {cap} bytes, draining the rest"
                    );
                    truncated_reason = Some(TruncatedReason::SizeCap);
                }
            }
            Ok(Err(e)) => {
                warn!(error = %e, drained, "core stream read error - finalizing partial core as truncated");
                // A core cut at the size cap or the spool budget keeps that
                // reason: the stored prefix is complete.
                truncated_reason.get_or_insert(TruncatedReason::StreamError);
                break;
            }
        }
    }

    encoder_step(encoder.shutdown(), finalize_by, drained, "finalizing").await?;

    let HashingWriter {
        inner,
        hasher,
        bytes: stored_bytes,
    } = encoder.into_inner();
    Ok((
        CoreStats {
            bytes: drained,
            stored_bytes,
            sha256: Some(hex(&hasher.finalize())),
            truncated: truncated_reason,
        },
        inner,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::io;
    use std::os::unix::fs::MetadataExt;

    use async_compression::tokio::bufread::ZstdDecoder;
    use tempfile::TempDir;

    use super::*;
    use crate::testutil::{FlakyReader, StallReader};

    use crate::spool::{Bundle, CORE_FILE, Spool};

    /// A spool in a temp dir with one fresh bundle.
    fn bundle() -> (TempDir, Spool, Bundle) {
        let dir = TempDir::new().unwrap();
        let spool = Spool::new(dir.path().join("spool"));
        let bundle = spool.create_bundle("1-2-pod-cid").unwrap();
        (dir, spool, bundle)
    }

    async fn unzstd(bytes: &[u8]) -> Vec<u8> {
        let mut dec = ZstdDecoder::new(io::Cursor::new(bytes.to_vec()));
        let mut out = Vec::new();
        dec.read_to_end(&mut out).await.unwrap();
        out
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        hex(&Sha256::digest(bytes))
    }

    async fn drain(
        bundle: &Bundle,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        max_core_bytes: u64,
        max_stored_bytes: u64,
        deadline: Option<Duration>,
    ) -> Result<CoreStats> {
        CoreDrain::new(
            bundle.path(CORE_FILE),
            max_core_bytes,
            max_stored_bytes,
            None,
        )
        .drain_core(reader, deadline)
        .await
    }

    #[tokio::test]
    async fn streams_zstd_with_integrity_and_round_trips() {
        let core: Vec<u8> = (0..200_000u32).map(|i| (i % 7) as u8).collect();
        let (_dir, _spool, bundle) = bundle();

        let mut reader: &[u8] = &core;
        let stats = drain(&bundle, &mut reader, 0, u64::MAX, None)
            .await
            .unwrap();

        let stored = std::fs::read(bundle.path(CORE_FILE)).unwrap();
        assert_eq!(stats.truncated, None);
        assert_eq!(stats.bytes, core.len() as u64);
        assert_eq!(stats.stored_bytes, stored.len() as u64);
        assert!(
            stats.stored_bytes < stats.bytes,
            "zstd should compress this"
        );
        assert_eq!(stats.sha256.as_deref(), Some(sha256_hex(&stored).as_str()));
        assert_eq!(unzstd(&stored).await, core);
        let mode = std::fs::metadata(bundle.path(CORE_FILE)).unwrap().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[tokio::test]
    async fn marks_truncated_on_a_short_read_but_finalizes_the_partial() {
        let chunk = vec![0x5Au8; 50_000];
        let (_dir, _spool, bundle) = bundle();

        let mut reader = FlakyReader {
            chunk: chunk.clone(),
            sent: false,
        };
        let stats = drain(&bundle, &mut reader, 0, u64::MAX, None)
            .await
            .unwrap();

        assert_eq!(stats.truncated, Some(TruncatedReason::StreamError));
        assert_eq!(stats.bytes, chunk.len() as u64);
        let stored = std::fs::read(bundle.path(CORE_FILE)).unwrap();
        assert_eq!(unzstd(&stored).await, chunk);
    }

    #[tokio::test]
    async fn a_read_error_after_the_cap_keeps_the_size_cap_reason() {
        let chunk = vec![0x5Au8; 50_000];
        let (_dir, _spool, bundle) = bundle();

        let mut reader = FlakyReader {
            chunk: chunk.clone(),
            sent: false,
        };
        let stats = drain(&bundle, &mut reader, 1_000, u64::MAX, None)
            .await
            .unwrap();

        assert_eq!(stats.truncated, Some(TruncatedReason::SizeCap));
        let stored = std::fs::read(bundle.path(CORE_FILE)).unwrap();
        assert_eq!(unzstd(&stored).await, chunk[..1_000]);
    }

    #[tokio::test]
    async fn caps_stored_core_but_drains_and_counts_everything() {
        let core: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
        let (_dir, _spool, bundle) = bundle();

        let mut reader: &[u8] = &core;
        let stats = drain(&bundle, &mut reader, 10_000, u64::MAX, None)
            .await
            .unwrap();

        assert_eq!(stats.bytes, 50_000, "full stream drained and counted");
        assert_eq!(stats.truncated, Some(TruncatedReason::SizeCap));
        let stored = std::fs::read(bundle.path(CORE_FILE)).unwrap();
        assert_eq!(
            unzstd(&stored).await,
            &core[..10_000],
            "stored core holds exactly the first cap bytes"
        );
    }

    /// `len` bytes of xorshift output. It is incompressible, so compressed
    /// bytes track the input closely.
    fn incompressible(len: usize) -> Vec<u8> {
        let mut xorshift = 0x2545_f491_4f6c_dd1du64;
        (0..len)
            .map(|_| {
                xorshift ^= xorshift << 13;
                xorshift ^= xorshift >> 7;
                xorshift ^= xorshift << 17;
                xorshift.to_le_bytes()[0]
            })
            .collect()
    }

    #[tokio::test]
    async fn stops_storing_when_the_spool_budget_is_used_up() {
        let core = incompressible(4_000_000);
        let (_dir, _spool, bundle) = bundle();

        let mut reader: &[u8] = &core;
        let stats = drain(&bundle, &mut reader, 0, 300_000, None).await.unwrap();

        assert_eq!(stats.bytes, core.len() as u64, "full stream drained");
        assert_eq!(stats.truncated, Some(TruncatedReason::SpoolFull));
        assert!(
            stats.stored_bytes < 300_000 + 2 * BUDGET_FLUSH_INTERVAL,
            "stored {} bytes",
            stats.stored_bytes
        );
        let stored = std::fs::read(bundle.path(CORE_FILE)).unwrap();
        let prefix = unzstd(&stored).await;
        assert_eq!(prefix, &core[..prefix.len()], "stored core is a prefix");
    }

    /// Two captures start with the same budget, from the same spool usage.
    /// The recheck during the write counts each other's bytes, so together
    /// they stay near the cap instead of each filling it.
    #[tokio::test]
    async fn concurrent_drains_share_the_spool_cap() {
        const MAX: u64 = 4 * 1024 * 1024;
        let core = incompressible(12 * 1024 * 1024);
        let dir = TempDir::new().unwrap();
        let spool = Spool::new(dir.path().join("spool"));
        let a = spool.create_bundle("1-1-pod-a").unwrap();
        let b = spool.create_bundle("1-2-pod-b").unwrap();
        let budget = spool.core_budget(MAX, 0);
        assert_eq!(budget, MAX);
        let drain_for = |bundle: &Bundle| {
            let mut spool_budget = SpoolBudget::new(spool.clone(), MAX, 0);
            spool_budget.recheck_every = 0;
            CoreDrain::new(bundle.path(CORE_FILE), 0, budget, Some(spool_budget))
        };
        let (drain_a, drain_b) = (drain_for(&a), drain_for(&b));

        let (mut reader_a, mut reader_b): (&[u8], &[u8]) = (&core, &core);
        let (stats_a, stats_b) = tokio::join!(
            drain_a.drain_core(&mut reader_a, None),
            drain_b.drain_core(&mut reader_b, None),
        );
        let (stats_a, stats_b) = (stats_a.unwrap(), stats_b.unwrap());

        for stats in [&stats_a, &stats_b] {
            assert_eq!(stats.truncated, Some(TruncatedReason::SpoolFull));
        }
        // Each capture can pass the cap by one flush interval plus one read
        // chunk before its next check.
        let slack = 2 * (BUDGET_FLUSH_INTERVAL + CORE_READ_CHUNK as u64);
        assert!(
            spool.usage_bytes() <= MAX + slack,
            "spool holds {} bytes, cap {MAX}",
            spool.usage_bytes()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_after_the_size_cap_keeps_the_stored_part() {
        let core: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
        let (_dir, _spool, bundle) = bundle();
        // The kernel sends the first part of the core, then stalls.
        let mut reader = (&core[..]).chain(StallReader);

        let stats = drain(
            &bundle,
            &mut reader,
            10_000,
            u64::MAX,
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap();

        assert_eq!(stats.truncated, Some(TruncatedReason::SizeCap));
        assert_eq!(stats.bytes, 50_000, "the bytes read before the deadline");
        let stored = std::fs::read(bundle.path(CORE_FILE)).unwrap();
        assert_eq!(unzstd(&stored).await, &core[..10_000]);
    }

    #[tokio::test(start_paused = true)]
    async fn drain_errors_on_deadline_before_any_bytes_arrive() {
        let (_dir, _spool, bundle) = bundle();
        let mut reader = StallReader;
        let result = drain(
            &bundle,
            &mut reader,
            0,
            u64::MAX,
            Some(Duration::from_secs(1)),
        )
        .await;
        let err = result.unwrap_err();
        assert_eq!(
            err.downcast_ref::<CoreWriteTimeout>(),
            Some(&CoreWriteTimeout { bytes: 0 }),
            "{err:#}"
        );
    }
}
