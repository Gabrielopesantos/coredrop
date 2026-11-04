//! The node-local spool: capture bundles the handler writes and the daemon
//! uploads.
//!
//! The kernel holds the crashed process until the handler releases the core
//! pipe, so the handler only writes to local disk; the network is the
//! daemon's job. One capture is one bundle directory. The handler builds it in
//! `incoming/<name>/` and renames it to `ready/<name>/` once every file is
//! complete, so the daemon, which reads only `ready/`, never sees a partial
//! bundle. A bundle holds:
//!
//! - [`CORE_FILE`] - the zstd-compressed core; absent when no core was kept
//! - [`SNAPSHOT_FILE`] - the pre-reap `/proc` snapshot tar
//! - [`MANIFEST_FILE`] - the manifest, uploaded last
//! - [`CAPTURE_FILE`] - the capture-event payload the daemon reports once the
//!   bundle is uploaded
//!
//! The core streams stdin -> zstd -> sha256 -> file in one pass, so the
//! uncompressed core never lands on disk or in memory.

use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use async_compression::tokio::write::ZstdEncoder;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Instant, timeout_at};
use tracing::warn;

use crate::backend::{CaptureBackend, CoreStats};
use crate::config::ensure_private_dir;

/// Default spool root on the node. Not under `/run`: that is a tmpfs, so a
/// spooled core would sit in RAM.
pub const DEFAULT_SPOOL_DIR: &str = "/var/lib/coredrop/spool";

/// Default cap on the spool's total size: 10 GiB.
pub const DEFAULT_SPOOL_MAX_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Default free-space floor of the spool's filesystem, in percent. The spool
/// shares `/var/lib` with the kubelet, which evicts pods under disk pressure,
/// so the handler stops writing well before the disk is full.
pub const DEFAULT_SPOOL_MIN_FREE_PERCENT: u8 = 15;

/// The zstd-compressed core in a bundle.
pub const CORE_FILE: &str = "core.zst";
/// The `/proc` snapshot tar in a bundle.
pub const SNAPSHOT_FILE: &str = "procsnapshot.tar";
/// The manifest in a bundle.
pub const MANIFEST_FILE: &str = "manifest.json";
/// The capture-event payload in a bundle.
pub const CAPTURE_FILE: &str = "capture.json";

const INCOMING_DIR: &str = "incoming";
const READY_DIR: &str = "ready";

// Read the core in 256 KiB chunks.
const CORE_READ_CHUNK: usize = 256 * 1024;

// With a spool budget, flush the encoder after this much input. zstd holds
// input back until it has a full window, so without a flush the compressed
// size the budget check reads can trail the input by megabytes. A flush ends
// the current block but keeps the window, so it costs almost no ratio.
const BUDGET_FLUSH_INTERVAL: u64 = 1024 * 1024;

/// The spool root and its `incoming/` and `ready/` directories.
#[derive(Debug, Clone)]
pub struct Spool {
    root: PathBuf,
}

impl Spool {
    /// A spool rooted at `root`. Nothing is created until
    /// [`Self::create_bundle`].
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Where the handler builds bundles.
    #[must_use]
    pub fn incoming_dir(&self) -> PathBuf {
        self.root.join(INCOMING_DIR)
    }

    /// Where complete bundles wait for upload.
    #[must_use]
    pub fn ready_dir(&self) -> PathBuf {
        self.root.join(READY_DIR)
    }

    /// Create an empty bundle `incoming/<name>/`, mode `0700`. A leftover
    /// directory of the same name, from a handler killed mid-write, is
    /// replaced.
    ///
    /// # Errors
    ///
    /// Fails when the spool directories or the bundle directory cannot be
    /// created.
    pub fn create_bundle(&self, name: &str) -> std::io::Result<Bundle> {
        ensure_private_dir(&self.root)?;
        ensure_private_dir(&self.incoming_dir())?;
        ensure_private_dir(&self.ready_dir())?;
        let dir = self.incoming_dir().join(name);
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        std::fs::create_dir(&dir)?;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        Ok(Bundle {
            dir,
            ready: self.ready_dir().join(name),
        })
    }

    /// Total size of the files in all bundles, `incoming/` and `ready/`.
    /// Unreadable entries count as zero.
    #[must_use]
    pub fn usage_bytes(&self) -> u64 {
        [self.incoming_dir(), self.ready_dir()]
            .iter()
            .flat_map(|dir| read_dir_paths(dir))
            .flat_map(|bundle| read_dir_paths(&bundle))
            .filter_map(|file| std::fs::symlink_metadata(file).ok())
            .filter(std::fs::Metadata::is_file)
            .map(|m| m.len())
            .sum()
    }

    /// How many compressed bytes the next core may use: the lower of the
    /// headroom under `max_bytes` (`0` = no cap) and the headroom above the
    /// `min_free_percent` floor of the filesystem. `0` means the spool is
    /// full. A failed free-space check does not block the capture.
    #[must_use]
    pub fn core_budget(&self, max_bytes: u64, min_free_percent: u8) -> u64 {
        let quota = if max_bytes == 0 {
            u64::MAX
        } else {
            max_bytes.saturating_sub(self.usage_bytes())
        };
        let disk = match free_headroom(&self.root, min_free_percent) {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(error = %e, root = %self.root.display(), "spool free-space check failed; not limiting by free space");
                u64::MAX
            }
        };
        quota.min(disk)
    }
}

/// The entries of `dir`, or none when it cannot be read.
fn read_dir_paths(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .map(|entries| entries.filter_map(|e| e.ok().map(|e| e.path())).collect())
        .unwrap_or_default()
}

/// Bytes available to unprivileged writers on the filesystem of `path`, above
/// a floor of `min_free_percent` of its size.
fn free_headroom(path: &Path, min_free_percent: u8) -> std::io::Result<u64> {
    let st = rustix::fs::statvfs(path)?;
    let total = st.f_blocks.saturating_mul(st.f_frsize);
    let available = st.f_bavail.saturating_mul(st.f_frsize);
    let floor = total / 100 * u64::from(min_free_percent.min(100));
    Ok(available.saturating_sub(floor))
}

/// The bundle directory name for one capture:
/// `{timestamp}-{hostPid}-{podUID}-{containerID}`. Sorting by name sorts by
/// crash time. Every part comes from the kernel or a validated cgroup path, so
/// the name has no `/`.
#[must_use]
pub fn bundle_name(timestamp: i64, host_pid: i32, pod_uid: &str, container_id: &str) -> String {
    format!("{timestamp}-{host_pid}-{pod_uid}-{container_id}")
}

/// One bundle being built in `incoming/`.
#[derive(Debug)]
pub struct Bundle {
    dir: PathBuf,
    ready: PathBuf,
}

impl Bundle {
    /// Path of `file` inside the bundle.
    #[must_use]
    pub fn path(&self, file: &str) -> PathBuf {
        self.dir.join(file)
    }

    /// Write `bytes` to a new file in the bundle, mode `0600`, and `fsync` it.
    ///
    /// # Errors
    ///
    /// Fails when the file already exists or cannot be written.
    pub fn write_file(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.path(file))?;
        f.write_all(bytes)?;
        f.sync_all()
    }

    /// Move the complete bundle to `ready/`, where the daemon picks it up.
    /// Returns the bundle's new path.
    ///
    /// # Errors
    ///
    /// Fails when the rename fails; the bundle then stays in `incoming/`.
    pub fn commit(self) -> std::io::Result<PathBuf> {
        std::fs::rename(&self.dir, &self.ready)?;
        // Make the rename itself durable, so a node crash cannot lose a
        // bundle the handler already reported. Best-effort: the rename is done.
        if let Some(parent) = self.ready.parent()
            && let Err(e) = std::fs::File::open(parent).and_then(|d| d.sync_all())
        {
            warn!(error = %e, dir = %parent.display(), "syncing the spool ready dir failed");
        }
        Ok(self.ready)
    }

    /// Delete the bundle. Used when the capture failed and nothing in it will
    /// be uploaded.
    pub fn discard(self) {
        if let Err(e) = std::fs::remove_dir_all(&self.dir) {
            warn!(error = %e, dir = %self.dir.display(), "removing a failed spool bundle failed");
        }
    }
}

/// The spool capture backend: stream the core through zstd into the bundle's
/// [`CORE_FILE`].
pub struct SpoolBackend {
    path: PathBuf,
    /// Max uncompressed bytes stored per core; `0` = unlimited.
    max_core_bytes: u64,
    /// Max compressed bytes stored, from [`Spool::core_budget`].
    max_stored_bytes: u64,
}

impl SpoolBackend {
    /// Build a backend that writes the core to `path`, capped at
    /// `max_core_bytes` uncompressed (`0` = unlimited) and about
    /// `max_stored_bytes` compressed.
    #[must_use]
    pub fn new(path: PathBuf, max_core_bytes: u64, max_stored_bytes: u64) -> Self {
        Self {
            path,
            max_core_bytes,
            max_stored_bytes,
        }
    }
}

#[async_trait]
impl CaptureBackend for SpoolBackend {
    async fn drain_core(
        &self,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        deadline: Option<Duration>,
    ) -> Result<CoreStats> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&self.path)
            .await
            .with_context(|| format!("creating {}", self.path.display()))?;
        let (stats, file) = stream_core_through_zstd(
            reader,
            file,
            self.max_core_bytes,
            self.max_stored_bytes,
            deadline,
        )
        .await?;
        file.sync_all()
            .await
            .with_context(|| format!("syncing {}", self.path.display()))?;
        Ok(stats)
    }
}

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
            Err(anyhow!("core drain exceeded the deadline"))
        }
        Ok(result) => result.with_context(|| format!("{what} the zstd stream")),
    }
}

/// Stream `core` through zstd into `sink` until EOF, hashing what is stored.
/// Past `max_core_bytes` uncompressed or `max_stored_bytes` compressed the
/// rest of the stream is drained but not stored. The compressed check runs
/// after each [`BUDGET_FLUSH_INTERVAL`] of input, so the stored size can pass
/// `max_stored_bytes` by up to that much. Returns the stats and the sink.
pub(crate) async fn stream_core_through_zstd<R, W>(
    core: &mut R,
    sink: W,
    max_core_bytes: u64,
    max_stored_bytes: u64,
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
    let mut storing = true;
    let mut truncated_reason: Option<String> = None;
    loop {
        match race(core.read(&mut buf), deadline_at).await {
            Err(_elapsed) => {
                warn!(drained, "core drain exceeded the deadline while reading");
                return Err(anyhow!("core drain exceeded the deadline"));
            }
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                drained += n as u64;
                if storing && encoder.get_ref().bytes >= max_stored_bytes {
                    warn!(
                        max_stored_bytes,
                        "spool budget used up - keeping the core so far, draining the rest"
                    );
                    storing = false;
                    truncated_reason = Some("spool_full".to_string());
                }
                // min() keeps the value <= n, so the conversion back to usize cannot fail.
                let take = if storing {
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
                    }
                }
                if storing && take < n {
                    warn!(
                        cap,
                        "core exceeds size cap - storing first {cap} bytes, draining the rest"
                    );
                    storing = false;
                    truncated_reason = Some("size_cap".to_string());
                }
            }
            Ok(Err(e)) => {
                warn!(error = %e, drained, "core stream read error - finalizing partial core as truncated");
                truncated_reason = Some("stream_error".to_string());
                break;
            }
        }
    }

    encoder_step(encoder.shutdown(), deadline_at, drained, "finalizing").await?;

    let HashingWriter {
        inner,
        hasher,
        bytes: stored_bytes,
    } = encoder.into_inner();
    Ok((
        CoreStats {
            bytes: drained,
            stored_bytes,
            sha256: Some(hex_lower(&hasher.finalize())),
            truncated: truncated_reason.is_some(),
            truncated_reason,
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

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
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

    async fn unzstd(bytes: &[u8]) -> Vec<u8> {
        let mut dec = ZstdDecoder::new(io::Cursor::new(bytes.to_vec()));
        let mut out = Vec::new();
        dec.read_to_end(&mut out).await.unwrap();
        out
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        hex_lower(&Sha256::digest(bytes))
    }

    /// A spool in a temp dir with one fresh bundle.
    fn bundle() -> (TempDir, Spool, Bundle) {
        let dir = TempDir::new().unwrap();
        let spool = Spool::new(dir.path().join("spool"));
        let bundle = spool.create_bundle("1-2-pod-cid").unwrap();
        (dir, spool, bundle)
    }

    async fn drain(
        bundle: &Bundle,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        max_core_bytes: u64,
        max_stored_bytes: u64,
        deadline: Option<Duration>,
    ) -> Result<CoreStats> {
        SpoolBackend::new(bundle.path(CORE_FILE), max_core_bytes, max_stored_bytes)
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
        assert!(!stats.truncated);
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

        assert!(stats.truncated);
        assert_eq!(stats.truncated_reason.as_deref(), Some("stream_error"));
        assert_eq!(stats.bytes, chunk.len() as u64);
        let stored = std::fs::read(bundle.path(CORE_FILE)).unwrap();
        assert_eq!(unzstd(&stored).await, chunk);
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
        assert!(stats.truncated);
        assert_eq!(stats.truncated_reason.as_deref(), Some("size_cap"));
        let stored = std::fs::read(bundle.path(CORE_FILE)).unwrap();
        assert_eq!(
            unzstd(&stored).await,
            &core[..10_000],
            "stored core holds exactly the first cap bytes"
        );
    }

    #[tokio::test]
    async fn stops_storing_when_the_spool_budget_is_used_up() {
        // xorshift output is incompressible, so compressed bytes track the
        // input closely.
        let mut xorshift = 0x2545_f491_4f6c_dd1du64;
        let core: Vec<u8> = (0..4_000_000)
            .map(|_| {
                xorshift ^= xorshift << 13;
                xorshift ^= xorshift >> 7;
                xorshift ^= xorshift << 17;
                xorshift.to_le_bytes()[0]
            })
            .collect();
        let (_dir, _spool, bundle) = bundle();

        let mut reader: &[u8] = &core;
        let stats = drain(&bundle, &mut reader, 0, 300_000, None).await.unwrap();

        assert_eq!(stats.bytes, core.len() as u64, "full stream drained");
        assert!(stats.truncated);
        assert_eq!(stats.truncated_reason.as_deref(), Some("spool_full"));
        assert!(
            stats.stored_bytes < 300_000 + 2 * BUDGET_FLUSH_INTERVAL,
            "stored {} bytes",
            stats.stored_bytes
        );
        let stored = std::fs::read(bundle.path(CORE_FILE)).unwrap();
        let prefix = unzstd(&stored).await;
        assert_eq!(prefix, &core[..prefix.len()], "stored core is a prefix");
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
        assert!(result.is_err());
    }

    #[test]
    fn bundle_moves_to_ready_only_on_commit() {
        let (_dir, spool, bundle) = bundle();
        bundle.write_file(MANIFEST_FILE, b"{}").unwrap();
        assert!(
            spool
                .incoming_dir()
                .join("1-2-pod-cid")
                .join(MANIFEST_FILE)
                .exists(),
            "an uncommitted bundle stays in incoming/"
        );
        assert!(!spool.ready_dir().join("1-2-pod-cid").exists());

        let ready = bundle.commit().unwrap();
        assert_eq!(ready, spool.ready_dir().join("1-2-pod-cid"));
        assert_eq!(std::fs::read(ready.join(MANIFEST_FILE)).unwrap(), b"{}");
        assert!(!spool.incoming_dir().join("1-2-pod-cid").exists());
    }

    #[test]
    fn spool_dirs_are_private() {
        let (_dir, spool, bundle) = bundle();
        bundle.write_file(SNAPSHOT_FILE, b"tar").unwrap();
        for dir in [
            spool.root.clone(),
            spool.incoming_dir(),
            spool.ready_dir(),
            spool.incoming_dir().join("1-2-pod-cid"),
        ] {
            let mode = std::fs::metadata(&dir).unwrap().mode() & 0o777;
            assert_eq!(mode, 0o700, "{}", dir.display());
        }
        let mode = std::fs::metadata(bundle.path(SNAPSHOT_FILE))
            .unwrap()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn create_bundle_replaces_a_leftover_and_discard_removes_it() {
        let (_dir, spool, bundle) = bundle();
        bundle
            .write_file(CORE_FILE, b"left by a killed handler")
            .unwrap();

        let bundle = spool.create_bundle("1-2-pod-cid").unwrap();
        assert!(!bundle.path(CORE_FILE).exists(), "leftover files are gone");

        bundle.discard();
        assert!(!spool.incoming_dir().join("1-2-pod-cid").exists());
    }

    #[test]
    fn budget_is_the_quota_headroom() {
        let (_dir, spool, bundle) = bundle();
        bundle.write_file(CORE_FILE, &[0u8; 1000]).unwrap();
        let other = spool.create_bundle("3-4-pod-cid").unwrap();
        other.write_file(CORE_FILE, &[0u8; 500]).unwrap();
        other.commit().unwrap();

        assert_eq!(spool.usage_bytes(), 1500);
        // 0% floor: free space never binds on a test filesystem with room.
        assert_eq!(spool.core_budget(4000, 0), 2500);
        assert_eq!(spool.core_budget(1000, 0), 0, "over quota means full");
        assert!(spool.core_budget(0, 0) > 0, "0 = no quota");
        assert_eq!(
            spool.core_budget(u64::MAX, 100),
            0,
            "a 100% floor is never met"
        );
    }

    #[test]
    fn bundle_name_sorts_by_time() {
        assert_eq!(
            bundle_name(1_749_600_000, 4242, "pod-uid", "cid"),
            "1749600000-4242-pod-uid-cid"
        );
    }
}
