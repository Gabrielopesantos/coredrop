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
//! The handler writes the core into a bundle with
//! [`SpoolBackend`](crate::handler::drain::SpoolBackend).

use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use tracing::warn;

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
        self.ensure_root()?;
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

    /// Create the spool root, mode `0700`, when it is missing. The mode of a
    /// root that exists is not changed: `spool.hostDir` is user input and
    /// can be a shared directory. The bundles (`0700`) and their files
    /// (`0600`) keep the cores private.
    fn ensure_root(&self) -> std::io::Result<()> {
        match std::fs::metadata(&self.root) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => ensure_private_dir(&self.root),
            Err(e) => Err(e),
        }
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

/// `statfs` magic numbers of the filesystems that live in RAM.
const TMPFS_MAGIC: u64 = 0x0102_1994;

const RAMFS_MAGIC: u64 = 0x8584_58f6;

/// Whether the spool's filesystem is backed by node memory.
fn in_memory_fs(path: &Path) -> std::io::Result<bool> {
    let st = rustix::fs::statfs(path)?;
    // `f_type` is signed on some targets; keep only the low 32 bits.
    #[allow(clippy::cast_sign_loss)]
    let magic = (st.f_type as u64) & 0xffff_ffff;
    Ok(magic == TMPFS_MAGIC || magic == RAMFS_MAGIC)
}

impl Spool {
    /// Create the spool directory and reject a memory-backed filesystem. A
    /// compressed core on tmpfs or ramfs uses node memory, so a large capture
    /// causes memory pressure or pod eviction.
    ///
    /// # Errors
    ///
    /// Fails when the directory cannot be created or inspected, or when its
    /// filesystem is tmpfs or ramfs.
    pub fn ensure_disk_backed(&self) -> Result<()> {
        self.ensure_root()
            .with_context(|| format!("creating spool directory {}", self.root.display()))?;
        let memory = in_memory_fs(&self.root)
            .with_context(|| format!("inspecting spool filesystem {}", self.root.display()))?;
        if memory {
            return Err(anyhow!(
                "spool directory {} is on a memory-backed filesystem (tmpfs or ramfs); \
                 set spool.hostDir to a path on node disk",
                self.root.display()
            ));
        }
        Ok(())
    }
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

    /// `fsync` a file the bundle already holds.
    ///
    /// # Errors
    ///
    /// Fails when the file cannot be opened or synced.
    pub fn sync_file(&self, file: &str) -> std::io::Result<()> {
        std::fs::File::open(self.path(file))?.sync_all()
    }

    /// Move the complete bundle to `ready/`, where the daemon picks it up.
    /// Returns the bundle's new path. The bundle directory is synced first,
    /// so after a node crash a bundle in `ready/` still holds its files.
    ///
    /// # Errors
    ///
    /// Fails when the sync or the rename fails; the bundle is then deleted,
    /// so it does not count against the spool cap.
    pub fn commit(self) -> std::io::Result<PathBuf> {
        let moved = std::fs::File::open(&self.dir)
            .and_then(|d| d.sync_all())
            .and_then(|()| std::fs::rename(&self.dir, &self.ready));
        if let Err(e) = moved {
            self.discard();
            return Err(e);
        }
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

const SPOOL_RECHECK_INTERVAL: u64 = 8 * 1024 * 1024;

/// The spool's size cap and free-space floor, checked again while a core is
/// written. The budget a capture gets before its drain does not see the
/// bytes that concurrent captures write after it.
#[derive(Debug, Clone)]
pub struct SpoolBudget {
    spool: Spool,
    max_bytes: u64,
    min_free_percent: u8,
    pub(crate) recheck_every: u64,
}

impl SpoolBudget {
    /// The cap of `spool`: `max_bytes` in total (`0` = no cap) and a
    /// `min_free_percent` floor of free space on its filesystem.
    #[must_use]
    pub fn new(spool: Spool, max_bytes: u64, min_free_percent: u8) -> Self {
        Self {
            spool,
            max_bytes,
            min_free_percent,
            recheck_every: SPOOL_RECHECK_INTERVAL,
        }
    }

    /// Whether the spool still has room, counting every bundle, the partial
    /// cores of running captures included.
    pub(crate) fn has_room(&self) -> bool {
        self.spool
            .core_budget(self.max_bytes, self.min_free_percent)
            > 0
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {

    use std::os::unix::fs::MetadataExt;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn rejects_memory_backed_spool() {
        let shm = Path::new("/dev/shm");
        if !shm.is_dir() || !in_memory_fs(shm).unwrap() {
            return;
        }
        let dir = TempDir::new_in(shm).unwrap();
        let err = Spool::new(dir.path().join("spool"))
            .ensure_disk_backed()
            .unwrap_err();
        assert!(err.to_string().contains("memory-backed"), "{err}");
    }

    #[test]
    fn procfs_is_not_memory_backed() {
        assert!(!in_memory_fs(Path::new("/proc")).unwrap());
    }

    /// A spool in a temp dir with one fresh bundle.
    fn bundle() -> (TempDir, Spool, Bundle) {
        let dir = TempDir::new().unwrap();
        let spool = Spool::new(dir.path().join("spool"));
        let bundle = spool.create_bundle("1-2-pod-cid").unwrap();
        (dir, spool, bundle)
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
    fn keeps_the_mode_of_an_existing_spool_root() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("shared");
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
        let spool = Spool::new(&root);

        spool.create_bundle("1-2-pod-cid").unwrap();
        spool.ensure_disk_backed().unwrap();

        let mode = |p: &Path| std::fs::metadata(p).unwrap().mode() & 0o777;
        assert_eq!(mode(&root), 0o755, "an existing root keeps its mode");
        assert_eq!(mode(&spool.incoming_dir()), 0o700);
        assert_eq!(mode(&spool.ready_dir()), 0o700);
        assert_eq!(mode(&spool.incoming_dir().join("1-2-pod-cid")), 0o700);
    }

    #[test]
    fn a_failed_commit_deletes_the_bundle() {
        let (_dir, spool, bundle) = bundle();
        bundle.write_file(MANIFEST_FILE, b"{}").unwrap();
        // A non-empty directory at the target makes the rename fail.
        let taken = spool.ready_dir().join("1-2-pod-cid");
        std::fs::create_dir(&taken).unwrap();
        std::fs::write(taken.join("other"), b"x").unwrap();

        assert!(bundle.commit().is_err());
        assert!(
            !spool.incoming_dir().join("1-2-pod-cid").exists(),
            "the failed bundle is deleted"
        );
        assert!(taken.join("other").exists(), "the target is not touched");
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
