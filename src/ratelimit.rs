//! Per-pod core-upload rate limiting.
//!
//! A crash-looping pod would otherwise upload a full core every few seconds,
//! forever. Handlers are short-lived kernel-exec'd processes, so the limiter
//! keeps its state in a JSON file on the same hostPath as the handler config,
//! guarded by an exclusive `flock` against concurrent handlers (crash storms).
//!
//! Fail-open by design: any IO or parse error yields `Allowed` - a broken
//! limiter must never lose a core. Only *allowed* uploads are recorded, so a
//! crash-looping pod keeps getting its budget every window instead of
//! being starved forever.

use std::collections::BTreeMap;
use std::io::{Read, Seek, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::config::ensure_private_dir;

/// Sliding window the per-pod budget applies to.
pub const RATE_WINDOW_SECS: i64 = 3600;

/// Whether a core upload may proceed for a container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RateDecision {
    Allowed,
    /// Budget exhausted; `recent` uploads already happened in the window.
    Suppressed {
        recent: u32,
    },
}

/// `pod_uid -> epoch seconds of allowed uploads` (pruned to the window).
#[derive(Debug, Default, Serialize, Deserialize)]
struct RateState {
    events: BTreeMap<String, Vec<i64>>,
}

pub struct RateLimiter {
    state_path: PathBuf,
    max_per_hour: u32,
}

impl RateLimiter {
    pub fn new(state_path: impl Into<PathBuf>, max_per_hour: u32) -> Self {
        Self {
            state_path: state_path.into(),
            max_per_hour,
        }
    }

    /// Atomically check the budget and record the upload if allowed.
    /// `now_epoch_secs` is the kernel's `%t` crash timestamp - deterministic
    /// and testable, and all handlers on a node share the same clock.
    /// `scope` is the pod UID, which is stable across container restarts.
    pub fn check_and_record(&self, scope: &str, now_epoch_secs: i64) -> RateDecision {
        if self.max_per_hour == 0 {
            return RateDecision::Allowed;
        }
        match self.locked_check_and_record(scope, now_epoch_secs) {
            Ok(decision) => decision,
            Err(e) => {
                warn!(error = %e, path = %self.state_path.display(),
                    "rate-limit state unavailable; allowing upload (fail-open)");
                RateDecision::Allowed
            }
        }
    }

    /// Give back a slot recorded by `check_and_record` whose upload stored
    /// nothing (e.g. the object store was unreachable). Without the refund, a
    /// transient store outage would eat the whole budget with zero cores
    /// stored. Best-effort: errors are logged and swallowed.
    pub fn refund(&self, scope: &str, recorded_at: i64) {
        if self.max_per_hour == 0 {
            return;
        }
        if let Err(e) = self.locked_refund(scope, recorded_at) {
            warn!(error = %e, path = %self.state_path.display(),
                "rate-limit refund failed; one budget slot stays consumed");
        }
    }

    fn locked_refund(&self, scope: &str, recorded_at: i64) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.state_path)?;
        rustix::fs::flock(&file, rustix::fs::FlockOperation::LockExclusive)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut state: RateState = serde_json::from_slice(&bytes).unwrap_or_default();

        if let Some(times) = state.events.get_mut(scope) {
            if let Some(pos) = times.iter().rposition(|t| *t == recorded_at) {
                times.remove(pos);
            }
            if times.is_empty() {
                state.events.remove(scope);
            }
        }

        let json = serde_json::to_vec(&state)?;
        file.seek(std::io::SeekFrom::Start(0))?;
        file.set_len(0)?;
        file.write_all(&json)?;
        Ok(())
    }

    fn locked_check_and_record(&self, scope: &str, now: i64) -> std::io::Result<RateDecision> {
        if let Some(parent) = self.state_path.parent() {
            ensure_private_dir(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&self.state_path)?;
        // `OpenOptions::mode` only applies when `O_CREAT` creates a new inode,
        // so a planted file keeps its own mode - see `HandlerConfig::write`.
        std::fs::set_permissions(&self.state_path, std::fs::Permissions::from_mode(0o600))?;
        // Blocking exclusive lock: contenders are only concurrent handlers on
        // this node, each holding the lock for a few milliseconds.
        rustix::fs::flock(&file, rustix::fs::FlockOperation::LockExclusive)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        // Corrupt state self-heals to empty rather than blocking captures.
        let mut state: RateState = serde_json::from_slice(&bytes).unwrap_or_default();

        let cutoff = now - RATE_WINDOW_SECS;
        state.events.retain(|_, times| {
            times.retain(|t| *t > cutoff);
            !times.is_empty()
        });

        let recent = u32::try_from(state.events.get(scope).map_or(0, Vec::len)).unwrap_or(u32::MAX);
        let decision = if recent >= self.max_per_hour {
            RateDecision::Suppressed { recent }
        } else {
            state.events.entry(scope.to_string()).or_default().push(now);
            RateDecision::Allowed
        };

        // Rewrite in place - never temp-file + rename, which would swap the
        // inode out from under blocked flock waiters.
        let json = serde_json::to_vec(&state)?;
        file.seek(std::io::SeekFrom::Start(0))?;
        file.set_len(0)?;
        file.write_all(&json)?;
        // flock released on drop (fd close).
        Ok(decision)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]
mod tests {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    use tempfile::TempDir;

    use super::*;

    /// A state-file path inside a fresh temp dir. The `TempDir` is returned so
    /// the caller keeps it alive; dropping it removes the tree even on panic.
    fn tmp_state() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("recent.json");
        (dir, path)
    }

    #[test]
    fn allows_up_to_max_then_suppresses() {
        let (_dir, path) = tmp_state();
        let rl = RateLimiter::new(&path, 3);
        for _ in 0..3 {
            assert_eq!(rl.check_and_record("pod-a", 1000), RateDecision::Allowed);
        }
        assert_eq!(
            rl.check_and_record("pod-a", 1001),
            RateDecision::Suppressed { recent: 3 }
        );
    }

    #[test]
    fn check_and_record_creates_0700_dir_and_0600_state_file() {
        let dir = TempDir::new().unwrap();
        // A subdirectory the limiter has to create itself, so this covers
        // `ensure_private_dir` rather than the mode `TempDir` already sets.
        let path = dir.path().join("run").join("recent.json");
        let rl = RateLimiter::new(&path, 3);
        assert_eq!(rl.check_and_record("pod-perm", 1000), RateDecision::Allowed);

        let file_mode = std::fs::metadata(&path).unwrap().mode() & 0o777;
        assert_eq!(
            file_mode, 0o600,
            "rate-limit state file should be mode 0600"
        );

        let dir_mode = std::fs::metadata(path.parent().unwrap()).unwrap().mode() & 0o777;
        assert_eq!(
            dir_mode, 0o700,
            "rate-limit state parent dir should be mode 0700"
        );
    }

    #[test]
    fn check_and_record_tightens_an_existing_loose_state_file() {
        // Same hazard as `HandlerConfig::write`: the hostPath can already hold
        // a world-readable state file, and `OpenOptions::mode` only applies to
        // a newly created inode.
        let dir = TempDir::new().unwrap();
        let run_dir = dir.path().join("run");
        std::fs::create_dir_all(&run_dir).unwrap();
        std::fs::set_permissions(&run_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = run_dir.join("recent.json");
        std::fs::write(&path, b"{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let rl = RateLimiter::new(&path, 3);
        assert_eq!(rl.check_and_record("pod-perm", 1000), RateDecision::Allowed);

        let file_mode = std::fs::metadata(&path).unwrap().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "existing state file should become 0600");

        let dir_mode = std::fs::metadata(&run_dir).unwrap().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "existing state dir should become 0700");
    }

    #[test]
    fn window_pruning_restores_budget() {
        let (_dir, path) = tmp_state();
        let rl = RateLimiter::new(&path, 1);
        assert_eq!(rl.check_and_record("pod-a", 1000), RateDecision::Allowed);
        assert_eq!(
            rl.check_and_record("pod-a", 1001),
            RateDecision::Suppressed { recent: 1 }
        );
        // Past the window, the old event is pruned.
        assert_eq!(
            rl.check_and_record("pod-a", 1000 + RATE_WINDOW_SECS + 1),
            RateDecision::Allowed
        );
    }

    #[test]
    fn zero_means_unlimited() {
        // The path is never touched: a zero budget short-circuits before any IO.
        let rl = RateLimiter::new("/nonexistent/never-touched.json", 0);
        for i in 0..100 {
            assert_eq!(rl.check_and_record("pod-a", i), RateDecision::Allowed);
        }
        rl.refund("pod-a", 0);
    }

    #[test]
    fn pods_are_isolated() {
        let (_dir, path) = tmp_state();
        let rl = RateLimiter::new(&path, 1);
        assert_eq!(rl.check_and_record("pod-a", 1000), RateDecision::Allowed);
        assert_eq!(rl.check_and_record("pod-b", 1000), RateDecision::Allowed);
        assert_eq!(
            rl.check_and_record("pod-a", 1001),
            RateDecision::Suppressed { recent: 1 }
        );
    }

    #[test]
    fn fails_open_when_state_dir_is_unwritable() {
        let rl = RateLimiter::new("/proc/definitely/not/writable/state.json", 1);
        assert_eq!(rl.check_and_record("pod-a", 1000), RateDecision::Allowed);
        assert_eq!(rl.check_and_record("pod-a", 1001), RateDecision::Allowed);
    }

    #[test]
    fn corrupt_state_self_heals() {
        let (_dir, path) = tmp_state();
        std::fs::write(&path, b"{not json!").unwrap();
        let rl = RateLimiter::new(&path, 1);
        assert_eq!(rl.check_and_record("pod-a", 1000), RateDecision::Allowed);
        assert_eq!(
            rl.check_and_record("pod-a", 1001),
            RateDecision::Suppressed { recent: 1 }
        );
    }

    #[test]
    fn refund_restores_a_consumed_slot() {
        let (_dir, path) = tmp_state();
        let rl = RateLimiter::new(&path, 1);
        assert_eq!(rl.check_and_record("pod-a", 1000), RateDecision::Allowed);
        assert_eq!(
            rl.check_and_record("pod-a", 1001),
            RateDecision::Suppressed { recent: 1 }
        );
        rl.refund("pod-a", 1000);
        assert_eq!(rl.check_and_record("pod-a", 1002), RateDecision::Allowed);
    }

    #[test]
    fn refund_without_matching_record_is_harmless() {
        let (_dir, path) = tmp_state();
        let rl = RateLimiter::new(&path, 1);
        rl.refund("pod-never-seen", 1000); // state file doesn't even exist
        assert_eq!(rl.check_and_record("pod-a", 1000), RateDecision::Allowed);
        rl.refund("pod-a", 999); // wrong timestamp - removes nothing
        assert_eq!(
            rl.check_and_record("pod-a", 1001),
            RateDecision::Suppressed { recent: 1 }
        );
    }

    #[test]
    fn concurrent_handlers_admit_exactly_max() {
        let (_dir, path) = tmp_state();
        let max = 3u32;
        let mut handles = Vec::new();
        for _ in 0..16 {
            let path = path.clone();
            handles.push(std::thread::spawn(move || {
                let rl = RateLimiter::new(&path, max);
                rl.check_and_record("pod-storm", 1000)
            }));
        }
        let allowed = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|d| *d == RateDecision::Allowed)
            .count();
        assert_eq!(allowed as u32, max, "flock must serialize check-and-record");
    }
}
