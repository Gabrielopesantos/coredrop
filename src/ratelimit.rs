//! Per-container core-upload rate limiting.
//!
//! A crash-looping pod would otherwise upload a full core every few seconds,
//! forever. Handlers are short-lived kernel-exec'd processes, so the limiter
//! keeps its state in a JSON file on the same hostPath as the handler config,
//! guarded by an exclusive `flock` against concurrent handlers (crash storms).
//!
//! Fail-open by design: any IO or parse error yields `Allowed` - a broken
//! limiter must never lose a core. Only *allowed* uploads are recorded, so a
//! crash-looping container keeps getting its budget every window instead of
//! being starved forever.

use std::collections::BTreeMap;
use std::io::{Read, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::config::ensure_private_dir;

/// Sliding window the per-container budget applies to.
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

/// `container_id -> epoch seconds of allowed uploads` (pruned to the window).
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
    pub fn check_and_record(&self, container_id: &str, now_epoch_secs: i64) -> RateDecision {
        if self.max_per_hour == 0 {
            return RateDecision::Allowed;
        }
        match self.locked_check_and_record(container_id, now_epoch_secs) {
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
    pub fn refund(&self, container_id: &str, recorded_at: i64) {
        if self.max_per_hour == 0 {
            return;
        }
        if let Err(e) = self.locked_refund(container_id, recorded_at) {
            warn!(error = %e, path = %self.state_path.display(),
                "rate-limit refund failed; one budget slot stays consumed");
        }
    }

    fn locked_refund(&self, container_id: &str, recorded_at: i64) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.state_path)?;
        rustix::fs::flock(&file, rustix::fs::FlockOperation::LockExclusive)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut state: RateState = serde_json::from_slice(&bytes).unwrap_or_default();

        if let Some(times) = state.events.get_mut(container_id) {
            if let Some(pos) = times.iter().rposition(|t| *t == recorded_at) {
                times.remove(pos);
            }
            if times.is_empty() {
                state.events.remove(container_id);
            }
        }

        let json = serde_json::to_vec(&state)?;
        file.seek(std::io::SeekFrom::Start(0))?;
        file.set_len(0)?;
        file.write_all(&json)?;
        Ok(())
    }

    fn locked_check_and_record(
        &self,
        container_id: &str,
        now: i64,
    ) -> std::io::Result<RateDecision> {
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

        let recent =
            u32::try_from(state.events.get(container_id).map_or(0, Vec::len)).unwrap_or(u32::MAX);
        let decision = if recent >= self.max_per_hour {
            RateDecision::Suppressed { recent }
        } else {
            state
                .events
                .entry(container_id.to_string())
                .or_default()
                .push(now);
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
    use std::os::unix::fs::MetadataExt;

    use super::*;

    // Nested one level below the system temp dir so `ensure_private_dir`
    // only ever chmods a dir this test owns - never the shared system temp
    // dir itself.
    fn tmp_state(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir()
            .join(format!(
                "coredrop-ratelimit-{}-{tag}-{nanos}",
                std::process::id()
            ))
            .join("recent.json")
    }

    fn cleanup(path: &std::path::Path) {
        std::fs::remove_file(path).ok();
        if let Some(parent) = path.parent() {
            std::fs::remove_dir_all(parent).ok();
        }
    }

    #[test]
    fn allows_up_to_max_then_suppresses() {
        let path = tmp_state("cap");
        let rl = RateLimiter::new(&path, 3);
        for _ in 0..3 {
            assert_eq!(rl.check_and_record("cid-a", 1000), RateDecision::Allowed);
        }
        assert_eq!(
            rl.check_and_record("cid-a", 1001),
            RateDecision::Suppressed { recent: 3 }
        );
        cleanup(&path);
    }

    #[test]
    fn check_and_record_creates_0700_dir_and_0600_state_file() {
        let path = tmp_state("permtest");
        let rl = RateLimiter::new(&path, 3);
        assert_eq!(rl.check_and_record("cid-perm", 1000), RateDecision::Allowed);

        let file_mode = std::fs::metadata(&path).unwrap().mode() & 0o777;
        assert_eq!(
            file_mode, 0o600,
            "rate-limit state file should be mode 0600"
        );

        let parent = path.parent().unwrap();
        let dir_mode = std::fs::metadata(parent).unwrap().mode() & 0o777;
        assert_eq!(
            dir_mode, 0o700,
            "rate-limit state parent dir should be mode 0700"
        );

        cleanup(&path);
    }

    #[test]
    fn window_pruning_restores_budget() {
        let path = tmp_state("window");
        let rl = RateLimiter::new(&path, 1);
        assert_eq!(rl.check_and_record("cid-a", 1000), RateDecision::Allowed);
        assert_eq!(
            rl.check_and_record("cid-a", 1001),
            RateDecision::Suppressed { recent: 1 }
        );
        // Past the window, the old event is pruned.
        assert_eq!(
            rl.check_and_record("cid-a", 1000 + RATE_WINDOW_SECS + 1),
            RateDecision::Allowed
        );
        cleanup(&path);
    }

    #[test]
    fn zero_means_unlimited() {
        let rl = RateLimiter::new("/nonexistent/never-touched.json", 0);
        for i in 0..100 {
            assert_eq!(rl.check_and_record("cid-a", i), RateDecision::Allowed);
        }
    }

    #[test]
    fn containers_are_isolated() {
        let path = tmp_state("iso");
        let rl = RateLimiter::new(&path, 1);
        assert_eq!(rl.check_and_record("cid-a", 1000), RateDecision::Allowed);
        assert_eq!(rl.check_and_record("cid-b", 1000), RateDecision::Allowed);
        assert_eq!(
            rl.check_and_record("cid-a", 1001),
            RateDecision::Suppressed { recent: 1 }
        );
        cleanup(&path);
    }

    #[test]
    fn fails_open_when_state_dir_is_unwritable() {
        let rl = RateLimiter::new("/proc/definitely/not/writable/state.json", 1);
        assert_eq!(rl.check_and_record("cid-a", 1000), RateDecision::Allowed);
        assert_eq!(rl.check_and_record("cid-a", 1001), RateDecision::Allowed);
    }

    #[test]
    fn corrupt_state_self_heals() {
        let path = tmp_state("corrupt");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"{not json!").unwrap();
        let rl = RateLimiter::new(&path, 1);
        assert_eq!(rl.check_and_record("cid-a", 1000), RateDecision::Allowed);
        assert_eq!(
            rl.check_and_record("cid-a", 1001),
            RateDecision::Suppressed { recent: 1 }
        );
        cleanup(&path);
    }

    #[test]
    fn refund_restores_a_consumed_slot() {
        let path = tmp_state("refund");
        let rl = RateLimiter::new(&path, 1);
        assert_eq!(rl.check_and_record("cid-a", 1000), RateDecision::Allowed);
        assert_eq!(
            rl.check_and_record("cid-a", 1001),
            RateDecision::Suppressed { recent: 1 }
        );
        rl.refund("cid-a", 1000);
        assert_eq!(rl.check_and_record("cid-a", 1002), RateDecision::Allowed);
        cleanup(&path);
    }

    #[test]
    fn refund_without_matching_record_is_harmless() {
        let path = tmp_state("refund-nop");
        let rl = RateLimiter::new(&path, 1);
        rl.refund("cid-never-seen", 1000); // state file doesn't even exist
        assert_eq!(rl.check_and_record("cid-a", 1000), RateDecision::Allowed);
        rl.refund("cid-a", 999); // wrong timestamp - removes nothing
        assert_eq!(
            rl.check_and_record("cid-a", 1001),
            RateDecision::Suppressed { recent: 1 }
        );
        cleanup(&path);
    }

    #[test]
    fn concurrent_handlers_admit_exactly_max() {
        let path = tmp_state("flock");
        let max = 3u32;
        let mut handles = Vec::new();
        for _ in 0..16 {
            let path = path.clone();
            handles.push(std::thread::spawn(move || {
                let rl = RateLimiter::new(&path, max);
                rl.check_and_record("cid-storm", 1000)
            }));
        }
        let allowed = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|d| *d == RateDecision::Allowed)
            .count();
        assert_eq!(allowed as u32, max, "flock must serialize check-and-record");
        cleanup(&path);
    }
}
