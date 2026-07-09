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
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tracing::warn;

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

    fn locked_check_and_record(
        &self,
        container_id: &str,
        now: i64,
    ) -> std::io::Result<RateDecision> {
        if let Some(parent) = self.state_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
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

        let recent = state.events.get(container_id).map_or(0, |t| t.len()) as u32;
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
mod tests {
    use super::*;

    fn tmp_state(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "coredrop-ratelimit-{}-{tag}-{nanos}.json",
            std::process::id()
        ))
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
        std::fs::remove_file(&path).ok();
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
        std::fs::remove_file(&path).ok();
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
        std::fs::remove_file(&path).ok();
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
        std::fs::write(&path, b"{not json!").unwrap();
        let rl = RateLimiter::new(&path, 1);
        assert_eq!(rl.check_and_record("cid-a", 1000), RateDecision::Allowed);
        assert_eq!(
            rl.check_and_record("cid-a", 1001),
            RateDecision::Suppressed { recent: 1 }
        );
        std::fs::remove_file(&path).ok();
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
        std::fs::remove_file(&path).ok();
    }
}
