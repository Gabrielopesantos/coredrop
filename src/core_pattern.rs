//! Installing and restoring the kernel's `core_pattern` so faults route to our
//! handler.
//!
//! The daemon writes `/proc/sys/kernel/core_pattern` at startup and restores
//! the previous value on shutdown. It also raises `core_pipe_limit`: with the
//! default `0` the kernel does not wait for the pipe handler and may reap
//! the faulting process before the handler reads `/proc/<pid>` - which would
//! defeat the pre-reap snapshot. A non-zero limit makes the kernel wait and
//! bounds how many handlers run at once under a crash storm.
//!
//! The sysctl paths are injectable so the save -> set -> restore logic is
//! unit-testable against temp files instead of the real `/proc/sys`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use tracing::warn;

const DEFAULT_PIPE_LIMIT: u32 = 16;

/// Build the `core_pattern` value routing cores to our handler:
/// `|<handler> capture %P %s %t %E`.
#[must_use]
pub fn build_pattern(handler_path: &str) -> String {
    format!("|{handler_path} capture %P %s %t %E")
}

/// Installs our `core_pattern` + `core_pipe_limit` on construction and
/// restores the prior values on drop.
pub struct CorePatternGuard {
    pattern_path: PathBuf,
    pipe_limit_path: PathBuf,
    prev_pattern: String,
    prev_pipe_limit: String,
}

impl CorePatternGuard {
    /// Install against the real host sysctls.
    ///
    /// # Errors
    ///
    /// Fails when `core_pattern` cannot be read or written (see
    /// [`Self::install_at`]).
    pub fn install(handler_path: &str) -> Result<Self> {
        Self::install_at(
            handler_path,
            PathBuf::from("/proc/sys/kernel/core_pattern"),
            PathBuf::from("/proc/sys/kernel/core_pipe_limit"),
        )
    }

    /// Install against caller-supplied sysctl paths (the test seam).
    ///
    /// # Errors
    ///
    /// Fails when the pattern file cannot be read (to save the previous
    /// value) or written. A failed `core_pipe_limit` write only warns.
    pub fn install_at(
        handler_path: &str,
        pattern_path: PathBuf,
        pipe_limit_path: PathBuf,
    ) -> Result<Self> {
        let prev_pattern = std::fs::read_to_string(&pattern_path)
            .with_context(|| format!("reading {}", pattern_path.display()))?;
        let prev_pipe_limit = std::fs::read_to_string(&pipe_limit_path).unwrap_or_default();

        std::fs::write(&pattern_path, build_pattern(handler_path))
            .with_context(|| format!("writing {}", pattern_path.display()))?;
        if let Err(e) = std::fs::write(&pipe_limit_path, DEFAULT_PIPE_LIMIT.to_string()) {
            warn!(error = %e, "could not raise core_pipe_limit; /proc snapshot may race the reaper");
        }

        Ok(Self {
            pattern_path,
            pipe_limit_path,
            prev_pattern,
            prev_pipe_limit,
        })
    }

    pub fn restore(&self) {
        if let Err(e) = std::fs::write(&self.pattern_path, &self.prev_pattern) {
            warn!(error = %e, "failed to restore core_pattern");
        }
        if !self.prev_pipe_limit.is_empty()
            && let Err(e) = std::fs::write(&self.pipe_limit_path, &self.prev_pipe_limit)
        {
            warn!(error = %e, "failed to restore core_pipe_limit");
        }
    }
}

impl Drop for CorePatternGuard {
    fn drop(&mut self) {
        self.restore();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!("coredrop-cp-{}-{tag}-{nanos}", std::process::id()));
        p
    }

    #[test]
    fn pattern_pipes_to_the_handler() {
        assert_eq!(
            build_pattern("/opt/coredrop/bin/coredrop"),
            "|/opt/coredrop/bin/coredrop capture %P %s %t %E"
        );
    }

    #[test]
    fn install_sets_then_restores_both_sysctls() {
        let pattern = tmp("pattern");
        let pipe = tmp("pipe");
        std::fs::write(&pattern, "core\n").unwrap();
        std::fs::write(&pipe, "0\n").unwrap();

        {
            let _guard =
                CorePatternGuard::install_at("/h/coredrop", pattern.clone(), pipe.clone()).unwrap();
            assert_eq!(
                std::fs::read_to_string(&pattern).unwrap(),
                "|/h/coredrop capture %P %s %t %E"
            );
            assert_eq!(std::fs::read_to_string(&pipe).unwrap(), "16");
        }

        assert_eq!(std::fs::read_to_string(&pattern).unwrap(), "core\n");
        assert_eq!(std::fs::read_to_string(&pipe).unwrap(), "0\n");

        std::fs::remove_file(&pattern).ok();
        std::fs::remove_file(&pipe).ok();
    }
}
