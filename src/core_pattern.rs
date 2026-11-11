//! Installing and restoring the kernel's `core_pattern` so faults route to our
//! handler.
//!
//! The daemon writes `/proc/sys/kernel/core_pattern` at startup and restores
//! the previous value on shutdown. While it runs, it checks that the pattern
//! still routes to the handler: another tool (systemd-coredump, apport) can
//! overwrite it, and then captures stop. It also raises `core_pipe_limit`:
//! with the default `0` the kernel does not wait for the pipe handler and may
//! reap the faulting process before the handler reads `/proc/<pid>` - which
//! would defeat the pre-reap snapshot. A non-zero limit bounds how many
//! crashes may have `/proc/<pid>` held open for their handler at once.
//!
//! The sysctl paths are injectable so the save -> set -> restore logic is
//! unit-testable against temp files instead of the real `/proc/sys`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::warn;

/// Max crashes concurrently held open for a handler.
pub const DEFAULT_PIPE_LIMIT: u32 = 128;

/// Kernel sysctl holding the pattern cores are routed through.
pub const CORE_PATTERN_PATH: &str = "/proc/sys/kernel/core_pattern";

/// Kernel sysctl bounding how many dumps may be held open for a handler.
pub const CORE_PIPE_LIMIT_PATH: &str = "/proc/sys/kernel/core_pipe_limit";

/// The kernel's own `core_pattern` default.
const KERNEL_DEFAULT_PATTERN: &str = "core";

/// Build the `core_pattern` value routing cores to our handler:
/// `|<handler> capture %P %s %t %E`.
#[must_use]
pub fn build_pattern(handler_path: &Path) -> String {
    format!("|{} capture %P %s %t %E", handler_path.display())
}

/// Installs our `core_pattern` + `core_pipe_limit` on construction and
/// restores the prior values on drop.
pub struct CorePatternGuard {
    pattern: String,
    pattern_path: PathBuf,
    pipe_limit_path: PathBuf,
    prev_pattern: String,
    prev_pipe_limit: String,
}

impl CorePatternGuard {
    /// Install against caller-supplied sysctl paths. The daemon passes the real
    /// `/proc/sys/kernel/*` paths; tests pass temp files.
    ///
    /// # Errors
    ///
    /// Fails when the pattern file cannot be read (to save the previous
    /// value) or written. A failed `core_pipe_limit` write only warns.
    pub fn install_at(
        handler_path: &Path,
        pipe_limit: u32,
        pattern_path: PathBuf,
        pipe_limit_path: PathBuf,
    ) -> Result<Self> {
        let mut prev_pattern = std::fs::read_to_string(&pattern_path)
            .with_context(|| format!("reading {}", pattern_path.display()))?;
        // A daemon killed without a clean shutdown leaves our pattern in place.
        // Saving it as the previous value would point the node at the handler
        // after we stop. The real previous value is lost, so use the default.
        if prev_pattern.trim_end() == build_pattern(handler_path) {
            warn!(
                "core_pattern already routes to our handler (previous daemon did not restore it); will restore the kernel default `core`"
            );
            prev_pattern = KERNEL_DEFAULT_PATTERN.to_string();
        }
        let prev_pipe_limit = std::fs::read_to_string(&pipe_limit_path).unwrap_or_default();

        let pattern = build_pattern(handler_path);
        std::fs::write(&pattern_path, &pattern)
            .with_context(|| format!("writing {}", pattern_path.display()))?;
        if let Err(e) = std::fs::write(&pipe_limit_path, pipe_limit.to_string()) {
            warn!(error = %e, "could not raise core_pipe_limit; /proc snapshot may race the reaper");
        }

        Ok(Self {
            pattern,
            pattern_path,
            pipe_limit_path,
            prev_pattern,
            prev_pipe_limit,
        })
    }

    /// A check of the installed pattern that a task can own. The guard
    /// stays the only owner of the restore.
    #[must_use]
    pub fn checker(&self) -> CorePatternCheck {
        CorePatternCheck {
            pattern: self.pattern.clone(),
            pattern_path: self.pattern_path.clone(),
        }
    }

    /// Restore the previous `core_pattern`/pipe-limit sysctl values. A write
    /// failure is logged, not fatal.
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

/// Checks that the `core_pattern` sysctl still routes to the handler.
#[derive(Debug, Clone)]
pub struct CorePatternCheck {
    pattern: String,
    pattern_path: PathBuf,
}

impl CorePatternCheck {
    /// Read the `core_pattern` sysctl now. `Ok(None)` when it routes to the
    /// handler, else `Ok(Some(value))` with the value that replaced it.
    ///
    /// # Errors
    ///
    /// Fails when the sysctl cannot be read.
    pub fn check(&self) -> Result<Option<String>> {
        let current = std::fs::read_to_string(&self.pattern_path)
            .with_context(|| format!("reading {}", self.pattern_path.display()))?;
        let current = current.trim_end();
        Ok((current != self.pattern).then(|| current.to_string()))
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
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn pattern_pipes_to_the_handler() {
        assert_eq!(
            build_pattern(Path::new("/opt/coredrop/bin/coredrop")),
            "|/opt/coredrop/bin/coredrop capture %P %s %t %E"
        );
    }

    #[test]
    fn install_sets_then_restores_both_sysctls() {
        let dir = TempDir::new().unwrap();
        let pattern = dir.path().join("core_pattern");
        let pipe = dir.path().join("core_pipe_limit");
        std::fs::write(&pattern, "core\n").unwrap();
        std::fs::write(&pipe, "0\n").unwrap();

        {
            let _guard = CorePatternGuard::install_at(
                Path::new("/h/coredrop"),
                16,
                pattern.clone(),
                pipe.clone(),
            )
            .unwrap();
            assert_eq!(
                std::fs::read_to_string(&pattern).unwrap(),
                "|/h/coredrop capture %P %s %t %E"
            );
            assert_eq!(std::fs::read_to_string(&pipe).unwrap(), "16");
        }

        assert_eq!(std::fs::read_to_string(&pattern).unwrap(), "core\n");
        assert_eq!(std::fs::read_to_string(&pipe).unwrap(), "0\n");
    }

    #[test]
    fn check_sees_another_tool_overwrite_the_pattern() {
        let dir = TempDir::new().unwrap();
        let pattern = dir.path().join("core_pattern");
        let pipe = dir.path().join("core_pipe_limit");
        std::fs::write(&pattern, "core\n").unwrap();
        std::fs::write(&pipe, "0\n").unwrap();
        let guard =
            CorePatternGuard::install_at(Path::new("/h/coredrop"), 16, pattern.clone(), pipe)
                .unwrap();
        let guard = guard.checker();
        assert_eq!(guard.check().unwrap(), None);

        // The kernel shows the value with a trailing newline.
        std::fs::write(&pattern, "|/usr/lib/systemd/systemd-coredump %P\n").unwrap();
        assert_eq!(
            guard.check().unwrap().as_deref(),
            Some("|/usr/lib/systemd/systemd-coredump %P")
        );

        std::fs::write(&pattern, "|/h/coredrop capture %P %s %t %E\n").unwrap();
        assert_eq!(guard.check().unwrap(), None);

        std::fs::remove_file(&pattern).unwrap();
        assert!(guard.check().is_err());
    }

    #[test]
    fn install_fails_when_the_pattern_sysctl_is_unreadable() {
        // The previous value has to be saved before anything is written, or a
        // restore on shutdown would install garbage.
        let dir = TempDir::new().unwrap();
        let pipe = dir.path().join("core_pipe_limit");
        std::fs::write(&pipe, "0\n").unwrap();

        // `unwrap_err` would need `CorePatternGuard: Debug`; match instead so
        // the guard type stays free of a derive it has no other use for.
        let result = CorePatternGuard::install_at(
            Path::new("/h/coredrop"),
            16,
            dir.path().join("missing/core_pattern"),
            pipe.clone(),
        );
        let Err(err) = result else {
            panic!("installing against a missing sysctl must fail");
        };
        assert!(format!("{err:#}").contains("core_pattern"), "{err:#}");
        // Nothing was installed, so core_pipe_limit must be untouched too.
        assert_eq!(std::fs::read_to_string(&pipe).unwrap(), "0\n");
    }

    #[test]
    fn install_over_our_own_pattern_restores_the_kernel_default() {
        // A previous daemon was killed and left the handler installed.
        let dir = TempDir::new().unwrap();
        let pattern = dir.path().join("core_pattern");
        let pipe = dir.path().join("core_pipe_limit");
        std::fs::write(
            &pattern,
            format!("{}\n", build_pattern(Path::new("/h/coredrop"))),
        )
        .unwrap();
        std::fs::write(&pipe, "128\n").unwrap();

        drop(
            CorePatternGuard::install_at(Path::new("/h/coredrop"), 16, pattern.clone(), pipe)
                .unwrap(),
        );

        assert_eq!(std::fs::read_to_string(&pattern).unwrap(), "core");
    }
}
