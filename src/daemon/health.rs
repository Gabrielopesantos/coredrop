//! The check behind the chart's readiness and liveness probes.

use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::config::HandlerConfig;
use crate::core_pattern::build_pattern;

/// Check that the capture path is still wired up. `Ok(())` means healthy; the
/// error names the one thing that drifted, which is what the kubelet surfaces
/// on the pod.
///
/// # Errors
///
/// Fails when `core_pattern` cannot be read, does not point at our handler,
/// the handler config is missing or invalid, or the capture-event socket the
/// config advertises is gone.
pub fn check(handler_path: &Path, config_path: &Path, core_pattern_path: &Path) -> Result<()> {
    let current = std::fs::read_to_string(core_pattern_path)
        .with_context(|| format!("reading {}", core_pattern_path.display()))?;
    let expected = build_pattern(handler_path);
    if current.trim_end() != expected {
        bail!(
            "core_pattern is {:?}, expected {expected:?}",
            current.trim_end()
        );
    }

    // `read` returns `None` for missing, unparseable and invalid alike - it
    // logs which; the probe only needs the verdict.
    let Some(config) = HandlerConfig::read(config_path) else {
        bail!(
            "handler config {} is missing or invalid",
            config_path.display()
        );
    };

    let socket = &config.event_socket_path;
    if !socket.exists() {
        bail!("capture event socket {} is not bound", socket.display());
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;

    /// The `core_pattern` value the daemon installs for the default handler
    /// path, with the trailing newline the kernel writes back.
    const INSTALLED: &str = "|/opt/coredrop/bin/coredrop capture %P %s %t %E\n";
    const HANDLER: &str = "/opt/coredrop/bin/coredrop";

    /// A temp dir holding a `core_pattern` file, a written handler config
    /// and its bound event socket, plus the two paths `check` takes.
    fn fixture(pattern: &str) -> (TempDir, PathBuf, PathBuf) {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("core_pattern"), pattern).unwrap();
        let event_socket_path = dir.path().join("events.sock");
        // A bound socket leaves its file after the socket is closed.
        drop(std::os::unix::net::UnixDatagram::bind(&event_socket_path).unwrap());
        let config = HandlerConfig {
            event_socket_path,
            ..Default::default()
        };
        let config_path = dir.path().join("handler.json");
        config.write(&config_path).unwrap();
        let pattern_path = dir.path().join("core_pattern");
        (dir, config_path, pattern_path)
    }

    #[test]
    fn passes_when_pattern_config_and_socket_are_intact() {
        // The kernel writes back a trailing newline; the check must tolerate it.
        let (_dir, config, pattern) = fixture(INSTALLED);
        check(Path::new(HANDLER), &config, &pattern).unwrap();
    }

    #[test]
    fn fails_when_core_pattern_drifted() {
        let (_dir, config, pattern) = fixture("core\n");
        let err = check(Path::new(HANDLER), &config, &pattern).unwrap_err();
        assert!(format!("{err:#}").contains("core_pattern is"), "{err:#}");
    }

    #[test]
    fn fails_when_handler_config_is_gone() {
        let (_dir, config, pattern) = fixture(INSTALLED);
        std::fs::remove_file(&config).unwrap();

        let err = check(Path::new(HANDLER), &config, &pattern).unwrap_err();
        assert!(format!("{err:#}").contains("missing or invalid"), "{err:#}");
    }

    #[test]
    fn fails_when_advertised_event_socket_is_gone() {
        let (dir, config, pattern) = fixture(INSTALLED);
        std::fs::remove_file(dir.path().join("events.sock")).unwrap();

        let err = check(Path::new(HANDLER), &config, &pattern).unwrap_err();
        assert!(format!("{err:#}").contains("is not bound"), "{err:#}");
    }
}
