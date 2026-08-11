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
/// Fails when `core_pattern` cannot be read, no longer points at our handler,
/// the handler config is missing or invalid, or the capture-event socket the
/// config advertises is gone.
pub fn check(handler_path: &str, config_path: &str, core_pattern_path: &str) -> Result<()> {
    let current = std::fs::read_to_string(core_pattern_path)
        .with_context(|| format!("reading {core_pattern_path}"))?;
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
        bail!("handler config {config_path} is missing or invalid");
    };

    if let Some(socket) = &config.event_socket_path
        && !Path::new(socket).exists()
    {
        bail!("capture event socket {socket} is not bound");
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "coredrop-health-{}-{tag}-{nanos}",
            std::process::id()
        ));
        p
    }

    /// A temp dir holding a `core_pattern` file and a written handler config.
    fn fixture(tag: &str, pattern: &str, event_socket_path: Option<String>) -> std::path::PathBuf {
        let dir = tmp(tag);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("core_pattern"), pattern).unwrap();
        let config = HandlerConfig {
            event_socket_path,
            ..Default::default()
        };
        config
            .write(&dir.join("handler.json").to_string_lossy())
            .unwrap();
        dir
    }

    fn paths(dir: &Path) -> (String, String) {
        (
            dir.join("handler.json").to_string_lossy().into_owned(),
            dir.join("core_pattern").to_string_lossy().into_owned(),
        )
    }

    #[test]
    fn passes_when_pattern_and_config_are_intact() {
        // The kernel writes back a trailing newline; the check must tolerate it.
        let dir = fixture(
            "ok",
            "|/opt/coredrop/bin/coredrop capture %P %s %t %E\n",
            None,
        );
        let (config, pattern) = paths(&dir);

        check("/opt/coredrop/bin/coredrop", &config, &pattern).unwrap();

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn fails_when_core_pattern_drifted() {
        let dir = fixture("drift", "core\n", None);
        let (config, pattern) = paths(&dir);

        let err = check("/opt/coredrop/bin/coredrop", &config, &pattern).unwrap_err();
        assert!(format!("{err:#}").contains("core_pattern is"), "{err:#}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn fails_when_handler_config_is_gone() {
        let dir = fixture(
            "noconfig",
            "|/opt/coredrop/bin/coredrop capture %P %s %t %E\n",
            None,
        );
        let (config, pattern) = paths(&dir);
        std::fs::remove_file(&config).unwrap();

        let err = check("/opt/coredrop/bin/coredrop", &config, &pattern).unwrap_err();
        assert!(format!("{err:#}").contains("missing or invalid"), "{err:#}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn fails_when_advertised_event_socket_is_gone() {
        let dir = fixture(
            "nosock",
            "|/opt/coredrop/bin/coredrop capture %P %s %t %E\n",
            Some("/run/coredrop/definitely-not-bound.sock".to_string()),
        );
        let (config, pattern) = paths(&dir);

        let err = check("/opt/coredrop/bin/coredrop", &config, &pattern).unwrap_err();
        assert!(format!("{err:#}").contains("is not bound"), "{err:#}");

        std::fs::remove_dir_all(&dir).ok();
    }
}
