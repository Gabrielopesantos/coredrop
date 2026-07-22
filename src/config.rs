//! Daemon-written handler config.
//!
//! The kernel exec's the capture handler with a clean environment, so the
//! daemon's env vars never reach it. To bridge that, the daemon serializes its
//! capture config to a hostPath file at startup ([`HandlerConfig::write`]);
//! the handler reads it ([`HandlerConfig::read`]) from the same host path.

use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::upload;

pub const DEFAULT_CONFIG_PATH: &str = "/run/coredrop/handler.json";

/// Create `dir` (and parents) mode `0700` if missing - `create_dir_all`
/// is a no-op on an existing dir and does not touch its mode.
///
/// # Errors
///
/// Fails when the directory cannot be created or its mode cannot be set.
pub(crate) fn ensure_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
}

/// Default cap on stored (uncompressed) core bytes per crash: 2 GiB.
pub const DEFAULT_MAX_CORE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

fn default_max_core_bytes() -> u64 {
    DEFAULT_MAX_CORE_BYTES
}

/// Default per-container core-upload budget per hour.
pub const DEFAULT_MAX_CORES_PER_HOUR: u32 = 3;

fn default_max_cores_per_hour() -> u32 {
    DEFAULT_MAX_CORES_PER_HOUR
}

/// Default deadline for draining/uploading one core: 5 minutes.
/// The handler holds one of the node's `core_pipe_limit` slots for its whole
/// lifetime, so a hung store must not be able to hold it indefinitely.
pub const DEFAULT_UPLOAD_DEADLINE_SECS: u64 = 300;

fn default_upload_deadline_secs() -> u64 {
    DEFAULT_UPLOAD_DEADLINE_SECS
}

fn default_rate_state_path() -> String {
    "/run/coredrop/recent.json".to_string()
}

/// Rate-limit state file derived from the handler-config path: a `recent.json`
/// sibling on the same hostPath, so no extra flag or mount is needed.
pub fn rate_state_path_for(config_path: &str) -> String {
    Path::new(config_path)
        .parent()
        .map_or_else(default_rate_state_path, |p| {
            p.join("recent.json").to_string_lossy().into_owned()
        })
}

fn default_event_socket_path() -> String {
    "/run/coredrop/events.sock".to_string()
}

/// Capture-event unix datagram socket, derived from the handler-config path
/// the same way [`rate_state_path_for`] derives the rate-limit state file: an
/// `events.sock` sibling on the same hostPath.
pub fn event_socket_path_for(config_path: &str) -> String {
    Path::new(config_path)
        .parent()
        .map_or_else(default_event_socket_path, |p| {
            p.join("events.sock").to_string_lossy().into_owned()
        })
}

/// Everything the kernel-exec'd handler needs that env can't deliver. The
/// daemon fills it from its own env and writes it; the handler reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerConfig {
    /// Cluster name - the first path segment of the object key.
    pub cluster: String,
    /// Pass `environ` through un-redacted.
    pub no_redact: bool,
    /// `/proc` root (overridable for tests / non-standard layouts).
    pub proc_root: String,
    /// Object-store URL (e.g. `s3://crash-artifacts`); `None` disables upload.
    pub store_url: Option<String>,
    /// `object_store` config options (the `AWS_*` / cloud keys) forwarded verbatim.
    pub store_options: Vec<(String, String)>,
    /// Path to the `crictl` binary for post-drain container enrichment.
    pub crictl_path: String,
    /// CRI runtime endpoint (e.g. `unix:///run/containerd/containerd.sock`).
    /// `None` lets crictl use its own default / `CONTAINER_RUNTIME_ENDPOINT`.
    pub cri_runtime_endpoint: Option<String>,
    /// Max uncompressed core bytes stored per crash; `0` = unlimited. The
    /// remainder is drained but not stored (`serde(default)` keeps configs
    /// written by older daemons parseable).
    #[serde(default = "default_max_core_bytes")]
    pub max_core_bytes: u64,
    /// Max core uploads per container per hour; `0` = unlimited. Suppressed
    /// crashes still get a proc snapshot and manifest, just no core.
    #[serde(default = "default_max_cores_per_hour")]
    pub max_cores_per_hour: u32,
    /// Deadline in seconds for draining/uploading the core; `0` =
    /// no deadline. On expiry the handler abandons the upload and exits,
    /// freeing its `core_pipe_limit` slot instead of letting a slow store
    /// hold it.
    #[serde(default = "default_upload_deadline_secs")]
    pub upload_deadline_secs: u64,
    /// Rate-limit state file, sibling of the handler config on the hostPath.
    #[serde(default = "default_rate_state_path")]
    pub rate_state_path: String,
    /// Capture-event unix datagram socket the daemon listens on, sibling of
    /// the handler config on the hostPath. `None` when events are disabled
    /// (`serde(default)` keeps configs written by older daemons parseable).
    #[serde(default)]
    pub event_socket_path: Option<String>,
}

impl Default for HandlerConfig {
    fn default() -> Self {
        Self {
            cluster: "local".to_string(),
            no_redact: false,
            proc_root: "/proc".to_string(),
            store_url: None,
            store_options: Vec::new(),
            crictl_path: "/usr/local/bin/crictl".to_string(),
            cri_runtime_endpoint: None,
            max_core_bytes: DEFAULT_MAX_CORE_BYTES,
            max_cores_per_hour: DEFAULT_MAX_CORES_PER_HOUR,
            upload_deadline_secs: DEFAULT_UPLOAD_DEADLINE_SECS,
            rate_state_path: default_rate_state_path(),
            event_socket_path: Some(default_event_socket_path()),
        }
    }
}

impl HandlerConfig {
    /// Build from the daemon's environment.
    #[must_use]
    pub fn from_env() -> Self {
        let env_flag = |k: &str| std::env::var(k).is_ok_and(|v| !v.is_empty() && v != "0");
        let store_url = std::env::var("CAPTURE_STORE_URL")
            .ok()
            .filter(|s| !s.is_empty());
        let store_options = upload::store_options_from_env();
        Self {
            cluster: std::env::var("CAPTURE_CLUSTER").unwrap_or_else(|_| "local".to_string()),
            no_redact: env_flag("CAPTURE_NO_REDACT"),
            proc_root: std::env::var("CAPTURE_PROC_ROOT").unwrap_or_else(|_| "/proc".to_string()),
            store_url,
            store_options,
            crictl_path: std::env::var("CRICTL_PATH")
                .unwrap_or_else(|_| "/usr/local/bin/crictl".to_string()),
            cri_runtime_endpoint: std::env::var("CONTAINER_RUNTIME_ENDPOINT").ok(),
            max_core_bytes: std::env::var("CAPTURE_MAX_CORE_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_MAX_CORE_BYTES),
            max_cores_per_hour: std::env::var("CAPTURE_MAX_CORES_PER_HOUR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_MAX_CORES_PER_HOUR),
            upload_deadline_secs: std::env::var("CAPTURE_UPLOAD_DEADLINE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_UPLOAD_DEADLINE_SECS),
            rate_state_path: rate_state_path_for(
                &std::env::var("CAPTURE_CONFIG_PATH")
                    .unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string()),
            ),
            event_socket_path: (!env_flag("CAPTURE_NO_EVENTS")).then(|| {
                event_socket_path_for(
                    &std::env::var("CAPTURE_CONFIG_PATH")
                        .unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string()),
                )
            }),
        }
    }

    /// Read the config from `path`. `None` when absent or unparseable - the
    /// handler then falls back to [`Self::from_env`].
    pub fn read(path: &str) -> Option<Self> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::debug!(error = %e, path, "handler config not readable; falling back to env");
                return None;
            }
        };
        match serde_json::from_slice(&bytes) {
            Ok(cfg) => Some(cfg),
            Err(e) => {
                tracing::warn!(error = %e, path, "handler config present but unparseable; ignoring");
                None
            }
        }
    }

    /// Serialize the config to `path` (creating the parent dir), so the
    /// kernel-exec'd handler can read it.
    ///
    /// The parent dir is created mode `0700` and the file itself mode `0600`:
    /// `store_options` carries object-store credentials forwarded from the
    /// daemon's environment, written here in plaintext on a hostPath.
    ///
    /// # Errors
    ///
    /// Fails when the parent dir cannot be created/chmod'd, or the file
    /// cannot be created/written/chmod'd.
    pub fn write(&self, path: &str) -> Result<()> {
        if let Some(parent) = Path::new(path).parent() {
            ensure_private_dir(parent)
                .with_context(|| format!("creating config dir {}", parent.display()))?;
        }
        let json = serde_json::to_vec_pretty(self).context("serializing handler config")?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("opening handler config {path}"))?;

        let mut writer = std::io::BufWriter::new(file);
        writer
            .write_all(&json)
            .with_context(|| format!("writing handler config {path}"))?;
        Ok(())
    }

    #[must_use]
    pub fn object_store(&self) -> Option<Arc<dyn ObjectStore>> {
        let url = self.store_url.as_deref()?;
        upload::object_store_from_url_opts(url, self.store_options.clone())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::os::unix::fs::MetadataExt;

    use super::*;

    // Nested one level below the system temp dir so `write()`'s
    // `ensure_private_dir` only ever chmods a dir this test owns - never the
    // shared system temp dir itself.
    fn tmp(tag: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!("coredrop-cfg-{}-{tag}-{nanos}", std::process::id()));
        p.push("handler.json");
        p.to_string_lossy().into_owned()
    }

    #[test]
    fn write_then_read_round_trips() {
        let cfg = HandlerConfig {
            cluster: "prod".into(),
            no_redact: true,
            proc_root: "/proc".into(),
            store_url: Some("s3://crash-artifacts".into()),
            store_options: vec![
                ("AWS_ACCESS_KEY_ID".into(), "minioadmin".into()),
                ("AWS_ENDPOINT".into(), "http://minio:9000".into()),
            ],
            crictl_path: "/usr/local/bin/crictl".into(),
            cri_runtime_endpoint: Some("unix:///run/containerd/containerd.sock".into()),
            max_core_bytes: 1024,
            max_cores_per_hour: 5,
            upload_deadline_secs: 60,
            rate_state_path: "/run/coredrop/recent.json".into(),
            event_socket_path: Some("/run/coredrop/events.sock".into()),
        };
        let path = tmp("rt");
        cfg.write(&path).unwrap();
        let got = HandlerConfig::read(&path).unwrap();
        assert_eq!(got, cfg);
        std::fs::remove_file(&path).ok();
        if let Some(parent) = std::path::Path::new(&path).parent() {
            std::fs::remove_dir_all(parent).ok();
        }
    }

    #[test]
    fn write_sets_0600_file_and_0700_parent_dir() {
        let path = tmp("perm");
        HandlerConfig::default().write(&path).unwrap();

        let file_mode = std::fs::metadata(&path).unwrap().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "config file should be mode 0600");

        let parent = std::path::Path::new(&path).parent().unwrap();
        let dir_mode = std::fs::metadata(parent).unwrap().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "config parent dir should be mode 0700");

        std::fs::remove_file(&path).ok();
        std::fs::remove_dir_all(parent).ok();
    }

    #[test]
    fn read_absent_file_is_none() {
        assert!(HandlerConfig::read("/no/such/coredrop/handler.json").is_none());
    }

    #[test]
    fn event_socket_path_for_derives_sibling_of_config_path() {
        assert_eq!(
            event_socket_path_for("/run/coredrop/handler.json"),
            "/run/coredrop/events.sock"
        );
    }

    #[test]
    fn no_store_url_yields_no_object_store() {
        let cfg = HandlerConfig::default();
        assert!(cfg.object_store().is_none());
    }
}
