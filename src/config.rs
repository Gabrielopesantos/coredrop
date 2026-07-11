//! Daemon-written handler config.
//!
//! The kernel exec's the capture handler with a clean environment, so the
//! daemon's env vars never reach it. To bridge that, the daemon serializes its
//! capture config to a hostPath file at startup ([`HandlerConfig::write`]);
//! the handler reads it ([`HandlerConfig::read`]) from the same host path.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::backend::CaptureBackendKind;
use crate::upload::{self, ALLOWED_STORE_OPTS};

pub const DEFAULT_CONFIG_PATH: &str = "/run/coredrop/handler.json";

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

/// Everything the kernel-exec'd handler needs that env can't deliver. The
/// daemon fills it from its own env and writes it; the handler reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerConfig {
    /// Cluster name - the first path segment of the object key.
    pub cluster: String,
    /// Capture backend selector: `standalone` (default) | `systemd-coredump`.
    pub backend: String,
    /// Pass `environ` through un-redacted.
    pub no_redact: bool,
    /// `/proc` root (overridable for tests / non-standard layouts).
    pub proc_root: String,
    /// systemd-coredump binary path for the chaining backend.
    pub systemd_coredump_path: Option<String>,
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
    /// Rate-limit state file, sibling of the handler config on the hostPath.
    #[serde(default = "default_rate_state_path")]
    pub rate_state_path: String,
}

impl Default for HandlerConfig {
    fn default() -> Self {
        Self {
            cluster: "local".to_string(),
            backend: "standalone".to_string(),
            no_redact: false,
            proc_root: "/proc".to_string(),
            systemd_coredump_path: None,
            store_url: None,
            store_options: Vec::new(),
            crictl_path: "/usr/local/bin/crictl".to_string(),
            cri_runtime_endpoint: None,
            max_core_bytes: DEFAULT_MAX_CORE_BYTES,
            max_cores_per_hour: DEFAULT_MAX_CORES_PER_HOUR,
            rate_state_path: default_rate_state_path(),
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
        let store_options: Vec<(String, String)> = std::env::vars()
            .filter(|(k, _)| ALLOWED_STORE_OPTS.contains(&k.as_str()))
            .collect();
        Self {
            cluster: std::env::var("CAPTURE_CLUSTER").unwrap_or_else(|_| "local".to_string()),
            backend: std::env::var("CAPTURE_BACKEND").unwrap_or_else(|_| "standalone".to_string()),
            no_redact: env_flag("CAPTURE_NO_REDACT"),
            proc_root: std::env::var("CAPTURE_PROC_ROOT").unwrap_or_else(|_| "/proc".to_string()),
            systemd_coredump_path: std::env::var("CAPTURE_SYSTEMD_COREDUMP_PATH").ok(),
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
            rate_state_path: rate_state_path_for(
                &std::env::var("CAPTURE_CONFIG_PATH")
                    .unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string()),
            ),
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
    /// # Errors
    ///
    /// Fails when the parent dir cannot be created or the file cannot be
    /// written.
    pub fn write(&self, path: &str) -> Result<()> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating config dir {}", parent.display()))?;
        }
        let json = serde_json::to_vec_pretty(self).context("serializing handler config")?;
        std::fs::write(path, json).with_context(|| format!("writing handler config {path}"))?;
        Ok(())
    }

    #[must_use]
    pub fn backend_kind(&self) -> CaptureBackendKind {
        CaptureBackendKind::parse(&self.backend)
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
    use super::*;

    fn tmp(tag: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "coredrop-cfg-{}-{tag}-{nanos}.json",
            std::process::id()
        ));
        p.to_string_lossy().into_owned()
    }

    #[test]
    fn write_then_read_round_trips() {
        let cfg = HandlerConfig {
            cluster: "prod".into(),
            backend: "standalone".into(),
            no_redact: true,
            proc_root: "/proc".into(),
            systemd_coredump_path: Some("/usr/lib/systemd/systemd-coredump".into()),
            store_url: Some("s3://crash-artifacts".into()),
            store_options: vec![
                ("AWS_ACCESS_KEY_ID".into(), "minioadmin".into()),
                ("AWS_ENDPOINT".into(), "http://minio:9000".into()),
            ],
            crictl_path: "/usr/local/bin/crictl".into(),
            cri_runtime_endpoint: Some("unix:///run/containerd/containerd.sock".into()),
            max_core_bytes: 1024,
            max_cores_per_hour: 5,
            rate_state_path: "/run/coredrop/recent.json".into(),
        };
        let path = tmp("rt");
        cfg.write(&path).unwrap();
        let got = HandlerConfig::read(&path).unwrap();
        assert_eq!(got, cfg);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn read_absent_file_is_none() {
        assert!(HandlerConfig::read("/no/such/coredrop/handler.json").is_none());
    }

    #[test]
    fn backend_kind_parses_from_config() {
        let mut cfg = HandlerConfig::default();
        assert_eq!(cfg.backend_kind(), CaptureBackendKind::Standalone);
        cfg.backend = "systemd-coredump".into();
        assert_eq!(cfg.backend_kind(), CaptureBackendKind::SystemdCoredump);
    }

    #[test]
    fn no_store_url_yields_no_object_store() {
        let cfg = HandlerConfig::default();
        assert!(cfg.object_store().is_none());
    }
}
