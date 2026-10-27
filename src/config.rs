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

use anyhow::{Context, Result, bail};
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

/// Default cluster name, the first segment of every object key.
pub const DEFAULT_CLUSTER: &str = "local";

/// Default `/proc` root.
pub const DEFAULT_PROC_ROOT: &str = "/proc";

/// Default path of the node's `crictl`.
pub const DEFAULT_CRICTL_PATH: &str = "/usr/local/bin/crictl";

/// Default cap on stored (uncompressed) core bytes per crash: 2 GiB.
pub const DEFAULT_MAX_CORE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Default per-pod core-upload budget per hour.
pub const DEFAULT_MAX_CORES_PER_HOUR: u32 = 3;

/// Default deadline for draining/uploading one core: 5 minutes.
/// The handler holds one of the node's `core_pipe_limit` slots for its whole
/// lifetime, so a hung store must not be able to hold it indefinitely.
pub const DEFAULT_UPLOAD_DEADLINE_SECS: u64 = 300;

/// Default timeout for `crictl inspect` in the handler.
pub const DEFAULT_CRICTL_TIMEOUT_SECS: u64 = 10;

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
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerConfig {
    /// Cluster name - the first path segment of the object key.
    pub cluster: String,
    /// Pass `environ` and `cmdline` through un-redacted.
    pub no_redact: bool,
    /// `/proc` root (overridable for tests/non-standard layouts).
    pub proc_root: String,
    /// Object-store URL (e.g. `s3://crash-artifacts`); `None` disables upload.
    pub store_url: Option<String>,
    /// `object_store` config options (the `AWS_*`/cloud keys) forwarded verbatim.
    pub store_options: Vec<(String, String)>,
    /// Path to the `crictl` binary for post-drain container enrichment.
    pub crictl_path: String,
    /// CRI runtime endpoint (e.g. `unix:///run/containerd/containerd.sock`).
    /// `None` lets crictl use its own default/`CONTAINER_RUNTIME_ENDPOINT`.
    pub cri_runtime_endpoint: Option<String>,
    /// Max uncompressed core bytes stored per crash; `0` = unlimited. The
    /// remainder is drained but not stored.
    pub max_core_bytes: u64,
    /// Max core uploads per pod per hour; `0` = unlimited. Suppressed
    /// crashes still get a proc snapshot and manifest, just no core.
    pub max_cores_per_hour: u32,
    /// Deadline in seconds for draining/uploading the core; `0` =
    /// no deadline. On expiry the handler abandons the upload and exits,
    /// freeing its `core_pipe_limit` slot instead of letting a slow store
    /// hold it.
    pub upload_deadline_secs: u64,
    /// Timeout in seconds for `crictl inspect` in the handler; `0` = no timeout.
    /// crictl runs after the core drain, so a wedged CRI socket must not pin the
    /// handler's `core_pipe_limit` slot forever.
    pub crictl_timeout_secs: u64,
    /// Rate-limit state file, sibling of the handler config on the hostPath.
    pub rate_state_path: String,
    /// Capture-event unix datagram socket the daemon listens on, sibling of
    /// the handler config on the hostPath. `None` when events are disabled.
    pub event_socket_path: Option<String>,
}

/// Hand-written so `store_options` values (object-store credentials) never
/// reach a log line. The destructuring makes a new field a compile error here
/// until it is listed.
impl std::fmt::Debug for HandlerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            cluster,
            no_redact,
            proc_root,
            store_url,
            store_options,
            crictl_path,
            cri_runtime_endpoint,
            max_core_bytes,
            max_cores_per_hour,
            upload_deadline_secs,
            crictl_timeout_secs,
            rate_state_path,
            event_socket_path,
        } = self;
        let store_option_keys: Vec<(&str, &str)> = store_options
            .iter()
            .map(|(key, _)| (key.as_str(), "<redacted>"))
            .collect();
        f.debug_struct("HandlerConfig")
            .field("cluster", cluster)
            .field("no_redact", no_redact)
            .field("proc_root", proc_root)
            .field("store_url", store_url)
            .field("store_options", &store_option_keys)
            .field("crictl_path", crictl_path)
            .field("cri_runtime_endpoint", cri_runtime_endpoint)
            .field("max_core_bytes", max_core_bytes)
            .field("max_cores_per_hour", max_cores_per_hour)
            .field("upload_deadline_secs", upload_deadline_secs)
            .field("crictl_timeout_secs", crictl_timeout_secs)
            .field("rate_state_path", rate_state_path)
            .field("event_socket_path", event_socket_path)
            .finish()
    }
}

impl Default for HandlerConfig {
    fn default() -> Self {
        Self {
            cluster: DEFAULT_CLUSTER.to_string(),
            no_redact: false,
            proc_root: DEFAULT_PROC_ROOT.to_string(),
            store_url: None,
            store_options: Vec::new(),
            crictl_path: DEFAULT_CRICTL_PATH.to_string(),
            cri_runtime_endpoint: None,
            max_core_bytes: DEFAULT_MAX_CORE_BYTES,
            max_cores_per_hour: DEFAULT_MAX_CORES_PER_HOUR,
            upload_deadline_secs: DEFAULT_UPLOAD_DEADLINE_SECS,
            crictl_timeout_secs: DEFAULT_CRICTL_TIMEOUT_SECS,
            rate_state_path: default_rate_state_path(),
            event_socket_path: Some(default_event_socket_path()),
        }
    }
}

impl HandlerConfig {
    /// Validate that paths are absolute and the store URL is a supported scheme.
    /// Called by [`Self::read`] before trusting a daemon-written config file.
    fn validate(&self) -> Result<()> {
        let must_be_absolute = |path: &str, label: &str| -> Result<()> {
            if Path::new(path).is_absolute() {
                Ok(())
            } else {
                bail!("{label} must be absolute: {path}")
            }
        };

        must_be_absolute(&self.crictl_path, "crictl_path")?;
        must_be_absolute(&self.proc_root, "proc_root")?;
        must_be_absolute(&self.rate_state_path, "rate_state_path")?;
        if let Some(event_socket_path) = &self.event_socket_path {
            must_be_absolute(event_socket_path, "event_socket_path")?;
        }
        if let Some(store_url) = &self.store_url {
            let url = url::Url::parse(store_url)
                .with_context(|| format!("store_url is not a valid URL: {store_url}"))?;
            let (scheme, _) = object_store::ObjectStoreScheme::parse(&url)
                .with_context(|| format!("store_url has unrecognized scheme: {store_url}"))?;
            match scheme {
                object_store::ObjectStoreScheme::AmazonS3
                | object_store::ObjectStoreScheme::GoogleCloudStorage
                | object_store::ObjectStoreScheme::MicrosoftAzure => {}
                _ => bail!("store_url scheme must be s3://, gs://, or az://: {store_url}"),
            }
        }
        Ok(())
    }

    /// Read the config from `path`. `None` when absent, unparseable, or invalid -
    /// the handler then uses [`Self::default`], which stores nothing. The
    /// kernel runs the handler with an empty environment, so there is no env
    /// to fall back to.
    pub fn read(path: &str) -> Option<Self> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(error = %e, path, "handler config not readable; using defaults, nothing is stored");
                return None;
            }
        };
        let cfg: Self = match serde_json::from_slice(&bytes) {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::warn!(error = %e, path, "handler config present but unparseable; ignoring");
                return None;
            }
        };
        if let Err(e) = cfg.validate() {
            tracing::warn!(error = %e, path, "handler config failed validation; ignoring");
            return None;
        }
        Some(cfg)
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

        // Write a temp file and rename it over `path`. A handler that starts
        // while the daemon writes must see the old file or the new one, never
        // a half-written one: it would fall back to defaults and discard the
        // core. `create_new` (`O_EXCL`) also means the file is always a new
        // inode, so `mode(0o600)` always applies, and a planted file or
        // symlink at either path is replaced, never written through.
        let tmp = format!("{path}.tmp");
        match std::fs::remove_file(&tmp) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("removing stale {tmp}")),
        }
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)
            .with_context(|| format!("creating {tmp}"))?;
        file.write_all(&json)
            .and_then(|()| file.sync_all())
            .with_context(|| format!("writing {tmp}"))?;
        std::fs::rename(&tmp, path).with_context(|| format!("renaming {tmp} to {path}"))?;
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

    use tempfile::TempDir;

    use super::*;

    /// A config path inside a fresh temp dir. The `TempDir` is returned so the
    /// caller keeps it alive; dropping it removes the tree even on panic.
    fn tmp_config() -> (TempDir, String) {
        let dir = TempDir::new().unwrap();
        let path = dir
            .path()
            .join("handler.json")
            .to_string_lossy()
            .into_owned();
        (dir, path)
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
            crictl_timeout_secs: 15,
            rate_state_path: "/run/coredrop/recent.json".into(),
            event_socket_path: Some("/run/coredrop/events.sock".into()),
        };
        let (_dir, path) = tmp_config();
        cfg.write(&path).unwrap();
        assert_eq!(HandlerConfig::read(&path).unwrap(), cfg);
    }

    #[test]
    fn write_sets_0600_file_and_0700_parent_dir() {
        let (dir, path) = tmp_config();
        HandlerConfig::default().write(&path).unwrap();

        let file_mode = std::fs::metadata(&path).unwrap().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "config file should be mode 0600");

        let dir_mode = std::fs::metadata(dir.path()).unwrap().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "config parent dir should be mode 0700");
    }

    #[test]
    fn write_tightens_an_existing_loose_file() {
        // The file can already exist with a loose mode - see
        // `HandlerConfig::write`.
        let dir = TempDir::new().unwrap();
        let run_dir = dir.path().join("run");
        std::fs::create_dir_all(&run_dir).unwrap();
        std::fs::set_permissions(&run_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = run_dir.join("handler.json");
        std::fs::write(&path, b"{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        HandlerConfig::default()
            .write(&path.to_string_lossy())
            .unwrap();

        let file_mode = std::fs::metadata(&path).unwrap().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "existing config file should become 0600");

        let dir_mode = std::fs::metadata(&run_dir).unwrap().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "existing config dir should become 0700");
    }

    #[test]
    fn write_replaces_a_planted_symlink_and_a_stale_temp_file() {
        let (dir, path) = tmp_config();
        let target = dir.path().join("elsewhere");
        std::fs::write(&target, b"untouched").unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        std::fs::write(format!("{path}.tmp"), b"left by a killed daemon").unwrap();

        let cfg = HandlerConfig::default();
        cfg.write(&path).unwrap();

        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"untouched",
            "symlink target must not be written"
        );
        assert!(!std::fs::symlink_metadata(&path).unwrap().is_symlink());
        assert_eq!(HandlerConfig::read(&path), Some(cfg));
        assert!(
            !Path::new(&format!("{path}.tmp")).exists(),
            "temp file must be renamed away"
        );
    }

    #[test]
    fn debug_hides_store_option_values() {
        let cfg = HandlerConfig {
            store_options: vec![("AWS_SECRET_ACCESS_KEY".into(), "s3cr3t-debug-canary".into())],
            ..HandlerConfig::default()
        };
        let debug = format!("{cfg:?}");
        assert!(debug.contains("AWS_SECRET_ACCESS_KEY"), "{debug}");
        assert!(!debug.contains("s3cr3t-debug-canary"), "{debug}");
    }

    #[test]
    fn read_absent_file_is_none() {
        assert!(HandlerConfig::read("/no/such/coredrop/handler.json").is_none());
    }

    #[test]
    fn read_unparseable_config_returns_none() {
        // Present but not JSON at all: the handler uses the defaults rather
        // than propagating a parse error out of a crash-time code path.
        let (_dir, path) = tmp_config();
        std::fs::write(&path, b"{not json!").unwrap();
        assert!(HandlerConfig::read(&path).is_none());
    }

    #[test]
    fn read_invalid_config_returns_none() {
        // Parses as JSON, but `validate` rejects the relative crictl path.
        let (_dir, path) = tmp_config();
        let cfg = HandlerConfig {
            crictl_path: "relative".into(),
            ..Default::default()
        };
        cfg.write(&path).unwrap();
        assert!(HandlerConfig::read(&path).is_none());
    }

    #[test]
    fn sibling_paths_are_derived_from_the_config_path() {
        assert_eq!(
            event_socket_path_for("/run/coredrop/handler.json"),
            "/run/coredrop/events.sock"
        );
        assert_eq!(
            rate_state_path_for("/run/coredrop/handler.json"),
            "/run/coredrop/recent.json"
        );
        // A parentless path falls back to the defaults rather than producing a
        // relative sibling `validate` would then reject.
        assert_eq!(event_socket_path_for(""), "/run/coredrop/events.sock");
        assert_eq!(rate_state_path_for(""), "/run/coredrop/recent.json");
    }

    #[test]
    fn no_store_url_yields_no_object_store() {
        assert!(HandlerConfig::default().object_store().is_none());
    }

    #[test]
    fn validate_accepts_default_config() {
        assert!(HandlerConfig::default().validate().is_ok());
    }

    #[test]
    fn validate_rejects_relative_paths() {
        let relative = [
            HandlerConfig {
                crictl_path: "crictl".into(),
                ..Default::default()
            },
            HandlerConfig {
                proc_root: "proc".into(),
                ..Default::default()
            },
            HandlerConfig {
                rate_state_path: "recent.json".into(),
                ..Default::default()
            },
            HandlerConfig {
                event_socket_path: Some("events.sock".into()),
                ..Default::default()
            },
        ];
        for cfg in relative {
            assert!(cfg.validate().is_err(), "{cfg:?} should be rejected");
        }
    }

    #[test]
    fn validate_accepts_only_supported_store_url_schemes() {
        for url in [
            "s3://crash-artifacts",
            "gs://crash-artifacts",
            "az://crash-artifacts",
        ] {
            let cfg = HandlerConfig {
                store_url: Some(url.into()),
                ..Default::default()
            };
            assert!(cfg.validate().is_ok(), "{url} should be valid");
        }
        // Not a URL at all, and a URL whose scheme we do not serve
        // from a daemon-written config.
        for url in ["not a url", "memory://bucket", "file:///var/tmp"] {
            let cfg = HandlerConfig {
                store_url: Some(url.into()),
                ..Default::default()
            };
            assert!(cfg.validate().is_err(), "{url} should be rejected");
        }
    }
}
