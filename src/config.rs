//! Daemon-written handler config.
//!
//! The kernel exec's the capture handler with a clean environment, so the
//! daemon's env vars never reach it. To bridge that, the daemon serializes its
//! capture config to a hostPath file at startup ([`HandlerConfig::write`]);
//! the handler reads it ([`HandlerConfig::read`]) from the same host path.

use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::filter::CaptureFilter;
use crate::spool;

/// Host path the daemon writes the serialized [`HandlerConfig`] to, and the
/// handler reads it back from.
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

/// Parse a size in Kubernetes quantity form into bytes: a whole number with
/// an optional binary suffix (`Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei`) or decimal
/// suffix (`k`, `M`, `G`, `T`, `P`, `E`). No suffix means bytes.
///
/// # Errors
///
/// Fails on an empty number, an unknown suffix, or a size above `u64::MAX`.
pub fn parse_size(s: &str) -> Result<u64, String> {
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (digits, suffix) = s.split_at(split);
    if digits.is_empty() {
        return Err(format!(
            "{s:?}: expected a whole number with an optional suffix, e.g. 10Gi or 500M"
        ));
    }
    let multiplier: u64 = match suffix {
        "" => 1,
        "Ki" => 1 << 10,
        "Mi" => 1 << 20,
        "Gi" => 1 << 30,
        "Ti" => 1 << 40,
        "Pi" => 1 << 50,
        "Ei" => 1 << 60,
        "k" => 1_000,
        "M" => 1_000_000,
        "G" => 1_000_000_000,
        "T" => 1_000_000_000_000,
        "P" => 1_000_000_000_000_000,
        "E" => 1_000_000_000_000_000_000,
        _ => {
            return Err(format!(
                "{s:?}: unknown suffix {suffix:?}; use Ki, Mi, Gi, Ti, Pi, Ei, k, M, G, T, P or E"
            ));
        }
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .ok_or_else(|| format!("{s:?}: larger than {} bytes", u64::MAX))
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

/// Default deadline for draining one core into the spool: 1 minute.
/// The handler holds one of the node's `core_pipe_limit` slots while it
/// drains, so a hung disk must not be able to hold it indefinitely.
pub const DEFAULT_DRAIN_DEADLINE_SECS: u64 = 60;

/// Default timeout for `crictl inspect` in the handler.
pub const DEFAULT_CRICTL_TIMEOUT_SECS: u64 = 10;

fn default_rate_state_path() -> PathBuf {
    PathBuf::from("/run/coredrop/recent.json")
}

/// Rate-limit state file derived from the handler-config path: a `recent.json`
/// sibling on the same hostPath, so no extra flag or mount is needed.
#[must_use]
pub fn rate_state_path_for(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .map_or_else(default_rate_state_path, |p| p.join("recent.json"))
}

fn default_event_socket_path() -> PathBuf {
    PathBuf::from("/run/coredrop/events.sock")
}

/// Capture-event unix datagram socket, derived from the handler-config path
/// the same way [`rate_state_path_for`] derives the rate-limit state file: an
/// `events.sock` sibling on the same hostPath.
#[must_use]
pub fn event_socket_path_for(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .map_or_else(default_event_socket_path, |p| p.join("events.sock"))
}

/// Everything the kernel-exec'd handler needs that env can't deliver. The
/// daemon fills it from its own env and writes it; the handler reads it. It
/// holds no credentials: only the daemon talks to the object store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerConfig {
    /// Cluster name - the first path segment of the object key.
    pub cluster: String,
    /// Pass `environ` and `cmdline` through un-redacted.
    pub no_redact: bool,
    /// `/proc` root (overridable for tests/non-standard layouts).
    pub proc_root: PathBuf,
    /// Whether the daemon has an object store to upload to. `false` makes the
    /// handler discard cores instead of spooling them.
    pub store_configured: bool,
    /// Path to the `crictl` binary for post-drain container enrichment.
    pub crictl_path: PathBuf,
    /// CRI runtime endpoint (e.g. `unix:///run/containerd/containerd.sock`).
    /// `None` lets crictl use its own default/`CONTAINER_RUNTIME_ENDPOINT`.
    pub cri_runtime_endpoint: Option<String>,
    /// Max uncompressed core bytes stored per crash; `0` = unlimited. The
    /// remainder is drained but not stored.
    pub max_core_bytes: u64,
    /// Max core uploads per pod per hour; `0` = unlimited. Suppressed
    /// crashes still get a proc snapshot and manifest, just no core.
    pub max_cores_per_hour: u32,
    /// Deadline in seconds for draining the core into the spool; `0` = no
    /// deadline. On expiry the handler drops the core and exits, freeing its
    /// `core_pipe_limit` slot instead of letting a hung disk hold it.
    pub drain_deadline_secs: u64,
    /// Timeout in seconds for `crictl inspect` in the handler; `0` = no timeout.
    /// crictl runs after the handler releases the core pipe, so this only
    /// bounds how long the capture takes to reach the spool. A capture filter
    /// on namespace, pod, container or image runs crictl before the drain
    /// instead: then this also bounds how long the kernel waits.
    pub crictl_timeout_secs: u64,
    /// Spool root on the node: where the handler writes capture bundles for
    /// the daemon to upload.
    pub spool_dir: PathBuf,
    /// Cap on the spool's total size in bytes; `0` = no cap. A core that does
    /// not fit is dropped; its snapshot and manifest are still spooled.
    pub spool_max_bytes: u64,
    /// Free-space floor of the spool's filesystem, in percent.
    pub spool_min_free_percent: u8,
    /// Rate-limit state file, sibling of the handler config on the hostPath.
    pub rate_state_path: PathBuf,
    /// Capture-event unix datagram socket the daemon listens on, sibling of
    /// the handler config on the hostPath. The daemon always sets it; `None`
    /// (tests, a hand-written config) means the handler reports nothing.
    pub event_socket_path: Option<PathBuf>,
    /// Which crashes the handler keeps. A config without the field keeps
    /// every crash.
    #[serde(default)]
    pub filter: CaptureFilter,
}

impl Default for HandlerConfig {
    fn default() -> Self {
        Self {
            cluster: DEFAULT_CLUSTER.to_string(),
            no_redact: false,
            proc_root: PathBuf::from(DEFAULT_PROC_ROOT),
            store_configured: false,
            crictl_path: PathBuf::from(DEFAULT_CRICTL_PATH),
            cri_runtime_endpoint: None,
            max_core_bytes: DEFAULT_MAX_CORE_BYTES,
            max_cores_per_hour: DEFAULT_MAX_CORES_PER_HOUR,
            drain_deadline_secs: DEFAULT_DRAIN_DEADLINE_SECS,
            crictl_timeout_secs: DEFAULT_CRICTL_TIMEOUT_SECS,
            spool_dir: PathBuf::from(spool::DEFAULT_SPOOL_DIR),
            spool_max_bytes: spool::DEFAULT_SPOOL_MAX_BYTES,
            spool_min_free_percent: spool::DEFAULT_SPOOL_MIN_FREE_PERCENT,
            rate_state_path: default_rate_state_path(),
            event_socket_path: Some(default_event_socket_path()),
            filter: CaptureFilter::default(),
        }
    }
}

impl HandlerConfig {
    /// Validate that paths are absolute and the free-space floor is a percent.
    /// Called by [`Self::read`] before trusting a daemon-written config file.
    fn validate(&self) -> Result<()> {
        let must_be_absolute = |path: &Path, label: &str| -> Result<()> {
            if path.is_absolute() {
                Ok(())
            } else {
                bail!("{label} must be absolute: {}", path.display())
            }
        };

        must_be_absolute(&self.crictl_path, "crictl_path")?;
        must_be_absolute(&self.proc_root, "proc_root")?;
        must_be_absolute(&self.rate_state_path, "rate_state_path")?;
        if let Some(event_socket_path) = &self.event_socket_path {
            must_be_absolute(event_socket_path, "event_socket_path")?;
        }
        must_be_absolute(&self.spool_dir, "spool_dir")?;
        self.filter.validate()?;
        if self.spool_min_free_percent > 100 {
            bail!(
                "spool_min_free_percent must be at most 100: {}",
                self.spool_min_free_percent
            );
        }
        Ok(())
    }

    /// Read the config from `path`. `None` when absent, unparseable, or invalid -
    /// the handler then uses [`Self::default`], which stores nothing. The
    /// kernel runs the handler with an empty environment, so there is no env
    /// to fall back to.
    pub fn read(path: impl AsRef<Path>) -> Option<Self> {
        let path = path.as_ref();
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(), "handler config not readable; using defaults, nothing is stored");
                return None;
            }
        };
        let cfg: Self = match serde_json::from_slice(&bytes) {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(), "handler config present but unparseable; ignoring");
                return None;
            }
        };
        if let Err(e) = cfg.validate() {
            tracing::warn!(error = %e, path = %path.display(), "handler config failed validation; ignoring");
            return None;
        }
        Some(cfg)
    }

    /// Serialize the config to `path` (creating the parent dir), so the
    /// kernel-exec'd handler can read it.
    ///
    /// The parent dir is created mode `0700` and the file itself mode `0600`:
    /// the config holds no secrets, but it steers a root process that runs
    /// on every crash, so only root may read or replace it.
    ///
    /// # Errors
    ///
    /// Fails when the parent dir cannot be created/chmod'd, or the file
    /// cannot be created/written/chmod'd.
    pub fn write(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
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
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        match std::fs::remove_file(&tmp) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("removing stale {}", tmp.display())),
        }
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)
            .with_context(|| format!("creating {}", tmp.display()))?;
        file.write_all(&json)
            .and_then(|()| file.sync_all())
            .with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("renaming {} to {}", tmp.display(), path.display()))?;
        Ok(())
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
    fn tmp_config() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("handler.json");
        (dir, path)
    }

    #[test]
    fn write_then_read_round_trips() {
        let cfg = HandlerConfig {
            cluster: "prod".into(),
            no_redact: true,
            proc_root: "/proc".into(),
            store_configured: true,
            crictl_path: "/usr/local/bin/crictl".into(),
            cri_runtime_endpoint: Some("unix:///run/containerd/containerd.sock".into()),
            max_core_bytes: 1024,
            max_cores_per_hour: 5,
            drain_deadline_secs: 60,
            crictl_timeout_secs: 15,
            spool_dir: "/var/lib/coredrop/spool".into(),
            spool_max_bytes: 1 << 30,
            spool_min_free_percent: 20,
            rate_state_path: "/run/coredrop/recent.json".into(),
            event_socket_path: Some("/run/coredrop/events.sock".into()),
            filter: CaptureFilter::parse(r#"{"exclude":[{"namespace":"kube-system"}]}"#).unwrap(),
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

        HandlerConfig::default().write(&path).unwrap();

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
        let tmp = dir.path().join("handler.json.tmp");
        std::fs::write(&tmp, b"left by a killed daemon").unwrap();

        let cfg = HandlerConfig::default();
        cfg.write(&path).unwrap();

        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"untouched",
            "symlink target must not be written"
        );
        assert!(!std::fs::symlink_metadata(&path).unwrap().is_symlink());
        assert_eq!(HandlerConfig::read(&path), Some(cfg));
        assert!(!tmp.exists(), "temp file must be renamed away");
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
            event_socket_path_for(Path::new("/run/coredrop/handler.json")),
            Path::new("/run/coredrop/events.sock")
        );
        assert_eq!(
            rate_state_path_for(Path::new("/run/coredrop/handler.json")),
            Path::new("/run/coredrop/recent.json")
        );
        // A parentless path falls back to the defaults rather than producing a
        // relative sibling `validate` would then reject.
        assert_eq!(
            event_socket_path_for(Path::new("")),
            Path::new("/run/coredrop/events.sock")
        );
        assert_eq!(
            rate_state_path_for(Path::new("")),
            Path::new("/run/coredrop/recent.json")
        );
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
            HandlerConfig {
                spool_dir: "spool".into(),
                ..Default::default()
            },
        ];
        for cfg in relative {
            assert!(cfg.validate().is_err(), "{cfg:?} should be rejected");
        }
    }

    #[test]
    fn validate_rejects_a_free_space_floor_over_100_percent() {
        let cfg = HandlerConfig {
            spool_min_free_percent: 101,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn parse_size_reads_quantities() {
        assert_eq!(parse_size("0"), Ok(0));
        assert_eq!(parse_size("1536"), Ok(1536));
        assert_eq!(parse_size("10Gi"), Ok(10 << 30));
        assert_eq!(parse_size("2Gi"), Ok(DEFAULT_MAX_CORE_BYTES));
        assert_eq!(parse_size("10Gi"), Ok(spool::DEFAULT_SPOOL_MAX_BYTES));
        assert_eq!(parse_size("100M"), Ok(100_000_000));
        assert_eq!(parse_size("4k"), Ok(4_000));
        assert_eq!(parse_size("15Ei"), Ok(15 << 60));
    }

    #[test]
    fn parse_size_rejects_bad_input() {
        for bad in [
            "",
            "Gi",
            "-1",
            "1.5Gi",
            "10GB",
            "10 Gi",
            "10gi",
            "10K",
            "16Ei",
            "99999999999999999999",
        ] {
            assert!(parse_size(bad).is_err(), "{bad:?} parsed");
        }
    }
}
