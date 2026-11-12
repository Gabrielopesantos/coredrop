//! The daemon's flags ([`DaemonArgs`]) and what the daemon builds from them.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{CommandFactory, FromArgMatches, Parser};
use object_store::ObjectStore;
use tracing::{info, warn};

use super::config_file;
use crate::config::HandlerConfig;
use crate::daemon::uploader::UploadSettings;
use crate::filter::CaptureFilter;

/// Default time the daemon keeps uploading the spool after SIGTERM. The
/// chart's termination grace period adds a margin on top.
const DEFAULT_SHUTDOWN_FLUSH_SECS: u64 = 25;

/// `coredrop` daemon config. Every knob is a flag with an env-var fallback,
/// and `--config` names a YAML file that sets the same knobs (see
/// [`DaemonArgs::load`]). Secrets (store creds) stay env-only: never flags
/// (they leak via `ps`/cmdline) and never the file (a `ConfigMap` in the
/// chart). The kernel-exec'd `capture` subcommand reads the daemon-written
/// hostPath config file (see `src/main.rs`), not these flags.
#[derive(Debug, Parser)]
#[command(name = "coredrop", version, about)]
pub(crate) struct DaemonArgs {
    /// YAML config file. Its keys are the long flag names without `--`, for
    /// example `max-core-size: 2Gi`. A flag or its env var wins over the
    /// file.
    #[arg(long, env = "CAPTURE_CONFIG_FILE", value_name = "FILE")]
    pub(crate) config: Option<PathBuf>,

    /// Host path the kernel exec's as the `core_pattern` handler.
    #[arg(long, env = "CAPTURE_HANDLER_PATH", default_value = crate::DEFAULT_HANDLER_PATH)]
    pub(crate) handler_path: PathBuf,

    /// hostPath the daemon serializes the handler config to.
    #[arg(long, env = "CAPTURE_CONFIG_PATH", default_value = crate::config::DEFAULT_CONFIG_PATH)]
    pub(crate) handler_config_path: PathBuf,

    /// Cluster name - the first path segment of the object key.
    #[arg(long, env = "CAPTURE_CLUSTER", default_value = crate::config::DEFAULT_CLUSTER)]
    pub(crate) cluster: String,

    /// Pass `environ` and `cmdline` through un-redacted.
    #[arg(long, env = "CAPTURE_NO_REDACT")]
    pub(crate) no_redact: bool,

    /// `/proc` root (overridable for tests/non-standard layouts).
    #[arg(long, env = "CAPTURE_PROC_ROOT", default_value = crate::config::DEFAULT_PROC_ROOT)]
    pub(crate) proc_root: PathBuf,

    /// Object-store URL the daemon uploads spooled captures to (e.g.
    /// `s3://crash-artifacts`); unset disables upload. Store creds come from
    /// env (`AWS_*` etc.), not flags.
    #[arg(long, env = "CAPTURE_STORE_URL")]
    pub(crate) store_url: Option<String>,

    /// Path to the `crictl` binary for post-drain container enrichment.
    #[arg(long, env = "CRICTL_PATH", default_value = crate::config::DEFAULT_CRICTL_PATH)]
    pub(crate) crictl_path: PathBuf,

    /// CRI runtime endpoint (e.g. `unix:///run/containerd/containerd.sock`).
    #[arg(long, env = "CONTAINER_RUNTIME_ENDPOINT")]
    pub(crate) cri_runtime_endpoint: Option<String>,

    /// Max uncompressed core size stored per crash, as a quantity (`2Gi`,
    /// `500M`, bytes without a suffix); 0 = unlimited. The remainder of the
    /// stream is drained but not stored.
    #[arg(long = "max-core-size", env = "CAPTURE_MAX_CORE_SIZE", default_value = "2Gi", value_name = "SIZE", value_parser = crate::config::parse_size)]
    pub(crate) max_core_bytes: u64,

    /// Max core uploads per pod per hour; 0 = unlimited. Suppressed
    /// crashes still get a proc snapshot and manifest, just no core.
    #[arg(long, env = "CAPTURE_MAX_CORES_PER_HOUR", default_value_t = crate::config::DEFAULT_MAX_CORES_PER_HOUR)]
    pub(crate) max_cores_per_hour: u32,

    /// Wall-clock deadline (seconds) for draining one core into the spool;
    /// 0 = no deadline. On expiry the handler drops the core and exits,
    /// freeing its `core_pipe_limit` slot instead of letting a hung disk hold it.
    #[arg(long, env = "CAPTURE_DRAIN_DEADLINE_SECS", default_value_t = crate::config::DEFAULT_DRAIN_DEADLINE_SECS)]
    pub(crate) drain_deadline_secs: u64,

    /// Spool root on the node: the handler writes capture bundles here and
    /// the daemon uploads them.
    #[arg(long, env = "CAPTURE_SPOOL_DIR", default_value = crate::spool::DEFAULT_SPOOL_DIR)]
    pub(crate) spool_dir: PathBuf,

    /// Cap on the spool's total size, as a quantity (`10Gi`, `500M`, bytes
    /// without a suffix); 0 = no cap. A core that does not fit is dropped;
    /// its snapshot and manifest are still spooled.
    #[arg(long = "spool-max-size", env = "CAPTURE_SPOOL_MAX_SIZE", default_value = "10Gi", value_name = "SIZE", value_parser = crate::config::parse_size)]
    pub(crate) spool_max_bytes: u64,

    /// Free-space floor of the spool's filesystem, in percent. The handler
    /// stops writing cores below it.
    #[arg(long, env = "CAPTURE_SPOOL_MIN_FREE_PERCENT", default_value_t = crate::spool::DEFAULT_SPOOL_MIN_FREE_PERCENT, value_parser = clap::value_parser!(u8).range(0..=100))]
    pub(crate) spool_min_free_percent: u8,

    /// Age in seconds past which a spooled capture that still fails to
    /// upload is deleted.
    #[arg(long, env = "CAPTURE_SPOOL_MAX_AGE_SECS", default_value_t = crate::daemon::uploader::DEFAULT_SPOOL_MAX_AGE_SECS)]
    pub(crate) spool_max_age_secs: u64,

    /// Spooled captures the daemon uploads at the same time.
    #[arg(long, env = "CAPTURE_UPLOAD_CONCURRENCY", default_value_t = crate::daemon::uploader::DEFAULT_UPLOAD_CONCURRENCY, value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..=64))]
    pub(crate) upload_concurrency: usize,

    /// Time limit in seconds for one upload attempt of one spooled capture;
    /// 0 = no limit. A timed-out capture is retried later.
    #[arg(long, env = "CAPTURE_UPLOAD_TIMEOUT_SECS", default_value_t = crate::daemon::uploader::DEFAULT_UPLOAD_TIMEOUT_SECS)]
    pub(crate) upload_timeout_secs: u64,

    /// On SIGTERM, how long in seconds the daemon keeps uploading the spool
    /// before it exits. Keep it below the pod's termination grace period.
    #[arg(long, env = "CAPTURE_SHUTDOWN_FLUSH_SECS", default_value_t = DEFAULT_SHUTDOWN_FLUSH_SECS)]
    pub(crate) shutdown_flush_secs: u64,

    /// Timeout in seconds for `crictl inspect` in the handler; 0 = no timeout.
    #[arg(long, env = "CRICTL_TIMEOUT_SECS", default_value_t = crate::config::DEFAULT_CRICTL_TIMEOUT_SECS)]
    pub(crate) crictl_timeout_secs: u64,

    /// Which crashes the handler keeps, as JSON:
    /// `{"include":[{...}],"exclude":[{...}]}`; in the config file, a YAML
    /// mapping. Each entry holds glob patterns for `namespace`, `pod`,
    /// `container`, `image` and `exe`; all of an entry's patterns must match.
    /// Empty keeps every crash.
    #[arg(long, env = "CAPTURE_FILTER", default_value = "", value_name = "JSON", value_parser = CaptureFilter::parse)]
    pub(crate) capture_filter: CaptureFilter,

    /// Disable k8s Event emission on capture (`kubectl describe pod`/
    /// `kubectl get events` surfacing). Events are on by default. Captures are
    /// still logged by the daemon either way.
    #[arg(long, env = "CAPTURE_NO_EVENTS")]
    pub(crate) no_events: bool,

    /// Max crashes concurrently held open for the handler via
    /// `core_pipe_limit` (node-global sysctl); beyond it the kernel skips the
    /// dump entirely rather than exec'ing the handler.
    #[arg(long, env = "CAPTURE_PIPE_LIMIT", default_value_t = crate::core_pattern::DEFAULT_PIPE_LIMIT)]
    pub(crate) pipe_limit: u32,

    /// Node name reported on k8s Events (the chart sets it from
    /// `spec.nodeName`). Unset falls back to the hostname, which in a pod
    /// without `hostNetwork` is the pod name, not the node.
    #[arg(long, env = "NODE_NAME")]
    pub(crate) node_name: Option<String>,

    /// File the `core_pattern` sysctl is written through. The chart points it
    /// at a writable bind mount of the node's sysctl file, because the
    /// container's own `/proc/sys` is read-only.
    #[arg(long, env = "CAPTURE_CORE_PATTERN_PATH", default_value = crate::core_pattern::CORE_PATTERN_PATH)]
    pub(crate) core_pattern_path: PathBuf,

    /// File the `core_pipe_limit` sysctl is written through. Same bind mount
    /// approach as `core_pattern_path`.
    #[arg(long, env = "CAPTURE_CORE_PIPE_LIMIT_PATH", default_value = crate::core_pattern::CORE_PIPE_LIMIT_PATH)]
    pub(crate) core_pipe_limit_path: PathBuf,

    /// Address of the Prometheus `/metrics` endpoint (`host:port`); empty
    /// turns the endpoint off.
    #[arg(long, env = "CAPTURE_METRICS_ADDR", default_value = crate::daemon::metrics::DEFAULT_METRICS_ADDR, value_parser = parse_metrics_addr)]
    pub(crate) metrics_addr: MetricsAddr,
}

/// `--metrics-addr`: the endpoint's address; `None` = no endpoint. A newtype,
/// because clap reads an `Option` field as "flag may be absent".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetricsAddr(pub(crate) Option<SocketAddr>);

fn parse_metrics_addr(s: &str) -> Result<MetricsAddr, String> {
    if s.is_empty() {
        return Ok(MetricsAddr(None));
    }
    s.parse()
        .map(|addr| MetricsAddr(Some(addr)))
        .map_err(|e| format!("{s:?} is not a host:port socket address: {e}"))
}

impl DaemonArgs {
    /// Parse `argv` (program name first) and the env. With `--config`, the
    /// file's values replace the built-in defaults, so the order is: flag,
    /// env var, file, built-in default.
    ///
    /// # Errors
    ///
    /// A clap error for a bad flag, an unreadable or invalid file, an
    /// unknown key, or a value its flag does not accept. `--help` and
    /// `--version` also come back as a clap error, which prints them.
    pub(crate) fn load<I, T>(argv: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString>,
    {
        let argv: Vec<std::ffi::OsString> = argv.into_iter().map(Into::into).collect();
        let first = Self::try_parse_from(&argv)?;
        let Some(path) = first.config else {
            return Ok(first);
        };
        let cmd = config_file::apply(Self::command(), &path)?;
        let matches = cmd.try_get_matches_from(&argv)?;
        Self::from_arg_matches(&matches)
    }

    /// The object store the daemon uploads to, built from `--store-url` and
    /// the store env vars. Only the daemon holds store credentials; the
    /// handler learns only whether there is a store to spool for. `None`
    /// when no URL is set (observe-only).
    ///
    /// # Errors
    ///
    /// Fails when a URL is set but the store cannot be built from it: a
    /// daemon that silently discarded every core would look healthy.
    pub(crate) fn object_store(&self) -> Result<Option<Arc<dyn ObjectStore>>> {
        let Some(url) = self.store_url.as_deref().filter(|url| !url.is_empty()) else {
            info!("no object store configured; the handler discards cores");
            return Ok(None);
        };
        let store_options = crate::daemon::upload::store_options_from_env();
        if crate::daemon::upload::is_plaintext_endpoint(&store_options) {
            warn!(
                "object-store endpoint is plaintext HTTP; cores traverse the network unencrypted"
            );
        }
        crate::daemon::upload::object_store_from_url_opts(url, store_options)
            .map(Some)
            .with_context(|| {
                format!("--store-url {url} is set, but the object store cannot be built")
            })
    }

    pub(crate) fn upload_settings(&self) -> UploadSettings {
        UploadSettings {
            concurrency: self.upload_concurrency,
            attempt_timeout: (self.upload_timeout_secs != 0)
                .then(|| Duration::from_secs(self.upload_timeout_secs)),
            max_age: Duration::from_secs(self.spool_max_age_secs),
            ..UploadSettings::default()
        }
    }

    pub(crate) fn to_handler_config(&self, store_configured: bool) -> HandlerConfig {
        HandlerConfig {
            cluster: self.cluster.clone(),
            no_redact: self.no_redact,
            proc_root: self.proc_root.clone(),
            store_configured,
            crictl_path: self.crictl_path.clone(),
            cri_runtime_endpoint: self.cri_runtime_endpoint.clone(),
            max_core_bytes: self.max_core_bytes,
            max_cores_per_hour: self.max_cores_per_hour,
            drain_deadline_secs: self.drain_deadline_secs,
            crictl_timeout_secs: self.crictl_timeout_secs,
            spool_dir: self.spool_dir.clone(),
            spool_max_bytes: self.spool_max_bytes,
            spool_min_free_percent: self.spool_min_free_percent,
            rate_state_path: crate::config::rate_state_path_for(&self.handler_config_path),
            event_socket_path: Some(crate::config::event_socket_path_for(
                &self.handler_config_path,
            )),
            filter: self.capture_filter.clone(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn metrics_addr_is_a_socket_address_or_empty() {
        assert!(parse_metrics_addr("9464").is_err());
        let parse = |argv: &[&str]| DaemonArgs::try_parse_from(argv).unwrap().metrics_addr;
        assert_eq!(
            parse(&["coredrop"]),
            MetricsAddr(Some(
                crate::daemon::metrics::DEFAULT_METRICS_ADDR
                    .parse()
                    .unwrap()
            ))
        );
        assert_eq!(
            parse(&["coredrop", "--metrics-addr", "127.0.0.1:9000"]),
            MetricsAddr(Some("127.0.0.1:9000".parse().unwrap()))
        );
        assert_eq!(parse(&["coredrop", "--metrics-addr="]), MetricsAddr(None));
    }

    /// Writes `yaml` to a temp `config.yaml` and loads it with `flags`.
    fn load_with_file(yaml: &str, flags: &[&str]) -> Result<DaemonArgs, clap::Error> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, yaml).unwrap();
        let mut argv = vec![
            "coredrop".to_string(),
            "--config".into(),
            path.display().to_string(),
        ];
        argv.extend(flags.iter().map(ToString::to_string));
        DaemonArgs::load(argv)
    }

    #[test]
    fn config_file_sets_the_flags() {
        let args = load_with_file(
            "cluster: prod\n\
             max-core-size: 1Gi\n\
             max-cores-per-hour: 7\n\
             no-redact: true\n\
             store-url: s3://bucket\n\
             metrics-addr: \"\"\n\
             capture-filter:\n  exclude:\n    - namespace: kube-system\n",
            &[],
        )
        .unwrap();
        assert_eq!(args.cluster, "prod");
        assert_eq!(args.max_core_bytes, 1 << 30);
        assert_eq!(args.max_cores_per_hour, 7);
        assert!(args.no_redact);
        assert_eq!(args.store_url.as_deref(), Some("s3://bucket"));
        assert_eq!(args.metrics_addr, MetricsAddr(None));
        assert_eq!(
            args.capture_filter,
            CaptureFilter::parse(r#"{"exclude":[{"namespace":"kube-system"}]}"#).unwrap()
        );
        // Keys the file leaves out keep their built-in default.
        assert_eq!(
            args.spool_dir,
            PathBuf::from(crate::spool::DEFAULT_SPOOL_DIR)
        );
    }

    #[test]
    fn a_flag_wins_over_the_config_file() {
        let args = load_with_file(
            "cluster: prod\nmax-cores-per-hour: 7\n",
            &["--cluster", "dev"],
        )
        .unwrap();
        assert_eq!(args.cluster, "dev");
        assert_eq!(args.max_cores_per_hour, 7);
    }

    #[test]
    fn an_empty_config_file_or_a_null_value_keeps_the_defaults() {
        for yaml in ["", "# nothing\n", "cluster: null\n"] {
            let args = load_with_file(yaml, &[]).unwrap();
            assert_eq!(args.cluster, crate::config::DEFAULT_CLUSTER, "{yaml:?}");
        }
    }

    #[test]
    fn config_file_errors_name_the_problem() {
        for (yaml, want) in [
            ("clusters: prod\n", "unknown key `clusters`"),
            ("config: /other.yaml\n", "unknown key `config`"),
            ("help: true\n", "unknown key `help`"),
            ("max-cores-per-hour: many\n", "`max-cores-per-hour`"),
            ("max-core-size: 2Zi\n", "`max-core-size`"),
            ("spool-min-free-percent: 101\n", "`spool-min-free-percent`"),
            ("no-redact: yes please\n", "`no-redact`"),
            ("capture-filter:\n  exclude: [{}]\n", "`capture-filter`"),
            ("- cluster\n", "top level must be a mapping"),
            ("cluster: [\n", "config file"),
        ] {
            let err = load_with_file(yaml, &[]).unwrap_err().to_string();
            assert!(err.contains(want), "{yaml:?}: {err}");
            assert!(err.contains("config.yaml"), "{yaml:?}: {err}");
        }
        let missing = DaemonArgs::load(["coredrop", "--config", "/nonexistent/config.yaml"]);
        assert!(
            missing
                .unwrap_err()
                .to_string()
                .contains("/nonexistent/config.yaml")
        );
    }

    #[test]
    fn capture_filter_flag_reaches_the_handler_config() {
        let parse = |argv: &[&str]| DaemonArgs::try_parse_from(argv).map(|a| a.capture_filter);
        assert_eq!(parse(&["coredrop"]).unwrap(), CaptureFilter::default());
        let filter = parse(&[
            "coredrop",
            "--capture-filter",
            r#"{"exclude":[{"namespace":"kube-system"}],"include":[]}"#,
        ])
        .unwrap();
        assert_eq!(filter.exclude.len(), 1);
        assert!(parse(&["coredrop", "--capture-filter", r#"{"exclude":[{}]}"#]).is_err());
        let args = DaemonArgs::try_parse_from([
            "coredrop",
            "--capture-filter",
            r#"{"exclude":[{"exe":"*/crasher"}]}"#,
        ])
        .unwrap();
        assert_eq!(args.to_handler_config(true).filter, args.capture_filter);
    }
}
