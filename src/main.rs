//! `coredrop` - standalone Kubernetes coredump handler.
//!
//! The binary runs in three modes:
//!   - capture handler (`coredrop capture %P %s %t %E`): the short-lived
//!     process the kernel exec's per fault. Snapshots `/proc`, drains the core
//!     into the node-local spool, enriches identity via crictl, writes a JSON
//!     manifest sidecar.
//!   - daemon (`coredrop`): the long-running `DaemonSet` container. Installs
//!     `core_pattern` so faults route to the handler, writes the handler
//!     config to a hostPath the kernel-exec'd handler can read (the kernel
//!     exec's with a clean environment), uploads the spool, and holds the
//!     restore guard until shutdown.
//!   - health check (`coredrop check`): what the chart's readiness and
//!     liveness probes exec. Exits non-zero when the capture path has drifted.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{CommandFactory, FromArgMatches, Parser};
use object_store::ObjectStore;
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::{error, info, warn};

use coredrop::config::HandlerConfig;
use coredrop::core_pattern::{CorePatternCheck, CorePatternGuard};
use coredrop::daemon::metrics::Metrics;
use coredrop::daemon::uploader::{UploadSettings, Uploader};
use coredrop::filter::CaptureFilter;
use coredrop::handler::{CaptureArgs, KernelPipe, run as run_handler};
use coredrop::spool::Spool;

/// Default time the daemon keeps uploading the spool after SIGTERM. The
/// chart's termination grace period adds a margin on top.
const DEFAULT_SHUTDOWN_FLUSH_SECS: u64 = 25;

/// On shutdown, how long the daemon waits for the event listener to report
/// the uploads that finished during the flush. It fits in the 5 s the pod's
/// termination grace period leaves after the flush.
const LISTENER_DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// `coredrop` daemon config. Every knob is a flag with an env-var fallback,
/// and `--config` names a YAML file that sets the same knobs (see
/// [`DaemonArgs::load`]). Secrets
/// (store creds) stay env-only - never flags (leak via `ps`/cmdline) and
/// never the file (a `ConfigMap` in the chart). The kernel-exec'd `capture` subcommand reads
/// the daemon-written hostPath config file (see `main`), not these flags.
#[derive(Debug, Parser)]
#[command(name = "coredrop", version, about)]
struct DaemonArgs {
    /// YAML config file. Its keys are the long flag names without `--`, for
    /// example `max-core-size: 2Gi`. A flag or its env var wins over the
    /// file.
    #[arg(long, env = "CAPTURE_CONFIG_FILE", value_name = "FILE")]
    config: Option<PathBuf>,

    /// Host path the kernel exec's as the `core_pattern` handler.
    #[arg(long, env = "CAPTURE_HANDLER_PATH", default_value = coredrop::DEFAULT_HANDLER_PATH)]
    handler_path: PathBuf,

    /// hostPath the daemon serializes the handler config to.
    #[arg(long, env = "CAPTURE_CONFIG_PATH", default_value = coredrop::config::DEFAULT_CONFIG_PATH)]
    handler_config_path: PathBuf,

    /// Cluster name - the first path segment of the object key.
    #[arg(long, env = "CAPTURE_CLUSTER", default_value = coredrop::config::DEFAULT_CLUSTER)]
    cluster: String,

    /// Pass `environ` and `cmdline` through un-redacted.
    #[arg(long, env = "CAPTURE_NO_REDACT")]
    no_redact: bool,

    /// `/proc` root (overridable for tests/non-standard layouts).
    #[arg(long, env = "CAPTURE_PROC_ROOT", default_value = coredrop::config::DEFAULT_PROC_ROOT)]
    proc_root: PathBuf,

    /// Object-store URL the daemon uploads spooled captures to (e.g.
    /// `s3://crash-artifacts`); unset disables upload. Store creds come from
    /// env (`AWS_*` etc.), not flags.
    #[arg(long, env = "CAPTURE_STORE_URL")]
    store_url: Option<String>,

    /// Path to the `crictl` binary for post-drain container enrichment.
    #[arg(long, env = "CRICTL_PATH", default_value = coredrop::config::DEFAULT_CRICTL_PATH)]
    crictl_path: PathBuf,

    /// CRI runtime endpoint (e.g. `unix:///run/containerd/containerd.sock`).
    #[arg(long, env = "CONTAINER_RUNTIME_ENDPOINT")]
    cri_runtime_endpoint: Option<String>,

    /// Max uncompressed core size stored per crash, as a quantity (`2Gi`,
    /// `500M`, bytes without a suffix); 0 = unlimited. The remainder of the
    /// stream is drained but not stored.
    #[arg(long = "max-core-size", env = "CAPTURE_MAX_CORE_SIZE", default_value = "2Gi", value_name = "SIZE", value_parser = coredrop::config::parse_size)]
    max_core_bytes: u64,

    /// Max core uploads per pod per hour; 0 = unlimited. Suppressed
    /// crashes still get a proc snapshot and manifest, just no core.
    #[arg(long, env = "CAPTURE_MAX_CORES_PER_HOUR", default_value_t = coredrop::config::DEFAULT_MAX_CORES_PER_HOUR)]
    max_cores_per_hour: u32,

    /// Wall-clock deadline (seconds) for draining one core into the spool;
    /// 0 = no deadline. On expiry the handler drops the core and exits,
    /// freeing its `core_pipe_limit` slot instead of letting a hung disk hold it.
    #[arg(long, env = "CAPTURE_DRAIN_DEADLINE_SECS", default_value_t = coredrop::config::DEFAULT_DRAIN_DEADLINE_SECS)]
    drain_deadline_secs: u64,

    /// Spool root on the node: the handler writes capture bundles here and
    /// the daemon uploads them.
    #[arg(long, env = "CAPTURE_SPOOL_DIR", default_value = coredrop::spool::DEFAULT_SPOOL_DIR)]
    spool_dir: PathBuf,

    /// Cap on the spool's total size, as a quantity (`10Gi`, `500M`, bytes
    /// without a suffix); 0 = no cap. A core that does not fit is dropped;
    /// its snapshot and manifest are still spooled.
    #[arg(long = "spool-max-size", env = "CAPTURE_SPOOL_MAX_SIZE", default_value = "10Gi", value_name = "SIZE", value_parser = coredrop::config::parse_size)]
    spool_max_bytes: u64,

    /// Free-space floor of the spool's filesystem, in percent. The handler
    /// stops writing cores below it.
    #[arg(long, env = "CAPTURE_SPOOL_MIN_FREE_PERCENT", default_value_t = coredrop::spool::DEFAULT_SPOOL_MIN_FREE_PERCENT, value_parser = clap::value_parser!(u8).range(0..=100))]
    spool_min_free_percent: u8,

    /// Age in seconds past which a spooled capture that still fails to
    /// upload is deleted.
    #[arg(long, env = "CAPTURE_SPOOL_MAX_AGE_SECS", default_value_t = coredrop::daemon::uploader::DEFAULT_SPOOL_MAX_AGE_SECS)]
    spool_max_age_secs: u64,

    /// Spooled captures the daemon uploads at the same time.
    #[arg(long, env = "CAPTURE_UPLOAD_CONCURRENCY", default_value_t = coredrop::daemon::uploader::DEFAULT_UPLOAD_CONCURRENCY, value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..=64))]
    upload_concurrency: usize,

    /// Time limit in seconds for one upload attempt of one spooled capture;
    /// 0 = no limit. A timed-out capture is retried later.
    #[arg(long, env = "CAPTURE_UPLOAD_TIMEOUT_SECS", default_value_t = coredrop::daemon::uploader::DEFAULT_UPLOAD_TIMEOUT_SECS)]
    upload_timeout_secs: u64,

    /// On SIGTERM, how long in seconds the daemon keeps uploading the spool
    /// before it exits. Keep it below the pod's termination grace period.
    #[arg(long, env = "CAPTURE_SHUTDOWN_FLUSH_SECS", default_value_t = DEFAULT_SHUTDOWN_FLUSH_SECS)]
    shutdown_flush_secs: u64,

    /// Timeout in seconds for `crictl inspect` in the handler; 0 = no timeout.
    #[arg(long, env = "CRICTL_TIMEOUT_SECS", default_value_t = coredrop::config::DEFAULT_CRICTL_TIMEOUT_SECS)]
    crictl_timeout_secs: u64,

    /// Which crashes the handler keeps, as JSON:
    /// `{"include":[{...}],"exclude":[{...}]}`; in the config file, a YAML
    /// mapping. Each entry holds glob patterns for `namespace`, `pod`,
    /// `container`, `image` and `exe`; all of an entry's patterns must match.
    /// Empty keeps every crash.
    #[arg(long, env = "CAPTURE_FILTER", default_value = "", value_name = "JSON", value_parser = CaptureFilter::parse)]
    capture_filter: CaptureFilter,

    /// Disable k8s Event emission on capture (`kubectl describe pod`/
    /// `kubectl get events` surfacing). Events are on by default. Captures are
    /// still logged by the daemon either way.
    #[arg(long, env = "CAPTURE_NO_EVENTS")]
    no_events: bool,

    /// Max crashes concurrently held open for the handler via
    /// `core_pipe_limit` (node-global sysctl); beyond it the kernel skips the
    /// dump entirely rather than exec'ing the handler.
    #[arg(long, env = "CAPTURE_PIPE_LIMIT", default_value_t = coredrop::core_pattern::DEFAULT_PIPE_LIMIT)]
    pipe_limit: u32,

    /// Node name reported on k8s Events (the chart sets it from
    /// `spec.nodeName`). Unset falls back to the hostname, which in a pod
    /// without `hostNetwork` is the pod name, not the node.
    #[arg(long, env = "NODE_NAME")]
    node_name: Option<String>,

    /// File the `core_pattern` sysctl is written through. The chart points it
    /// at a writable bind mount of the node's sysctl file, because the
    /// container's own `/proc/sys` is read-only.
    #[arg(long, env = "CAPTURE_CORE_PATTERN_PATH", default_value = coredrop::core_pattern::CORE_PATTERN_PATH)]
    core_pattern_path: PathBuf,

    /// File the `core_pipe_limit` sysctl is written through. Same bind mount
    /// approach as `core_pattern_path`.
    #[arg(long, env = "CAPTURE_CORE_PIPE_LIMIT_PATH", default_value = coredrop::core_pattern::CORE_PIPE_LIMIT_PATH)]
    core_pipe_limit_path: PathBuf,

    /// Address of the Prometheus `/metrics` endpoint (`host:port`); empty
    /// turns the endpoint off.
    #[arg(long, env = "CAPTURE_METRICS_ADDR", default_value = coredrop::daemon::metrics::DEFAULT_METRICS_ADDR, value_parser = parse_metrics_addr)]
    metrics_addr: MetricsAddr,
}

/// Flags the config file cannot set.
const FILE_ONLY_FLAGS: [&str; 3] = ["config", "help", "version"];

/// Whether `arg` accepts `value`. Checked before the value becomes a default:
/// clap checks defaults only in debug builds, and then with a panic.
fn check_file_value(arg: &clap::Arg, value: &str) -> Result<(), String> {
    if !arg.get_action().takes_values() {
        return match value {
            "true" | "false" => Ok(()),
            _ => Err(format!("{value:?} is not true or false")),
        };
    }
    let long = arg.get_long().unwrap_or_default();
    clap::Command::new("coredrop")
        .no_binary_name(true)
        .arg(arg.clone())
        .try_get_matches_from([format!("--{long}"), value.to_string()])
        .map(drop)
        .map_err(|e| {
            let msg = e.to_string();
            let first = msg.lines().next().unwrap_or_default();
            first.trim_start_matches("error: ").to_string()
        })
}

/// The `key: value` pairs of a config file, each value as its flag would
/// take it. A list or mapping (`capture-filter`) becomes JSON; a `null`
/// value leaves the default. An empty file sets nothing.
fn config_file_values(text: &str) -> Result<Vec<(String, String)>, String> {
    use serde_norway::Value;
    let mapping = match serde_norway::from_str::<Value>(text).map_err(|e| e.to_string())? {
        Value::Null => return Ok(Vec::new()),
        Value::Mapping(mapping) => mapping,
        _ => return Err("the top level must be a mapping of flag names to values".into()),
    };
    let mut values = Vec::new();
    for (key, value) in mapping {
        let Value::String(key) = key else {
            return Err(format!("key {key:?} is not a string"));
        };
        let value = match value {
            Value::Null => continue,
            Value::String(s) => s,
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            Value::Sequence(_) | Value::Mapping(_) => {
                serde_json::to_string(&value).map_err(|e| format!("`{key}`: {e}"))?
            }
            Value::Tagged(_) => return Err(format!("`{key}`: YAML tags are not supported")),
        };
        values.push((key, value));
    }
    Ok(values)
}

/// `--metrics-addr`: the endpoint's address; `None` = no endpoint. A newtype,
/// because clap reads an `Option` field as "flag may be absent".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetricsAddr(Option<SocketAddr>);

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
    fn load<I, T>(argv: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString>,
    {
        let argv: Vec<std::ffi::OsString> = argv.into_iter().map(Into::into).collect();
        let first = Self::try_parse_from(&argv)?;
        let Some(path) = first.config else {
            return Ok(first);
        };
        let file_error = |msg: String| {
            clap::Error::raw(
                clap::error::ErrorKind::InvalidValue,
                format!("config file {}: {msg}\n", path.display()),
            )
        };
        let text = std::fs::read_to_string(&path).map_err(|e| file_error(e.to_string()))?;
        let mut cmd = Self::command();
        for (key, value) in config_file_values(&text).map_err(file_error)? {
            let arg = cmd
                .get_arguments()
                .find(|a| {
                    a.get_long() == Some(key.as_str()) && !FILE_ONLY_FLAGS.contains(&key.as_str())
                })
                .ok_or_else(|| file_error(format!("unknown key `{key}`")))?;
            check_file_value(arg, &value).map_err(|e| file_error(format!("`{key}`: {e}")))?;
            let id = arg.get_id().clone();
            cmd = cmd.mut_arg(id, |a| a.default_value(value));
        }
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
    fn object_store(&self) -> Result<Option<Arc<dyn ObjectStore>>> {
        let Some(url) = self.store_url.as_deref().filter(|url| !url.is_empty()) else {
            info!("no object store configured; the handler discards cores");
            return Ok(None);
        };
        let store_options = coredrop::daemon::upload::store_options_from_env();
        if coredrop::daemon::upload::is_plaintext_endpoint(&store_options) {
            warn!(
                "object-store endpoint is plaintext HTTP; cores traverse the network unencrypted"
            );
        }
        coredrop::daemon::upload::object_store_from_url_opts(url, store_options)
            .map(Some)
            .with_context(|| {
                format!("--store-url {url} is set, but the object store cannot be built")
            })
    }

    fn upload_settings(&self) -> UploadSettings {
        UploadSettings {
            concurrency: self.upload_concurrency,
            attempt_timeout: (self.upload_timeout_secs != 0)
                .then(|| Duration::from_secs(self.upload_timeout_secs)),
            max_age: Duration::from_secs(self.spool_max_age_secs),
            ..UploadSettings::default()
        }
    }

    fn to_handler_config(&self, store_configured: bool) -> HandlerConfig {
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
            rate_state_path: coredrop::config::rate_state_path_for(&self.handler_config_path),
            event_socket_path: Some(coredrop::config::event_socket_path_for(
                &self.handler_config_path,
            )),
            filter: self.capture_filter.clone(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // The kernel exec's us as `coredrop capture <args>`; the probes exec
    // `coredrop check`; anything else is the daemon. The capture subcommand
    // carries kernel-supplied positionals (`%P %s %t %E`), not user config, so
    // it is parsed by hand - not clap - and both subcommands are dispatched
    // before the daemon's flag parsing.
    // `args_os`, not `args`: `args` panics on a non-UTF-8 argument, and `%E`
    // is a file name the crashing workload controls.
    let argv: Vec<std::ffi::OsString> = std::env::args_os().collect();
    match argv.get(1).and_then(|a| a.to_str()) {
        Some("capture") => {
            let capture_args = CaptureArgs::parse(&argv[2..])?;
            // The kernel exec's with a clean environment, so read the daemon-written
            // hostPath config. Without it, the defaults store nothing.
            let config_path =
                env_path("CAPTURE_CONFIG_PATH", coredrop::config::DEFAULT_CONFIG_PATH);
            let config = HandlerConfig::read(&config_path).unwrap_or_default();
            run_handler(capture_args, &config, KernelPipe::stdin()).await
        }
        // Run by the readiness/liveness probes, with the daemon's flags (in
        // the chart, `--config`); the error is the probe's failure reason.
        Some("check") => {
            let daemon = DaemonArgs::load(argv.iter().take(1).chain(&argv[2..]).cloned())
                .unwrap_or_else(|e| e.exit());
            coredrop::daemon::health::check(
                &daemon.handler_path,
                &daemon.handler_config_path,
                &daemon.core_pattern_path,
            )
        }
        _ => run_daemon(DaemonArgs::load(argv).unwrap_or_else(|e| e.exit())).await,
    }
}

fn env_path(key: &str, default: &str) -> PathBuf {
    std::env::var_os(key).map_or_else(|| PathBuf::from(default), PathBuf::from)
}

/// Run the daemon: wire the kernel to the handler, then hold the restore guard
/// until shutdown.
async fn run_daemon(args: DaemonArgs) -> Result<()> {
    info!(
        handler = %args.handler_path.display(),
        config = %args.handler_config_path.display(),
        cluster = %args.cluster,
        "coredrop starting"
    );

    let store = args.object_store().inspect_err(|e| {
        error!(error = %format!("{e:#}"), "unusable object store; exiting for restart");
    })?;
    let config = args.to_handler_config(store.is_some());

    // Reject a RAM-backed spool before any capture can be routed to it.
    // Without a store the handler writes nothing to the spool.
    if store.is_some()
        && let Err(e) = Spool::new(&args.spool_dir).ensure_disk_backed()
    {
        error!(error = %e, "unusable spool; exiting for restart");
        return Err(e);
    }

    // crictl enrichment is best-effort, so a missing socket is not fatal - but
    // it is otherwise invisible until a crash produces a manifest with no
    // namespace/pod/container name.
    match &config.cri_runtime_endpoint {
        // Unset is the default: the handler runs in the host mount namespace,
        // so the node's own crictl resolves the endpoint better than we can.
        // Say so, or a missing identity looks like a bug rather than a setting.
        None => info!("no CRI endpoint configured; crictl will resolve one on the node"),
        Some(endpoint) => {
            if let Some(socket) = coredrop::handler::crictl::unix_socket_path(endpoint)
                && !std::path::Path::new(socket).exists()
            {
                warn!(
                    socket,
                    "CRI socket does not exist; crictl enrichment will degrade to cgroup-only identity"
                );
            }
        }
    }

    // Bind the capture-event socket before writing the config, so the handler
    // only ever gets a path the daemon is listening on rather than one
    // it would send datagrams into the void. It is bound even with
    // --no-events: the daemon logs every capture it receives there.
    let socket_path = coredrop::config::event_socket_path_for(&args.handler_config_path);
    let events_socket = match coredrop::daemon::k8s_events::bind_socket(&socket_path) {
        Ok(socket) => {
            info!(path = %socket_path.display(), "capture event socket bound");
            socket
        }
        Err(e) => {
            error!(error = %e, path = %socket_path.display(), "failed to bind capture event socket; exiting for restart");
            return Err(e).with_context(|| {
                format!("binding capture event socket {}", socket_path.display())
            });
        }
    };

    let config_path = &args.handler_config_path;

    if let Err(e) = config.write(config_path) {
        error!(error = %e, path = %config_path.display(), "failed to write handler config; exiting for restart");
        return Err(e);
    }

    let guard = match CorePatternGuard::install_at(
        &args.handler_path,
        args.pipe_limit,
        args.core_pattern_path.clone(),
        args.core_pipe_limit_path.clone(),
    ) {
        Ok(g) => {
            info!(
                handler = %args.handler_path.display(),
                config = %config_path.display(),
                "core_pattern installed - capture path active"
            );
            g
        }
        Err(e) => {
            error!(error = %e, "failed to install core_pattern; exiting for restart");
            return Err(e);
        }
    };

    let node = node_name(&args);
    let metrics = start_metrics(args.metrics_addr.0, args.spool_max_bytes).await;
    let pattern_check = tokio::spawn(check_core_pattern(guard.checker(), metrics.clone()));
    let wake = Arc::new(Notify::new());
    let (reports_tx, reports_rx) = mpsc::unbounded_channel();
    let (listener_stop_tx, listener_stop_rx) = oneshot::channel::<()>();
    let listener = tokio::spawn(coredrop::daemon::k8s_events::run_listener(
        events_socket,
        reports_rx,
        wake.clone(),
        node,
        !args.no_events,
        metrics.clone(),
        listener_stop_rx,
    ));

    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let uploader =
        store.map(|store| spawn_uploader(&args, store, wake, reports_tx, metrics, stop_rx));

    shutdown_signal().await?;
    info!("shutdown signal received");

    // Stop new captures first, then give the spool the rest of the grace
    // period. Bundles not uploaded by then stay for the next daemon. The
    // check stops before the restore, so the restore is not reported.
    pattern_check.abort();
    drop(guard);
    if let Some(uploader) = uploader {
        let _ = stop_tx.send(());
        if let Err(e) = uploader.await {
            warn!(error = %e, "spool uploader task failed");
        }
    }
    stop_listener(listener_stop_tx, listener).await;
    Ok(())
}

/// The node name for k8s Events: `--node-name`, else the host name.
fn node_name(args: &DaemonArgs) -> String {
    args.node_name
        .clone()
        .filter(|n| !n.is_empty())
        .unwrap_or_else(coredrop::handler::node_hostname)
}

/// How often the daemon checks that `core_pattern` still routes to the handler.
const CORE_PATTERN_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Check `core_pattern` every [`CORE_PATTERN_CHECK_INTERVAL`] and set
/// `coredrop_core_pattern_installed`. Logs when the state changes. It does not
/// write the pattern back: two tools that each write it would fight.
async fn check_core_pattern(check: CorePatternCheck, metrics: Arc<Metrics>) {
    let mut interval = tokio::time::interval(CORE_PATTERN_CHECK_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut was_installed = true;
    loop {
        interval.tick().await;
        let installed = match check.check() {
            Ok(None) => true,
            Ok(Some(current)) => {
                if was_installed {
                    warn!(
                        core_pattern = current,
                        "core_pattern no longer routes to the handler; captures stop until the daemon restarts"
                    );
                }
                false
            }
            Err(e) => {
                if was_installed {
                    warn!(error = %format!("{e:#}"), "cannot read core_pattern; reporting it as not installed");
                }
                false
            }
        };
        if installed && !was_installed {
            info!("core_pattern routes to the handler again");
        }
        metrics.set_core_pattern_installed(installed);
        was_installed = installed;
    }
}

/// The daemon's metrics, served on `addr` when it is set. A bind failure is
/// logged and the daemon runs on without the endpoint: capture must not
/// depend on it. The endpoint runs until the runtime stops.
async fn start_metrics(addr: Option<SocketAddr>, spool_max_bytes: u64) -> Arc<Metrics> {
    let metrics = Arc::new(Metrics::new());
    metrics.set_spool_max_bytes(spool_max_bytes);
    // The daemon installs core_pattern before it starts the endpoint. The
    // check task updates the gauge from then on.
    metrics.set_core_pattern_installed(true);
    let Some(addr) = addr else {
        info!("metrics endpoint disabled (--metrics-addr is empty)");
        return metrics;
    };
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => {
            info!(%addr, "metrics endpoint listening on /metrics");
            tokio::spawn(coredrop::daemon::metrics::serve(listener, metrics.clone()));
        }
        Err(e) => {
            error!(error = %e, %addr, "failed to bind the metrics endpoint; running without metrics");
        }
    }
    metrics
}

/// Stop the event listener once it has reported the uploads that finished
/// during the flush. It gets at most [`LISTENER_DRAIN_TIMEOUT`].
async fn stop_listener(stop: oneshot::Sender<()>, listener: tokio::task::JoinHandle<()>) {
    let _ = stop.send(());
    if tokio::time::timeout(LISTENER_DRAIN_TIMEOUT, listener)
        .await
        .is_err()
    {
        warn!(
            "reporting the last uploads took too long; some captures have no completion log or Event"
        );
    }
}

/// Start the spool uploader. It runs until `stop` fires, then flushes for
/// `--shutdown-flush-secs`.
fn spawn_uploader(
    args: &DaemonArgs,
    store: Arc<dyn ObjectStore>,
    wake: Arc<Notify>,
    reports: mpsc::UnboundedSender<coredrop::events::CaptureEventPayload>,
    metrics: Arc<Metrics>,
    stop: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    let settings = args.upload_settings();
    info!(
        spool = %args.spool_dir.display(),
        concurrency = settings.concurrency,
        "spool uploader started"
    );
    let uploader = Uploader::new(
        Spool::new(&args.spool_dir),
        store,
        settings,
        reports,
        metrics,
    );
    tokio::spawn(uploader.run(
        wake,
        async {
            let _ = stop.await;
        },
        Duration::from_secs(args.shutdown_flush_secs),
    ))
}

/// Wait for a termination signal. Kubernetes sends SIGTERM on pod shutdown
/// (then SIGKILL after the grace period); `ctrl_c` alone catches only SIGINT, so
/// under k8s the daemon would be hard-killed and `CorePatternGuard`'s restore
/// would never run. Wake on either SIGTERM or SIGINT so the guard always drops.
async fn shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;

    /// Daemon args that touch nothing real: temp sysctl paths, no store, no
    /// k8s Events. Each test overrides the one field whose failure it exercises.
    fn args(config_path: &Path, pattern: &Path, pipe: &Path) -> DaemonArgs {
        DaemonArgs {
            handler_path: "/opt/coredrop/bin/coredrop".into(),
            config: None,
            handler_config_path: config_path.to_path_buf(),
            cluster: "test".into(),
            no_redact: false,
            proc_root: "/proc".into(),
            store_url: None,
            crictl_path: "/usr/local/bin/crictl".into(),
            cri_runtime_endpoint: None,
            max_core_bytes: 0,
            max_cores_per_hour: 0,
            drain_deadline_secs: 0,
            spool_dir: config_path.with_file_name("spool"),
            spool_max_bytes: 0,
            spool_min_free_percent: 0,
            spool_max_age_secs: 0,
            upload_concurrency: 1,
            upload_timeout_secs: 0,
            shutdown_flush_secs: 0,
            crictl_timeout_secs: 0,
            capture_filter: CaptureFilter::default(),
            no_events: true,
            pipe_limit: 16,
            node_name: None,
            core_pattern_path: pattern.to_path_buf(),
            core_pipe_limit_path: pipe.to_path_buf(),
            metrics_addr: MetricsAddr(None),
        }
    }

    #[test]
    fn metrics_addr_is_a_socket_address_or_empty() {
        assert!(parse_metrics_addr("9464").is_err());
        let parse = |argv: &[&str]| DaemonArgs::try_parse_from(argv).unwrap().metrics_addr;
        assert_eq!(
            parse(&["coredrop"]),
            MetricsAddr(Some(
                coredrop::daemon::metrics::DEFAULT_METRICS_ADDR
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
            PathBuf::from(coredrop::spool::DEFAULT_SPOOL_DIR)
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
            assert_eq!(args.cluster, coredrop::config::DEFAULT_CLUSTER, "{yaml:?}");
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

    /// Temp stand-ins for the two kernel sysctls, seeded with plausible
    /// pre-install values so a stray write is visible.
    fn sysctls(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let pattern = dir.join("core_pattern");
        let pipe = dir.join("core_pipe_limit");
        std::fs::write(&pattern, "core\n").unwrap();
        std::fs::write(&pipe, "0\n").unwrap();
        (pattern, pipe)
    }

    #[tokio::test]
    async fn errors_when_config_write_fails() {
        let dir = TempDir::new().unwrap();
        let (pattern, pipe) = sysctls(dir.path());
        // The config path is a directory, so the file cannot be written. The
        // socket bind before it still succeeds in the same parent dir.
        let config_path = dir.path().join("run").join("handler.json");
        std::fs::create_dir_all(&config_path).unwrap();

        let err = run_daemon(args(&config_path, &pattern, &pipe))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("handler.json"), "{err:#}");

        // The sysctls must not have been touched - install runs after the write.
        assert_eq!(std::fs::read_to_string(&pattern).unwrap(), "core\n");
    }

    /// Without a store the handler writes nothing to the spool, so a spool on
    /// tmpfs does not stop the daemon: the run gets past the spool check and
    /// fails later, on the unwritable config.
    #[tokio::test]
    async fn accepts_a_tmpfs_spool_without_a_store() {
        let shm = std::path::Path::new("/dev/shm");
        if !shm.is_dir() {
            return;
        }
        let spool = TempDir::new_in(shm).unwrap();
        let dir = TempDir::new().unwrap();
        let (pattern, pipe) = sysctls(dir.path());
        let config_path = dir.path().join("run").join("handler.json");
        std::fs::create_dir_all(&config_path).unwrap();
        let mut daemon_args = args(&config_path, &pattern, &pipe);
        daemon_args.spool_dir = spool.path().join("spool");

        let err = run_daemon(daemon_args).await.unwrap_err();
        let err = format!("{err:#}");
        assert!(!err.contains("memory-backed"), "{err}");
        assert!(err.contains("handler.json"), "{err}");
    }

    /// A set but unusable store URL stops the daemon before it routes any
    /// crash to the handler: running on would discard every core.
    #[tokio::test]
    async fn errors_when_the_store_url_is_unusable() {
        for url in ["not a url", "memory://bucket"] {
            let dir = TempDir::new().unwrap();
            let (pattern, pipe) = sysctls(dir.path());
            let config_path = dir.path().join("run").join("handler.json");
            let mut daemon_args = args(&config_path, &pattern, &pipe);
            daemon_args.store_url = Some(url.into());

            let err = run_daemon(daemon_args).await.unwrap_err();
            assert!(format!("{err:#}").contains("--store-url"), "{url}: {err:#}");
            assert!(!config_path.exists(), "{url}: no handler config is written");
            assert_eq!(std::fs::read_to_string(&pattern).unwrap(), "core\n");
        }
    }

    #[tokio::test]
    async fn errors_when_core_pattern_install_fails() {
        let dir = TempDir::new().unwrap();
        let (_pattern, pipe) = sysctls(dir.path());
        // Nonexistent sysctl: reading the previous value fails.
        let missing_pattern = dir.path().join("no-such-dir").join("core_pattern");

        let err = run_daemon(args(
            &dir.path().join("run").join("handler.json"),
            &missing_pattern,
            &pipe,
        ))
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("core_pattern"), "{err:#}");
        // The config write ran first and succeeded, so the install is what failed.
        assert!(dir.path().join("run").join("handler.json").exists());
        // The socket is bound even with k8s Events off: the daemon logs every
        // capture it receives there.
        assert!(dir.path().join("run").join("events.sock").exists());
    }

    #[tokio::test]
    async fn errors_when_event_socket_bind_fails() {
        // A unix socket path must fit in `sockaddr_un.sun_path` (108 bytes), so
        // an over-long run dir makes the bind fail deterministically - no root,
        // no special filesystem needed.
        let dir = TempDir::new().unwrap();
        let (pattern, pipe) = sysctls(dir.path());
        let run_dir = dir.path().join("d".repeat(120));

        let err = run_daemon(args(&run_dir.join("handler.json"), &pattern, &pipe))
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("capture event socket"),
            "{err:#}"
        );

        // Bind runs first: neither the config nor the sysctls were touched.
        assert!(!run_dir.join("handler.json").exists());
        assert_eq!(std::fs::read_to_string(&pattern).unwrap(), "core\n");
    }
}
