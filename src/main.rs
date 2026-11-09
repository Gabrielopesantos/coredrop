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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use object_store::ObjectStore;
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::{error, info, warn};

use coredrop::config::HandlerConfig;
use coredrop::core_pattern::CorePatternGuard;
use coredrop::handler::{CaptureArgs, KernelPipe, run as run_handler};
use coredrop::spool::Spool;
use coredrop::uploader::{UploadSettings, Uploader};

/// Default time the daemon keeps uploading the spool after SIGTERM. The
/// chart's termination grace period adds a margin on top.
const DEFAULT_SHUTDOWN_FLUSH_SECS: u64 = 25;

/// On shutdown, how long the daemon waits for the event listener to report
/// the uploads that finished during the flush. It fits in the 5 s the pod's
/// termination grace period leaves after the flush.
const LISTENER_DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// `coredrop` daemon config. Every knob is a flag with an env-var fallback.
/// Secrets (store creds) stay env-only - never flags (leak via `ps`/cmdline).
/// The kernel-exec'd `capture` subcommand reads the daemon-written hostPath
/// config file (see `main`), not these flags.
#[derive(Debug, Parser)]
#[command(name = "coredrop", version, about)]
struct DaemonArgs {
    /// Host path the kernel exec's as the `core_pattern` handler.
    #[arg(long, env = "CAPTURE_HANDLER_PATH", default_value = coredrop::DEFAULT_HANDLER_PATH)]
    handler_path: PathBuf,

    /// hostPath the daemon serializes the handler config to.
    #[arg(long, env = "CAPTURE_CONFIG_PATH", default_value = coredrop::config::DEFAULT_CONFIG_PATH)]
    config_path: PathBuf,

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
    #[arg(long, env = "CAPTURE_SPOOL_MAX_AGE_SECS", default_value_t = coredrop::uploader::DEFAULT_SPOOL_MAX_AGE_SECS)]
    spool_max_age_secs: u64,

    /// Spooled captures the daemon uploads at the same time.
    #[arg(long, env = "CAPTURE_UPLOAD_CONCURRENCY", default_value_t = coredrop::uploader::DEFAULT_UPLOAD_CONCURRENCY, value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..=64))]
    upload_concurrency: usize,

    /// Time limit in seconds for one upload attempt of one spooled capture;
    /// 0 = no limit. A timed-out capture is retried later.
    #[arg(long, env = "CAPTURE_UPLOAD_TIMEOUT_SECS", default_value_t = coredrop::uploader::DEFAULT_UPLOAD_TIMEOUT_SECS)]
    upload_timeout_secs: u64,

    /// On SIGTERM, how long in seconds the daemon keeps uploading the spool
    /// before it exits. Keep it below the pod's termination grace period.
    #[arg(long, env = "CAPTURE_SHUTDOWN_FLUSH_SECS", default_value_t = DEFAULT_SHUTDOWN_FLUSH_SECS)]
    shutdown_flush_secs: u64,

    /// Timeout in seconds for `crictl inspect` in the handler; 0 = no timeout.
    #[arg(long, env = "CRICTL_TIMEOUT_SECS", default_value_t = coredrop::config::DEFAULT_CRICTL_TIMEOUT_SECS)]
    crictl_timeout_secs: u64,

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
}

impl DaemonArgs {
    /// The object store the daemon uploads to, built from `--store-url` and
    /// the store env vars. Only the daemon holds store credentials; the
    /// handler learns only whether there is a store to spool for.
    fn object_store(&self) -> Option<Arc<dyn ObjectStore>> {
        let store_options = coredrop::upload::store_options_from_env();
        let store = self
            .store_url
            .as_deref()
            .filter(|url| !url.is_empty())
            .and_then(|url| {
                if coredrop::upload::is_plaintext_endpoint(&store_options) {
                    warn!(
                        "object-store endpoint is plaintext HTTP; cores traverse the network unencrypted"
                    );
                }
                coredrop::upload::object_store_from_url_opts(url, store_options)
            });
        if store.is_none() {
            info!("no object store configured; the handler discards cores");
        }
        store
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
            rate_state_path: coredrop::config::rate_state_path_for(&self.config_path),
            event_socket_path: Some(coredrop::config::event_socket_path_for(&self.config_path)),
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
    let mut argv = std::env::args_os();
    let _bin = argv.next();
    match argv.next().as_deref().and_then(std::ffi::OsStr::to_str) {
        Some("capture") => {
            let rest: Vec<std::ffi::OsString> = argv.collect();
            let capture_args = CaptureArgs::parse(&rest)?;
            // The kernel exec's with a clean environment, so read the daemon-written
            // hostPath config. Without it, the defaults store nothing.
            let config_path =
                env_path("CAPTURE_CONFIG_PATH", coredrop::config::DEFAULT_CONFIG_PATH);
            let config = HandlerConfig::read(&config_path).unwrap_or_default();
            run_handler(capture_args, &config, KernelPipe::stdin()).await
        }
        // Run by the readiness/liveness probes. Reads the same env the daemon
        // container is configured with; the error is the probe's failure reason.
        Some("check") => coredrop::health::check(
            &env_path("CAPTURE_HANDLER_PATH", coredrop::DEFAULT_HANDLER_PATH),
            &env_path("CAPTURE_CONFIG_PATH", coredrop::config::DEFAULT_CONFIG_PATH),
            &env_path(
                "CAPTURE_CORE_PATTERN_PATH",
                coredrop::core_pattern::CORE_PATTERN_PATH,
            ),
        ),
        _ => run_daemon(DaemonArgs::parse()).await,
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
        config = %args.config_path.display(),
        cluster = %args.cluster,
        "coredrop starting"
    );

    let store = args.object_store();
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
            if let Some(socket) = coredrop::crictl::unix_socket_path(endpoint)
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
    let socket_path = coredrop::config::event_socket_path_for(&args.config_path);
    let events_socket = match coredrop::k8s_events::bind_socket(&socket_path) {
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

    let config_path = &args.config_path;

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

    let node = args
        .node_name
        .clone()
        .filter(|n| !n.is_empty())
        .unwrap_or_else(coredrop::handler::node_hostname);
    let wake = Arc::new(Notify::new());
    let (reports_tx, reports_rx) = mpsc::unbounded_channel();
    let (listener_stop_tx, listener_stop_rx) = oneshot::channel::<()>();
    let listener = tokio::spawn(coredrop::k8s_events::run_listener(
        events_socket,
        reports_rx,
        wake.clone(),
        node,
        !args.no_events,
        listener_stop_rx,
    ));

    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let uploader = store.map(|store| spawn_uploader(&args, store, wake, reports_tx, stop_rx));

    shutdown_signal().await?;
    info!("shutdown signal received");

    // Stop new captures first, then give the spool the rest of the grace
    // period. Bundles not uploaded by then stay for the next daemon.
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
    stop: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    let settings = args.upload_settings();
    info!(
        spool = %args.spool_dir.display(),
        concurrency = settings.concurrency,
        "spool uploader started"
    );
    let uploader = Uploader::new(Spool::new(&args.spool_dir), store, settings, reports);
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
            config_path: config_path.to_path_buf(),
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
            no_events: true,
            pipe_limit: 16,
            node_name: None,
            core_pattern_path: pattern.to_path_buf(),
            core_pipe_limit_path: pipe.to_path_buf(),
        }
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
