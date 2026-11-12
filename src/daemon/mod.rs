//! The long-running daemon pod: it installs `core_pattern`, writes the
//! handler config, uploads the spool, and reports each capture as a log
//! line, a k8s Event and metrics.

mod args;
mod config_file;
pub mod health;
pub mod k8s_events;
pub mod metrics;
pub mod upload;
pub mod uploader;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::{error, info, warn};

use self::args::DaemonArgs;
use self::metrics::Metrics;
use self::uploader::Uploader;
use crate::core_pattern::{CorePatternCheck, CorePatternGuard};
use crate::spool::Spool;

/// Run the daemon with `argv` (program name first). Exits the process on a
/// bad flag or config file, after clap prints the error (or `--help`).
///
/// # Errors
///
/// Fails when the daemon cannot start: unusable store or spool, the event
/// socket, the handler config or `core_pattern` cannot be set up.
pub async fn main(argv: Vec<std::ffi::OsString>) -> Result<()> {
    run(DaemonArgs::load(argv).unwrap_or_else(|e| e.exit())).await
}

/// `coredrop check`, run by the probes with the daemon's flags (`argv`,
/// program name first). Exits the process on a bad flag or config file.
///
/// # Errors
///
/// Names the first part of the capture path that drifted.
pub fn check(argv: Vec<std::ffi::OsString>) -> Result<()> {
    let daemon = DaemonArgs::load(argv).unwrap_or_else(|e| e.exit());
    health::check(
        &daemon.handler_path,
        &daemon.handler_config_path,
        &daemon.core_pattern_path,
    )
}

/// On shutdown, how long the daemon waits for the event listener to report
/// the uploads that finished during the flush. It fits in the 5 s the pod's
/// termination grace period leaves after the flush.
const LISTENER_DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// Run the daemon: wire the kernel to the handler, then hold the restore guard
/// until shutdown.
async fn run(args: DaemonArgs) -> Result<()> {
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
            if let Some(socket) = crate::handler::crictl::unix_socket_path(endpoint)
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
    let socket_path = crate::config::event_socket_path_for(&args.handler_config_path);
    let events_socket = match crate::daemon::k8s_events::bind_socket(&socket_path) {
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
    let listener = tokio::spawn(crate::daemon::k8s_events::run_listener(
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
        .unwrap_or_else(crate::handler::node_hostname)
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
            tokio::spawn(crate::daemon::metrics::serve(listener, metrics.clone()));
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
    reports: mpsc::UnboundedSender<crate::events::CaptureEventPayload>,
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

    use super::args::MetricsAddr;
    use super::*;
    use crate::filter::CaptureFilter;

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

        let err = run(args(&config_path, &pattern, &pipe)).await.unwrap_err();
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

        let err = run(daemon_args).await.unwrap_err();
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

            let err = run(daemon_args).await.unwrap_err();
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

        let err = run(args(
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

        let err = run(args(&run_dir.join("handler.json"), &pattern, &pipe))
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
