//! `coredrop` - standalone Kubernetes coredump handler.
//!
//! The binary runs in two modes:
//!   - capture handler (`coredrop capture %P %s %t %E`): the short-lived
//!     process the kernel exec's per fault. Snapshots `/proc`, drains the core
//!     to the object store, enriches identity via crictl, writes a JSON
//!     manifest sidecar.
//!   - daemon (`coredrop`): the long-running `DaemonSet` container. Installs
//!     `core_pattern` so faults route to the handler, writes the handler
//!     config to a hostPath the kernel-exec'd handler can read (the kernel
//!     exec's with a clean environment), and holds the restore guard until
//!     shutdown.

use anyhow::Result;
use clap::Parser;
use tracing::{info, warn};

use coredrop::config::HandlerConfig;
use coredrop::core_pattern::CorePatternGuard;
use coredrop::handler::{CaptureArgs, run as run_handler};

/// `coredrop` daemon config. Every knob is a flag with an env-var fallback.
/// Secrets (store creds) stay env-only - never flags (leak via `ps`/cmdline).
/// The kernel-exec'd `capture` subcommand reads the daemon-written hostPath
/// config file (see `main`), not these flags.
#[derive(Debug, Parser)]
#[command(name = "coredrop", version, about)]
struct DaemonArgs {
    /// Host path the kernel exec's as the `core_pattern` handler.
    #[arg(long, env = "CAPTURE_HANDLER_PATH", default_value = coredrop::DEFAULT_HANDLER_PATH)]
    handler_path: String,

    /// hostPath the daemon serializes the handler config to.
    #[arg(long, env = "CAPTURE_CONFIG_PATH", default_value = coredrop::config::DEFAULT_CONFIG_PATH)]
    config_path: String,

    /// Cluster name - the first path segment of the object key.
    #[arg(long, env = "CAPTURE_CLUSTER", default_value = "local")]
    cluster: String,

    /// Pass `environ` through un-redacted.
    #[arg(long, env = "CAPTURE_NO_REDACT")]
    no_redact: bool,

    /// `/proc` root (overridable for tests / non-standard layouts).
    #[arg(long, env = "CAPTURE_PROC_ROOT", default_value = "/proc")]
    proc_root: String,

    /// Object-store URL for the streamed core (e.g. `s3://crash-artifacts`);
    /// unset disables upload. Store creds come from env (`AWS_*` etc.), not flags.
    #[arg(long, env = "CAPTURE_STORE_URL")]
    store_url: Option<String>,

    /// Path to the `crictl` binary for post-drain container enrichment.
    #[arg(long, env = "CRICTL_PATH", default_value = "/usr/local/bin/crictl")]
    crictl_path: String,

    /// CRI runtime endpoint (e.g. `unix:///run/containerd/containerd.sock`).
    #[arg(long, env = "CONTAINER_RUNTIME_ENDPOINT")]
    cri_runtime_endpoint: Option<String>,

    /// Max uncompressed core bytes stored per crash; 0 = unlimited. The
    /// remainder of the stream is drained but not stored.
    #[arg(long, env = "CAPTURE_MAX_CORE_BYTES", default_value_t = coredrop::config::DEFAULT_MAX_CORE_BYTES)]
    max_core_bytes: u64,

    /// Max core uploads per container per hour; 0 = unlimited. Suppressed
    /// crashes still get a proc snapshot and manifest, just no core.
    #[arg(long, env = "CAPTURE_MAX_CORES_PER_HOUR", default_value_t = coredrop::config::DEFAULT_MAX_CORES_PER_HOUR)]
    max_cores_per_hour: u32,

    /// Wall-clock deadline (seconds) for draining/uploading one core; 0 = no
    /// deadline. On expiry the handler abandons the upload and exits, freeing
    /// its `core_pipe_limit` slot instead of letting a slow store hold it.
    #[arg(long, env = "CAPTURE_UPLOAD_DEADLINE_SECS", default_value_t = coredrop::config::DEFAULT_UPLOAD_DEADLINE_SECS)]
    upload_deadline_secs: u64,

    /// Disable k8s Event emission on capture (`kubectl describe pod` /
    /// `kubectl get events` surfacing). Events are on by default.
    #[arg(long, env = "CAPTURE_NO_EVENTS")]
    no_events: bool,

    /// Max crashes concurrently held open for the handler via
    /// `core_pipe_limit` (node-global sysctl); beyond it the kernel skips the
    /// dump entirely rather than exec'ing the handler.
    #[arg(long, env = "CAPTURE_PIPE_LIMIT", default_value_t = coredrop::core_pattern::DEFAULT_PIPE_LIMIT)]
    pipe_limit: u32,
}

impl DaemonArgs {
    fn to_handler_config(&self) -> HandlerConfig {
        let store_options = coredrop::upload::store_options_from_env();
        HandlerConfig {
            cluster: self.cluster.clone(),
            no_redact: self.no_redact,
            proc_root: self.proc_root.clone(),
            store_url: self.store_url.clone().filter(|s| !s.is_empty()),
            store_options,
            crictl_path: self.crictl_path.clone(),
            cri_runtime_endpoint: self.cri_runtime_endpoint.clone(),
            max_core_bytes: self.max_core_bytes,
            max_cores_per_hour: self.max_cores_per_hour,
            upload_deadline_secs: self.upload_deadline_secs,
            rate_state_path: coredrop::config::rate_state_path_for(&self.config_path),
            event_socket_path: (!self.no_events)
                .then(|| coredrop::config::event_socket_path_for(&self.config_path)),
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

    // The kernel exec's us as `coredrop capture <args>`; anything else is the
    // daemon. The capture subcommand carries kernel-supplied positionals
    // (`%P %s %t %E`), not user config, so it is parsed by hand - not clap -
    // and dispatched before the daemon's flag parsing.
    let mut argv = std::env::args();
    let _bin = argv.next();
    if argv.next().as_deref() == Some("capture") {
        let rest: Vec<String> = argv.collect();
        let capture_args = CaptureArgs::parse(&rest)?;
        // The kernel exec's with a clean environment, so read the daemon-written
        // hostPath config, not env. Absent (local/test runs) -> env.
        let config_path = std::env::var("CAPTURE_CONFIG_PATH")
            .unwrap_or_else(|_| coredrop::config::DEFAULT_CONFIG_PATH.to_string());
        let config = HandlerConfig::read(&config_path).unwrap_or_else(HandlerConfig::from_env);
        let mut stdin = tokio::io::stdin();
        return run_handler(capture_args, &config, &mut stdin, None).await;
    }

    run_daemon(DaemonArgs::parse()).await
}

async fn run_daemon(args: DaemonArgs) -> Result<()> {
    info!(
        handler = %args.handler_path,
        config = %args.config_path,
        cluster = %args.cluster,
        "coredrop starting"
    );

    let mut config = args.to_handler_config();

    // Bind the capture-event socket before writing the config, so the
    // handler only ever gets a path the daemon is actually listening on -
    // a bind failure degrades to events-disabled rather than the handler
    // sending datagrams into the void.
    let events_socket = if args.no_events {
        info!("capture events disabled (--no-events)");
        None
    } else if let Some(path) = &config.event_socket_path {
        match coredrop::k8s_events::bind_socket(path) {
            Ok(socket) => {
                info!(path = %path, "capture event socket bound");
                Some(socket)
            }
            Err(e) => {
                warn!(error = %e, path = %path, "failed to bind capture event socket; capture events disabled");
                None
            }
        }
    } else {
        warn!(
            "capture events enabled but no capture event socket path configured; capture events disabled"
        );
        None
    };
    if events_socket.is_none() {
        config.event_socket_path = None;
    }

    let config_path = &args.config_path;

    if let Err(e) = config.write(config_path) {
        warn!(error = %e, path = %config_path, "failed to write handler config; capture path disabled");
        // Wait for shutdown anyway - no point crashing, operator can fix and restart.
        shutdown_signal().await?;
        return Ok(());
    }

    let _guard = match CorePatternGuard::install(&args.handler_path, args.pipe_limit) {
        Ok(g) => {
            info!(
                handler = %args.handler_path,
                config = %config_path,
                "core_pattern installed - capture path active"
            );
            g
        }
        Err(e) => {
            warn!(error = %e, "failed to install core_pattern; waiting for shutdown");
            shutdown_signal().await?;
            return Ok(());
        }
    };

    if let Some(socket) = events_socket {
        let node = coredrop::handler::node_hostname();
        tokio::spawn(coredrop::k8s_events::run_listener(socket, node));
    }

    shutdown_signal().await?;
    info!("shutdown signal received");
    // `_guard` drops here -> core_pattern restored.
    Ok(())
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
