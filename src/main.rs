//! `coredrop` - standalone Kubernetes coredump handler.
//!
//! The binary runs in three modes:
//!   - capture handler (`coredrop capture %P %s %t %E`): the short-lived
//!     process the kernel exec's per fault. Snapshots `/proc`, drains the core
//!     to the object store, enriches identity via crictl, writes a JSON
//!     manifest sidecar.
//!   - daemon (`coredrop`): the long-running `DaemonSet` container. Installs
//!     `core_pattern` so faults route to the handler, writes the handler
//!     config to a hostPath the kernel-exec'd handler can read (the kernel
//!     exec's with a clean environment), and holds the restore guard until
//!     shutdown.
//!   - health check (`coredrop check`): what the chart's readiness and
//!     liveness probes exec. Exits non-zero when the capture path has drifted.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{error, info, warn};

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

    /// Pass `environ` and `cmdline` through un-redacted.
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

    /// Max core uploads per pod per hour; 0 = unlimited. Suppressed
    /// crashes still get a proc snapshot and manifest, just no core.
    #[arg(long, env = "CAPTURE_MAX_CORES_PER_HOUR", default_value_t = coredrop::config::DEFAULT_MAX_CORES_PER_HOUR)]
    max_cores_per_hour: u32,

    /// Wall-clock deadline (seconds) for draining/uploading one core; 0 = no
    /// deadline. On expiry the handler abandons the upload and exits, freeing
    /// its `core_pipe_limit` slot instead of letting a slow store hold it.
    #[arg(long, env = "CAPTURE_UPLOAD_DEADLINE_SECS", default_value_t = coredrop::config::DEFAULT_UPLOAD_DEADLINE_SECS)]
    upload_deadline_secs: u64,

    /// Timeout in seconds for `crictl inspect` in the handler; 0 = no timeout.
    #[arg(long, env = "CRICTL_TIMEOUT_SECS", default_value_t = coredrop::config::DEFAULT_CRICTL_TIMEOUT_SECS)]
    crictl_timeout_secs: u64,

    /// Disable k8s Event emission on capture (`kubectl describe pod` /
    /// `kubectl get events` surfacing). Events are on by default.
    #[arg(long, env = "CAPTURE_NO_EVENTS")]
    no_events: bool,

    /// Max crashes concurrently held open for the handler via
    /// `core_pipe_limit` (node-global sysctl); beyond it the kernel skips the
    /// dump entirely rather than exec'ing the handler.
    #[arg(long, env = "CAPTURE_PIPE_LIMIT", default_value_t = coredrop::core_pattern::DEFAULT_PIPE_LIMIT)]
    pipe_limit: u32,

    /// Kernel sysctl the handler is installed into. Not a flag or env knob -
    /// an internal seam so tests can drive `run_daemon` against temp files
    /// instead of the real `/proc/sys`.
    #[arg(skip = PathBuf::from(coredrop::core_pattern::CORE_PATTERN_PATH))]
    core_pattern_path: PathBuf,

    /// Kernel sysctl bounding concurrently-held dumps. Same test seam as
    /// `core_pattern_path`.
    #[arg(skip = PathBuf::from(coredrop::core_pattern::CORE_PIPE_LIMIT_PATH))]
    core_pipe_limit_path: PathBuf,
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
            crictl_timeout_secs: self.crictl_timeout_secs,
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

    // The kernel exec's us as `coredrop capture <args>`; the probes exec
    // `coredrop check`; anything else is the daemon. The capture subcommand
    // carries kernel-supplied positionals (`%P %s %t %E`), not user config, so
    // it is parsed by hand - not clap - and both subcommands are dispatched
    // before the daemon's flag parsing.
    let mut argv = std::env::args();
    let _bin = argv.next();
    match argv.next().as_deref() {
        Some("capture") => {
            let rest: Vec<String> = argv.collect();
            let capture_args = CaptureArgs::parse(&rest)?;
            // The kernel exec's with a clean environment, so read the daemon-written
            // hostPath config, not env. Absent (local/test runs) -> env.
            let config_path = env_or("CAPTURE_CONFIG_PATH", coredrop::config::DEFAULT_CONFIG_PATH);
            let config = HandlerConfig::read(&config_path).unwrap_or_else(HandlerConfig::from_env);
            let mut stdin = tokio::io::stdin();
            run_handler(capture_args, &config, &mut stdin, None).await
        }
        // Run by the readiness/liveness probes. Reads the same env the daemon
        // container is configured with; the error is the probe's failure reason.
        Some("check") => coredrop::health::check(
            &env_or("CAPTURE_HANDLER_PATH", coredrop::DEFAULT_HANDLER_PATH),
            &env_or("CAPTURE_CONFIG_PATH", coredrop::config::DEFAULT_CONFIG_PATH),
            coredrop::core_pattern::CORE_PATTERN_PATH,
        ),
        _ => run_daemon(DaemonArgs::parse()).await,
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Run the daemon: wire the kernel to the handler, then hold the restore guard
/// until shutdown.
async fn run_daemon(args: DaemonArgs) -> Result<()> {
    info!(
        handler = %args.handler_path,
        config = %args.config_path,
        cluster = %args.cluster,
        "coredrop starting"
    );

    let config = args.to_handler_config();

    if config.store_url.is_some() && coredrop::upload::is_plaintext_endpoint(&config.store_options)
    {
        warn!("object-store endpoint is plaintext HTTP; cores traverse the network unencrypted");
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
    // only ever gets a path the daemon is actually listening on rather than one
    // it would send datagrams into the void. `event_socket_path` is `Some`
    // exactly when events are enabled.
    let events_socket = match &config.event_socket_path {
        None => {
            info!("capture events disabled (--no-events)");
            None
        }
        Some(path) => match coredrop::k8s_events::bind_socket(path) {
            Ok(socket) => {
                info!(path = %path, "capture event socket bound");
                Some(socket)
            }
            Err(e) => {
                error!(error = %e, path = %path, "failed to bind capture event socket; exiting for restart");
                return Err(e).with_context(|| format!("binding capture event socket {path}"));
            }
        },
    };

    let config_path = &args.config_path;

    if let Err(e) = config.write(config_path) {
        error!(error = %e, path = %config_path, "failed to write handler config; exiting for restart");
        return Err(e);
    }

    let _guard = match CorePatternGuard::install_at(
        &args.handler_path,
        args.pipe_limit,
        args.core_pattern_path.clone(),
        args.core_pipe_limit_path.clone(),
    ) {
        Ok(g) => {
            info!(
                handler = %args.handler_path,
                config = %config_path,
                "core_pattern installed - capture path active"
            );
            g
        }
        Err(e) => {
            error!(error = %e, "failed to install core_pattern; exiting for restart");
            return Err(e);
        }
    };

    if let Some(socket) = events_socket {
        let node = coredrop::handler::node_hostname();
        tokio::spawn(coredrop::k8s_events::run_listener(socket, node));
    }

    shutdown_signal().await?;
    info!("shutdown signal received");

    // `_guard` drops here -> core_pattern / core_pipe_limit restored.
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::Path;

    use super::*;

    /// Unique temp path per test; `tag` keeps concurrent tests apart.
    fn tmp(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "coredrop-daemon-{}-{tag}-{nanos}",
            std::process::id()
        ));
        p
    }

    /// Daemon args that touch nothing real: temp sysctl paths, no store, no
    /// events. Each test overrides the one field whose failure it exercises.
    fn args(config_path: &Path, pattern: &Path, pipe: &Path) -> DaemonArgs {
        DaemonArgs {
            handler_path: "/opt/coredrop/bin/coredrop".into(),
            config_path: config_path.to_string_lossy().into_owned(),
            cluster: "test".into(),
            no_redact: false,
            proc_root: "/proc".into(),
            store_url: None,
            crictl_path: "/usr/local/bin/crictl".into(),
            cri_runtime_endpoint: None,
            max_core_bytes: 0,
            max_cores_per_hour: 0,
            upload_deadline_secs: 0,
            crictl_timeout_secs: 0,
            no_events: true,
            pipe_limit: 16,
            core_pattern_path: pattern.to_path_buf(),
            core_pipe_limit_path: pipe.to_path_buf(),
        }
    }

    #[tokio::test]
    async fn errors_when_config_write_fails() {
        // Parent of the config path is a regular file, so the dir cannot be made.
        let blocker = tmp("blocker");
        std::fs::write(&blocker, "not a directory").unwrap();
        let pattern = tmp("cw-pattern");
        std::fs::write(&pattern, "core\n").unwrap();
        let pipe = tmp("cw-pipe");
        std::fs::write(&pipe, "0\n").unwrap();

        let err = run_daemon(args(&blocker.join("handler.json"), &pattern, &pipe))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("config dir"), "{err:#}");

        // The sysctls must not have been touched - install runs after the write.
        assert_eq!(std::fs::read_to_string(&pattern).unwrap(), "core\n");

        std::fs::remove_file(&blocker).ok();
        std::fs::remove_file(&pattern).ok();
        std::fs::remove_file(&pipe).ok();
    }

    #[tokio::test]
    async fn errors_when_core_pattern_install_fails() {
        let dir = tmp("cp-dir");
        std::fs::create_dir_all(&dir).unwrap();
        let pipe = tmp("cp-pipe");
        std::fs::write(&pipe, "0\n").unwrap();
        // Nonexistent sysctl: reading the previous value fails.
        let pattern = tmp("cp-missing").join("core_pattern");

        let err = run_daemon(args(&dir.join("handler.json"), &pattern, &pipe))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("core_pattern"), "{err:#}");
        // The config write ran first and succeeded, so the install is what failed.
        assert!(dir.join("handler.json").exists());

        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_file(&pipe).ok();
    }

    #[tokio::test]
    async fn errors_when_event_socket_bind_fails() {
        // A unix socket path must fit in `sockaddr_un.sun_path` (108 bytes), so
        // an over-long run dir makes the bind fail deterministically - no root,
        // no special filesystem needed.
        let mut dir = tmp("sock");
        dir.push("d".repeat(120));
        let pattern = tmp("sock-pattern");
        std::fs::write(&pattern, "core\n").unwrap();
        let pipe = tmp("sock-pipe");
        std::fs::write(&pipe, "0\n").unwrap();

        let mut a = args(&dir.join("handler.json"), &pattern, &pipe);
        a.no_events = false;

        let err = run_daemon(a).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("capture event socket"),
            "{err:#}"
        );

        // Bind runs first: neither the config nor the sysctls were touched.
        assert!(!dir.join("handler.json").exists());
        assert_eq!(std::fs::read_to_string(&pattern).unwrap(), "core\n");

        std::fs::remove_dir_all(dir.parent().unwrap()).ok();
        std::fs::remove_file(&pattern).ok();
        std::fs::remove_file(&pipe).ok();
    }
}
