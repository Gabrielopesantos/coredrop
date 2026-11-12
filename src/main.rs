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

use anyhow::Result;

use coredrop::config::HandlerConfig;
use coredrop::handler::{CaptureArgs, KernelPipe, run as run_handler};

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
            coredrop::daemon::check(argv.iter().take(1).chain(&argv[2..]).cloned().collect())
        }
        _ => coredrop::daemon::main(argv).await,
    }
}

fn env_path(key: &str, default: &str) -> PathBuf {
    std::env::var_os(key).map_or_else(|| PathBuf::from(default), PathBuf::from)
}
