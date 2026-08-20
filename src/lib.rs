pub mod backend;
pub mod buildid;
pub mod cgroup;
pub mod config;
pub mod core_pattern;
pub mod crictl;
pub mod events;
pub mod handler;
pub mod health;
pub mod k8s_events;
pub mod manifest;
pub mod ratelimit;
pub mod redact;
pub mod snapshot;
pub mod upload;

/// Host path the `DaemonSet` installs the binary to, so the kernel can exec it as
/// the `core_pattern` pipe target. Overridable via `CAPTURE_HANDLER_PATH`.
pub const DEFAULT_HANDLER_PATH: &str = "/opt/coredrop/bin/coredrop";

/// Helpers shared by the unit-test modules in this crate.
#[cfg(test)]
pub(crate) mod testutil {
    /// `std::env` is process-global and `set_var` is unsound to race, so every
    /// env-mutating test in this binary takes this one lock. A per-module lock
    /// would serialize only against itself, which is no guarantee at all.
    pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// A core stream that never yields data nor EOF - stands in for a hung
    /// kernel pipe or a wedged store, so deadline handling can be exercised.
    pub(crate) struct StallReader;

    impl tokio::io::AsyncRead for StallReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }
}
