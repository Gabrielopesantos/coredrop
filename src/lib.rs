pub mod backend;
pub mod buildid;
pub mod cgroup;
pub mod config;
pub mod core_pattern;
pub mod crictl;
pub mod events;
pub mod handler;
pub mod k8s_events;
pub mod manifest;
pub mod ratelimit;
pub mod redact;
pub mod snapshot;
pub mod systemd;
pub mod upload;

/// Host path the `DaemonSet` installs the binary to, so the kernel can exec it as
/// the `core_pattern` pipe target. Overridable via `CAPTURE_HANDLER_PATH`.
pub const DEFAULT_HANDLER_PATH: &str = "/opt/coredrop/bin/coredrop";
