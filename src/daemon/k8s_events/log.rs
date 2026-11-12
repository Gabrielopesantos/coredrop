//! The daemon's per-capture log line (`capture spooled` / `capture complete`).

use tracing::{info, warn};

use crate::events::{CaptureEventPayload, Outcome};

/// Which log line a capture gets.
#[derive(Clone, Copy)]
pub(super) enum Stage {
    /// The handler wrote a spool bundle; the uploader has it next.
    Spooled,
    /// The capture is done: uploaded, or ended without a bundle to upload.
    Complete,
}

/// The daemon's copy of the handler's capture line. The kernel gives the
/// handler no stdout, so this is the one operators see.
pub(super) fn log_capture(p: &CaptureEventPayload, stage: Stage) {
    let none = "<none>";
    macro_rules! line {
        ($level:ident, $msg:literal) => {
            $level!(
                outcome = p.outcome.as_str(),
                namespace = p.namespace.as_deref().unwrap_or(none),
                pod = p.pod_name.as_deref().unwrap_or(none),
                container = p.container_name.as_deref().unwrap_or(none),
                pod_uid = p.pod_uid.as_deref().unwrap_or(none),
                host_pid = p.host_pid,
                exe = %p.exe,
                signal = p.signal_name.clone().unwrap_or_else(|| p.signal.to_string()),
                manifest_key = p.manifest_key.as_deref().unwrap_or(none),
                core_bytes = p.core_bytes,
                stored_bytes = p.stored_bytes.unwrap_or(0),
                truncated = p.truncated,
                error = p.error.as_deref().unwrap_or(""),
                $msg
            )
        };
    }
    match stage {
        Stage::Spooled => line!(info, "capture spooled"),
        Stage::Complete if p.outcome == Outcome::Failed => line!(warn, "capture complete"),
        Stage::Complete => line!(info, "capture complete"),
    }
}
