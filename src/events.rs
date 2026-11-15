//! Capture-event payload and handler-side (fire-and-forget) sender.
//!
//! Flow: handler (kernel-exec'd, clean env) -> `SOCK_DGRAM` unix socket ->
//! daemon (holds the SA token) -> API server. The handler reports every
//! capture. The daemon logs each one, because the kernel-exec'd handler has no
//! stdout to log to. A capture that left a spool bundle is reported again by
//! the daemon's uploader once the bundle is uploaded; that final report, and
//! every other outcome, can become a k8s Event. This keeps the SA token and a
//! kube client out of the kernel-exec'd handler, which stays fire-and-forget:
//! a missing socket, a full send buffer, or an unreachable API server never
//! affects the capture itself. The spool scan does not depend on the
//! datagram either. The daemon-side listener and k8s Event client live in
//! [`crate::daemon::k8s_events`].

use std::fmt::Display;
use std::os::unix::net::UnixDatagram as StdUnixDatagram;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::warn;

/// Terminal state of one capture, for the single `capture complete` summary
/// log line and the capture-event payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[allow(missing_docs)] // DTO: variant names are self-explanatory.
pub enum Outcome {
    /// The handler wrote the core into the spool; the daemon uploads it.
    Spooled,
    Uploaded,
    SuppressedRateLimit,
    /// The spool had no room for the core; snapshot and manifest are spooled.
    SpoolFull,
    /// The core did not drain before the deadline; snapshot and manifest are
    /// spooled.
    CoreWriteTimeout,
    SkippedNonK8s,
    /// The capture filter dropped the crash; the core is not read.
    Filtered,
    NoStoreDiscard,
    Failed,
}

impl Outcome {
    /// The outcomes a capture can end with. [`Outcome::Spooled`] is not one:
    /// the uploader turns it into [`Outcome::Uploaded`] or [`Outcome::Failed`].
    pub const FINAL: [Outcome; 8] = [
        Outcome::Uploaded,
        Outcome::SuppressedRateLimit,
        Outcome::SpoolFull,
        Outcome::CoreWriteTimeout,
        Outcome::SkippedNonK8s,
        Outcome::Filtered,
        Outcome::NoStoreDiscard,
        Outcome::Failed,
    ];

    /// Whether the handler left a spool bundle for this capture. The daemon
    /// reports such a capture once the bundle is uploaded; every other
    /// outcome is final when the handler reports it.
    #[must_use]
    pub fn has_bundle(self) -> bool {
        matches!(
            self,
            Outcome::Spooled
                | Outcome::SuppressedRateLimit
                | Outcome::SpoolFull
                | Outcome::CoreWriteTimeout
        )
    }

    /// The `kebab-case` wire/log representation of this outcome.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Spooled => "spooled",
            Outcome::Uploaded => "uploaded",
            Outcome::SuppressedRateLimit => "suppressed-rate-limit",
            Outcome::SpoolFull => "spool-full",
            Outcome::CoreWriteTimeout => "core-write-timeout",
            Outcome::SkippedNonK8s => "skipped-non-k8s",
            Outcome::Filtered => "filtered",
            Outcome::NoStoreDiscard => "no-store-discard",
            Outcome::Failed => "failed",
        }
    }
}

impl Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// One capture, reported by the handler to the daemon over the events socket.
/// Sent for every outcome. The identity fields are `None` when they were not
/// resolved: `pod_uid`/`container_id` need a kubernetes cgroup, and
/// `namespace`/`pod_name`/`container_name` need crictl enrichment. Only a
/// payload with namespace + pod name can target a k8s Event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)] // DTO: undocumented fields are self-explanatory.
pub struct CaptureEventPayload {
    pub namespace: Option<String>,
    pub pod_name: Option<String>,
    pub pod_uid: Option<String>,
    pub container_id: Option<String>,
    pub container_name: Option<String>,
    /// PID of the crashed process in the host PID namespace (`%P`).
    pub host_pid: i32,
    /// Executable name of the crashed process (`%E`).
    pub exe: String,
    pub signal: i32,
    pub signal_name: Option<String>,
    pub outcome: Outcome,
    pub manifest_key: Option<String>,
    /// Uncompressed core bytes the handler read from the kernel; `0` when
    /// the handler did not read the core (rate limit, filter, no store).
    pub core_bytes: u64,
    pub stored_bytes: Option<u64>,
    pub truncated: bool,
    /// Why the capture failed, for `Outcome::Failed`.
    pub error: Option<String>,
    /// Crash timestamp, unix epoch seconds (the kernel's `%t` specifier).
    pub timestamp: i64,
}

/// Send one capture event datagram to the daemon. Best-effort: any failure is
/// logged and the capture is unaffected.
pub fn send_capture_event(socket_path: &Path, payload: &CaptureEventPayload) {
    let bytes = match serde_json::to_vec(payload) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "serializing capture event payload failed");
            return;
        }
    };
    let socket = match StdUnixDatagram::unbound() {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "creating capture event socket failed");
            return;
        }
    };
    if let Err(e) = socket.set_nonblocking(true) {
        warn!(error = %e, "setting capture event socket non-blocking failed");
        return;
    }
    if let Err(e) = socket.send_to(&bytes, socket_path) {
        warn!(error = %e, socket_path = %socket_path.display(), "sending capture event failed");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    fn payload() -> CaptureEventPayload {
        CaptureEventPayload {
            namespace: Some("default".into()),
            pod_name: Some("my-pod".into()),
            pod_uid: Some("abc-123".into()),
            container_id: Some("def456".into()),
            container_name: Some("app".into()),
            host_pid: 4242,
            exe: "server".into(),
            signal: 11,
            signal_name: Some("SIGSEGV".into()),
            outcome: Outcome::Uploaded,
            manifest_key: Some("local/abc-123/def456/1234567890-manifest.json".into()),
            core_bytes: 4096,
            stored_bytes: Some(1024),
            truncated: false,
            error: None,
            timestamp: 1_749_600_000,
        }
    }

    #[test]
    fn payload_round_trips_through_json() {
        // The handler serializes and the daemon deserializes across a socket,
        // so the two ends only agree if this holds - including `outcome`'s
        // kebab-case representation.
        let p = payload();
        let json = serde_json::to_vec(&p).unwrap();
        assert!(
            String::from_utf8_lossy(&json).contains(r#""outcome":"uploaded""#),
            "outcome must travel as its kebab-case name"
        );
        let back: CaptureEventPayload = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn payload_without_identity_round_trips() {
        // A host process crash: no kubernetes cgroup, no crictl identity.
        let p = CaptureEventPayload {
            namespace: None,
            pod_name: None,
            pod_uid: None,
            container_id: None,
            container_name: None,
            outcome: Outcome::SkippedNonK8s,
            manifest_key: None,
            stored_bytes: None,
            ..payload()
        };
        let back: CaptureEventPayload =
            serde_json::from_slice(&serde_json::to_vec(&p).unwrap()).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn send_capture_event_delivers_to_a_bound_socket() {
        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join("events.sock");
        let listener = StdUnixDatagram::bind(&sock_path).unwrap();
        listener
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        send_capture_event(&sock_path, &payload());

        let mut buf = vec![0u8; 64 * 1024];
        let n = listener.recv(&mut buf).unwrap();
        let got: CaptureEventPayload = serde_json::from_slice(&buf[..n]).unwrap();
        assert_eq!(got, payload());
    }

    #[test]
    fn send_capture_event_is_best_effort() {
        // Nothing listening (daemon restarting, socket removed): the send
        // fails, and the capture must be unaffected.
        send_capture_event(Path::new("/run/coredrop/no-such-listener.sock"), &payload());
    }
}
