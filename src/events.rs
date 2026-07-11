//! Capture-event payload and handler-side (fire-and-forget) sender.
//!
//! Flow: handler (kernel-exec'd, clean env) -> `SOCK_DGRAM` unix socket ->
//! daemon (holds the SA token) -> API server. This keeps the SA token and a
//! kube client out of the kernel-exec'd handler, which stays fire-and-forget:
//! a missing socket, a full send buffer, or an unreachable API server never
//! affects the capture itself. The daemon-side listener and k8s Event client
//! live in [`crate::k8s_events`].
//!
//! The payload also carries `outcome` and `stored_bytes` - not needed for
//! Events, but designed in now so a future metrics exporter needs no protocol
//! change.

use std::fmt::Display;
use std::os::unix::net::UnixDatagram as StdUnixDatagram;

use serde::{Deserialize, Serialize};
use tracing::warn;

/// Terminal state of one capture, for the single `capture complete` summary
/// log line and (when it reaches the manifest) the capture-event payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Outcome {
    Uploaded,
    ForwardedSystemd,
    SuppressedRateLimit,
    SkippedNonK8s,
    NoStoreDiscard,
    Failed,
}

impl Outcome {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Uploaded => "uploaded",
            Outcome::ForwardedSystemd => "forwarded-systemd",
            Outcome::SuppressedRateLimit => "suppressed-rate-limit",
            Outcome::SkippedNonK8s => "skipped-non-k8s",
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
/// `namespace` + `pod_name` are required (crictl enrichment must have
/// succeeded - a cgroup-only identity can't target a k8s object).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureEventPayload {
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub container_name: Option<String>,
    pub signal: i32,
    pub signal_name: Option<String>,
    pub outcome: Outcome,
    pub manifest_key: Option<String>,
    pub stored_bytes: Option<u64>,
    /// Crash timestamp, unix epoch seconds (the kernel's `%t` specifier).
    pub timestamp: i64,
}

/// Send one capture event datagram to the daemon. Best-effort: any failure is
/// logged and the capture is unaffected. `socket_path` is `None` when events
/// are disabled, which skips silently (no log noise on the hot path).
pub fn send_capture_event(socket_path: Option<&str>, payload: &CaptureEventPayload) {
    let Some(socket_path) = socket_path else {
        return;
    };
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
        warn!(error = %e, socket_path, "sending capture event failed");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn payload() -> CaptureEventPayload {
        CaptureEventPayload {
            namespace: "default".into(),
            pod_name: "my-pod".into(),
            pod_uid: "abc-123".into(),
            container_name: Some("app".into()),
            signal: 11,
            signal_name: Some("SIGSEGV".into()),
            outcome: Outcome::Uploaded,
            manifest_key: Some("local/abc-123/def456/1234567890-manifest.json".into()),
            stored_bytes: Some(1024),
            timestamp: 1_749_600_000,
        }
    }

    #[test]
    fn payload_round_trips_through_json() {
        let p = payload();
        let json = serde_json::to_vec(&p).unwrap();
        let back: CaptureEventPayload = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn send_capture_event_delivers_to_a_bound_socket() {
        let dir = std::env::temp_dir().join(format!(
            "coredrop-events-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let sock_path = dir.join("events.sock");
        let listener = StdUnixDatagram::bind(&sock_path).unwrap();
        listener
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        send_capture_event(sock_path.to_str(), &payload());

        let mut buf = vec![0u8; 64 * 1024];
        let n = listener.recv(&mut buf).unwrap();
        let got: CaptureEventPayload = serde_json::from_slice(&buf[..n]).unwrap();
        assert_eq!(got, payload());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn send_capture_event_skips_cleanly_when_socket_path_is_none() {
        // No socket, no panic, no log-worthy failure path exercised.
        send_capture_event(None, &payload());
    }

    #[test]
    fn send_capture_event_is_best_effort_when_nothing_is_listening() {
        // A path with no bound listener: send fails, but must not panic.
        send_capture_event(Some("/run/coredrop/no-such-listener.sock"), &payload());
    }
}
