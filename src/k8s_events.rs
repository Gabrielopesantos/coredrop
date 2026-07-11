//! Daemon side: listener for [`crate::events::CaptureEventPayload`] datagrams
//! plus a minimal k8s Event client, so `kubectl describe pod` /
//! `kubectl get events` can surface a capture.
//!
//! The handler sends the payload (see [`crate::events`]); this module owns
//! everything downstream of that socket - aggregation and the API calls -
//! which needs the projected `ServiceAccount` token and only ever runs in the
//! long-lived daemon.

use std::collections::HashMap;
use std::os::unix::net::UnixDatagram as StdUnixDatagram;
use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::Value;
use tracing::{debug, warn};

use crate::events::{CaptureEventPayload, Outcome};

const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Aggregation TTL: bounds how long a crash-loop's Event `series` count keeps
/// growing before a fresh Event (and count) starts. Matches the API server's
/// default Event TTL, so this never outlives the object it's tracking.
const AGGREGATION_TTL: Duration = Duration::from_hours(1);

/// Bind the capture-event unix datagram socket, removing a stale file left by
/// an unclean previous shutdown first (binding to an existing path otherwise
/// fails with `AddrInUse`).
///
/// # Errors
///
/// Fails when the parent dir cannot be created or the bind itself fails.
pub fn bind_socket(path: &str) -> std::io::Result<tokio::net::UnixDatagram> {
    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _ = std::fs::remove_file(path);
    let std_socket = StdUnixDatagram::bind(path)?;
    std_socket.set_nonblocking(true)?;
    tokio::net::UnixDatagram::from_std(std_socket)
}

/// Map a capture outcome to the k8s Event `reason` it produces. Outcomes that
/// never reach this point (`skipped-non-k8s`, `failed`) or are otherwise
/// unrecognized yield `None` - the caller drops the payload.
fn k8s_reason(outcome: Outcome) -> Option<&'static str> {
    match outcome {
        Outcome::Uploaded => Some("CoreDumped"),
        Outcome::SuppressedRateLimit => Some("CoreDumpSuppressed"),
        Outcome::NoStoreDiscard => Some("CoreDiscardedNoStore"),
        _ => None,
    }
}

/// Aggregation key: repeats of the same (pod, reason) bump a `series` count
/// on one Event object instead of creating a new one per crash.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AggKey {
    namespace: String,
    pod_uid: String,
    reason: &'static str,
}

struct AggEntry {
    event_name: String,
    count: u32,
    created_at: Instant,
}

/// In-memory `(namespace, pod_uid, reason) -> Event` aggregation map. Entries
/// older than [`AGGREGATION_TTL`] are dropped and re-created rather than kept
/// alive forever - simpler than tracking the API server's actual Event TTL.
#[derive(Default)]
struct Aggregator {
    entries: HashMap<AggKey, AggEntry>,
}

/// Outcome of recording one capture against the aggregator.
struct Recorded {
    event_name: String,
    is_new: bool,
    count: u32,
}

impl Aggregator {
    fn prune(&mut self, now: Instant) {
        self.entries
            .retain(|_, e| now.duration_since(e.created_at) < AGGREGATION_TTL);
    }

    /// Record one occurrence for `key`. `make_name` is only called when a new
    /// entry is created (first occurrence, or the previous one expired).
    fn record(
        &mut self,
        key: AggKey,
        now: Instant,
        make_name: impl FnOnce() -> String,
    ) -> Recorded {
        self.prune(now);
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.count += 1;
            return Recorded {
                event_name: entry.event_name.clone(),
                is_new: false,
                count: entry.count,
            };
        }
        let event_name = make_name();
        self.entries.insert(
            key,
            AggEntry {
                event_name: event_name.clone(),
                count: 1,
                created_at: now,
            },
        );
        Recorded {
            event_name,
            is_new: true,
            count: 1,
        }
    }
}

/// Minimal `events.k8s.io/v1` client: create on first occurrence, `PATCH`
/// `series` on repeats. Hand-rolled with `reqwest` rather than `kube-rs` - one
/// endpoint doesn't justify the dependency tree.
struct EventClient {
    http: reqwest::Client,
    api_server: String,
    token: String,
}

impl EventClient {
    /// Build a client from the projected `ServiceAccount` token/CA and the
    /// in-cluster API server env vars. `None` when any of those are missing -
    /// e.g. running outside a cluster - and the caller logs + drops events.
    fn from_env() -> Option<Self> {
        let token = std::fs::read_to_string(format!("{SERVICE_ACCOUNT_DIR}/token"))
            .ok()?
            .trim()
            .to_string();
        let ca_pem = std::fs::read(format!("{SERVICE_ACCOUNT_DIR}/ca.crt")).ok()?;
        let cert = reqwest::Certificate::from_pem(&ca_pem).ok()?;
        let host = std::env::var("KUBERNETES_SERVICE_HOST").ok()?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".to_string());
        let host = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host
        };
        let http = reqwest::Client::builder()
            .add_root_certificate(cert)
            .build()
            .ok()?;
        Some(Self {
            http,
            api_server: format!("https://{host}:{port}"),
            token,
        })
    }

    async fn create_event(&self, namespace: &str, body: &Value) -> Result<(), String> {
        let url = format!(
            "{}/apis/events.k8s.io/v1/namespaces/{namespace}/events",
            self.api_server
        );
        let resp = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        api_result(resp, "create event").await
    }

    async fn patch_event_series(
        &self,
        namespace: &str,
        name: &str,
        body: &Value,
    ) -> Result<(), String> {
        let url = format!(
            "{}/apis/events.k8s.io/v1/namespaces/{namespace}/events/{name}",
            self.api_server
        );
        let resp = self
            .http
            .patch(url)
            .bearer_auth(&self.token)
            .header("Content-Type", "application/merge-patch+json")
            .json(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        api_result(resp, "patch event series").await
    }
}

/// Turn a non-2xx response into an `Err` carrying the API server's response
/// body - the status code alone ("HTTP 400") gives no way to tell a bad
/// request body from an RBAC/admission rejection.
async fn api_result(resp: reqwest::Response, what: &str) -> Result<(), String> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    let body = body.chars().take(500).collect::<String>();
    Err(format!("{what}: HTTP {status}: {body}"))
}

/// Listen for capture-event datagrams and post/patch k8s Events until the
/// socket is closed. Every failure path is best-effort: logged and swallowed,
/// never propagated - a broken API server must never crash the daemon.
pub async fn run_listener(socket: tokio::net::UnixDatagram, node: String) {
    let client = EventClient::from_env();
    if client.is_none() {
        warn!(
            "kubernetes API client unavailable (no in-cluster ServiceAccount/API server found); capture events will be dropped"
        );
    }
    let mut aggregator = Aggregator::default();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match socket.recv(&mut buf).await {
            Ok(n) => n,
            Err(e) => {
                warn!(error = %e, "capture event socket recv failed");
                continue;
            }
        };
        let payload: CaptureEventPayload = match serde_json::from_slice(&buf[..n]) {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "capture event payload parse failed; dropping");
                continue;
            }
        };
        handle_payload(client.as_ref(), &mut aggregator, &node, &payload).await;
    }
}

async fn handle_payload(
    client: Option<&EventClient>,
    aggregator: &mut Aggregator,
    node: &str,
    payload: &CaptureEventPayload,
) {
    let Some(reason) = k8s_reason(payload.outcome) else {
        debug!(outcome = %payload.outcome, "capture outcome has no k8s Event mapping; ignoring");
        return;
    };
    let Some(client) = client else {
        return;
    };

    let key = AggKey {
        namespace: payload.namespace.clone(),
        pod_uid: payload.pod_uid.clone(),
        reason,
    };
    let recorded = aggregator.record(key, Instant::now(), || {
        event_name(&payload.pod_uid, reason, payload.timestamp)
    });
    let last_observed = rfc3339_micro(payload.timestamp);

    let result = if recorded.is_new {
        let body = event_body(node, &recorded.event_name, reason, payload);
        client.create_event(&payload.namespace, &body).await
    } else {
        let body = series_patch(recorded.count, &last_observed);
        client
            .patch_event_series(&payload.namespace, &recorded.event_name, &body)
            .await
    };
    if let Err(e) = result {
        warn!(error = %e, event = %recorded.event_name, namespace = %payload.namespace, "posting kubernetes event failed");
    }
}

/// DNS-1123-subdomain-safe Event name: pod UID is already a lowercase UUID,
/// the reason lowercased has no invalid characters, and the timestamp keeps
/// re-created (post-TTL-expiry) entries for the same pod+reason unique.
fn event_name(pod_uid: &str, reason: &str, timestamp: i64) -> String {
    format!("coredrop-{pod_uid}-{}-{timestamp}", reason.to_lowercase())
}

fn event_body(
    node: &str,
    name: &str,
    reason: &'static str,
    payload: &CaptureEventPayload,
) -> Value {
    serde_json::json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {
            "name": name,
            "namespace": payload.namespace,
        },
        "eventTime": rfc3339_micro(payload.timestamp),
        "reportingController": "coredrop",
        "reportingInstance": node,
        "action": "CoreCaptured",
        "reason": reason,
        "regarding": {
            "kind": "Pod",
            "apiVersion": "v1",
            "namespace": payload.namespace,
            "name": payload.pod_name,
            "uid": payload.pod_uid,
        },
        "note": note(payload),
        "type": "Warning",
    })
}

fn series_patch(count: u32, last_observed: &str) -> Value {
    serde_json::json!({
        "series": {
            "count": count,
            "lastObservedTime": last_observed,
        }
    })
}

fn note(payload: &CaptureEventPayload) -> String {
    let signal = payload
        .signal_name
        .clone()
        .unwrap_or_else(|| payload.signal.to_string());
    match &payload.manifest_key {
        Some(key) => format!("core dumped (signal {signal}); artifacts at {key}"),
        None => format!("core dumped (signal {signal})"),
    }
}

/// Format as `metav1.MicroTime` expects: exactly 6 fractional-second digits
/// and a literal `Z`. Go's JSON decoder for `MicroTime` parses against a
/// fixed-width reference layout (`.000000`), so a value with zero or a
/// different number of fractional digits - which plain `to_rfc3339()`
/// produces for a whole-second timestamp - fails to decode and the API
/// server 400s the request.
fn rfc3339_micro(epoch_secs: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(epoch_secs, 0).map_or_else(
        || epoch_secs.to_string(),
        |dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn outcome_as_str_matches_k8s_reason_mapping() {
        assert_eq!(k8s_reason(Outcome::Uploaded), Some("CoreDumped"));
        assert_eq!(
            k8s_reason(Outcome::SuppressedRateLimit),
            Some("CoreDumpSuppressed")
        );
        assert_eq!(
            k8s_reason(Outcome::NoStoreDiscard),
            Some("CoreDiscardedNoStore")
        );
        assert_eq!(k8s_reason(Outcome::SkippedNonK8s), None);
        assert_eq!(k8s_reason(Outcome::Failed), None);
    }

    #[test]
    fn aggregator_bumps_count_for_repeats_within_ttl() {
        let mut agg = Aggregator::default();
        let key = AggKey {
            namespace: "default".into(),
            pod_uid: "abc-123".into(),
            reason: "CoreDumped",
        };
        let t0 = Instant::now();

        let first = agg.record(key.clone(), t0, || "ev-1".to_string());
        assert!(first.is_new);
        assert_eq!(first.count, 1);

        let second = agg.record(key.clone(), t0 + Duration::from_secs(10), || {
            "ev-2".to_string()
        });
        assert!(!second.is_new);
        assert_eq!(second.count, 2);
        assert_eq!(second.event_name, first.event_name);

        let third = agg.record(key, t0 + Duration::from_secs(20), || "ev-3".to_string());
        assert!(!third.is_new);
        assert_eq!(third.count, 3);
    }

    #[test]
    fn aggregator_separates_distinct_keys() {
        let mut agg = Aggregator::default();
        let t0 = Instant::now();
        let a = AggKey {
            namespace: "default".into(),
            pod_uid: "pod-a".into(),
            reason: "CoreDumped",
        };
        let b = AggKey {
            namespace: "default".into(),
            pod_uid: "pod-b".into(),
            reason: "CoreDumped",
        };
        let c = AggKey {
            namespace: "default".into(),
            pod_uid: "pod-a".into(),
            reason: "CoreDumpSuppressed",
        };
        assert!(agg.record(a, t0, || "a".into()).is_new);
        assert!(agg.record(b, t0, || "b".into()).is_new);
        assert!(agg.record(c, t0, || "c".into()).is_new);
    }

    #[test]
    fn aggregator_re_creates_after_ttl_expiry() {
        let mut agg = Aggregator::default();
        let key = AggKey {
            namespace: "default".into(),
            pod_uid: "abc-123".into(),
            reason: "CoreDumped",
        };
        let t0 = Instant::now();
        let first = agg.record(key.clone(), t0, || "ev-1".to_string());
        assert!(first.is_new);

        let after_ttl = t0 + AGGREGATION_TTL + Duration::from_secs(1);
        let renewed = agg.record(key, after_ttl, || "ev-2".to_string());
        assert!(renewed.is_new, "entry past its TTL should be re-created");
        assert_eq!(renewed.count, 1);
        assert_eq!(renewed.event_name, "ev-2");
    }

    /// Regression test: `metav1.MicroTime`'s Go decoder parses against a
    /// fixed-width `.000000` reference layout, so a value missing the
    /// fractional part (what plain `to_rfc3339()` produces for a
    /// zero-nanosecond timestamp) fails to decode and the API server 400s
    /// event creation - this exact bug shipped once already.
    #[test]
    fn rfc3339_micro_always_has_six_fractional_digits() {
        assert_eq!(rfc3339_micro(1_749_600_000), "2025-06-11T00:00:00.000000Z");
    }

    #[test]
    fn event_name_is_dns_subdomain_safe() {
        let name = event_name("abc-123-def", "CoreDumped", 1_749_600_000);
        assert_eq!(name, "coredrop-abc-123-def-coredumped-1749600000");
        assert!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        );
    }

    #[test]
    fn bind_socket_replaces_a_stale_socket_file() {
        let dir = std::env::temp_dir().join(format!(
            "coredrop-events-bind-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let sock_path = dir.join("events.sock");
        // Simulate a stale file left by an unclean shutdown (not a real socket).
        std::fs::write(&sock_path, b"stale").unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        rt.block_on(async {
            let _socket = bind_socket(sock_path.to_str().unwrap()).unwrap();
        });

        std::fs::remove_dir_all(&dir).ok();
    }
}
