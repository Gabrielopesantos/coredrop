//! Daemon side: listener for [`crate::events::CaptureEventPayload`] datagrams
//! plus a minimal k8s Event client, so `kubectl describe pod` /
//! `kubectl get events` can surface a capture.
//!
//! The handler sends the payload (see [`crate::events`]); this module owns
//! everything downstream of that socket - aggregation and the API calls -
//! which needs the projected `ServiceAccount` token and only ever runs in the
//! long-lived daemon.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixDatagram as StdUnixDatagram;
use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::Value;
use tracing::{debug, warn};

use crate::config::ensure_private_dir;
use crate::events::{CaptureEventPayload, Outcome};

const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Aggregation TTL: bounds how long a crash-loop's Event `series` count keeps
/// growing before a fresh Event (and count) starts. Matches the API server's
/// default Event TTL, so this never outlives the object it's tracking.
const AGGREGATION_TTL: Duration = Duration::from_hours(1);

/// Mode for the capture-event unix socket. The parent directory is already
/// `0700` (only owner/root can reach it);
const EVENT_SOCKET_MODE: u32 = 0o600;

/// Bind the capture-event unix datagram socket, removing a stale file left by
/// an unclean previous shutdown first (binding to an existing path otherwise
/// fails with `AddrInUse`).
///
/// The parent dir is created mode `0700`, matching the handler-config
/// dir on the same hostPath. The socket file itself is chmod'd to `0600`.
///
/// # Errors
///
/// Fails when the parent dir cannot be created/chmod'd, the bind itself fails,
/// or the socket's permissions cannot be tightened.
pub fn bind_socket(path: &str) -> std::io::Result<tokio::net::UnixDatagram> {
    if let Some(parent) = Path::new(path).parent() {
        ensure_private_dir(parent)?;
    }
    let _ = std::fs::remove_file(path);
    let std_socket = StdUnixDatagram::bind(path)?;
    std_socket.set_nonblocking(true)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(EVENT_SOCKET_MODE))?;
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

/// Maximum number of distinct `(namespace, pod_uid, reason)` aggregation
/// keys held in memory. Beyond this, new keys are dropped rather than
/// allowing unbounded growth.
const MAX_AGGREGATION_KEYS: usize = 10_000;

/// How often the aggregator is swept for expired entries, even when no new
/// capture events arrive. Quiet daemons must not hold stale entries forever.
const AGGREGATION_SWEEP_INTERVAL: Duration = Duration::from_mins(5);

/// Maximum length Kubernetes accepts for a namespace or resource name. Used as a
/// cheap sanity bound on the capture-event payload so obviously spoofed values
/// are rejected before any API server call.
const MAX_K8S_NAME_LEN: usize = 253;

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

    /// Sweep expired entries. Public so `run_listener` can call it on a
    /// periodic timer even when no events are being recorded.
    fn sweep(&mut self, now: Instant) {
        self.prune(now);
    }

    /// Record one occurrence for `key`. `make_name` is only called when a new
    /// entry is created (first occurrence, or the previous one expired).
    /// Returns `None` when the in-memory key cap has been reached and `key`
    /// is not already tracked.
    fn record(
        &mut self,
        key: AggKey,
        now: Instant,
        make_name: impl FnOnce() -> String,
    ) -> Option<Recorded> {
        self.prune(now);
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.count += 1;
            return Some(Recorded {
                event_name: entry.event_name.clone(),
                is_new: false,
                count: entry.count,
            });
        }
        if self.entries.len() >= MAX_AGGREGATION_KEYS {
            warn!(
                cap = MAX_AGGREGATION_KEYS,
                "aggregator key cap reached; dropping capture event"
            );
            return None;
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
        Some(Recorded {
            event_name,
            is_new: true,
            count: 1,
        })
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
            // The API server must never wedge the capture-event loop on an
            // in-flight request; all event posts are best-effort.
            .timeout(Duration::from_secs(30))
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

#[cfg(test)]
impl EventClient {
    /// Build an `EventClient` directly for unit tests. The underlying HTTP
    /// client is supplied by the caller so the test can control timeouts.
    fn new_for_test(http: reqwest::Client, api_server: String, token: String) -> Self {
        Self {
            http,
            api_server,
            token,
        }
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
///
/// Even when no events arrive, the aggregator is swept periodically so stale
/// entries do not accumulate indefinitely.
pub async fn run_listener(socket: tokio::net::UnixDatagram, node: String) {
    let client = EventClient::from_env();
    if client.is_none() {
        warn!(
            "kubernetes API client unavailable (no in-cluster ServiceAccount/API server found); capture events will be dropped"
        );
    }
    let mut aggregator = Aggregator::default();
    let mut buf = vec![0u8; 64 * 1024];
    let mut sweep_interval = tokio::time::interval(AGGREGATION_SWEEP_INTERVAL);
    sweep_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            n = socket.recv(&mut buf) => {
                let n = match n {
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
            _ = sweep_interval.tick() => {
                aggregator.sweep(Instant::now());
            }
        }
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

    if payload.namespace.is_empty()
        || payload.namespace.len() > MAX_K8S_NAME_LEN
        || payload.pod_uid.is_empty()
        || payload.pod_uid.len() > MAX_K8S_NAME_LEN
    {
        warn!(
            namespace = %payload.namespace,
            pod_uid = %payload.pod_uid,
            "capture event payload has empty or oversized namespace/pod_uid; dropping"
        );
        return;
    }

    let key = AggKey {
        namespace: payload.namespace.clone(),
        pod_uid: payload.pod_uid.clone(),
        reason,
    };
    let Some(recorded) = aggregator.record(key, Instant::now(), || {
        event_name(&payload.pod_uid, reason, payload.timestamp)
    }) else {
        return;
    };
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
    use tempfile::TempDir;

    use super::*;

    fn payload(namespace: &str, pod_uid: &str) -> CaptureEventPayload {
        CaptureEventPayload {
            namespace: namespace.into(),
            pod_name: "my-pod".into(),
            pod_uid: pod_uid.into(),
            container_name: Some("app".into()),
            signal: 11,
            signal_name: Some("SIGSEGV".into()),
            outcome: Outcome::Uploaded,
            manifest_key: Some("local/abc-123/def456/1234567890-manifest.json".into()),
            stored_bytes: Some(1024),
            timestamp: 1_749_600_000,
        }
    }

    fn agg_key(pod_uid: &str, reason: &'static str) -> AggKey {
        AggKey {
            namespace: "default".into(),
            pod_uid: pod_uid.into(),
            reason,
        }
    }

    /// Bind a socket at `<tempdir>/events.sock` on a current-thread runtime,
    /// returning the dir so the caller can inspect modes before it is removed.
    fn bind_in_temp_dir(pre: impl FnOnce(&Path)) -> TempDir {
        let dir = TempDir::new().unwrap();
        pre(dir.path());
        let sock_path = dir.path().join("events.sock");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        rt.block_on(async {
            let _socket = bind_socket(sock_path.to_str().unwrap()).unwrap();
        });
        dir
    }

    #[test]
    fn k8s_reason_maps_only_reportable_outcomes() {
        assert_eq!(k8s_reason(Outcome::Uploaded), Some("CoreDumped"));
        assert_eq!(
            k8s_reason(Outcome::SuppressedRateLimit),
            Some("CoreDumpSuppressed")
        );
        assert_eq!(
            k8s_reason(Outcome::NoStoreDiscard),
            Some("CoreDiscardedNoStore")
        );
        // These two never reach the listener: `skipped-non-k8s` has no pod to
        // target, and `failed` returns before the event is sent.
        assert_eq!(k8s_reason(Outcome::SkippedNonK8s), None);
        assert_eq!(k8s_reason(Outcome::Failed), None);
    }

    #[test]
    fn aggregator_bumps_count_for_repeats_within_ttl() {
        let mut agg = Aggregator::default();
        let key = agg_key("abc-123", "CoreDumped");
        let t0 = Instant::now();

        let first = agg.record(key.clone(), t0, || "ev-1".to_string()).unwrap();
        assert!(first.is_new);
        assert_eq!(first.count, 1);

        let second = agg
            .record(key.clone(), t0 + Duration::from_secs(10), || {
                "ev-2".to_string()
            })
            .unwrap();
        assert!(!second.is_new);
        assert_eq!(second.count, 2);
        assert_eq!(
            second.event_name, first.event_name,
            "a repeat must patch the original Event, not name a new one"
        );

        let third = agg
            .record(key, t0 + Duration::from_secs(20), || "ev-3".to_string())
            .unwrap();
        assert!(!third.is_new);
        assert_eq!(third.count, 3);
    }

    #[test]
    fn aggregator_separates_distinct_keys() {
        let mut agg = Aggregator::default();
        let t0 = Instant::now();
        // Different pod, and same pod with a different reason: both are their
        // own Event series.
        assert!(
            agg.record(agg_key("pod-a", "CoreDumped"), t0, || "a".into())
                .unwrap()
                .is_new
        );
        assert!(
            agg.record(agg_key("pod-b", "CoreDumped"), t0, || "b".into())
                .unwrap()
                .is_new
        );
        assert!(
            agg.record(agg_key("pod-a", "CoreDumpSuppressed"), t0, || "c".into())
                .unwrap()
                .is_new
        );
    }

    #[test]
    fn aggregator_re_creates_after_ttl_expiry() {
        let mut agg = Aggregator::default();
        let key = agg_key("abc-123", "CoreDumped");
        let t0 = Instant::now();
        assert!(
            agg.record(key.clone(), t0, || "ev-1".to_string())
                .unwrap()
                .is_new
        );

        let after_ttl = t0 + AGGREGATION_TTL + Duration::from_secs(1);
        let renewed = agg.record(key, after_ttl, || "ev-2".to_string()).unwrap();
        assert!(renewed.is_new, "entry past its TTL should be re-created");
        assert_eq!(renewed.count, 1);
        assert_eq!(renewed.event_name, "ev-2");
    }

    #[test]
    fn aggregator_stops_inserting_new_keys_at_cap() {
        let mut agg = Aggregator::default();
        let t0 = Instant::now();

        for i in 0..MAX_AGGREGATION_KEYS {
            let recorded = agg
                .record(agg_key(&format!("pod-{i}"), "CoreDumped"), t0, || {
                    format!("ev-{i}")
                })
                .unwrap();
            assert!(recorded.is_new);
        }
        assert_eq!(agg.entries.len(), MAX_AGGREGATION_KEYS);

        assert!(
            agg.record(agg_key("overflow", "CoreDumped"), t0, || "overflow".into())
                .is_none()
        );
        assert_eq!(agg.entries.len(), MAX_AGGREGATION_KEYS);

        // Repeating an existing key still bumps its count.
        let recorded = agg
            .record(agg_key("pod-0", "CoreDumped"), t0, || {
                "should-not-run".into()
            })
            .unwrap();
        assert!(!recorded.is_new);
        assert_eq!(recorded.count, 2);
    }

    #[test]
    fn aggregator_sweep_removes_expired_entries() {
        // The sweep exists for quiet daemons: without it, entries only expire
        // when a new capture happens to call `record`.
        let mut agg = Aggregator::default();
        let key = agg_key("abc-123", "CoreDumped");
        let t0 = Instant::now();
        agg.record(key.clone(), t0, || "ev-1".into()).unwrap();
        assert_eq!(agg.entries.len(), 1);

        let after_ttl = t0 + AGGREGATION_TTL + Duration::from_secs(1);
        agg.sweep(after_ttl);
        assert!(agg.entries.is_empty());

        let recorded = agg.record(key, after_ttl, || "ev-2".into()).unwrap();
        assert!(recorded.is_new);
        assert_eq!(recorded.event_name, "ev-2");
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

    /// The body is what the API server validates; a wrong field name or a
    /// mistyped `eventTime` is a 400 that only shows up in production.
    #[test]
    fn event_body_targets_the_pod_and_carries_a_decodable_event_time() {
        let payload = payload("default", "abc-123");
        let name = event_name(&payload.pod_uid, "CoreDumped", payload.timestamp);
        let body = event_body("node-a", &name, "CoreDumped", &payload);

        assert_eq!(body["apiVersion"], "events.k8s.io/v1");
        assert_eq!(body["kind"], "Event");
        assert_eq!(body["metadata"]["name"], name.as_str());
        assert_eq!(body["metadata"]["namespace"], "default");
        assert_eq!(body["reportingController"], "coredrop");
        assert_eq!(body["reportingInstance"], "node-a");
        assert_eq!(body["reason"], "CoreDumped");
        assert_eq!(body["type"], "Warning");
        assert_eq!(body["eventTime"], "2025-06-11T00:00:00.000000Z");

        // `regarding` is what makes this show up under `kubectl describe pod`.
        assert_eq!(body["regarding"]["kind"], "Pod");
        assert_eq!(body["regarding"]["apiVersion"], "v1");
        assert_eq!(body["regarding"]["namespace"], "default");
        assert_eq!(body["regarding"]["name"], "my-pod");
        assert_eq!(body["regarding"]["uid"], "abc-123");
    }

    #[test]
    fn note_points_at_the_artifacts_when_a_manifest_was_written() {
        let mut payload = payload("default", "abc-123");
        assert_eq!(
            note(&payload),
            "core dumped (signal SIGSEGV); artifacts at local/abc-123/def456/1234567890-manifest.json"
        );

        // Manifest write failed: the note still reports the crash.
        payload.manifest_key = None;
        assert_eq!(note(&payload), "core dumped (signal SIGSEGV)");

        // Unmapped signal number: fall back to the raw number, never omit it.
        payload.signal = 31;
        payload.signal_name = None;
        assert_eq!(note(&payload), "core dumped (signal 31)");
    }

    #[test]
    fn series_patch_carries_the_running_count() {
        let body = series_patch(7, "2025-06-11T00:00:00.000000Z");
        assert_eq!(body["series"]["count"], 7);
        assert_eq!(
            body["series"]["lastObservedTime"],
            "2025-06-11T00:00:00.000000Z"
        );
    }

    #[test]
    fn bind_socket_replaces_a_stale_socket_file() {
        // An unclean shutdown leaves the path behind; binding over it would
        // otherwise fail with AddrInUse and take the daemon down on restart.
        bind_in_temp_dir(|dir| {
            std::fs::write(dir.join("events.sock"), b"stale").unwrap();
        });
    }

    #[test]
    fn bind_socket_creates_a_0700_dir_holding_a_0600_socket() {
        use std::os::unix::fs::MetadataExt;

        let dir = bind_in_temp_dir(|_| {});
        let sock_path = dir.path().join("events.sock");

        let dir_mode = std::fs::metadata(dir.path()).unwrap().mode() & 0o777;
        assert_eq!(
            dir_mode, 0o700,
            "events socket parent dir should be mode 0700"
        );
        let sock_mode = std::fs::metadata(&sock_path).unwrap().mode() & 0o777;
        assert_eq!(sock_mode, 0o600, "events socket should be mode 0600");
    }

    #[tokio::test]
    async fn handle_payload_rejects_invalid_namespace_or_pod_uid() {
        // A spoofed payload must be rejected before any API server call, so
        // nothing is recorded in the aggregator either.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        let client = EventClient::new_for_test(http, "http://127.0.0.1:1".into(), "test".into());

        let mut agg = Aggregator::default();
        let long = "x".repeat(MAX_K8S_NAME_LEN + 1);

        for (namespace, pod_uid, label) in [
            ("", "valid-uid", "empty namespace"),
            (long.as_str(), "valid-uid", "oversized namespace"),
            ("default", "", "empty pod uid"),
            ("default", long.as_str(), "oversized pod uid"),
        ] {
            handle_payload(
                Some(&client),
                &mut agg,
                "test-node",
                &payload(namespace, pod_uid),
            )
            .await;
            assert!(agg.entries.is_empty(), "{label} must be dropped");
        }

        // A valid payload is recorded even though the API call itself fails -
        // a broken API server must not lose the aggregation state.
        handle_payload(
            Some(&client),
            &mut agg,
            "test-node",
            &payload("default", "valid-uid"),
        )
        .await;
        assert_eq!(agg.entries.len(), 1);
    }
}
