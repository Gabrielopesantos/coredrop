//! Daemon side: listener for [`crate::events::CaptureEventPayload`]s plus a
//! minimal k8s Event client. The listener logs one `capture complete` line
//! per capture, so `kubectl logs` shows every capture, and posts a k8s Event
//! for the ones that target a pod, so `kubectl describe pod`/
//! `kubectl get events` can surface them.
//!
//! Payloads come from two places: the handler's datagrams (see
//! [`crate::events`]) and the uploader's reports (see [`crate::daemon::uploader`]).
//! A datagram for a capture that left a spool bundle is logged as
//! `capture spooled` and wakes the uploader; the capture is complete once the
//! uploader reports the uploaded bundle. This module owns logging,
//! aggregation and the API calls - which need the projected `ServiceAccount`
//! token and only ever run in the long-lived daemon.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixDatagram as StdUnixDatagram;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::cgroup::is_pod_uid;
use crate::config::ensure_private_dir;
use crate::daemon::metrics::Metrics;
use crate::events::{CaptureEventPayload, Outcome};

const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Aggregation TTL: bounds how long a crash-loop's Event `series` count keeps
/// growing before a fresh Event (and count) starts. Matches the API server's
/// default Event TTL, so this never outlives the object it's tracking.
const AGGREGATION_TTL: Duration = Duration::from_hours(1);

/// Mode for the capture-event unix socket. The parent directory is `0700`.
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
pub fn bind_socket(path: &Path) -> std::io::Result<tokio::net::UnixDatagram> {
    if let Some(parent) = path.parent() {
        ensure_private_dir(parent)?;
    }
    let _ = std::fs::remove_file(path);
    let std_socket = StdUnixDatagram::bind(path)?;
    std_socket.set_nonblocking(true)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(EVENT_SOCKET_MODE))?;
    tokio::net::UnixDatagram::from_std(std_socket)
}

/// The k8s Event `reason` a capture produces.
// Variant names match the `reason` strings users filter on.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Reason {
    CoreDumped,
    CoreDumpSuppressed,
    CoreDumpSpoolFull,
    CoreDumpDrainDeadline,
    CoreDiscardedNoStore,
}

impl Reason {
    /// Map a capture outcome to its Event reason. Outcomes with no Event
    /// (`spooled`, which is not final, `skipped-non-k8s`, `failed`) yield
    /// `None` - the caller drops the payload.
    fn from_outcome(outcome: Outcome) -> Option<Self> {
        match outcome {
            Outcome::Uploaded => Some(Self::CoreDumped),
            Outcome::SuppressedRateLimit => Some(Self::CoreDumpSuppressed),
            Outcome::SpoolFull => Some(Self::CoreDumpSpoolFull),
            Outcome::DrainDeadline => Some(Self::CoreDumpDrainDeadline),
            Outcome::NoStoreDiscard => Some(Self::CoreDiscardedNoStore),
            Outcome::Spooled | Outcome::SkippedNonK8s | Outcome::Filtered | Outcome::Failed => None,
        }
    }

    /// The `reason` field value.
    fn as_str(self) -> &'static str {
        match self {
            Self::CoreDumped => "CoreDumped",
            Self::CoreDumpSuppressed => "CoreDumpSuppressed",
            Self::CoreDumpSpoolFull => "CoreDumpSpoolFull",
            Self::CoreDumpDrainDeadline => "CoreDumpDrainDeadline",
            Self::CoreDiscardedNoStore => "CoreDiscardedNoStore",
        }
    }

    /// Lowercase form for Event object names, which must be DNS subdomains.
    fn slug(self) -> &'static str {
        match self {
            Self::CoreDumped => "coredumped",
            Self::CoreDumpSuppressed => "coredumpsuppressed",
            Self::CoreDumpSpoolFull => "coredumpspoolfull",
            Self::CoreDumpDrainDeadline => "coredumpdraindeadline",
            Self::CoreDiscardedNoStore => "corediscardednostore",
        }
    }
}

/// Aggregation key: repeats of the same (pod, reason) bump a `series` count
/// on one Event object instead of creating a new one per crash.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AggKey {
    namespace: String,
    pod_uid: String,
    reason: Reason,
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

/// Pause after a failed `recv` on the capture-event socket.
const RECV_ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// Maximum length of a namespace, which Kubernetes requires to be a DNS-1123
/// label.
const MAX_NAMESPACE_LEN: usize = 63;

/// Whether `s` is a DNS-1123 label, the form Kubernetes requires for a
/// namespace: lowercase alphanumerics and `-`, alphanumeric at both ends.
fn is_dns1123_label(s: &str) -> bool {
    let alnum = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    let bytes = s.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_NAMESPACE_LEN
        && bytes.iter().all(|&c| alnum(c) || c == b'-')
        && alnum(bytes[0])
        && alnum(bytes[bytes.len() - 1])
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

/// HTTP client for the API server that trusts every certificate in `ca_pem`
/// and nothing else. The API server certificate is signed by the cluster CA,
/// so trusting the system bundle too would let any public CA impersonate it
/// and receive the `ServiceAccount` token. `ca.crt` holds two CAs during a
/// cluster CA rotation, so every certificate in it is read. `None` when
/// `ca_pem` holds no certificate.
fn build_http(ca_pem: &[u8]) -> Option<reqwest::Client> {
    let certs = reqwest::Certificate::from_pem_bundle(ca_pem).ok()?;
    if certs.is_empty() {
        return None;
    }
    reqwest::Client::builder()
        // The API server must never wedge the capture-event loop on an
        // in-flight request; all event posts are best-effort.
        .timeout(Duration::from_secs(30))
        .tls_certs_only(certs)
        .build()
        .ok()
}

/// Minimal `events.k8s.io/v1` client: create on first occurrence, `PATCH`
/// `series` on repeats. Hand-rolled with `reqwest` rather than `kube-rs` - one
/// endpoint doesn't justify the dependency tree.
struct EventClient {
    http: reqwest::Client,
    /// `https://<api server>/apis/events.k8s.io/v1/namespaces`, parsed once.
    /// Requests append their path segments to it.
    events_base: reqwest::Url,
    token: String,
}

/// The events API base for an API server origin (`https://host:port`).
fn events_base(origin: &str) -> Option<reqwest::Url> {
    reqwest::Url::parse(&format!("{origin}/apis/events.k8s.io/v1/namespaces")).ok()
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
        let Some(http) = build_http(&ca_pem) else {
            warn!(
                "no usable certificate in {SERVICE_ACCOUNT_DIR}/ca.crt; cannot reach the API server"
            );
            return None;
        };
        let host = std::env::var("KUBERNETES_SERVICE_HOST").ok()?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".to_string());
        let host = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host
        };
        let Some(events_base) = events_base(&format!("https://{host}:{port}")) else {
            warn!(
                host,
                port, "invalid KUBERNETES_SERVICE_HOST/PORT; cannot reach the API server"
            );
            return None;
        };
        Some(Self {
            http,
            events_base,
            token,
        })
    }

    /// URL of the namespace's events collection, or of one event when `name`
    /// is set. Each segment is percent-encoded, so a `/` or `?` in a value
    /// cannot change the path. `.` and `..` segments are dropped by `url`;
    /// `handle_payload` rejects those before any request.
    fn events_url(&self, namespace: &str, name: Option<&str>) -> Result<reqwest::Url> {
        let mut url = self.events_base.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| anyhow!("{} cannot be a base URL", self.events_base))?;
            segments.extend([namespace, "events"]);
            if let Some(name) = name {
                segments.push(name);
            }
        }
        Ok(url)
    }

    async fn create_event(&self, namespace: &str, body: &Value) -> Result<()> {
        let url = self.events_url(namespace, None)?;
        let resp = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .context("create event")?;
        api_result(resp, "create event").await
    }

    async fn patch_event_series(&self, namespace: &str, name: &str, body: &Value) -> Result<()> {
        let url = self.events_url(namespace, Some(name))?;
        let resp = self
            .http
            .patch(url)
            .bearer_auth(&self.token)
            .header("Content-Type", "application/merge-patch+json")
            .json(body)
            .send()
            .await
            .context("patch event series")?;
        api_result(resp, "patch event series").await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
impl EventClient {
    /// Build an `EventClient` directly for unit tests. The underlying HTTP
    /// client is supplied by the caller so the test can control timeouts.
    fn new_for_test(http: reqwest::Client, api_server: &str, token: String) -> Self {
        Self {
            http,
            events_base: events_base(api_server).unwrap(),
            token,
        }
    }
}

/// Turn a non-2xx response into an `Err` carrying the API server's response
/// body - the status code alone ("HTTP 400") gives no way to tell a bad
/// request body from an RBAC/admission rejection.
async fn api_result(resp: reqwest::Response, what: &str) -> Result<()> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    let body = body.chars().take(500).collect::<String>();
    Err(anyhow!("{what}: HTTP {status}: {body}"))
}

/// Listen for capture-event datagrams and uploader reports: log each capture,
/// and post/patch k8s Events when `post_events` is set. A datagram for a
/// capture with a spool bundle wakes the uploader through `wake`. Every
/// failure path is best-effort: logged and swallowed, never propagated - a
/// broken API server must never crash the daemon.
///
/// Even when no events arrive, the aggregator is swept periodically so stale
/// entries do not accumulate indefinitely.
///
/// When `stop` fires, the listener handles the reports still queued, then
/// returns. Uploads that finish during the shutdown flush are reported this
/// way, and their bundles are already gone.
pub async fn run_listener(
    socket: tokio::net::UnixDatagram,
    mut reports: mpsc::UnboundedReceiver<CaptureEventPayload>,
    wake: Arc<Notify>,
    node: String,
    post_events: bool,
    metrics: Arc<Metrics>,
    mut stop: oneshot::Receiver<()>,
) {
    let client = if post_events {
        let client = EventClient::from_env();
        if client.is_none() {
            warn!(
                "kubernetes API client unavailable (no in-cluster ServiceAccount/API server found); captures are logged but no k8s Events are posted"
            );
        }
        client
    } else {
        info!("k8s Event emission disabled (--no-events); captures are still logged");
        None
    };
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
                        // A persistent error would otherwise spin this loop
                        // and log on every pass.
                        warn!(error = %e, "capture event socket recv failed; retrying in 1s");
                        tokio::time::sleep(RECV_ERROR_BACKOFF).await;
                        continue;
                    }
                };
                let payload: CaptureEventPayload = match serde_json::from_slice(&buf[..n]) {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(error = %e, "capture event payload parse failed; dropping");
                        metrics.payload_dropped();
                        continue;
                    }
                };
                if payload.outcome.has_bundle() {
                    log_capture(&payload, Stage::Spooled);
                    wake.notify_one();
                } else {
                    complete(client.as_ref(), &mut aggregator, &node, &metrics, &payload).await;
                }
            }
            Some(payload) = reports.recv() => {
                complete(client.as_ref(), &mut aggregator, &node, &metrics, &payload).await;
            }
            _ = sweep_interval.tick() => {
                aggregator.sweep(Instant::now());
            }
            _ = &mut stop => {
                drain_reports(&mut reports, client.as_ref(), &mut aggregator, &node, &metrics).await;
                return;
            }
        }
    }
}

/// Handle every report already queued in `reports`. Returns how many there
/// were.
async fn drain_reports(
    reports: &mut mpsc::UnboundedReceiver<CaptureEventPayload>,
    client: Option<&EventClient>,
    aggregator: &mut Aggregator,
    node: &str,
    metrics: &Metrics,
) -> usize {
    let mut handled = 0;
    while let Ok(payload) = reports.try_recv() {
        complete(client, aggregator, node, metrics, &payload).await;
        handled += 1;
    }
    handled
}

/// A capture is done: log it, count it, and post its k8s Event.
async fn complete(
    client: Option<&EventClient>,
    aggregator: &mut Aggregator,
    node: &str,
    metrics: &Metrics,
    payload: &CaptureEventPayload,
) {
    log_capture(payload, Stage::Complete);
    metrics.capture_done(payload);
    handle_payload(client, aggregator, node, metrics, payload).await;
}

/// Which log line a capture gets.
#[derive(Clone, Copy)]
enum Stage {
    /// The handler wrote a spool bundle; the uploader has it next.
    Spooled,
    /// The capture is done: uploaded, or ended without a bundle to upload.
    Complete,
}

/// The daemon's copy of the handler's capture line. The kernel gives the
/// handler no stdout, so this is the one operators see.
fn log_capture(p: &CaptureEventPayload, stage: Stage) {
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

/// The pod a k8s Event is posted on.
struct EventTarget<'a> {
    namespace: &'a str,
    pod_name: &'a str,
    pod_uid: &'a str,
}

async fn handle_payload(
    client: Option<&EventClient>,
    aggregator: &mut Aggregator,
    node: &str,
    metrics: &Metrics,
    payload: &CaptureEventPayload,
) {
    let Some(reason) = Reason::from_outcome(payload.outcome) else {
        debug!(outcome = %payload.outcome, "capture outcome has no k8s Event mapping; ignoring");
        return;
    };
    let Some(client) = client else {
        return;
    };
    // An Event targets a pod object, so it needs the crictl-enriched identity.
    let (Some(namespace), Some(pod_name), Some(pod_uid)) = (
        payload.namespace.as_deref(),
        payload.pod_name.as_deref(),
        payload.pod_uid.as_deref(),
    ) else {
        debug!("capture has no namespace/pod identity; no k8s Event to post");
        return;
    };
    let target = EventTarget {
        namespace,
        pod_name,
        pod_uid,
    };

    // Both values go into request URLs unencoded, so check their characters,
    // not only their length.
    if !is_dns1123_label(namespace) || !is_pod_uid(pod_uid) {
        warn!(
            namespace = %namespace,
            pod_uid = %pod_uid,
            "capture event payload has an invalid namespace/pod_uid; dropping"
        );
        return;
    }

    let key = AggKey {
        namespace: namespace.to_string(),
        pod_uid: pod_uid.to_string(),
        reason,
    };
    let Some(recorded) = aggregator.record(key, Instant::now(), || {
        event_name(pod_uid, reason, payload.timestamp)
    }) else {
        return;
    };
    let last_observed = rfc3339_micro(payload.timestamp);

    let result = if recorded.is_new {
        let body = event_body(node, &recorded.event_name, reason, &target, payload);
        client.create_event(namespace, &body).await
    } else {
        let body = series_patch(recorded.count, &last_observed);
        client
            .patch_event_series(namespace, &recorded.event_name, &body)
            .await
    };
    metrics.event_posted(result.is_ok());
    if let Err(e) = result {
        warn!(error = %e, event = %recorded.event_name, namespace = %namespace, "posting kubernetes event failed");
    }
}

/// DNS-1123-subdomain-safe Event name: pod UID is already a lowercase UUID,
/// the reason lowercased has no invalid characters, and the timestamp keeps
/// re-created (post-TTL-expiry) entries for the same pod+reason unique.
fn event_name(pod_uid: &str, reason: Reason, timestamp: i64) -> String {
    format!("coredrop-{pod_uid}-{}-{timestamp}", reason.slug())
}

fn event_body(
    node: &str,
    name: &str,
    reason: Reason,
    target: &EventTarget<'_>,
    payload: &CaptureEventPayload,
) -> Value {
    serde_json::json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {
            "name": name,
            "namespace": target.namespace,
        },
        "eventTime": rfc3339_micro(payload.timestamp),
        "reportingController": "coredrop",
        "reportingInstance": node,
        "action": "CoreCaptured",
        "reason": reason.as_str(),
        "regarding": {
            "kind": "Pod",
            "apiVersion": "v1",
            "namespace": target.namespace,
            "name": target.pod_name,
            "uid": target.pod_uid,
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
            namespace: Some(namespace.into()),
            pod_name: Some("my-pod".into()),
            pod_uid: Some(pod_uid.into()),
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

    #[tokio::test]
    async fn drain_reports_handles_every_queued_report() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(payload("default", "abc-123")).unwrap();
        tx.send(payload("default", "def-456")).unwrap();
        let mut aggregator = Aggregator::default();
        let metrics = Metrics::new();

        let handled = drain_reports(&mut rx, None, &mut aggregator, "node-1", &metrics).await;

        assert_eq!(handled, 2);
        assert!(rx.try_recv().is_err(), "the queue is empty");
        assert!(
            metrics
                .encode()
                .contains("coredrop_captures_total{outcome=\"uploaded\"} 2")
        );
    }

    /// A datagram with a final outcome is counted once. A spooled capture is
    /// counted later, when the uploader reports it, and a bad datagram is
    /// counted as dropped.
    #[tokio::test]
    async fn listener_counts_final_outcomes_and_dropped_payloads() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.sock");
        let socket = bind_socket(&path).unwrap();
        let (_tx, rx) = mpsc::unbounded_channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        let metrics = Arc::new(Metrics::new());
        let listener = tokio::spawn(run_listener(
            socket,
            rx,
            Arc::new(Notify::new()),
            "node-1".into(),
            false,
            metrics.clone(),
            stop_rx,
        ));

        let sender = StdUnixDatagram::unbound().unwrap();
        for outcome in [Outcome::Spooled, Outcome::SkippedNonK8s] {
            let p = CaptureEventPayload {
                outcome,
                ..payload("default", "abc-123")
            };
            sender
                .send_to(&serde_json::to_vec(&p).unwrap(), &path)
                .unwrap();
        }
        sender.send_to(b"not json", &path).unwrap();

        let expected = [
            "coredrop_captures_total{outcome=\"skipped-non-k8s\"} 1",
            "coredrop_event_payloads_dropped_total 1",
        ];
        tokio::time::timeout(Duration::from_secs(5), async {
            while !expected.iter().all(|l| metrics.encode().contains(l)) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("metrics never matched:\n{}", metrics.encode()));
        assert!(!metrics.encode().contains("outcome=\"spooled\""));

        stop_tx.send(()).unwrap();
        listener.await.unwrap();
    }

    #[tokio::test]
    async fn listener_returns_after_stop() {
        let dir = TempDir::new().unwrap();
        let socket = bind_socket(&dir.path().join("events.sock")).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(payload("default", "abc-123")).unwrap();
        let (stop_tx, stop_rx) = oneshot::channel();
        let listener = tokio::spawn(run_listener(
            socket,
            rx,
            Arc::new(Notify::new()),
            "node-1".into(),
            false,
            Arc::new(Metrics::new()),
            stop_rx,
        ));

        stop_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), listener)
            .await
            .expect("the listener returns after stop")
            .unwrap();
        assert!(tx.is_closed(), "the listener dropped its receiver");
    }

    fn agg_key(pod_uid: &str, reason: Reason) -> AggKey {
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
            let _socket = bind_socket(&sock_path).unwrap();
        });
        dir
    }

    #[test]
    fn reason_maps_only_reportable_outcomes() {
        let cases = [
            (Outcome::Uploaded, "CoreDumped"),
            (Outcome::SuppressedRateLimit, "CoreDumpSuppressed"),
            (Outcome::SpoolFull, "CoreDumpSpoolFull"),
            (Outcome::DrainDeadline, "CoreDumpDrainDeadline"),
            (Outcome::NoStoreDiscard, "CoreDiscardedNoStore"),
        ];
        for (outcome, reason) in cases {
            let got = Reason::from_outcome(outcome).unwrap();
            assert_eq!(got.as_str(), reason);
            // The name slug is the reason lowercased.
            assert_eq!(got.slug(), reason.to_lowercase());
        }
        // No Event: `spooled` is not final, `skipped-non-k8s` has no pod to
        // target, and a failed capture has no artifacts to point at.
        assert_eq!(Reason::from_outcome(Outcome::Spooled), None);
        assert_eq!(Reason::from_outcome(Outcome::SkippedNonK8s), None);
        assert_eq!(Reason::from_outcome(Outcome::Filtered), None);
        assert_eq!(Reason::from_outcome(Outcome::Failed), None);
    }

    #[test]
    fn aggregator_bumps_count_for_repeats_within_ttl() {
        let mut agg = Aggregator::default();
        let key = agg_key("abc-123", Reason::CoreDumped);
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
            agg.record(agg_key("pod-a", Reason::CoreDumped), t0, || "a".into())
                .unwrap()
                .is_new
        );
        assert!(
            agg.record(agg_key("pod-b", Reason::CoreDumped), t0, || "b".into())
                .unwrap()
                .is_new
        );
        assert!(
            agg.record(agg_key("pod-a", Reason::CoreDumpSuppressed), t0, || "c"
                .into())
                .unwrap()
                .is_new
        );
    }

    #[test]
    fn aggregator_re_creates_after_ttl_expiry() {
        let mut agg = Aggregator::default();
        let key = agg_key("abc-123", Reason::CoreDumped);
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
                .record(agg_key(&format!("pod-{i}"), Reason::CoreDumped), t0, || {
                    format!("ev-{i}")
                })
                .unwrap();
            assert!(recorded.is_new);
        }
        assert_eq!(agg.entries.len(), MAX_AGGREGATION_KEYS);

        assert!(
            agg.record(agg_key("overflow", Reason::CoreDumped), t0, || "overflow"
                .into())
                .is_none()
        );
        assert_eq!(agg.entries.len(), MAX_AGGREGATION_KEYS);

        // Repeating an existing key still bumps its count.
        let recorded = agg
            .record(agg_key("pod-0", Reason::CoreDumped), t0, || {
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
        let key = agg_key("abc-123", Reason::CoreDumped);
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

    /// A whole-second timestamp must still have six fractional digits - see
    /// `rfc3339_micro`.
    #[test]
    fn rfc3339_micro_always_has_six_fractional_digits() {
        assert_eq!(rfc3339_micro(1_749_600_000), "2025-06-11T00:00:00.000000Z");
    }

    #[test]
    fn event_name_is_dns_subdomain_safe() {
        let name = event_name("abc-123-def", Reason::CoreDumped, 1_749_600_000);
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
        let target = EventTarget {
            namespace: "default",
            pod_name: "my-pod",
            pod_uid: "abc-123",
        };
        let name = event_name(target.pod_uid, Reason::CoreDumped, payload.timestamp);
        let body = event_body("node-a", &name, Reason::CoreDumped, &target, &payload);

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
        let client = EventClient::new_for_test(http, "http://127.0.0.1:1", "test".into());

        let mut agg = Aggregator::default();
        let metrics = Metrics::new();
        let uid = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";
        let long_namespace = "x".repeat(MAX_NAMESPACE_LEN + 1);
        let long_uid = "a".repeat(crate::cgroup::MAX_POD_UID_LEN + 1);

        for (namespace, pod_uid, label) in [
            ("", uid, "empty namespace"),
            (long_namespace.as_str(), uid, "oversized namespace"),
            ("kube/system", uid, "namespace with /"),
            ("default?x=1", uid, "namespace with ?"),
            ("default%2F", uid, "namespace with %"),
            ("..", uid, "namespace .."),
            ("Default", uid, "uppercase namespace"),
            ("-default", uid, "namespace with leading -"),
            ("default-", uid, "namespace with trailing -"),
            ("default", "", "empty pod uid"),
            ("default", long_uid.as_str(), "oversized pod uid"),
            ("default", "0f1e/../2d3c", "pod uid with /"),
            ("default", "0f1e?x=1", "pod uid with ?"),
            ("default", "0f1e%2F", "pod uid with %"),
            ("default", "..", "pod uid .."),
            ("default", "0F1E2D3C", "uppercase pod uid"),
        ] {
            handle_payload(
                Some(&client),
                &mut agg,
                "test-node",
                &metrics,
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
            &metrics,
            &payload("default", uid),
        )
        .await;
        assert_eq!(agg.entries.len(), 1);
        let text = metrics.encode();
        assert!(
            text.contains("coredrop_events_posted_total{result=\"error\"} 1"),
            "only the valid payload reaches the API server: {text}"
        );
    }

    #[tokio::test]
    async fn handle_payload_skips_a_capture_without_pod_identity() {
        // cgroup-only identity (crictl failed) or a host process: there is no
        // pod object to target, so nothing is recorded or posted.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        let client = EventClient::new_for_test(http, "http://127.0.0.1:1", "test".into());
        let mut agg = Aggregator::default();
        let uid = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";

        let mut no_namespace = payload("default", uid);
        no_namespace.namespace = None;
        let mut no_pod_name = payload("default", uid);
        no_pod_name.pod_name = None;
        let mut no_pod_uid = payload("default", uid);
        no_pod_uid.pod_uid = None;
        for p in [no_namespace, no_pod_name, no_pod_uid] {
            handle_payload(Some(&client), &mut agg, "test-node", &Metrics::new(), &p).await;
        }
        assert!(agg.entries.is_empty());
    }

    /// Self-signed P-256 test CAs (`CN=test-ca-1`, `CN=test-ca-2`).
    const TEST_CA_1: &str = "\
-----BEGIN CERTIFICATE-----
MIIBfzCCASWgAwIBAgIUJbJWkLJJqhX+NszCpokTPUg+DE0wCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJdGVzdC1jYS0xMCAXDTI2MTAyNTE2MTc1NloYDzIxMjYxMDAx
MTYxNzU2WjAUMRIwEAYDVQQDDAl0ZXN0LWNhLTEwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAAT3smZYjInK1qwoqXGKMQX5RywlkqeTC5fRpgDVwRFxhpKJd3WNE8vQ
mgrVZUTi7xyZItEi0U2QXPp9IuLiQInJo1MwUTAdBgNVHQ4EFgQUwASJEzdMF4QZ
bjQVQ8bwWlVBtSkwHwYDVR0jBBgwFoAUwASJEzdMF4QZbjQVQ8bwWlVBtSkwDwYD
VR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiAaAPKpfQx65kd2zpdXYDQM
WCb01nHlU5vOrJ0xdzMbTwIhAIoUy34vYSThgzJ2z6dLPdY6IT8+MjkzoEJH1lS9
S4WP
-----END CERTIFICATE-----
";
    const TEST_CA_2: &str = "\
-----BEGIN CERTIFICATE-----
MIIBfzCCASWgAwIBAgIUMx1Z0GqBi7U/bcDVQX0KYFApxtUwCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJdGVzdC1jYS0yMCAXDTI2MTAyNTE2MTc1NloYDzIxMjYxMDAx
MTYxNzU2WjAUMRIwEAYDVQQDDAl0ZXN0LWNhLTIwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAAT3mLiKb/NOxIbvlJH9J5wygOjhoxh2Ha34vqcCVUsdozyzzsNEC7sJ
lmeJR4hyKlZ4RO35MQhwrl3+Ay6x+Jy9o1MwUTAdBgNVHQ4EFgQUu5B3Fe20Rm8X
qoPmEMZWfA8pxUswHwYDVR0jBBgwFoAUu5B3Fe20Rm8XqoPmEMZWfA8pxUswDwYD
VR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiEAxvgpt/B7x2CHfvJgQxsz
Qk3ftY8cokYo8zDjg7phpOkCICYrIQ0gyWa4NboPK2ncWw/n6IqzPVKisAzgbK5v
Kf3s
-----END CERTIFICATE-----
";

    #[test]
    fn build_http_trusts_every_cert_in_the_bundle() {
        // `ca.crt` during a cluster CA rotation: old and new CA back to back.
        let bundle = format!("{TEST_CA_1}{TEST_CA_2}");
        let certs = reqwest::Certificate::from_pem_bundle(bundle.as_bytes()).unwrap();
        assert_eq!(certs.len(), 2, "both CAs in ca.crt must be parsed");
        assert!(build_http(bundle.as_bytes()).is_some());
    }

    #[test]
    fn build_http_rejects_a_bundle_without_certs() {
        assert!(build_http(b"").is_none());
        assert!(build_http(b"not a pem").is_none());
    }

    fn test_client(origin: &str) -> EventClient {
        EventClient::new_for_test(
            build_http(TEST_CA_1.as_bytes()).unwrap(),
            origin,
            "test".into(),
        )
    }

    #[test]
    fn events_url_builds_create_and_patch_paths() {
        let client = test_client("https://10.0.0.1:443");
        assert_eq!(
            client.events_url("default", None).unwrap().as_str(),
            "https://10.0.0.1/apis/events.k8s.io/v1/namespaces/default/events"
        );
        assert_eq!(
            client
                .events_url("default", Some("my-event.abc"))
                .unwrap()
                .as_str(),
            "https://10.0.0.1/apis/events.k8s.io/v1/namespaces/default/events/my-event.abc"
        );
    }

    #[test]
    fn events_url_encodes_each_segment() {
        let client = test_client("https://10.0.0.1:6443");
        let url = client.events_url("a/b?c%", Some("x/y")).unwrap();
        assert_eq!(
            url.path(),
            "/apis/events.k8s.io/v1/namespaces/a%2Fb%3Fc%25/events/x%2Fy"
        );
        assert_eq!(url.query(), None);
    }

    #[test]
    fn events_base_keeps_ipv6_brackets() {
        let client = test_client("https://[fd00::1]:6443");
        assert_eq!(
            client.events_url("default", None).unwrap().as_str(),
            "https://[fd00::1]:6443/apis/events.k8s.io/v1/namespaces/default/events"
        );
    }
}
