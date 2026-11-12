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
//! uploader reports the uploaded bundle.
//!
//! This module holds the socket and the listener loop. The parts:
//!
//! - `log.rs`: the `capture spooled` / `capture complete` log line.
//! - `event.rs`: the Event reason, name, body and `series` patch.
//! - `aggregate.rs`: one Event per crash loop, with a growing `series` count.
//! - `client.rs`: the API calls, with the projected `ServiceAccount` token.

mod aggregate;
mod client;
mod event;
mod log;

use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixDatagram as StdUnixDatagram;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Notify, mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::cgroup::is_pod_uid;
use crate::config::ensure_private_dir;
use crate::daemon::metrics::Metrics;
use crate::events::CaptureEventPayload;

use self::aggregate::{AGGREGATION_SWEEP_INTERVAL, AggKey, Aggregator};
use self::client::EventClient;
use self::event::{Reason, event_body, event_name, rfc3339_micro, series_patch};
use self::log::{Stage, log_capture};

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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::events::Outcome;

    pub(super) fn payload(namespace: &str, pod_uid: &str) -> CaptureEventPayload {
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
}
