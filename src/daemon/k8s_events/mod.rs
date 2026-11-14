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
use crate::daemon::metrics::Metrics;
use crate::events::CaptureEventPayload;
use crate::host::ensure_private_dir;

use self::aggregate::{AGGREGATION_SWEEP_INTERVAL, AggKey, Aggregator};
use self::client::{EventClient, is_conflict, is_not_found};
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

/// Captures whose k8s Event waits to be posted. Beyond this, while the API
/// server is slow, new Events are dropped.
const EVENT_QUEUE_CAPACITY: usize = 256;

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
/// count it, and queue its k8s Event when `post_events` is set. A datagram
/// for a capture with a spool bundle wakes the uploader through `wake`. Every
/// failure path is best-effort: logged and swallowed, never propagated - a
/// broken API server must never crash the daemon.
///
/// The Events are posted by a separate task (see `post_events`), so a slow
/// API server never delays the capture log, the metrics or the reports.
///
/// When `stop` fires, the listener handles the reports still queued, then
/// waits for the Event task to post the queued Events. Uploads that finish
/// during the shutdown flush are reported this way, and their bundles are
/// already gone.
pub async fn run_listener(
    socket: tokio::net::UnixDatagram,
    reports: mpsc::UnboundedReceiver<CaptureEventPayload>,
    wake: Arc<Notify>,
    node: String,
    post_events: bool,
    metrics: Arc<Metrics>,
    stop: oneshot::Receiver<()>,
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
    listen(socket, reports, wake, client, node, metrics, stop).await;
}

/// [`run_listener`] with the Event client already built: `None` posts no
/// Events.
async fn listen(
    socket: tokio::net::UnixDatagram,
    mut reports: mpsc::UnboundedReceiver<CaptureEventPayload>,
    wake: Arc<Notify>,
    client: Option<EventClient>,
    node: String,
    metrics: Arc<Metrics>,
    mut stop: oneshot::Receiver<()>,
) {
    let (events, poster) = match client {
        Some(client) => {
            let (tx, rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
            let poster = tokio::spawn(post_events(rx, client, node, metrics.clone()));
            (Some(tx), Some(poster))
        }
        None => (None, None),
    };
    let mut buf = vec![0u8; 64 * 1024];

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
                    complete(events.as_ref(), &metrics, &payload);
                }
            }
            Some(payload) = reports.recv() => {
                complete(events.as_ref(), &metrics, &payload);
            }
            _ = &mut stop => {
                drain_reports(&mut reports, events.as_ref(), &metrics);
                // Closing the queue ends the Event task after its last Event.
                drop(events);
                if let Some(poster) = poster
                    && let Err(e) = poster.await
                {
                    warn!(error = %e, "k8s Event task failed");
                }
                return;
            }
        }
    }
}

/// Handle every report already queued in `reports`. Returns how many there
/// were.
fn drain_reports(
    reports: &mut mpsc::UnboundedReceiver<CaptureEventPayload>,
    events: Option<&mpsc::Sender<CaptureEventPayload>>,
    metrics: &Metrics,
) -> usize {
    let mut handled = 0;
    while let Ok(payload) = reports.try_recv() {
        complete(events, metrics, &payload);
        handled += 1;
    }
    handled
}

/// A capture is done: log it, count it, and queue its k8s Event. When the
/// queue is full, the Event is dropped and counted.
fn complete(
    events: Option<&mpsc::Sender<CaptureEventPayload>>,
    metrics: &Metrics,
    payload: &CaptureEventPayload,
) {
    log_capture(payload, Stage::Complete);
    metrics.capture_done(payload);
    let Some(events) = events else {
        return;
    };
    if Reason::from_outcome(payload.outcome).is_none() {
        return;
    }
    match events.try_send(payload.clone()) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            warn!(
                capacity = EVENT_QUEUE_CAPACITY,
                pod = payload.pod_name.as_deref().unwrap_or_default(),
                "k8s Event queue full; dropping the Event"
            );
            metrics.event_dropped();
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            warn!(
                pod = payload.pod_name.as_deref().unwrap_or_default(),
                "k8s Event task has stopped; dropping the Event"
            );
            metrics.event_dropped();
        }
    }
}

/// Post the k8s Events that the listener queues, one at a time, so the
/// repeats of one pod keep their order. Sweeps the aggregator on a timer,
/// even when no Events arrive. Returns when the queue is closed and empty.
async fn post_events(
    mut queue: mpsc::Receiver<CaptureEventPayload>,
    client: EventClient,
    node: String,
    metrics: Arc<Metrics>,
) {
    let mut aggregator = Aggregator::default();
    let mut sweep_interval = tokio::time::interval(AGGREGATION_SWEEP_INTERVAL);
    sweep_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            payload = queue.recv() => {
                let Some(payload) = payload else {
                    return;
                };
                handle_payload(&client, &mut aggregator, &node, &metrics, &payload).await;
            }
            _ = sweep_interval.tick() => {
                aggregator.prune(Instant::now());
            }
        }
    }
}

/// The pod a k8s Event is posted on.
struct EventTarget<'a> {
    namespace: &'a str,
    pod_name: &'a str,
    pod_uid: &'a str,
}

async fn handle_payload(
    client: &EventClient,
    aggregator: &mut Aggregator,
    node: &str,
    metrics: &Metrics,
    payload: &CaptureEventPayload,
) {
    let Some(reason) = Reason::from_outcome(payload.outcome) else {
        debug!(outcome = %payload.outcome, "capture outcome has no k8s Event mapping; ignoring");
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
    let now = Instant::now();
    let make_name = || event_name(pod_uid, reason, payload.timestamp);
    let Some(mut recorded) = aggregator.record(key.clone(), now, make_name) else {
        return;
    };
    let last_observed = rfc3339_micro(payload.timestamp);

    // A failed create keeps the entry: the create can time out after the API
    // server stored the Event. The next patch then finds out.
    let mut result = if recorded.is_new {
        Ok(())
    } else {
        let body = series_patch(recorded.count, &last_observed);
        client
            .patch_event_series(namespace, &recorded.event_name, &body)
            .await
    };
    // No Event to patch: its create failed, or it was deleted. Start a new
    // Event and count.
    if let Err(e) = &result
        && is_not_found(e)
    {
        debug!(event = %recorded.event_name, namespace = %namespace, "kubernetes event not found; creating a new one");
        aggregator.forget(&key);
        let Some(fresh) = aggregator.record(key, now, make_name) else {
            return;
        };
        recorded = fresh;
    }
    if recorded.is_new {
        let body = event_body(node, &recorded.event_name, reason, &target, payload);
        result = client.create_event(namespace, &body).await;
        // The Event exists already: an earlier daemon posted it for this
        // capture before it was killed, and the capture is reported again.
        if result.as_ref().is_err_and(is_conflict) {
            debug!(event = %recorded.event_name, namespace = %namespace, "kubernetes event exists already");
            result = Ok(());
        }
    }
    metrics.event_posted(result.is_ok());
    if let Err(e) = result {
        warn!(error = %e, event = %recorded.event_name, namespace = %namespace, "posting kubernetes event failed");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use tempfile::TempDir;

    use super::client::token_file;
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
        let metrics = Metrics::new();

        let handled = drain_reports(&mut rx, None, &metrics);

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

    /// An API server that accepts connections and never answers.
    async fn stalled_api_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                held.push(stream);
            }
        });
        (origin, server)
    }

    #[tokio::test]
    async fn a_stalled_api_server_does_not_delay_the_capture_log_or_metrics() {
        let (origin, server) = stalled_api_server().await;
        let token_dir = TempDir::new().unwrap();
        let client =
            EventClient::new_for_test(reqwest::Client::new(), &origin, token_file(&token_dir));
        let dir = TempDir::new().unwrap();
        let socket = bind_socket(&dir.path().join("events.sock")).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let (_stop_tx, stop_rx) = oneshot::channel();
        let metrics = Arc::new(Metrics::new());
        let listener = tokio::spawn(listen(
            socket,
            rx,
            Arc::new(Notify::new()),
            Some(client),
            "node-1".into(),
            metrics.clone(),
            stop_rx,
        ));

        // Two uploader reports. Both get an Event, and the first Event
        // request never finishes.
        for uid in [
            "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0",
            "1f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0",
        ] {
            tx.send(payload("default", uid)).unwrap();
        }

        let expected = "coredrop_captures_total{outcome=\"uploaded\"} 2";
        tokio::time::timeout(Duration::from_secs(5), async {
            while !metrics.encode().contains(expected) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("both captures are counted:\n{}", metrics.encode()));

        listener.abort();
        server.abort();
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
        let token_dir = TempDir::new().unwrap();
        let client = EventClient::new_for_test(http, "http://127.0.0.1:1", token_file(&token_dir));

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
                &client,
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
            &client,
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

    #[test]
    fn a_full_event_queue_counts_a_drop_not_a_post_error() {
        let metrics = Metrics::new();
        let (tx, _rx) = mpsc::channel(1);
        let uid = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";
        complete(Some(&tx), &metrics, &payload("default", uid));
        complete(Some(&tx), &metrics, &payload("default", uid));

        let text = metrics.encode();
        assert!(text.contains("coredrop_events_dropped_total 1"), "{text}");
        assert!(
            text.contains("coredrop_events_posted_total{result=\"error\"} 0"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn handle_payload_counts_an_existing_event_as_posted() {
        // A restarted daemon reports a capture again; its Event exists.
        let (origin, _seen) = fake_api_server(vec![409]).await;
        let token_dir = TempDir::new().unwrap();
        let client =
            EventClient::new_for_test(reqwest::Client::new(), &origin, token_file(&token_dir));
        let mut agg = Aggregator::default();
        let metrics = Metrics::new();
        let uid = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";
        handle_payload(
            &client,
            &mut agg,
            "test-node",
            &metrics,
            &payload("default", uid),
        )
        .await;

        let text = metrics.encode();
        assert!(
            text.contains("coredrop_events_posted_total{result=\"ok\"} 1"),
            "{text}"
        );
    }

    /// A plain-HTTP API server. It answers each request with the next status
    /// in `statuses` (then 500) and records `METHOD path` of each request.
    async fn fake_api_server(statuses: Vec<u16>) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let statuses = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from(
            statuses,
        )));
        let server_seen = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let seen = Arc::clone(&server_seen);
                let statuses = Arc::clone(&statuses);
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(
                        move |req: hyper::Request<hyper::body::Incoming>| {
                            seen.lock().unwrap().push(format!(
                                "{} {}",
                                req.method(),
                                req.uri().path()
                            ));
                            let status = statuses.lock().unwrap().pop_front().unwrap_or(500);
                            let mut resp = hyper::Response::new(http_body_util::Full::new(
                                hyper::body::Bytes::new(),
                            ));
                            *resp.status_mut() = hyper::StatusCode::from_u16(status).unwrap();
                            async move { Ok::<_, std::convert::Infallible>(resp) }
                        },
                    );
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (origin, seen)
    }

    #[tokio::test]
    async fn handle_payload_creates_a_new_event_when_the_patch_finds_none() {
        // The first create fails, so the Event does not exist. The next
        // capture's patch gets 404 and creates a new Event instead.
        let (origin, seen) = fake_api_server(vec![500, 404, 201, 200]).await;
        let token_dir = TempDir::new().unwrap();
        let client =
            EventClient::new_for_test(reqwest::Client::new(), &origin, token_file(&token_dir));
        let mut agg = Aggregator::default();
        let metrics = Metrics::new();
        let uid = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";
        let mut first = payload("default", uid);
        first.timestamp = 1_749_600_000;
        let mut second = payload("default", uid);
        second.timestamp = 1_749_600_060;
        let mut third = payload("default", uid);
        third.timestamp = 1_749_600_120;

        for p in [&first, &second, &third] {
            handle_payload(&client, &mut agg, "test-node", &metrics, p).await;
        }

        let events = "/apis/events.k8s.io/v1/namespaces/default/events";
        let first_name = event_name(uid, Reason::CoreDumped, first.timestamp);
        let second_name = event_name(uid, Reason::CoreDumped, second.timestamp);
        assert_eq!(
            *seen.lock().unwrap(),
            [
                format!("POST {events}"),
                format!("PATCH {events}/{first_name}"),
                format!("POST {events}"),
                format!("PATCH {events}/{second_name}"),
            ]
        );
        let entry = agg.entries.values().next().unwrap();
        assert_eq!(entry.event_name, second_name);
        assert_eq!(entry.count, 2);
        let text = metrics.encode();
        assert!(
            text.contains("coredrop_events_posted_total{result=\"error\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("coredrop_events_posted_total{result=\"ok\"} 2"),
            "{text}"
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
        let token_dir = TempDir::new().unwrap();
        let client = EventClient::new_for_test(http, "http://127.0.0.1:1", token_file(&token_dir));
        let mut agg = Aggregator::default();
        let uid = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";

        let mut no_namespace = payload("default", uid);
        no_namespace.namespace = None;
        let mut no_pod_name = payload("default", uid);
        no_pod_name.pod_name = None;
        let mut no_pod_uid = payload("default", uid);
        no_pod_uid.pod_uid = None;
        for p in [no_namespace, no_pod_name, no_pod_uid] {
            handle_payload(&client, &mut agg, "test-node", &Metrics::new(), &p).await;
        }
        assert!(agg.entries.is_empty());
    }
}
