//! Daemon side: Prometheus metrics and the `/metrics` HTTP endpoint.
//!
//! The kernel-exec'd handler lives for one crash, so it keeps no metrics.
//! The daemon counts at the points every capture passes through: the event
//! listener (final outcome, core sizes, k8s Event posts), the uploader
//! (upload attempts and durations, spool size and age) and the
//! `core_pattern` check. Labels hold bounded values only - never a pod,
//! namespace or object key - so the series count stays fixed.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::events::{CaptureEventPayload, Outcome};

/// Default address of the `/metrics` endpoint.
pub const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:9464";

/// Time a client has to send a full request head, also on an idle
/// keep-alive connection. Past it the server closes the connection, so an
/// idle or slow client cannot hold a socket. Prometheus opens a new
/// connection on its next scrape.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Connections the endpoint serves at the same time. More clients wait in
/// the listen backlog, so they cannot use up the daemon's file descriptors.
const MAX_CONNECTIONS: usize = 32;

const OPENMETRICS_CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

type OutcomeLabel = [(&'static str, &'static str); 1];
type ResultLabel = [(&'static str, &'static str); 1];

/// Buckets of `coredrop_upload_duration_seconds`: a small core uploads in
/// under a second, a 2 GiB core on a slow link in minutes.
const UPLOAD_DURATION_BUCKETS: [f64; 11] = [
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

/// The daemon's metrics. One instance is shared by the listener, the
/// uploader and the HTTP endpoint.
pub struct Metrics {
    registry: Registry,
    captures: Family<OutcomeLabel, Counter>,
    upload_attempts: Family<ResultLabel, Counter>,
    upload_duration: Histogram,
    bundles_expired: Counter,
    spool_bundles: Gauge,
    spool_bytes: Gauge,
    spool_oldest_age: Gauge,
    spool_max_bytes: Gauge,
    core_bytes: Counter,
    stored_bytes: Counter,
    cores_truncated: Counter,
    core_pattern_installed: Gauge,
    events_posted: Family<ResultLabel, Counter>,
    payloads_dropped: Counter,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// A registry with every coredrop metric. Each labelled series starts at
    /// 0, so `increase()` sees the first capture of each outcome.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("coredrop");

        let captures = Family::<OutcomeLabel, Counter>::default();
        for outcome in Outcome::FINAL {
            let _ = captures.get_or_create(&[("outcome", outcome.as_str())]);
        }
        registry.register("captures", "Captures by final outcome", captures.clone());

        let upload_attempts = Family::<ResultLabel, Counter>::default();
        let events_posted = Family::<ResultLabel, Counter>::default();
        for result in ["ok", "error"] {
            let _ = upload_attempts.get_or_create(&[("result", result)]);
            let _ = events_posted.get_or_create(&[("result", result)]);
        }
        registry.register(
            "upload_attempts",
            "Upload attempts of spooled captures, by result",
            upload_attempts.clone(),
        );

        let upload_duration = Histogram::new(UPLOAD_DURATION_BUCKETS);
        registry.register(
            "upload_duration_seconds",
            "Time to upload one spooled capture (core, proc snapshot and manifest)",
            upload_duration.clone(),
        );

        let bundles_expired: Counter = register(
            &mut registry,
            "bundles_expired",
            "Spooled captures deleted because they were past the spool max age",
        );
        let spool_bundles: Gauge = register(
            &mut registry,
            "spool_bundles",
            "Captures in the spool waiting for upload",
        );
        let spool_bytes: Gauge =
            register(&mut registry, "spool_bytes", "Size of the spool in bytes");
        let spool_oldest_age: Gauge = register(
            &mut registry,
            "spool_oldest_bundle_age_seconds",
            "Age of the oldest capture in the spool waiting for upload; 0 when there is none",
        );
        let spool_max_bytes: Gauge = register(
            &mut registry,
            "spool_max_bytes",
            "Size limit of the spool in bytes; 0 = no limit",
        );
        let core_bytes: Counter = register(
            &mut registry,
            "core_bytes",
            "Uncompressed core bytes read from the kernel by done captures",
        );
        let stored_bytes: Counter = register(
            &mut registry,
            "stored_bytes",
            "Compressed core bytes stored by done captures",
        );
        let cores_truncated: Counter = register(
            &mut registry,
            "cores_truncated",
            "Done captures whose stored core is truncated (size cap, full spool or stream error)",
        );
        let core_pattern_installed: Gauge = register(
            &mut registry,
            "core_pattern_installed",
            "1 when the node's core_pattern routes cores to the handler, else 0",
        );
        registry.register(
            "events_posted",
            "Kubernetes Event create and patch requests, by result",
            events_posted.clone(),
        );

        let payloads_dropped: Counter = register(
            &mut registry,
            "event_payloads_dropped",
            "Capture-event datagrams the daemon could not parse",
        );

        registry.register(
            "build",
            "coredrop build information",
            Info::new([("version", env!("CARGO_PKG_VERSION"))]),
        );

        Self {
            registry,
            captures,
            upload_attempts,
            upload_duration,
            bundles_expired,
            spool_bundles,
            spool_bytes,
            spool_oldest_age,
            spool_max_bytes,
            core_bytes,
            stored_bytes,
            cores_truncated,
            core_pattern_installed,
            events_posted,
            payloads_dropped,
        }
    }

    /// Count a capture that is done: its outcome, its core sizes and
    /// whether its core is truncated. A failed capture keeps nothing, so its
    /// stored bytes and truncation are not counted, even when the handler
    /// stored the core in the spool before the failure.
    pub fn capture_done(&self, payload: &CaptureEventPayload) {
        self.captures
            .get_or_create(&[("outcome", payload.outcome.as_str())])
            .inc();
        self.core_bytes.inc_by(payload.core_bytes);
        if payload.outcome == Outcome::Failed {
            return;
        }
        self.stored_bytes.inc_by(payload.stored_bytes.unwrap_or(0));
        if payload.truncated {
            self.cores_truncated.inc();
        }
    }

    /// Count one upload attempt of one spooled capture.
    pub fn upload_attempt(&self, ok: bool) {
        self.upload_attempts
            .get_or_create(&[("result", result(ok))])
            .inc();
    }

    /// Record the time to upload one spooled capture.
    pub fn upload_duration(&self, duration: Duration) {
        self.upload_duration.observe(duration.as_secs_f64());
    }

    /// Count a spooled capture deleted past the spool max age.
    pub fn bundle_expired(&self) {
        self.bundles_expired.inc();
    }

    /// Set the spool gauges. `oldest_age` is the age of the oldest capture
    /// waiting for upload, zero when there is none.
    pub fn set_spool(&self, bundles: usize, bytes: u64, oldest_age: Duration) {
        self.spool_bundles
            .set(i64::try_from(bundles).unwrap_or(i64::MAX));
        self.spool_bytes
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
        self.spool_oldest_age
            .set(i64::try_from(oldest_age.as_secs()).unwrap_or(i64::MAX));
    }

    /// Set the spool size limit; `0` = no limit.
    pub fn set_spool_max_bytes(&self, bytes: u64) {
        self.spool_max_bytes
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    /// Set whether the node's `core_pattern` routes cores to the handler.
    pub fn set_core_pattern_installed(&self, installed: bool) {
        self.core_pattern_installed.set(i64::from(installed));
    }

    /// Count one k8s Event create or patch request.
    pub fn event_posted(&self, ok: bool) {
        self.events_posted
            .get_or_create(&[("result", result(ok))])
            .inc();
    }

    /// Count a capture-event datagram that did not parse.
    pub fn payload_dropped(&self) {
        self.payloads_dropped.inc();
    }

    /// The registry in the `OpenMetrics` text format.
    #[must_use]
    pub fn encode(&self) -> String {
        let mut out = String::new();
        // Writing into a String cannot fail.
        let _ = encode(&mut out, &self.registry);
        out
    }
}

/// Register a metric with no labels and return it.
fn register<M>(registry: &mut Registry, name: &str, help: &str) -> M
where
    M: Default + Clone + prometheus_client::registry::Metric,
{
    let metric = M::default();
    registry.register(name, help, metric.clone());
    metric
}

fn result(ok: bool) -> &'static str {
    if ok { "ok" } else { "error" }
}

/// Serve `GET /metrics` on `listener` until the task is dropped. A failed
/// connection is logged and does not stop the server.
pub async fn serve(listener: TcpListener, metrics: Arc<Metrics>) {
    serve_with(listener, metrics, HEADER_READ_TIMEOUT, MAX_CONNECTIONS).await;
}

async fn serve_with(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    header_read_timeout: Duration,
    max_connections: usize,
) {
    let slots = Arc::new(Semaphore::new(max_connections));
    loop {
        // Take a slot before the accept, so a client past the limit waits in
        // the backlog and holds no file descriptor of the daemon.
        let Ok(slot) = slots.clone().acquire_owned().await else {
            return;
        };
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                warn!(error = %e, "metrics endpoint accept failed");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let _slot = slot;
            let service = hyper::service::service_fn(move |req| {
                let metrics = metrics.clone();
                async move { Ok::<_, Infallible>(respond(&req, &metrics)) }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(header_read_timeout)
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                debug!(error = %e, "metrics connection ended with an error");
            }
        });
    }
}

fn respond(req: &Request<Incoming>, metrics: &Metrics) -> Response<Full<Bytes>> {
    if req.method() != Method::GET || req.uri().path() != "/metrics" {
        let mut not_found = Response::new(Full::new(Bytes::from_static(b"not found\n")));
        *not_found.status_mut() = StatusCode::NOT_FOUND;
        return not_found;
    }
    let mut ok = Response::new(Full::new(Bytes::from(metrics.encode())));
    ok.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(OPENMETRICS_CONTENT_TYPE),
    );
    ok
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn encodes_every_family_with_zeroed_series() {
        let text = Metrics::new().encode();
        for line in [
            "coredrop_captures_total{outcome=\"uploaded\"} 0",
            "coredrop_captures_total{outcome=\"failed\"} 0",
            "coredrop_upload_attempts_total{result=\"error\"} 0",
            "coredrop_bundles_expired_total 0",
            "coredrop_spool_bundles 0",
            "coredrop_spool_bytes 0",
            "coredrop_spool_oldest_bundle_age_seconds 0",
            "coredrop_spool_max_bytes 0",
            "coredrop_core_bytes_total 0",
            "coredrop_stored_bytes_total 0",
            "coredrop_cores_truncated_total 0",
            "coredrop_core_pattern_installed 0",
            "coredrop_upload_duration_seconds_count 0",
            "coredrop_events_posted_total{result=\"ok\"} 0",
            "coredrop_event_payloads_dropped_total 0",
        ] {
            assert!(text.contains(line), "missing {line:?} in:\n{text}");
        }
        assert!(
            text.contains(&format!(
                "coredrop_build_info{{version=\"{}\"}} 1",
                env!("CARGO_PKG_VERSION")
            )),
            "{text}"
        );
        assert!(
            !text.contains("outcome=\"spooled\""),
            "spooled is not a final outcome"
        );
    }

    #[test]
    fn counts_and_sets() {
        let metrics = Metrics::new();
        let uploaded = CaptureEventPayload {
            namespace: None,
            pod_name: None,
            pod_uid: None,
            container_id: None,
            container_name: None,
            host_pid: 1,
            exe: String::new(),
            signal: 11,
            signal_name: None,
            outcome: Outcome::Uploaded,
            manifest_key: None,
            core_bytes: 1000,
            stored_bytes: Some(100),
            truncated: false,
            error: None,
            timestamp: 0,
        };
        metrics.capture_done(&uploaded);
        metrics.capture_done(&CaptureEventPayload {
            truncated: true,
            ..uploaded.clone()
        });
        // An expired bundle: the handler stored a truncated core in the
        // spool, but nothing reached the bucket.
        metrics.capture_done(&CaptureEventPayload {
            outcome: Outcome::Failed,
            truncated: true,
            ..uploaded.clone()
        });
        metrics.upload_attempt(false);
        metrics.upload_duration(Duration::from_millis(700));
        metrics.set_spool(3, 4096, Duration::from_secs(90));
        metrics.set_spool_max_bytes(1 << 30);
        metrics.set_core_pattern_installed(true);
        let text = metrics.encode();
        assert!(
            text.contains("coredrop_captures_total{outcome=\"uploaded\"} 2"),
            "{text}"
        );
        assert!(
            text.contains("coredrop_upload_attempts_total{result=\"error\"} 1"),
            "{text}"
        );
        assert!(text.contains("coredrop_spool_bundles 3"), "{text}");
        assert!(text.contains("coredrop_spool_bytes 4096"), "{text}");
        for line in [
            "coredrop_captures_total{outcome=\"failed\"} 1",
            "coredrop_core_bytes_total 3000",
            "coredrop_stored_bytes_total 200",
            "coredrop_cores_truncated_total 1",
            "coredrop_spool_oldest_bundle_age_seconds 90",
            "coredrop_spool_max_bytes 1073741824",
            "coredrop_core_pattern_installed 1",
            "coredrop_upload_duration_seconds_count 1",
            "coredrop_upload_duration_seconds_bucket{le=\"1.0\"} 1",
            "coredrop_upload_duration_seconds_bucket{le=\"0.5\"} 0",
        ] {
            assert!(text.contains(line), "missing {line:?} in:\n{text}");
        }
    }

    #[tokio::test]
    async fn serves_metrics_and_404s_other_paths() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(serve(listener, Arc::new(Metrics::new())));

        let client = reqwest::Client::new();
        let ok = client
            .get(format!("http://{addr}/metrics"))
            .send()
            .await
            .unwrap();
        assert_eq!(ok.status(), 200);
        assert!(
            ok.headers()[CONTENT_TYPE.as_str()]
                .to_str()
                .unwrap()
                .starts_with("application/openmetrics-text")
        );
        assert!(ok.text().await.unwrap().contains("coredrop_build_info"));

        let missing = client.get(format!("http://{addr}/x")).send().await.unwrap();
        assert_eq!(missing.status(), 404);

        server.abort();
    }

    #[tokio::test]
    async fn closes_a_connection_that_sends_no_request() {
        use tokio::io::AsyncReadExt;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_with(
            listener,
            Arc::new(Metrics::new()),
            Duration::from_millis(200),
            MAX_CONNECTIONS,
        ));

        // `read_to_end` returns only when the server closes the connection
        // (EOF, or a reset); any response bytes before it do not matter.
        let mut idle = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), idle.read_to_end(&mut buf))
            .await
            .expect("the server kept an idle connection open");

        server.abort();
    }

    #[tokio::test]
    async fn serves_at_most_max_connections_at_a_time() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_with(
            listener,
            Arc::new(Metrics::new()),
            Duration::from_secs(30),
            1,
        ));

        // An idle client holds the one slot.
        let idle = tokio::net::TcpStream::connect(addr).await.unwrap();
        let client = reqwest::Client::new();
        let url = format!("http://{addr}/metrics");
        let blocked = client
            .get(&url)
            .timeout(Duration::from_millis(500))
            .send()
            .await;
        assert!(blocked.is_err(), "a second connection was served");

        drop(idle);
        let ok = client
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();
        assert_eq!(ok.status(), 200);

        server.abort();
    }
}
