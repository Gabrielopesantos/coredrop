//! Daemon side: Prometheus metrics and the `/metrics` HTTP endpoint.
//!
//! The kernel-exec'd handler lives for one crash, so it keeps no metrics.
//! The daemon counts at the points every capture passes through: the event
//! listener (final outcome, k8s Event posts) and the uploader (upload
//! attempts, spool size). Labels hold bounded values only - never a pod,
//! namespace or object key - so the series count stays fixed.

use std::convert::Infallible;
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tracing::{debug, warn};

use crate::events::Outcome;

/// Default address of the `/metrics` endpoint.
pub const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:9464";

const OPENMETRICS_CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

type OutcomeLabel = [(&'static str, &'static str); 1];
type ResultLabel = [(&'static str, &'static str); 1];

/// The daemon's metrics. One instance is shared by the listener, the
/// uploader and the HTTP endpoint.
pub struct Metrics {
    registry: Registry,
    captures: Family<OutcomeLabel, Counter>,
    upload_attempts: Family<ResultLabel, Counter>,
    bundles_expired: Counter,
    spool_bundles: Gauge,
    spool_bytes: Gauge,
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

        let bundles_expired = Counter::default();
        registry.register(
            "bundles_expired",
            "Spooled captures deleted because they were past the spool max age",
            bundles_expired.clone(),
        );

        let spool_bundles = Gauge::default();
        registry.register(
            "spool_bundles",
            "Captures in the spool waiting for upload",
            spool_bundles.clone(),
        );

        let spool_bytes = Gauge::default();
        registry.register(
            "spool_bytes",
            "Size of the spool in bytes",
            spool_bytes.clone(),
        );

        registry.register(
            "events_posted",
            "Kubernetes Event create and patch requests, by result",
            events_posted.clone(),
        );

        let payloads_dropped = Counter::default();
        registry.register(
            "event_payloads_dropped",
            "Capture-event datagrams the daemon could not parse",
            payloads_dropped.clone(),
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
            bundles_expired,
            spool_bundles,
            spool_bytes,
            events_posted,
            payloads_dropped,
        }
    }

    /// Count a capture that is done.
    pub fn capture(&self, outcome: Outcome) {
        self.captures
            .get_or_create(&[("outcome", outcome.as_str())])
            .inc();
    }

    /// Count one upload attempt of one spooled capture.
    pub fn upload_attempt(&self, ok: bool) {
        self.upload_attempts
            .get_or_create(&[("result", result(ok))])
            .inc();
    }

    /// Count a spooled capture deleted past the spool max age.
    pub fn bundle_expired(&self) {
        self.bundles_expired.inc();
    }

    /// Set the spool gauges.
    pub fn set_spool(&self, bundles: usize, bytes: u64) {
        self.spool_bundles
            .set(i64::try_from(bundles).unwrap_or(i64::MAX));
        self.spool_bytes
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
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

fn result(ok: bool) -> &'static str {
    if ok { "ok" } else { "error" }
}

/// Serve `GET /metrics` on `listener` until the task is dropped. A failed
/// connection is logged and does not stop the server.
pub async fn serve(listener: TcpListener, metrics: Arc<Metrics>) {
    loop {
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
            let service = hyper::service::service_fn(move |req| {
                let metrics = metrics.clone();
                async move { Ok::<_, Infallible>(respond(&req, &metrics)) }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
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
#[allow(clippy::unwrap_used)]
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
        metrics.capture(Outcome::Uploaded);
        metrics.capture(Outcome::Uploaded);
        metrics.upload_attempt(false);
        metrics.set_spool(3, 4096);
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
}
