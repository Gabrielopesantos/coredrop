//! Minimal client for the `events.k8s.io/v1` API, with the pod's
//! projected `ServiceAccount` token and CA.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use tracing::warn;

pub(super) const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// HTTP client for the API server that trusts every certificate in `ca_pem`
/// and nothing else. The API server certificate is signed by the cluster CA,
/// so trusting the system bundle too would let any public CA impersonate it
/// and receive the `ServiceAccount` token. `ca.crt` holds two CAs during a
/// cluster CA rotation, so every certificate in it is read. `None` when
/// `ca_pem` holds no certificate.
pub(super) fn build_http(ca_pem: &[u8]) -> Option<reqwest::Client> {
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
pub(super) struct EventClient {
    pub(super) http: reqwest::Client,
    /// `https://<api server>/apis/events.k8s.io/v1/namespaces`, parsed once.
    /// Requests append their path segments to it.
    pub(super) events_base: reqwest::Url,
    /// The projected `ServiceAccount` token file. The kubelet rotates the
    /// token, so each request reads the file again: see [`Self::token`].
    pub(super) token_path: PathBuf,
}

/// The events API base for an API server origin (`https://host:port`).
pub(super) fn events_base(origin: &str) -> Option<reqwest::Url> {
    reqwest::Url::parse(&format!("{origin}/apis/events.k8s.io/v1/namespaces")).ok()
}

impl EventClient {
    /// Build a client from the projected `ServiceAccount` token/CA and the
    /// in-cluster API server env vars. `None` when any of those are missing -
    /// e.g. running outside a cluster - and the caller logs + drops events.
    pub(super) fn from_env() -> Option<Self> {
        let token_path = PathBuf::from(format!("{SERVICE_ACCOUNT_DIR}/token"));
        read_token(&token_path).ok()?;
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
            token_path,
        })
    }

    /// URL of the namespace's events collection, or of one event when `name`
    /// is set. Each segment is percent-encoded, so a `/` or `?` in a value
    /// cannot change the path. `.` and `..` segments are dropped by `url`;
    /// `handle_payload` rejects those before any request.
    pub(super) fn events_url(&self, namespace: &str, name: Option<&str>) -> Result<reqwest::Url> {
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

    /// The current `ServiceAccount` token. Read for each request: the kubelet
    /// replaces the file before the old token expires, and a cached token
    /// would fail every request after that until the daemon restarts.
    fn token(&self) -> Result<String> {
        read_token(&self.token_path)
    }

    pub(super) async fn create_event(&self, namespace: &str, body: &Value) -> Result<()> {
        let url = self.events_url(namespace, None)?;
        let resp = self
            .http
            .post(url)
            .bearer_auth(self.token()?)
            .json(body)
            .send()
            .await
            .context("create event")?;
        api_result(resp, "create event").await
    }

    pub(super) async fn patch_event_series(
        &self,
        namespace: &str,
        name: &str,
        body: &Value,
    ) -> Result<()> {
        let url = self.events_url(namespace, Some(name))?;
        let resp = self
            .http
            .patch(url)
            .bearer_auth(self.token()?)
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
    pub(super) fn new_for_test(
        http: reqwest::Client,
        api_server: &str,
        token_path: PathBuf,
    ) -> Self {
        Self {
            http,
            events_base: events_base(api_server).unwrap(),
            token_path,
        }
    }
}

/// Write the token `test` into `dir` and return the file's path.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
pub(super) fn token_file(dir: &tempfile::TempDir) -> PathBuf {
    let path = dir.path().join("token");
    std::fs::write(&path, "test\n").unwrap();
    path
}

/// Read the `ServiceAccount` token at `path`, without surrounding whitespace.
fn read_token(path: &Path) -> Result<String> {
    let token = std::fs::read_to_string(path)
        .with_context(|| format!("reading token {}", path.display()))?;
    Ok(token.trim().to_string())
}

/// A non-2xx response from the API server.
#[derive(Debug)]
struct ApiError {
    what: &'static str,
    status: reqwest::StatusCode,
    body: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: HTTP {}: {}", self.what, self.status, self.body)
    }
}

impl std::error::Error for ApiError {}

/// Whether `e` is an API server `404 Not Found`.
pub(super) fn is_not_found(e: &anyhow::Error) -> bool {
    e.downcast_ref::<ApiError>()
        .is_some_and(|e| e.status == reqwest::StatusCode::NOT_FOUND)
}

/// Turn a non-2xx response into an [`ApiError`] carrying the API server's
/// response body - the status code alone ("HTTP 400") gives no way to tell a
/// bad request body from an RBAC/admission rejection.
pub(super) async fn api_result(resp: reqwest::Response, what: &'static str) -> Result<()> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    let body = body.chars().take(500).collect::<String>();
    Err(ApiError { what, status, body }.into())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use tempfile::TempDir;

    use super::*;

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

    fn test_client(origin: &str) -> EventClient {
        EventClient::new_for_test(
            build_http(TEST_CA_1.as_bytes()).unwrap(),
            origin,
            PathBuf::from("/nonexistent/token"),
        )
    }

    #[test]
    fn token_is_read_again_after_rotation() {
        let dir = TempDir::new().unwrap();
        let path = token_file(&dir);
        let client =
            EventClient::new_for_test(reqwest::Client::new(), "http://127.0.0.1:1", path.clone());
        assert_eq!(client.token().unwrap(), "test");

        // The kubelet replaces the file with a new token.
        std::fs::write(&path, "rotated\n").unwrap();
        assert_eq!(client.token().unwrap(), "rotated");

        std::fs::remove_file(&path).unwrap();
        let err = client.token().unwrap_err();
        assert!(format!("{err:#}").contains("reading token"), "{err:#}");
    }

    #[test]
    fn is_not_found_matches_only_an_api_404() {
        let api = |status| -> anyhow::Error {
            ApiError {
                what: "patch event series",
                status,
                body: String::new(),
            }
            .into()
        };
        assert!(is_not_found(&api(reqwest::StatusCode::NOT_FOUND)));
        assert!(!is_not_found(&api(reqwest::StatusCode::FORBIDDEN)));
        assert!(!is_not_found(&anyhow!("connection refused")));
        assert_eq!(
            api(reqwest::StatusCode::NOT_FOUND).to_string(),
            "patch event series: HTTP 404 Not Found: "
        );
    }
}
