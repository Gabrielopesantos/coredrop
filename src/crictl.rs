//! Post-drain container identity enrichment via `crictl inspect`.
//!
//! After the core is drained (the time-critical section is done), we shell out
//! to `crictl` to resolve human-readable identity: namespace, pod name,
//! container name, image, and best-effort restart count. All fields are
//! `Option` - a failed or absent crictl call just leaves them `None` in the
//! manifest, which still carries cgroup-derived `podUID` + `containerID`.
//!
//! `crictl inspect <containerID>` returns JSON whose `status.labels` carry the
//! Kubernetes identity fields (the canonical source for
//! `io.kubernetes.pod.namespace` etc.); `status.metadata.attempt` carries the
//! restart count analog.

use serde_json::Value;
use std::time::Duration;
use tokio::time::timeout;
use tracing::warn;

use crate::config::HandlerConfig;

/// Error from running `crictl inspect` with an optional wall-clock timeout.
#[derive(Debug)]
enum RunError {
    /// The subprocess could not be spawned or exited with an IO error.
    Io(std::io::Error),
    /// The configured timeout elapsed before the subprocess completed.
    Timeout,
}

/// Run a `tokio::process::Command` with an optional timeout. A `None` timeout
/// means wait indefinitely (used when `crictl_timeout_secs` is 0).
async fn run_command(
    cmd: &mut tokio::process::Command,
    timeout_dur: Option<Duration>,
) -> Result<std::process::Output, RunError> {
    match timeout_dur {
        None => cmd.output().await.map_err(RunError::Io),
        Some(d) => timeout(d, cmd.output())
            .await
            .map_err(|_| RunError::Timeout)?
            .map_err(RunError::Io),
    }
}

/// The filesystem path of a `unix://` CRI endpoint, or `None` for any other
/// scheme (crictl also accepts e.g. `tcp://`, which has no path to check).
#[must_use]
pub fn unix_socket_path(endpoint: &str) -> Option<&str> {
    endpoint.strip_prefix("unix://").filter(|p| !p.is_empty())
}

/// Kubernetes identity enriched from `crictl inspect`. All fields best-effort.
#[derive(Debug, Clone, Default)]
pub struct ContainerInfo {
    pub namespace: Option<String>,
    pub pod_name: Option<String>,
    pub container_name: Option<String>,
    pub image: Option<String>,
    pub image_digest: Option<String>,
    /// Restart count analog (`status.metadata.attempt`).
    pub restart_count: Option<u32>,
}

/// Shell out to `crictl inspect <container_id>` and parse the result. Returns
/// `None` on any subprocess or parse failure - callers degrade gracefully to
/// cgroup-only identity.
pub async fn inspect(container_id: &str, config: &HandlerConfig) -> Option<ContainerInfo> {
    let mut cmd = tokio::process::Command::new(&config.crictl_path);
    cmd.arg("inspect").arg(container_id);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::null());

    if let Some(endpoint) = &config.cri_runtime_endpoint {
        cmd.env("CONTAINER_RUNTIME_ENDPOINT", endpoint);
    }

    let timeout_dur = if config.crictl_timeout_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(config.crictl_timeout_secs))
    };
    let output = match run_command(&mut cmd, timeout_dur).await {
        Ok(o) => o,
        Err(RunError::Io(e)) => {
            warn!(error = %e, container_id, crictl = %config.crictl_path, "crictl inspect failed; identity will be cgroup-only");
            return None;
        }
        Err(RunError::Timeout) => {
            warn!(
                container_id,
                crictl = %config.crictl_path,
                timeout_secs = config.crictl_timeout_secs,
                "crictl inspect timed out; identity will be cgroup-only"
            );
            return None;
        }
    };

    if !output.status.success() {
        warn!(
            container_id,
            status = %output.status,
            "crictl inspect exited non-zero; identity will be cgroup-only"
        );
        return None;
    }

    let json: Value = match serde_json::from_slice(&output.stdout) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "crictl inspect JSON parse failed; identity will be cgroup-only");
            return None;
        }
    };

    Some(extract(&json))
}

/// Extract `ContainerInfo` from the parsed `crictl inspect` JSON value.
/// All fields best-effort - missing keys produce `None`.
fn extract(v: &Value) -> ContainerInfo {
    let status = &v["status"];
    let labels = &status["labels"];
    let metadata = &status["metadata"];

    let namespace = str_field(labels, "io.kubernetes.pod.namespace");
    let pod_name = str_field(labels, "io.kubernetes.pod.name");
    let container_name = str_field(labels, "io.kubernetes.container.name");

    let image = status["image"]["image"].as_str().map(str::to_string);

    let image_digest = status["imageRef"]
        .as_str()
        .and_then(|r| r.split_once('@').map(|(_, d)| d.to_string()));

    let restart_count = metadata["attempt"]
        .as_u64()
        .and_then(|n| u32::try_from(n).ok());

    ContainerInfo {
        namespace,
        pod_name,
        container_name,
        image,
        image_digest,
        restart_count,
    }
}

fn str_field(obj: &Value, key: &str) -> Option<String> {
    obj[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    fn crictl_inspect_response(
        namespace: &str,
        pod: &str,
        container: &str,
        image: &str,
        image_ref: &str,
        attempt: u64,
    ) -> Value {
        json!({
            "status": {
                "id": "abc123def456",
                "metadata": { "name": container, "attempt": attempt },
                "image": { "image": image },
                "imageRef": image_ref,
                "labels": {
                    "io.kubernetes.pod.namespace": namespace,
                    "io.kubernetes.pod.name": pod,
                    "io.kubernetes.container.name": container,
                }
            }
        })
    }

    #[test]
    fn unix_socket_path_extracts_only_unix_endpoints() {
        assert_eq!(
            unix_socket_path("unix:///run/containerd/containerd.sock"),
            Some("/run/containerd/containerd.sock")
        );
        assert_eq!(unix_socket_path("tcp://127.0.0.1:1234"), None);
        assert_eq!(unix_socket_path("/run/containerd/containerd.sock"), None);
        assert_eq!(unix_socket_path("unix://"), None);
        assert_eq!(unix_socket_path(""), None);
    }

    #[test]
    fn extracts_all_fields_from_crictl_json() {
        let v = crictl_inspect_response(
            "default",
            "my-pod-abc",
            "app",
            "docker.io/library/ubuntu:latest",
            "docker.io/library/ubuntu@sha256:deadbeef",
            3,
        );
        let info = extract(&v);
        assert_eq!(info.namespace.as_deref(), Some("default"));
        assert_eq!(info.pod_name.as_deref(), Some("my-pod-abc"));
        assert_eq!(info.container_name.as_deref(), Some("app"));
        assert_eq!(
            info.image.as_deref(),
            Some("docker.io/library/ubuntu:latest")
        );
        assert_eq!(info.image_digest.as_deref(), Some("sha256:deadbeef"));
        assert_eq!(info.restart_count, Some(3));
    }

    #[test]
    fn tolerates_missing_fields() {
        let info = extract(&json!({ "status": {} }));
        assert!(info.namespace.is_none());
        assert!(info.pod_name.is_none());
        assert!(info.container_name.is_none());
        assert!(info.image.is_none());
        assert!(info.image_digest.is_none());
        assert!(info.restart_count.is_none());
    }

    #[test]
    fn image_digest_extracted_after_at_sign() {
        let v = json!({
            "status": {
                "imageRef": "registry.io/app@sha256:cafebabe",
                "labels": {}
            }
        });
        let info = extract(&v);
        assert_eq!(info.image_digest.as_deref(), Some("sha256:cafebabe"));
    }

    #[test]
    fn image_ref_without_digest_yields_none() {
        let v = json!({
            "status": {
                "imageRef": "registry.io/app:latest",
                "labels": {}
            }
        });
        let info = extract(&v);
        assert!(info.image_digest.is_none());
    }
}
