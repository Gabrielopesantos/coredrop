//! `crictl inspect` subprocess behavior: a real fork/exec against stand-in
//! scripts, covering what the unit tests in `src/crictl.rs` cannot (they only
//! exercise JSON extraction from an already-parsed value).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use tempfile::TempDir;

use coredrop::config::HandlerConfig;
use coredrop::crictl;

mod common;
use common::{SPAWN_LOCK, fake_crictl_script, write_fake_crictl};

fn config_with_crictl(crictl_path: &str) -> HandlerConfig {
    HandlerConfig {
        crictl_path: crictl_path.to_string(),
        crictl_timeout_secs: 1,
        ..HandlerConfig::default()
    }
}

/// A well-formed crictl: the subprocess is spawned, its stdout parsed, and
/// every identity field extracted.
#[tokio::test]
async fn inspect_fake_script_parses_container_info() {
    let _guard = SPAWN_LOCK.lock().await;
    let tmp = TempDir::new().unwrap();
    let script = write_fake_crictl(
        tmp.path(),
        &fake_crictl_script("production", "nginx-abc123", "mycontainer"),
    );

    let config = config_with_crictl(script.to_str().unwrap());
    let info = crictl::inspect("abc123def456abc123", &config)
        .await
        .expect("inspect must return Some for a well-formed crictl script");

    assert_eq!(info.namespace.as_deref(), Some("production"));
    assert_eq!(info.pod_name.as_deref(), Some("nginx-abc123"));
    assert_eq!(info.container_name.as_deref(), Some("mycontainer"));
    assert_eq!(info.image.as_deref(), Some("docker.io/library/nginx:1.25"));
    assert_eq!(info.image_digest.as_deref(), Some("sha256:cafebabe1234"));
    assert_eq!(info.restart_count, Some(2));
}

/// The configured CRI endpoint has to reach the subprocess as
/// `CONTAINER_RUNTIME_ENDPOINT`; the kernel exec's the handler with a clean
/// environment, so nothing else would set it. The script echoes the variable
/// back so its arrival is observable.
#[tokio::test]
async fn inspect_passes_the_cri_endpoint_to_the_subprocess() {
    let _guard = SPAWN_LOCK.lock().await;
    let tmp = TempDir::new().unwrap();
    let script = write_fake_crictl(
        tmp.path(),
        "#!/bin/sh\nprintf '{\"status\":{\"labels\":\
         {\"io.kubernetes.pod.name\":\"%s\"}}}' \"$CONTAINER_RUNTIME_ENDPOINT\"\n",
    );

    let mut config = config_with_crictl(script.to_str().unwrap());
    config.cri_runtime_endpoint = Some("unix:///run/containerd/containerd.sock".into());

    let info = crictl::inspect("any-id", &config).await.unwrap();
    assert_eq!(
        info.pod_name.as_deref(),
        Some("unix:///run/containerd/containerd.sock"),
        "crictl must be spawned with CONTAINER_RUNTIME_ENDPOINT set"
    );
}

/// Non-zero exit: enrichment degrades to `None` rather than failing the capture.
#[tokio::test]
async fn inspect_nonzero_exit_returns_none() {
    let _guard = SPAWN_LOCK.lock().await;
    let tmp = TempDir::new().unwrap();
    let script = write_fake_crictl(tmp.path(), "#!/bin/sh\nexit 1\n");

    let config = config_with_crictl(script.to_str().unwrap());
    assert!(
        crictl::inspect("any-id", &config).await.is_none(),
        "non-zero exit must degrade to None"
    );
}

/// Unparseable stdout degrades the same way a failed spawn does.
#[tokio::test]
async fn inspect_unparseable_output_returns_none() {
    let _guard = SPAWN_LOCK.lock().await;
    let tmp = TempDir::new().unwrap();
    let script = write_fake_crictl(tmp.path(), "#!/bin/sh\nprintf 'not json at all'\n");

    let config = config_with_crictl(script.to_str().unwrap());
    assert!(
        crictl::inspect("any-id", &config).await.is_none(),
        "unparseable crictl output must degrade to None"
    );
}

/// crictl runs after the core drain, but the handler still holds a
/// `core_pipe_limit` slot: a wedged CRI socket must not pin it indefinitely.
#[tokio::test]
async fn inspect_times_out_and_returns_none() {
    let _guard = SPAWN_LOCK.lock().await;
    let tmp = TempDir::new().unwrap();
    // Sleep longer than the 1s timeout configured in `config_with_crictl`.
    let script = write_fake_crictl(tmp.path(), "#!/bin/sh\nsleep 5\nprintf '%s' '{}'\n");

    let config = config_with_crictl(script.to_str().unwrap());
    let start = std::time::Instant::now();
    let info = crictl::inspect("any-id", &config).await;
    let elapsed = start.elapsed();

    assert!(info.is_none(), "timed-out crictl must degrade to None");
    assert!(
        elapsed < Duration::from_secs(3),
        "timed-out crictl should return quickly, got {elapsed:?}"
    );
}
