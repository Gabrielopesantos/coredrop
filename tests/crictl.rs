#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::os::unix::fs::PermissionsExt;

use coredrop::config::HandlerConfig;
use coredrop::crictl;

/// Serializes tests that write executable scripts and spawn subprocesses.
/// Without it, a concurrent test's fork can inherit another test's open
/// write-fd for a script, making that script's exec fail with ETXTBSY.
static SPAWN_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn unique_tmp(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::path::PathBuf::from(format!(
        "/tmp/coredrop-crictl-test-{}-{tag}-{nanos}",
        std::process::id()
    ))
}

fn write_executable(path: &std::path::Path, script: &str) {
    std::fs::write(path, script).unwrap();
    let mut perms = std::fs::metadata(path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms).unwrap();
}

fn config_with_crictl(crictl_path: &str) -> HandlerConfig {
    HandlerConfig {
        crictl_path: crictl_path.to_string(),
        ..HandlerConfig::default()
    }
}

// Canned `crictl inspect` JSON matching the shape the `extract` function expects.
const FAKE_CRICTL_JSON: &str = r#"{
  "status": {
    "id": "abc123def456abc123",
    "metadata": { "name": "mycontainer", "attempt": 2 },
    "image": { "image": "docker.io/library/nginx:1.25" },
    "imageRef": "docker.io/library/nginx@sha256:cafebabe1234",
    "labels": {
      "io.kubernetes.pod.namespace": "production",
      "io.kubernetes.pod.name": "nginx-abc123",
      "io.kubernetes.container.name": "mycontainer"
    }
  }
}"#;

/// 2b - fake crictl script: subprocess is spawned, output parsed, all fields
/// extracted correctly.
#[tokio::test]
async fn inspect_fake_script_parses_container_info() {
    let _guard = SPAWN_LOCK.lock().await;
    let tmp = unique_tmp("fake");
    std::fs::create_dir_all(&tmp).unwrap();

    let script = tmp.join("crictl");
    write_executable(
        &script,
        &format!("#!/bin/sh\nprintf '%s' '{FAKE_CRICTL_JSON}'\n"),
    );

    let config = config_with_crictl(script.to_str().unwrap());
    let info = crictl::inspect("abc123def456abc123", &config).await;

    let info = info.expect("inspect must return Some for a well-formed crictl script");
    assert_eq!(info.namespace.as_deref(), Some("production"));
    assert_eq!(info.pod_name.as_deref(), Some("nginx-abc123"));
    assert_eq!(info.container_name.as_deref(), Some("mycontainer"));
    assert_eq!(info.image.as_deref(), Some("docker.io/library/nginx:1.25"));
    assert_eq!(info.image_digest.as_deref(), Some("sha256:cafebabe1234"));
    assert_eq!(info.restart_count, Some(2));

    std::fs::remove_dir_all(&tmp).ok();
}

/// 2b - non-zero exit: crictl exits 1 → inspect degrades to None.
#[tokio::test]
async fn inspect_nonzero_exit_returns_none() {
    let _guard = SPAWN_LOCK.lock().await;
    let tmp = unique_tmp("fail");
    std::fs::create_dir_all(&tmp).unwrap();

    let script = tmp.join("crictl");
    write_executable(&script, "#!/bin/sh\nexit 1\n");

    let config = config_with_crictl(script.to_str().unwrap());
    let info = crictl::inspect("any-id", &config).await;
    assert!(info.is_none(), "non-zero exit must degrade to None");

    std::fs::remove_dir_all(&tmp).ok();
}

/// 2b - nonexistent binary: spawn fails → inspect degrades to None, no panic.
#[tokio::test]
async fn inspect_nonexistent_binary_returns_none() {
    let _guard = SPAWN_LOCK.lock().await;
    let config = config_with_crictl("/nonexistent/path/to/crictl");
    let info = crictl::inspect("any-id", &config).await;
    assert!(info.is_none(), "missing binary must degrade to None");
}
