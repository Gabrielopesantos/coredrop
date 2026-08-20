//! End-to-end capture-handler tests: `run()` against a fixture `/proc` tree and
//! an in-memory object store.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use tempfile::TempDir;

use coredrop::events::{CaptureEventPayload, Outcome};
use coredrop::handler::{CaptureArgs, run};
use coredrop::manifest::Manifest;
use coredrop::upload;

mod common;
use common::{
    FailingStore, StallReader, base_config, fake_crictl_script, get_object, is_absent, proc_root,
    unzstd, write_fake_crictl, write_fixture_proc,
};

const POD_UID: &str = "ed1e9c81-9a92-4f7e-be2c-8b26b56d3b98";
const CONTAINER_ID: &str = "abc123def456abc123def456"; // 24 hex chars -- valid
const TS: i64 = 1_749_600_000;

fn capture_args(pid: i32, signal: i32, timestamp: i64) -> CaptureArgs {
    CaptureArgs {
        host_pid: pid,
        signal,
        timestamp,
        exe: "!usr!bin!crasher".into(),
    }
}

/// The three object keys one capture produces, for the standard fixture identity.
fn keys(container_id: &str, ts: i64, pid: i32) -> (String, String, String) {
    (
        upload::core_object_key("test", POD_UID, container_id, ts, pid),
        upload::proc_snapshot_object_key("test", POD_UID, container_id, ts, pid),
        upload::manifest_object_key("test", POD_UID, container_id, ts, pid),
    )
}

async fn read_manifest(store: &Arc<dyn ObjectStore>, key: &str) -> Manifest {
    serde_json::from_slice(&get_object(store, key).await).unwrap()
}

/// Happy path: core, proc-snapshot, and manifest all land in the store.
/// Verifies the load-bearing blob-first invariant: the manifest's
/// `core.object_key` points at an object that actually exists (no dangling
/// manifests).
#[tokio::test]
async fn run_uploads_core_snapshot_and_writes_manifest() {
    let pid = 4242;
    let core_payload: &[u8] = b"fake core payload for testing - not a real ELF";

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = base_config(tmp.path());

    let mut core_in: &[u8] = core_payload;
    run(
        capture_args(pid, 11, TS),
        &config,
        &mut core_in,
        Some(store.clone()),
    )
    .await
    .unwrap();

    let (core_key, snap_key, manifest_key) = keys(CONTAINER_ID, TS, pid);

    // Core object: present and decompresses to the input bytes.
    let stored_core = get_object(&store, &core_key).await;
    assert_eq!(unzstd(&stored_core).await, core_payload);

    // Proc-snapshot tar: present.
    get_object(&store, &snap_key).await;

    let manifest = read_manifest(&store, &manifest_key).await;
    assert!(manifest.core.present, "core.present must be true");
    assert_eq!(manifest.signal, 11);
    assert_eq!(manifest.signal_name.as_deref(), Some("SIGSEGV"));
    assert_eq!(manifest.cluster, "test");
    assert_eq!(manifest.identity.pod_uid, POD_UID);
    assert_eq!(manifest.identity.container_id, CONTAINER_ID);
    assert!(manifest.core.sha256.is_some(), "sha256 populated");
    assert!(manifest.core.size_bytes.unwrap_or(0) > 0, "size_bytes > 0");
    assert!(
        manifest.core.stored_bytes.unwrap_or(0) > 0,
        "stored_bytes > 0"
    );
    assert!(!manifest.core.truncated);
    assert_eq!(manifest.core.codec, "zstd");

    // The blob-first invariant: manifest.core.object_key must exist in the
    // store. No manifest may point at a missing core.
    let manifest_core_key = manifest
        .core
        .object_key
        .as_deref()
        .expect("core key in manifest");
    assert_eq!(manifest_core_key, core_key);
    store
        .get(&ObjectPath::from(manifest_core_key))
        .await
        .expect("manifest.core.object_key must exist in the store");

    // Proc-snapshot ref in manifest.
    let snap_ref = manifest
        .proc_snapshot
        .as_ref()
        .expect("proc_snapshot in manifest");
    assert_eq!(snap_ref.object_key, snap_key);
    assert!(snap_ref.file_count > 0);
}

/// crictl enrichment succeeding is the only path that fills the manifest's
/// human-readable identity and the only one that reaches the capture-event
/// send at the end of the pipeline - every other test here runs with crictl
/// pointed at `/bin/false`.
#[tokio::test]
async fn run_enriches_identity_via_crictl_and_reports_the_capture() {
    let _guard = common::SPAWN_LOCK.lock().await;
    let pid = 4247;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let crictl = write_fake_crictl(
        tmp.path(),
        &fake_crictl_script("production", "nginx-abc123", "mycontainer"),
    );

    // Bind the events socket before the run: the handler's send is
    // fire-and-forget, so an unbound path would silently drop the datagram.
    let sock_path = tmp.path().join("events.sock");
    let listener = std::os::unix::net::UnixDatagram::bind(&sock_path).unwrap();
    listener
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut config = base_config(tmp.path());
    config.crictl_path = crictl.to_string_lossy().into_owned();
    config.crictl_timeout_secs = 10;
    config.event_socket_path = Some(sock_path.to_string_lossy().into_owned());

    let mut core_in: &[u8] = b"core payload for the enriched path";
    run(
        capture_args(pid, 11, TS),
        &config,
        &mut core_in,
        Some(store.clone()),
    )
    .await
    .unwrap();

    let (_core_key, _snap_key, manifest_key) = keys(CONTAINER_ID, TS, pid);
    let manifest = read_manifest(&store, &manifest_key).await;

    // Cgroup-derived identity is still there, now joined by the crictl fields.
    assert_eq!(manifest.identity.pod_uid, POD_UID);
    assert_eq!(manifest.identity.namespace.as_deref(), Some("production"));
    assert_eq!(manifest.identity.pod_name.as_deref(), Some("nginx-abc123"));
    assert_eq!(
        manifest.identity.container_name.as_deref(),
        Some("mycontainer")
    );
    assert_eq!(
        manifest.identity.image.as_deref(),
        Some("docker.io/library/nginx:1.25")
    );
    assert_eq!(
        manifest.identity.image_digest.as_deref(),
        Some("sha256:cafebabe1234")
    );
    assert_eq!(manifest.identity.restart_count, Some(2));

    // The capture event the daemon turns into a k8s Event.
    let mut buf = vec![0u8; 64 * 1024];
    let n = listener
        .recv(&mut buf)
        .expect("handler must report the capture on the events socket");
    let event: CaptureEventPayload = serde_json::from_slice(&buf[..n]).unwrap();
    assert_eq!(event.namespace, "production");
    assert_eq!(event.pod_name, "nginx-abc123");
    assert_eq!(event.pod_uid, POD_UID);
    assert_eq!(event.container_name.as_deref(), Some("mycontainer"));
    assert_eq!(event.outcome, Outcome::Uploaded);
    assert_eq!(event.signal_name.as_deref(), Some("SIGSEGV"));
    assert_eq!(event.timestamp, TS);
    assert_eq!(
        event.manifest_key.as_deref(),
        Some(manifest_key.as_str()),
        "the event must point at the manifest that was actually written"
    );
    assert_eq!(event.stored_bytes, manifest.core.stored_bytes);
}

/// Size cap: only the first `max_core_bytes` land in the store; the manifest
/// records the full drained size and the truncation reason.
#[tokio::test]
async fn run_size_cap_truncates_stored_core() {
    let pid = 4243;
    let core_payload: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut config = base_config(tmp.path());
    config.max_core_bytes = 1_000;

    let mut core_in: &[u8] = &core_payload;
    run(
        capture_args(pid, 11, TS),
        &config,
        &mut core_in,
        Some(store.clone()),
    )
    .await
    .unwrap();

    let (core_key, _snap_key, manifest_key) = keys(CONTAINER_ID, TS, pid);
    let stored_core = get_object(&store, &core_key).await;
    assert_eq!(
        unzstd(&stored_core).await,
        &core_payload[..1_000],
        "stored core holds exactly the first cap bytes"
    );

    let manifest = read_manifest(&store, &manifest_key).await;
    assert!(manifest.core.truncated);
    assert_eq!(manifest.core.truncated_reason.as_deref(), Some("size_cap"));
    assert_eq!(
        manifest.core.size_bytes,
        Some(core_payload.len() as u64),
        "size_bytes records the full drained size"
    );
}

/// Rate limit: with a budget of 1, the second crash gets no core object but
/// still gets a proc snapshot and a manifest marked `rate_limit`.
#[tokio::test]
async fn run_rate_limit_suppresses_core_keeps_manifest() {
    let pid = 4244;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut config = base_config(tmp.path());
    config.max_cores_per_hour = 1;

    for ts in [TS, TS + 10] {
        let mut core_in: &[u8] = b"core payload for rate limit test";
        run(
            capture_args(pid, 11, ts),
            &config,
            &mut core_in,
            Some(store.clone()),
        )
        .await
        .unwrap_or_else(|e| panic!("run at {ts} failed: {e}"));
    }

    // First crash: full capture.
    let (core1, _, _) = keys(CONTAINER_ID, TS, pid);
    get_object(&store, &core1).await;

    // Second crash: no core object...
    let (core2, snap2, manifest2_key) = keys(CONTAINER_ID, TS + 10, pid);
    assert!(
        is_absent(&store, &core2).await,
        "suppressed crash must not store a core"
    );
    // ...but proc snapshot and manifest are still written.
    get_object(&store, &snap2).await;
    let manifest2 = read_manifest(&store, &manifest2_key).await;
    assert!(!manifest2.core.present);
    assert!(manifest2.core.object_key.is_none());
    assert_eq!(manifest2.core.skipped_reason.as_deref(), Some("rate_limit"));
}

/// The budget is keyed by pod UID, not container ID: a restarted container gets
/// a new container ID but keeps the same pod UID, so the budget must follow it.
#[tokio::test]
async fn run_rate_limit_keys_by_pod_uid_not_container_id() {
    let container_id_2 = "fedcba654321fedcba654321";
    let (pid_1, pid_2) = (4250, 4251);

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid_1, POD_UID, CONTAINER_ID);
    write_fixture_proc(tmp.path(), pid_2, POD_UID, container_id_2);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut config = base_config(tmp.path());
    config.max_cores_per_hour = 1;

    for (pid, ts) in [(pid_1, TS), (pid_2, TS + 10)] {
        let mut core_in: &[u8] = b"core payload for pod-uid rate limit test";
        run(
            capture_args(pid, 11, ts),
            &config,
            &mut core_in,
            Some(store.clone()),
        )
        .await
        .unwrap_or_else(|e| panic!("run for pid {pid} failed: {e}"));
    }

    // First crash: full capture.
    let (core1, _, _) = keys(CONTAINER_ID, TS, pid_1);
    get_object(&store, &core1).await;

    // Second crash: different container ID but same pod UID -> suppressed.
    let (core2, _, manifest2_key) = keys(container_id_2, TS + 10, pid_2);
    assert!(
        is_absent(&store, &core2).await,
        "restarted container with same pod UID must inherit the rate-limit budget"
    );

    let manifest2 = read_manifest(&store, &manifest2_key).await;
    assert!(!manifest2.core.present);
    assert_eq!(manifest2.core.skipped_reason.as_deref(), Some("rate_limit"));
}

/// Rate-limit refund: a crash whose core upload fails must not consume budget,
/// or a transient store outage would exhaust it with zero cores kept.
#[tokio::test]
async fn run_failed_upload_refunds_rate_budget() {
    let pid = 4245;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);

    let mut config = base_config(tmp.path());
    config.max_cores_per_hour = 1;

    // First crash: the store rejects the core upload -> run() errors, slot refunded.
    let failing: Arc<dyn ObjectStore> = Arc::new(FailingStore::core(Arc::new(InMemory::new())));
    let mut core_in: &[u8] = b"core payload";
    assert!(
        run(
            capture_args(pid, 11, TS),
            &config,
            &mut core_in,
            Some(failing)
        )
        .await
        .is_err(),
        "core upload failure must surface as an error"
    );

    // Second crash: budget of 1 must still be available.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut core_in: &[u8] = b"core payload";
    run(
        capture_args(pid, 11, TS + 10),
        &config,
        &mut core_in,
        Some(store.clone()),
    )
    .await
    .unwrap();

    let (core_key, _, manifest_key) = keys(CONTAINER_ID, TS + 10, pid);
    get_object(&store, &core_key).await;
    let manifest = read_manifest(&store, &manifest_key).await;
    assert!(manifest.core.present, "refunded budget must allow the core");
    assert!(manifest.core.skipped_reason.is_none());
}

/// Upload deadline: a stalled drain cannot hold the handler (and its
/// `core_pipe_limit` slot) forever - `run()` errors once the deadline passes,
/// and the abandoned crash refunds its rate-limit budget.
#[tokio::test(start_paused = true)]
async fn run_upload_deadline_aborts_stalled_drain() {
    let pid = 4246;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut config = base_config(tmp.path());
    config.upload_deadline_secs = 5;
    config.max_cores_per_hour = 1;

    let mut stalled = StallReader;
    assert!(
        run(
            capture_args(pid, 11, TS),
            &config,
            &mut stalled,
            Some(store.clone())
        )
        .await
        .is_err(),
        "a stalled drain must error at the deadline"
    );
    let (core1, _, _) = keys(CONTAINER_ID, TS, pid);
    assert!(
        is_absent(&store, &core1).await,
        "abandoned upload must not store a core"
    );

    // The abandoned crash refunded the budget of 1: the next crash uploads.
    let mut core_in: &[u8] = b"core payload after deadline abort";
    run(
        capture_args(pid, 11, TS + 10),
        &config,
        &mut core_in,
        Some(store.clone()),
    )
    .await
    .unwrap();
    let (core2, _, _) = keys(CONTAINER_ID, TS + 10, pid);
    get_object(&store, &core2).await;
}

/// No store: the core is drained and discarded so the kernel's pipe completes,
/// and nothing else runs - including the rate limiter, which is only consulted
/// when a core would actually upload.
#[tokio::test]
async fn run_without_store_discards_silently() {
    let pid = 100;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);

    let mut config = base_config(tmp.path());
    config.max_cores_per_hour = 1;

    let mut core_in: &[u8] = b"some core bytes";
    // No store_override, config.store_url = None -> DiscardBackend, no manifest.
    run(capture_args(pid, 6, 1_000_000), &config, &mut core_in, None)
        .await
        .unwrap();

    assert!(
        !std::path::Path::new(&config.rate_state_path).exists(),
        "no store means no upload to budget, so the limiter must not run"
    );
}

/// Non-k8s cgroup: no identity, so no object key is derivable and nothing is
/// written - the core is still drained.
#[tokio::test]
async fn run_non_kubernetes_cgroup_skips_uploads() {
    let pid = 200;

    let tmp = TempDir::new().unwrap();
    let pid_dir = proc_root(tmp.path()).join(pid.to_string());
    std::fs::create_dir_all(&pid_dir).unwrap();
    // Non-k8s cgroup -> parse_cgroup returns None.
    std::fs::write(pid_dir.join("cgroup"), "0::/system.slice/sshd.service\n").unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = base_config(tmp.path());

    let mut core_in: &[u8] = b"core bytes";
    run(
        capture_args(pid, 9, 2_000_000),
        &config,
        &mut core_in,
        Some(store.clone()),
    )
    .await
    .unwrap();

    let result = store.list_with_delimiter(None).await.unwrap();
    assert!(
        result.objects.is_empty(),
        "no objects should be written without a k8s cgroup"
    );
}

/// Blob-first write ordering: when the manifest PUT fails, the core and
/// proc-snapshot are still present. An orphan blob is acceptable; a dangling
/// manifest (pointing at a missing core) is not.
#[tokio::test]
async fn run_blob_first_core_survives_manifest_failure() {
    let pid = 4242;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);

    let inner = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = Arc::new(FailingStore::manifest(inner.clone()));
    let config = base_config(tmp.path());

    let mut core_in: &[u8] = b"core payload for blob-first test";
    // run() must complete Ok even when manifest write fails (handler warns + continues).
    run(
        capture_args(pid, 11, TS),
        &config,
        &mut core_in,
        Some(store),
    )
    .await
    .unwrap();

    let (core_key, snap_key, manifest_key) = keys(CONTAINER_ID, TS, pid);
    let inner: Arc<dyn ObjectStore> = inner;
    // Core and proc-snapshot are present (written before the manifest attempt).
    get_object(&inner, &core_key).await;
    get_object(&inner, &snap_key).await;
    assert!(
        is_absent(&inner, &manifest_key).await,
        "manifest must not exist when its write failed"
    );
}
