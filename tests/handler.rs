//! End-to-end capture-handler tests: `run()` against a fixture `/proc` tree and
//! a spool in a temp dir.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};

use tempfile::TempDir;

use coredrop::config::HandlerConfig;
use coredrop::events::{CaptureEventPayload, Outcome};
use coredrop::handler::{CaptureArgs, run};
use coredrop::manifest::Manifest;
use coredrop::spool::{self, Spool};
use coredrop::upload;

mod common;
use common::{
    StallReader, base_config, bundle_names, fake_crictl_script, proc_root, tar_entry, unzstd,
    write_fake_crictl, write_fixture_proc,
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
    let keys = upload::ObjectKeys::new("test", POD_UID, container_id, ts, pid);
    (keys.core(), keys.proc_snapshot(), keys.manifest())
}

/// The committed bundle of one capture, for the standard fixture identity.
fn ready_bundle(config: &HandlerConfig, container_id: &str, ts: i64, pid: i32) -> PathBuf {
    Spool::new(&config.spool_dir)
        .ready_dir()
        .join(spool::bundle_name(ts, pid, POD_UID, container_id))
}

fn read_manifest(bundle: &Path) -> Manifest {
    serde_json::from_slice(&std::fs::read(bundle.join(spool::MANIFEST_FILE)).unwrap()).unwrap()
}

fn read_capture(bundle: &Path) -> CaptureEventPayload {
    serde_json::from_slice(&std::fs::read(bundle.join(spool::CAPTURE_FILE)).unwrap()).unwrap()
}

/// Bind a capture-event listener in `root` and return it with its path.
fn bind_events_listener(root: &Path) -> (std::os::unix::net::UnixDatagram, PathBuf) {
    let sock_path = root.join("events.sock");
    let listener = std::os::unix::net::UnixDatagram::bind(&sock_path).unwrap();
    listener
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    (listener, sock_path)
}

fn recv_event(listener: &std::os::unix::net::UnixDatagram) -> CaptureEventPayload {
    let mut buf = vec![0u8; 64 * 1024];
    let n = listener
        .recv(&mut buf)
        .expect("the capture must be reported on the events socket");
    serde_json::from_slice(&buf[..n]).unwrap()
}

/// Happy path: core, proc snapshot, manifest and capture event all land in
/// one committed bundle, and the manifest points at the object keys the
/// daemon uploads them to.
#[tokio::test]
async fn run_spools_core_snapshot_and_manifest() {
    let pid = 4242;
    let core_payload: &[u8] = b"fake core payload for testing - not a real ELF";

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let config = base_config(tmp.path());

    run(capture_args(pid, 11, TS), &config, core_payload)
        .await
        .unwrap();

    let spool = Spool::new(&config.spool_dir);
    assert!(
        bundle_names(&spool.incoming_dir()).is_empty(),
        "a committed bundle leaves nothing in incoming/"
    );
    let bundle = ready_bundle(&config, CONTAINER_ID, TS, pid);
    let (core_key, snap_key, manifest_key) = keys(CONTAINER_ID, TS, pid);

    let stored_core = std::fs::read(bundle.join(spool::CORE_FILE)).unwrap();
    assert_eq!(unzstd(&stored_core).await, core_payload);
    assert!(bundle.join(spool::SNAPSHOT_FILE).exists());

    let manifest = read_manifest(&bundle);
    assert!(manifest.core.present, "core.present must be true");
    assert_eq!(manifest.signal, 11);
    assert_eq!(manifest.signal_name.as_deref(), Some("SIGSEGV"));
    assert_eq!(manifest.cluster, "test");
    assert_eq!(manifest.identity.pod_uid, POD_UID);
    assert_eq!(manifest.identity.container_id, CONTAINER_ID);
    assert_eq!(manifest.core.object_key.as_deref(), Some(core_key.as_str()));
    assert_eq!(manifest.core.stored_bytes, Some(stored_core.len() as u64));
    assert!(manifest.core.sha256.is_some(), "sha256 populated");
    assert_eq!(manifest.core.size_bytes, Some(core_payload.len() as u64));
    assert!(!manifest.core.truncated);
    assert_eq!(manifest.core.codec, "zstd");
    let snap_ref = manifest
        .proc_snapshot
        .as_ref()
        .expect("proc_snapshot in manifest");
    assert_eq!(snap_ref.object_key, snap_key);
    assert!(snap_ref.file_count > 0);

    let capture = read_capture(&bundle);
    assert_eq!(capture.outcome, Outcome::Spooled);
    assert_eq!(capture.manifest_key.as_deref(), Some(manifest_key.as_str()));
    assert_eq!(capture.host_pid, pid);
}

/// crictl enrichment succeeding is the only path that fills the manifest's
/// human-readable identity - every other test here runs with crictl pointed
/// at `/bin/false`.
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
    let (listener, sock_path) = bind_events_listener(tmp.path());

    let mut config = base_config(tmp.path());
    config.crictl_path = crictl;
    config.crictl_timeout_secs = 10;
    config.event_socket_path = Some(sock_path);

    let core_in: &[u8] = b"core payload for the enriched path";
    run(capture_args(pid, 11, TS), &config, core_in)
        .await
        .unwrap();

    let bundle = ready_bundle(&config, CONTAINER_ID, TS, pid);
    let manifest = read_manifest(&bundle);

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

    // The datagram and the bundle's capture event carry the same payload.
    let event = recv_event(&listener);
    assert_eq!(event, read_capture(&bundle));
    assert_eq!(event.namespace.as_deref(), Some("production"));
    assert_eq!(event.pod_name.as_deref(), Some("nginx-abc123"));
    assert_eq!(event.pod_uid.as_deref(), Some(POD_UID));
    assert_eq!(event.container_id.as_deref(), Some(CONTAINER_ID));
    assert_eq!(event.container_name.as_deref(), Some("mycontainer"));
    assert_eq!(event.host_pid, pid);
    assert!(event.error.is_none());
    assert_eq!(event.outcome, Outcome::Spooled);
    assert_eq!(event.signal_name.as_deref(), Some("SIGSEGV"));
    assert_eq!(event.timestamp, TS);
    assert_eq!(event.stored_bytes, manifest.core.stored_bytes);
}

/// Each outcome must arrive on the events socket with the correct value. This
/// test covers spooling, rate-limit suppression and discard when there is no
/// store.
#[tokio::test]
async fn run_reports_each_outcome_on_the_events_socket() {
    let pid = 4248;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let (listener, sock_path) = bind_events_listener(tmp.path());

    let mut config = base_config(tmp.path());
    config.event_socket_path = Some(sock_path);
    config.max_cores_per_hour = 1;

    // The budget is 1. The first crash is spooled. The handler suppresses the
    // second crash.
    for ts in [TS, TS + 10] {
        let core_in: &[u8] = b"core payload for the outcome test";
        run(capture_args(pid, 11, ts), &config, core_in)
            .await
            .unwrap_or_else(|e| panic!("run at {ts} failed: {e}"));
    }

    // Without a store the handler drains the core and discards it. This path
    // does not use the limiter, so the empty budget does not change the outcome.
    config.store_configured = false;
    let core_in: &[u8] = b"core payload with nowhere to go";
    run(capture_args(pid, 11, TS + 20), &config, core_in)
        .await
        .unwrap();

    let outcomes: Vec<Outcome> = (0..3).map(|_| recv_event(&listener).outcome).collect();
    assert_eq!(
        outcomes,
        vec![
            Outcome::Spooled,
            Outcome::SuppressedRateLimit,
            Outcome::NoStoreDiscard,
        ]
    );
}

/// The fixture adds `SECRET_KEY=hunter2`. The spooled proc-snapshot tar must
/// not contain it, unless `no_redact` is set.
#[tokio::test]
async fn run_redacts_snapshot_environ_unless_no_redact_is_set() {
    let (redacting_pid, raw_pid) = (4252, 4253);

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), redacting_pid, POD_UID, CONTAINER_ID);
    write_fixture_proc(tmp.path(), raw_pid, POD_UID, CONTAINER_ID);
    let mut config = base_config(tmp.path());

    let snapshot_environ = |config: &HandlerConfig, pid| {
        let bundle = ready_bundle(config, CONTAINER_ID, TS, pid);
        let tar = std::fs::read(bundle.join(spool::SNAPSHOT_FILE)).unwrap();
        let environ = tar_entry(&tar, "environ").expect("environ in the proc-snapshot tar");
        String::from_utf8_lossy(&environ).into_owned()
    };

    // The default config redacts.
    let core_in: &[u8] = b"core payload for the redaction test";
    run(capture_args(redacting_pid, 11, TS), &config, core_in)
        .await
        .unwrap();
    let environ = snapshot_environ(&config, redacting_pid);
    assert!(
        environ.contains("SECRET_KEY=<redacted>"),
        "environ: {environ:?}"
    );
    assert!(!environ.contains("hunter2"), "environ: {environ:?}");
    assert!(
        environ.contains("LANG=en_US.UTF-8"),
        "innocuous vars survive: {environ:?}"
    );

    // `--no-redact` keeps the raw blob.
    config.no_redact = true;
    let core_in: &[u8] = b"core payload for the no-redact test";
    run(capture_args(raw_pid, 11, TS), &config, core_in)
        .await
        .unwrap();
    let environ = snapshot_environ(&config, raw_pid);
    assert!(
        environ.contains("SECRET_KEY=hunter2"),
        "no_redact must pass the raw value through: {environ:?}"
    );
}

/// Size cap: only the first `max_core_bytes` land in the spool; the manifest
/// records the full drained size and the truncation reason.
#[tokio::test]
async fn run_size_cap_truncates_stored_core() {
    let pid = 4243;
    let core_payload: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let mut config = base_config(tmp.path());
    config.max_core_bytes = 1_000;

    run(capture_args(pid, 11, TS), &config, core_payload.as_slice())
        .await
        .unwrap();

    let bundle = ready_bundle(&config, CONTAINER_ID, TS, pid);
    let stored_core = std::fs::read(bundle.join(spool::CORE_FILE)).unwrap();
    assert_eq!(
        unzstd(&stored_core).await,
        &core_payload[..1_000],
        "stored core holds exactly the first cap bytes"
    );

    let manifest = read_manifest(&bundle);
    assert!(manifest.core.truncated);
    assert_eq!(manifest.core.truncated_reason.as_deref(), Some("size_cap"));
    assert_eq!(
        manifest.core.size_bytes,
        Some(core_payload.len() as u64),
        "size_bytes records the full drained size"
    );
}

/// Rate limit: with a budget of 1, the second crash gets no core but still
/// gets a proc snapshot and a manifest marked `rate_limit`.
#[tokio::test]
async fn run_rate_limit_suppresses_core_keeps_manifest() {
    let pid = 4244;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let mut config = base_config(tmp.path());
    config.max_cores_per_hour = 1;

    for ts in [TS, TS + 10] {
        let core_in: &[u8] = b"core payload for rate limit test";
        run(capture_args(pid, 11, ts), &config, core_in)
            .await
            .unwrap_or_else(|e| panic!("run at {ts} failed: {e}"));
    }

    // First crash: full capture.
    let first = ready_bundle(&config, CONTAINER_ID, TS, pid);
    assert!(first.join(spool::CORE_FILE).exists());

    // Second crash: no core...
    let second = ready_bundle(&config, CONTAINER_ID, TS + 10, pid);
    assert!(
        !second.join(spool::CORE_FILE).exists(),
        "suppressed crash must not spool a core"
    );
    // ...but proc snapshot and manifest are still spooled.
    assert!(second.join(spool::SNAPSHOT_FILE).exists());
    let manifest = read_manifest(&second);
    assert!(!manifest.core.present);
    assert!(manifest.core.object_key.is_none());
    assert_eq!(manifest.core.skipped_reason.as_deref(), Some("rate_limit"));
    assert_eq!(read_capture(&second).outcome, Outcome::SuppressedRateLimit);
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
    let mut config = base_config(tmp.path());
    config.max_cores_per_hour = 1;

    for (pid, ts) in [(pid_1, TS), (pid_2, TS + 10)] {
        let core_in: &[u8] = b"core payload for pod-uid rate limit test";
        run(capture_args(pid, 11, ts), &config, core_in)
            .await
            .unwrap_or_else(|e| panic!("run for pid {pid} failed: {e}"));
    }

    let first = ready_bundle(&config, CONTAINER_ID, TS, pid_1);
    assert!(first.join(spool::CORE_FILE).exists());

    // Different container ID but same pod UID -> suppressed.
    let second = ready_bundle(&config, container_id_2, TS + 10, pid_2);
    assert!(
        !second.join(spool::CORE_FILE).exists(),
        "restarted container with same pod UID must inherit the rate-limit budget"
    );
    let manifest = read_manifest(&second);
    assert!(!manifest.core.present);
    assert_eq!(manifest.core.skipped_reason.as_deref(), Some("rate_limit"));
}

/// A full spool drops the core but keeps the snapshot and manifest, and gives
/// the rate-limit budget back: nothing was kept.
#[tokio::test]
async fn run_full_spool_drops_core_keeps_manifest_and_refunds_budget() {
    let pid = 4254;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let mut config = base_config(tmp.path());
    config.max_cores_per_hour = 1;
    // A 100% free-space floor can never be met.
    config.spool_min_free_percent = 100;

    let core_in: &[u8] = b"core payload with no room";
    run(capture_args(pid, 11, TS), &config, core_in)
        .await
        .unwrap();

    let bundle = ready_bundle(&config, CONTAINER_ID, TS, pid);
    assert!(!bundle.join(spool::CORE_FILE).exists());
    assert!(bundle.join(spool::SNAPSHOT_FILE).exists());
    let manifest = read_manifest(&bundle);
    assert!(!manifest.core.present);
    assert_eq!(manifest.core.skipped_reason.as_deref(), Some("spool_full"));
    assert_eq!(read_capture(&bundle).outcome, Outcome::SpoolFull);

    // The budget of 1 is still there once the spool has room.
    config.spool_min_free_percent = 0;
    let core_in: &[u8] = b"core payload with room";
    run(capture_args(pid, 11, TS + 10), &config, core_in)
        .await
        .unwrap();
    let bundle = ready_bundle(&config, CONTAINER_ID, TS + 10, pid);
    assert!(
        bundle.join(spool::CORE_FILE).exists(),
        "refunded budget must allow the core"
    );
}

/// Drain deadline: a stalled drain cannot hold the handler (and its
/// `core_pipe_limit` slot) forever. `run()` errors once the deadline passes,
/// the partial bundle is deleted, the failure is reported, and the abandoned
/// crash refunds its rate-limit budget.
#[tokio::test(start_paused = true)]
async fn run_drain_deadline_drops_stalled_capture_and_refunds_budget() {
    let pid = 4246;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let (listener, sock_path) = bind_events_listener(tmp.path());

    let mut config = base_config(tmp.path());
    config.drain_deadline_secs = 5;
    config.max_cores_per_hour = 1;
    config.event_socket_path = Some(sock_path);

    assert!(
        run(capture_args(pid, 11, TS), &config, StallReader)
            .await
            .is_err(),
        "a stalled drain must error at the deadline"
    );
    let spool = Spool::new(&config.spool_dir);
    assert!(bundle_names(&spool.incoming_dir()).is_empty());
    assert!(bundle_names(&spool.ready_dir()).is_empty());

    let event = recv_event(&listener);
    assert_eq!(event.outcome, Outcome::Failed);
    assert_eq!(event.pod_uid.as_deref(), Some(POD_UID));
    assert_eq!(event.container_id.as_deref(), Some(CONTAINER_ID));
    assert!(event.namespace.is_none(), "crictl has not run on this path");
    assert!(
        event
            .error
            .as_deref()
            .is_some_and(|e| e.contains("draining core stream")),
        "{:?}",
        event.error
    );

    // The abandoned crash refunded the budget of 1: the next crash is spooled.
    let core_in: &[u8] = b"core payload after deadline abort";
    run(capture_args(pid, 11, TS + 10), &config, core_in)
        .await
        .unwrap();
    let bundle = ready_bundle(&config, CONTAINER_ID, TS + 10, pid);
    assert!(bundle.join(spool::CORE_FILE).exists());
}

/// A rate-limited capture whose discard stalls past the deadline still gets
/// its snapshot and a `rate_limit` manifest: no core was to be kept, so the
/// deadline loses nothing.
#[tokio::test(start_paused = true)]
async fn run_drain_deadline_keeps_metadata_of_a_rate_limited_capture() {
    let pid = 4247;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let mut config = base_config(tmp.path());
    config.drain_deadline_secs = 5;
    config.max_cores_per_hour = 1;

    let core_in: &[u8] = b"core payload that uses the budget";
    run(capture_args(pid, 11, TS), &config, core_in)
        .await
        .unwrap();
    run(capture_args(pid, 11, TS + 10), &config, StallReader)
        .await
        .unwrap();

    let bundle = ready_bundle(&config, CONTAINER_ID, TS + 10, pid);
    assert!(!bundle.join(spool::CORE_FILE).exists());
    assert!(bundle.join(spool::SNAPSHOT_FILE).exists());
    let manifest = read_manifest(&bundle);
    assert!(!manifest.core.present);
    assert_eq!(manifest.core.skipped_reason.as_deref(), Some("rate_limit"));
    assert_eq!(read_capture(&bundle).outcome, Outcome::SuppressedRateLimit);
}

/// A spool-full capture whose discard stalls past the deadline still gets its
/// snapshot and a `spool_full` manifest.
#[tokio::test(start_paused = true)]
async fn run_drain_deadline_keeps_metadata_of_a_spool_full_capture() {
    let pid = 4248;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let mut config = base_config(tmp.path());
    config.drain_deadline_secs = 5;
    // A 100% free-space floor can never be met.
    config.spool_min_free_percent = 100;

    run(capture_args(pid, 11, TS), &config, StallReader)
        .await
        .unwrap();

    let bundle = ready_bundle(&config, CONTAINER_ID, TS, pid);
    assert!(!bundle.join(spool::CORE_FILE).exists());
    assert!(bundle.join(spool::SNAPSHOT_FILE).exists());
    let manifest = read_manifest(&bundle);
    assert!(!manifest.core.present);
    assert_eq!(manifest.core.skipped_reason.as_deref(), Some("spool_full"));
    assert_eq!(read_capture(&bundle).outcome, Outcome::SpoolFull);
}

/// An unusable spool still drains the core, so the kernel completes the dump,
/// and reports the capture as failed.
#[tokio::test]
async fn run_unusable_spool_drains_and_reports_failed() {
    let pid = 4249;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let (listener, sock_path) = bind_events_listener(tmp.path());

    let mut config = base_config(tmp.path());
    config.event_socket_path = Some(sock_path);
    // A file where the spool directory should be.
    std::fs::write(&config.spool_dir, b"not a directory").unwrap();

    let core_in: &[u8] = b"core payload";
    assert!(
        run(capture_args(pid, 11, TS), &config, core_in)
            .await
            .is_err()
    );

    let event = recv_event(&listener);
    assert_eq!(event.outcome, Outcome::Failed);
    assert_eq!(event.pod_uid.as_deref(), Some(POD_UID));
    assert!(
        event
            .error
            .as_deref()
            .is_some_and(|e| e.contains("creating the spool bundle")),
        "{:?}",
        event.error
    );
}

/// No store: the core is drained and discarded so the kernel's pipe completes,
/// and nothing else runs - no spool, and no rate limiter, which is only
/// consulted when a core would be kept.
#[tokio::test]
async fn run_without_store_discards_silently() {
    let pid = 100;

    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let mut config = base_config(tmp.path());
    config.store_configured = false;
    config.max_cores_per_hour = 1;

    let core_in: &[u8] = b"some core bytes";
    run(capture_args(pid, 6, 1_000_000), &config, core_in)
        .await
        .unwrap();

    assert!(
        !config.spool_dir.exists(),
        "no store means nothing to spool"
    );
    assert!(
        !config.rate_state_path.exists(),
        "no store means no core to budget, so the limiter must not run"
    );
}

/// A host process crash has no kubernetes identity, so no object key is
/// derivable and nothing is spooled - the core is still drained. It is still
/// reported, so the daemon can log it, even though no k8s Event can target it.
#[tokio::test]
async fn run_non_kubernetes_cgroup_spools_nothing_and_reports_skipped() {
    let pid = 201;

    let tmp = TempDir::new().unwrap();
    let pid_dir = proc_root(tmp.path()).join(pid.to_string());
    std::fs::create_dir_all(&pid_dir).unwrap();
    // Non-k8s cgroup -> parse_cgroup returns None.
    std::fs::write(pid_dir.join("cgroup"), "0::/system.slice/sshd.service\n").unwrap();
    let (listener, sock_path) = bind_events_listener(tmp.path());

    let mut config = base_config(tmp.path());
    config.event_socket_path = Some(sock_path);

    let core_in: &[u8] = b"core bytes";
    run(capture_args(pid, 9, 2_000_000), &config, core_in)
        .await
        .unwrap();

    assert!(
        !config.spool_dir.exists(),
        "nothing is spooled without a k8s cgroup"
    );
    let event = recv_event(&listener);
    assert_eq!(event.outcome, Outcome::SkippedNonK8s);
    assert_eq!(event.host_pid, pid);
    assert!(event.pod_uid.is_none());
    assert!(event.namespace.is_none());
    assert_eq!(event.core_bytes, b"core bytes".len() as u64);
}
