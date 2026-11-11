//! Spool uploader tests: bundles written by the real handler, uploaded to an
//! in-memory object store.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use tempfile::TempDir;
use tokio::sync::{Notify, mpsc};

use coredrop::config::HandlerConfig;
use coredrop::events::{CaptureEventPayload, Outcome};
use coredrop::handler::{CaptureArgs, run};
use coredrop::manifest::Manifest;
use coredrop::metrics::Metrics;
use coredrop::spool::Spool;
use coredrop::upload::ObjectKeys;
use coredrop::uploader::{UploadSettings, Uploader};

mod common;
use common::{
    FailingStore, base_config, bundle_names, get_object, is_absent, unzstd, write_fixture_proc,
};

const POD_UID: &str = "ed1e9c81-9a92-4f7e-be2c-8b26b56d3b98";
const CONTAINER_ID: &str = "abc123def456abc123def456";
/// A recent crash time, so bundles are well inside the default max age.
fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Run the real handler for one crash of `pid` at `ts`.
async fn capture(config: &HandlerConfig, pid: i32, ts: i64, core: &[u8]) {
    let args = CaptureArgs {
        host_pid: pid,
        signal: 11,
        timestamp: ts,
        exe: "!usr!bin!crasher".into(),
    };
    run(args, config, core).await.unwrap();
}

fn settings() -> UploadSettings {
    UploadSettings {
        concurrency: 2,
        attempt_timeout: Some(Duration::from_secs(10)),
        max_age: Duration::from_hours(1),
        retry_backoff: Duration::from_millis(20),
    }
}

/// Run the uploader in flush mode: it scans once, uploads what it can within
/// `flush`, and returns. Returns the reports it sent.
async fn flush(
    config: &HandlerConfig,
    store: Arc<dyn ObjectStore>,
    settings: UploadSettings,
    flush: Duration,
) -> Vec<CaptureEventPayload> {
    flush_metered(config, store, settings, flush).await.0
}

/// [`flush`], and also return the uploader's metrics in the text format.
async fn flush_metered(
    config: &HandlerConfig,
    store: Arc<dyn ObjectStore>,
    settings: UploadSettings,
    flush: Duration,
) -> (Vec<CaptureEventPayload>, String) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let metrics = Arc::new(Metrics::new());
    let uploader = Uploader::new(
        Spool::new(&config.spool_dir),
        store,
        settings,
        tx,
        metrics.clone(),
    );
    uploader
        .run(Arc::new(Notify::new()), std::future::ready(()), flush)
        .await;
    let mut reports = Vec::new();
    while let Ok(report) = rx.try_recv() {
        reports.push(report);
    }
    (reports, metrics.encode())
}

fn ready(config: &HandlerConfig) -> Vec<String> {
    bundle_names(&Spool::new(&config.spool_dir).ready_dir())
}

async fn manifest(store: &Arc<dyn ObjectStore>, key: &str) -> Manifest {
    serde_json::from_slice(&get_object(store, key).await).unwrap()
}

/// A spooled core and a rate-limited capture both upload: every object the
/// manifest names exists, the bundles are gone, and each capture is reported
/// with its final outcome.
#[tokio::test]
async fn uploads_bundles_deletes_them_and_reports_final_outcomes() {
    let pid = 5000;
    let ts = now_ts();
    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let mut config = base_config(tmp.path());
    config.max_cores_per_hour = 1;

    let core_payload = b"core payload for the uploader";
    capture(&config, pid, ts, core_payload).await;
    capture(&config, pid, ts + 1, b"suppressed core").await;
    assert_eq!(ready(&config).len(), 2);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut reports = flush(&config, store.clone(), settings(), Duration::from_secs(10)).await;

    assert!(ready(&config).is_empty(), "uploaded bundles are deleted");

    let first = ObjectKeys::new("test", POD_UID, CONTAINER_ID, ts, pid);
    let stored_core = get_object(&store, &first.core()).await;
    assert_eq!(unzstd(&stored_core).await, core_payload);
    get_object(&store, &first.proc_snapshot()).await;
    let m = manifest(&store, &first.manifest()).await;
    assert_eq!(m.core.object_key.as_deref(), Some(first.core().as_str()));

    let second = ObjectKeys::new("test", POD_UID, CONTAINER_ID, ts + 1, pid);
    assert!(is_absent(&store, &second.core()).await);
    get_object(&store, &second.proc_snapshot()).await;
    let m = manifest(&store, &second.manifest()).await;
    assert_eq!(m.core.skipped_reason.as_deref(), Some("rate_limit"));

    reports.sort_by_key(|r| r.timestamp);
    let outcomes: Vec<Outcome> = reports.iter().map(|r| r.outcome).collect();
    assert_eq!(
        outcomes,
        vec![Outcome::Uploaded, Outcome::SuppressedRateLimit]
    );
    assert_eq!(
        reports[0].manifest_key.as_deref(),
        Some(first.manifest().as_str())
    );
}

/// A store outage keeps the bundle: nothing is deleted and no manifest
/// appears in the bucket, because the manifest goes last. A later daemon
/// with a working store uploads it.
#[tokio::test]
async fn a_failed_bundle_stays_in_the_spool_until_the_store_recovers() {
    let pid = 5001;
    let ts = now_ts();
    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let config = base_config(tmp.path());
    capture(&config, pid, ts, b"core during an outage").await;

    let inner = Arc::new(InMemory::new());
    let failing = Arc::new(FailingStore::core(inner.clone()));
    let store: Arc<dyn ObjectStore> = failing.clone();
    let (reports, metrics) = flush_metered(
        &config,
        store.clone(),
        settings(),
        Duration::from_millis(200),
    )
    .await;

    assert!(reports.is_empty(), "a retried bundle is not reported yet");
    assert_eq!(ready(&config).len(), 1, "the bundle stays in the spool");
    assert!(
        metrics.contains("coredrop_upload_attempts_total{result=\"ok\"} 0"),
        "{metrics}"
    );
    assert!(
        !metrics.contains("coredrop_upload_attempts_total{result=\"error\"} 0"),
        "the failed attempts are counted: {metrics}"
    );
    assert!(metrics.contains("coredrop_spool_bundles 1"), "{metrics}");
    assert!(
        metrics.contains("coredrop_upload_duration_seconds_count 0"),
        "a failed upload has no duration: {metrics}"
    );
    let keys = ObjectKeys::new("test", POD_UID, CONTAINER_ID, ts, pid);
    let inner: Arc<dyn ObjectStore> = inner;
    assert!(
        is_absent(&inner, &keys.manifest()).await,
        "no manifest may point at a core that is not uploaded"
    );

    failing.set_failing(false);
    let (reports, metrics) =
        flush_metered(&config, store, settings(), Duration::from_secs(10)).await;
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].outcome, Outcome::Uploaded);
    assert!(
        metrics.contains("coredrop_upload_attempts_total{result=\"ok\"} 1"),
        "{metrics}"
    );
    assert!(
        metrics.contains("coredrop_upload_duration_seconds_count 1"),
        "{metrics}"
    );
    assert!(ready(&config).is_empty());
    get_object(&inner, &keys.core()).await;
    get_object(&inner, &keys.manifest()).await;
}

/// Within one run, a failed bundle is retried after its backoff.
#[tokio::test]
async fn retries_a_failed_bundle_after_backoff() {
    let pid = 5002;
    let ts = now_ts();
    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let config = base_config(tmp.path());
    capture(&config, pid, ts, b"core that uploads on retry").await;

    let failing = Arc::new(FailingStore::core(Arc::new(InMemory::new())));
    let recover = failing.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        recover.set_failing(false);
    });

    let reports = flush(&config, failing, settings(), Duration::from_secs(10)).await;
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].outcome, Outcome::Uploaded);
    assert!(ready(&config).is_empty());
}

/// The spool age gauge shows the oldest capture that waits for upload.
#[tokio::test]
async fn spool_age_is_the_age_of_the_oldest_waiting_capture() {
    let tmp = TempDir::new().unwrap();
    let config = base_config(tmp.path());
    // Two crashes, 10 and 2 minutes ago; the max age is one hour.
    for (pid, age) in [(5004, 600), (5005, 120)] {
        write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
        capture(&config, pid, now_ts() - age, b"core during an outage").await;
    }

    let store: Arc<dyn ObjectStore> = Arc::new(FailingStore::core(Arc::new(InMemory::new())));
    let (_, metrics) = flush_metered(&config, store, settings(), Duration::from_millis(200)).await;

    assert!(metrics.contains("coredrop_spool_bundles 2"), "{metrics}");
    let age: i64 = metrics
        .lines()
        .find_map(|l| l.strip_prefix("coredrop_spool_oldest_bundle_age_seconds "))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no age gauge in:\n{metrics}"));
    assert!((600..660).contains(&age), "age {age}");
}

/// A bundle past the max age that still fails is deleted and reported as a
/// failed capture.
#[tokio::test]
async fn deletes_a_bundle_past_max_age_and_reports_failed() {
    let pid = 5003;
    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let config = base_config(tmp.path());
    // A crash two hours ago, with a one-hour max age.
    capture(&config, pid, now_ts() - 7200, b"core that never uploads").await;

    let store: Arc<dyn ObjectStore> = Arc::new(FailingStore::core(Arc::new(InMemory::new())));
    let (reports, metrics) =
        flush_metered(&config, store, settings(), Duration::from_secs(10)).await;

    assert!(ready(&config).is_empty(), "the expired bundle is deleted");
    assert!(
        metrics.contains("coredrop_bundles_expired_total 1"),
        "{metrics}"
    );
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].outcome, Outcome::Failed);
    assert!(
        reports[0]
            .error
            .as_deref()
            .is_some_and(|e| e.contains("upload gave up")),
        "{:?}",
        reports[0].error
    );
}

/// An entry in `ready/` without a timestamp name is aged by its mtime, so a
/// stale one that never uploads is deleted instead of retried forever.
#[tokio::test]
async fn deletes_a_stale_unnamed_ready_entry() {
    let tmp = TempDir::new().unwrap();
    let config = base_config(tmp.path());
    let spool = Spool::new(&config.spool_dir);
    spool.create_bundle("seed").unwrap().commit().unwrap();
    let junk = spool.ready_dir().join("junk");
    std::fs::create_dir(&junk).unwrap();
    std::fs::write(junk.join("stray"), b"not a bundle").unwrap();
    let two_hours_ago = std::time::SystemTime::now() - Duration::from_hours(2);
    for dir in ["seed", "junk"] {
        set_mtime(&spool.ready_dir().join(dir), two_hours_ago);
    }
    set_mtime(&junk.join("stray"), two_hours_ago);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reports = flush(&config, store, settings(), Duration::from_secs(10)).await;

    assert!(ready(&config).is_empty(), "stale entries are deleted");
    assert!(
        reports.is_empty(),
        "an entry with no capture event has no report"
    );
}

/// A bundle whose delete fails after a good upload is reported once, as
/// uploaded. Only the delete is retried, past max age too: the capture is in
/// the bucket, so it is never reported as failed.
#[tokio::test]
async fn retries_only_the_delete_of_an_uploaded_bundle() {
    use std::os::unix::fs::PermissionsExt;

    let pid = 5004;
    let ts = now_ts();
    let tmp = TempDir::new().unwrap();
    write_fixture_proc(tmp.path(), pid, POD_UID, CONTAINER_ID);
    let config = base_config(tmp.path());
    // Past the one-hour max age, so a delete failure that counted as an
    // upload failure would give up and report the capture as failed.
    let ts = ts - 7200;
    capture(&config, pid, ts, b"core whose bundle sticks").await;
    let bundle = Spool::new(&config.spool_dir)
        .ready_dir()
        .join(&ready(&config)[0]);

    // A read-only bundle dir: its files can be read but not removed.
    let mode = |m| std::fs::set_permissions(&bundle, std::fs::Permissions::from_mode(m));
    mode(0o500).unwrap();
    if std::fs::write(bundle.join("probe"), b"").is_ok() {
        // Root ignores the mode; the delete cannot be made to fail.
        mode(0o700).unwrap();
        return;
    }
    let restore = bundle.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        std::fs::set_permissions(&restore, std::fs::Permissions::from_mode(0o700)).unwrap();
    });

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reports = flush(&config, store.clone(), settings(), Duration::from_secs(10)).await;

    assert_eq!(
        reports.len(),
        1,
        "the capture is reported once: {reports:?}"
    );
    assert_eq!(reports[0].outcome, Outcome::Uploaded);
    assert!(ready(&config).is_empty(), "the delete is retried");
    let keys = ObjectKeys::new("test", POD_UID, CONTAINER_ID, ts, pid);
    get_object(&store, &keys.manifest()).await;
}

/// A bundle a killed handler left in `incoming/` is deleted once it is
/// stale, and never uploaded.
#[tokio::test]
async fn sweeps_stale_incoming_bundles() {
    let tmp = TempDir::new().unwrap();
    let config = base_config(tmp.path());
    let spool = Spool::new(&config.spool_dir);
    let bundle = spool.create_bundle("1-2-pod-cid").unwrap();
    bundle.write_file("core.zst", b"partial").unwrap();
    let fresh = spool.create_bundle("3-4-pod-cid").unwrap();
    fresh.write_file("core.zst", b"in progress").unwrap();
    let two_hours_ago = std::time::SystemTime::now() - Duration::from_hours(2);
    set_mtime(&bundle.path("core.zst"), two_hours_ago);
    set_mtime(&spool.incoming_dir().join("1-2-pod-cid"), two_hours_ago);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reports = flush(&config, store.clone(), settings(), Duration::from_secs(10)).await;

    assert!(reports.is_empty());
    assert_eq!(
        bundle_names(&spool.incoming_dir()),
        vec!["3-4-pod-cid".to_string()],
        "only the stale bundle is swept"
    );
    let listed = store.list_with_delimiter(None).await.unwrap();
    assert!(listed.objects.is_empty() && listed.common_prefixes.is_empty());
}

/// A long drain writes its core file but not the bundle directory, so the
/// directory's mtime goes stale. The bundle stays while its core file is fresh.
#[tokio::test]
async fn keeps_an_incoming_bundle_whose_core_is_still_written() {
    let tmp = TempDir::new().unwrap();
    let config = base_config(tmp.path());
    let spool = Spool::new(&config.spool_dir);
    let bundle = spool.create_bundle("1-2-pod-cid").unwrap();
    bundle.write_file("core.zst", b"still draining").unwrap();
    set_mtime(
        &spool.incoming_dir().join("1-2-pod-cid"),
        std::time::SystemTime::now() - Duration::from_hours(2),
    );

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    flush(&config, store, settings(), Duration::from_secs(10)).await;

    assert_eq!(
        bundle_names(&spool.incoming_dir()),
        vec!["1-2-pod-cid".to_string()],
        "a bundle with a fresh core file is not swept"
    );
}

fn set_mtime(dir: &Path, mtime: std::time::SystemTime) {
    std::fs::File::open(dir)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
}
