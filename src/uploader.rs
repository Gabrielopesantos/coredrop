//! Daemon side: upload ready spool bundles to the object store.
//!
//! The uploader finds bundles in the spool's `ready/` directory (at startup,
//! on a timer, and when the handler reports a spooled capture), uploads at
//! most `concurrency` of them at a time, and deletes each bundle once its
//! manifest is uploaded. A failed bundle stays in the spool and is retried
//! with exponential backoff until it is older than `max_age`. Object keys are
//! fixed per capture, so a bundle uploaded twice (daemon killed after the
//! upload, before the delete) overwrites the same objects.
//!
//! Per bundle, the upload order is core -> proc snapshot -> manifest, so a
//! manifest in the bucket always points at complete objects.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use object_store::ObjectStore;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::events::{CaptureEventPayload, Outcome};
use crate::manifest::Manifest;
use crate::metrics::Metrics;
use crate::spool::{self, Spool};
use crate::upload;

/// Default number of bundles uploaded at the same time.
pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 2;

/// Default time limit for one upload attempt of one bundle: 10 minutes.
pub const DEFAULT_UPLOAD_TIMEOUT_SECS: u64 = 600;

/// Default age past which a bundle that still fails to upload is deleted:
/// 7 days.
pub const DEFAULT_SPOOL_MAX_AGE_SECS: u64 = 7 * 24 * 60 * 60;

/// How often the uploader scans `ready/` without a wake-up hint. Hints are
/// datagrams and can be lost.
const SCAN_INTERVAL: Duration = Duration::from_secs(30);

/// Age past which a bundle left in `incoming/` is deleted. A handler finishes
/// in seconds, so an older bundle belongs to a handler that was killed.
const INCOMING_MAX_AGE: Duration = Duration::from_hours(1);

/// Default first retry delay; it doubles per failed attempt up to
/// [`MAX_BACKOFF`].
const DEFAULT_RETRY_BACKOFF: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_mins(10);

/// Tunables for the uploader.
#[derive(Debug, Clone)]
pub struct UploadSettings {
    /// Bundles uploaded at the same time; at least 1.
    pub concurrency: usize,
    /// Time limit for one upload attempt of one bundle; `None` = no limit.
    pub attempt_timeout: Option<Duration>,
    /// Age past which a bundle that still fails to upload is deleted.
    pub max_age: Duration,
    /// Delay before the first retry of a failed bundle; it doubles per
    /// failed attempt, up to 10 minutes.
    pub retry_backoff: Duration,
}

impl Default for UploadSettings {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_UPLOAD_CONCURRENCY,
            attempt_timeout: Some(Duration::from_secs(DEFAULT_UPLOAD_TIMEOUT_SECS)),
            max_age: Duration::from_secs(DEFAULT_SPOOL_MAX_AGE_SECS),
            retry_backoff: DEFAULT_RETRY_BACKOFF,
        }
    }
}

/// Uploads the spool. Each finished bundle is sent on `reports` as the
/// capture's final event, for the daemon to log and turn into a k8s Event.
pub struct Uploader {
    spool: Spool,
    store: Arc<dyn ObjectStore>,
    settings: UploadSettings,
    reports: mpsc::UnboundedSender<CaptureEventPayload>,
    metrics: Arc<Metrics>,
}

/// A ready bundle the uploader knows about.
#[derive(Debug)]
struct Pending {
    attempts: u32,
    next_at: Instant,
    in_flight: bool,
    /// The capture is reported: the bundle is uploaded, or it is past the
    /// spool max age. Only its delete is left; it is not uploaded or
    /// reported again.
    reported: bool,
}

/// The result of one attempt that did not fail the upload.
struct Attempt {
    /// The capture's final event and the upload time, when this attempt
    /// uploaded the bundle.
    report: Option<(CaptureEventPayload, Duration)>,
    /// The result of deleting the bundle from the spool.
    deleted: Result<()>,
}

impl Uploader {
    /// An uploader for `spool` into `store`.
    #[must_use]
    pub fn new(
        spool: Spool,
        store: Arc<dyn ObjectStore>,
        settings: UploadSettings,
        reports: mpsc::UnboundedSender<CaptureEventPayload>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            spool,
            store,
            settings,
            reports,
            metrics,
        }
    }

    /// Upload until `shutdown` completes, then keep going until the spool is
    /// empty or `flush` has passed. `wake` triggers an immediate scan.
    /// Bundles left at the end stay in the spool for the next daemon; an
    /// upload cut off mid-way leaves parts for the bucket's lifecycle rule.
    pub async fn run(
        self,
        wake: Arc<Notify>,
        shutdown: impl std::future::Future<Output = ()>,
        flush: Duration,
    ) {
        let mut pending: BTreeMap<String, Pending> = BTreeMap::new();
        let mut tasks: JoinSet<Result<Attempt>> = JoinSet::new();
        let mut task_names: HashMap<tokio::task::Id, String> = HashMap::new();
        let mut scan = tokio::time::interval(SCAN_INTERVAL);
        scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut flush_deadline: Option<Instant> = None;
        tokio::pin!(shutdown);

        loop {
            self.dispatch(&mut pending, &mut tasks, &mut task_names);
            if flush_deadline.is_some() && pending.is_empty() {
                info!("spool flushed");
                return;
            }
            let next_retry = pending
                .values()
                .filter(|p| !p.in_flight)
                .map(|p| p.next_at)
                .min();

            tokio::select! {
                () = &mut shutdown, if flush_deadline.is_none() => {
                    info!(pending = pending.len(), flush_secs = flush.as_secs(), "flushing the spool before exit");
                    flush_deadline = Some(Instant::now() + flush);
                    self.scan(&mut pending);
                }
                () = sleep_until(flush_deadline), if flush_deadline.is_some() => {
                    warn!(pending = pending.len(), "flush time is up; remaining bundles stay in the spool");
                    return;
                }
                () = wake.notified() => self.scan(&mut pending),
                _ = scan.tick() => self.scan(&mut pending),
                Some(joined) = tasks.join_next_with_id() => {
                    let (id, result) = match joined {
                        Ok((id, result)) => (id, result),
                        Err(e) => (e.id(), Err(anyhow!("upload task failed: {e}"))),
                    };
                    if let Some(name) = task_names.remove(&id) {
                        self.finish(&name, result, &mut pending);
                    }
                }
                () = sleep_until(next_retry), if next_retry.is_some() => {}
            }
        }
    }

    /// Track every bundle in `ready/`, forget bundles that are gone, delete
    /// stale bundles from `incoming/`, and update the spool gauges.
    fn scan(&self, pending: &mut BTreeMap<String, Pending>) {
        let ready_dir = self.spool.ready_dir();
        let ready = list_names(&ready_dir);
        // A reported bundle waits only for its delete, not for an upload.
        let oldest = ready
            .iter()
            .filter(|name| !pending.get(*name).is_some_and(|p| p.reported))
            .filter_map(|name| bundle_age(&ready_dir, name))
            .max()
            .unwrap_or_default();
        self.metrics
            .set_spool(ready.len(), self.spool.usage_bytes(), oldest);
        let now = Instant::now();
        pending.retain(|name, p| p.in_flight || ready.contains(name));
        for name in ready {
            pending.entry(name).or_insert(Pending {
                attempts: 0,
                next_at: now,
                in_flight: false,
                reported: false,
            });
        }
        sweep_incoming(&self.spool.incoming_dir());
    }

    /// Start uploads for due bundles, oldest first, up to the concurrency
    /// limit.
    fn dispatch(
        &self,
        pending: &mut BTreeMap<String, Pending>,
        tasks: &mut JoinSet<Result<Attempt>>,
        task_names: &mut HashMap<tokio::task::Id, String>,
    ) {
        let now = Instant::now();
        let limit = self.settings.concurrency.max(1);
        for (name, p) in pending.iter_mut() {
            if tasks.len() >= limit {
                break;
            }
            if p.in_flight || p.next_at > now {
                continue;
            }
            p.in_flight = true;
            let dir = self.spool.ready_dir().join(name);
            let store = self.store.clone();
            let deadline_at = self.settings.attempt_timeout.map(|t| now + t);
            let reported = p.reported;
            let handle = tasks.spawn(async move {
                let report = if reported {
                    None
                } else {
                    let started = Instant::now();
                    let payload = upload_bundle(&store, &dir, deadline_at).await?;
                    Some((payload, started.elapsed()))
                };
                let deleted = tokio::fs::remove_dir_all(&dir)
                    .await
                    .with_context(|| format!("removing bundle {}", dir.display()));
                Ok(Attempt { report, deleted })
            });
            task_names.insert(handle.id(), name.clone());
        }
    }

    /// Record the result of one upload attempt.
    fn finish(&self, name: &str, result: Result<Attempt>, pending: &mut BTreeMap<String, Pending>) {
        match result {
            Ok(Attempt { report, deleted }) => {
                if let Some((payload, took)) = report {
                    self.metrics.upload_attempt(true);
                    self.metrics.upload_duration(took);
                    let _ = self.reports.send(payload);
                }
                let Err(e) = deleted else {
                    pending.remove(name);
                    return;
                };
                // Keep the entry, so the next scan does not upload and report
                // the bundle again. Only the delete is retried, past max age
                // too: the capture is reported already.
                let Some(p) = pending.get_mut(name) else {
                    return;
                };
                p.in_flight = false;
                p.reported = true;
                p.attempts += 1;
                let backoff = backoff(self.settings.retry_backoff, p.attempts);
                p.next_at = Instant::now() + backoff;
                // The first failure is a warning; the retries after it are
                // debug, and the metrics count them.
                let (attempts, retry_in_secs, error) =
                    (p.attempts, backoff.as_secs(), format!("{e:#}"));
                if attempts == 1 {
                    warn!(
                        bundle = name,
                        attempts,
                        retry_in_secs,
                        error,
                        "deleting a reported bundle failed; will retry"
                    );
                } else {
                    debug!(
                        bundle = name,
                        attempts,
                        retry_in_secs,
                        error,
                        "deleting a reported bundle failed; will retry"
                    );
                }
            }
            Err(e) => {
                let Some(p) = pending.get_mut(name) else {
                    return;
                };
                p.in_flight = false;
                p.attempts += 1;
                // An entry marked reported only retries its delete, which
                // cannot fail the task.
                if !p.reported {
                    self.metrics.upload_attempt(false);
                }
                let age = bundle_age(&self.spool.ready_dir(), name);
                if !p.reported && age.is_some_and(|age| age > self.settings.max_age) {
                    warn!(bundle = name, attempts = p.attempts, error = %format!("{e:#}"), "bundle is past the spool max age; deleting it");
                    // Report and count the capture once. The entry stays, so
                    // a failed delete is retried and not uploaded again.
                    p.reported = true;
                    p.attempts = 0;
                    p.next_at = Instant::now();
                    self.metrics.bundle_expired();
                    self.report_expired(name, &e);
                    return;
                }
                let backoff = backoff(self.settings.retry_backoff, p.attempts);
                p.next_at = Instant::now() + backoff;
                let (attempts, retry_in_secs, error) =
                    (p.attempts, backoff.as_secs(), format!("{e:#}"));
                if attempts == 1 {
                    warn!(
                        bundle = name,
                        attempts, retry_in_secs, error, "bundle upload failed; will retry"
                    );
                } else {
                    debug!(
                        bundle = name,
                        attempts, retry_in_secs, error, "bundle upload failed; will retry"
                    );
                }
            }
        }
    }

    /// Report the capture of a bundle that will never upload as failed. The
    /// next dispatch deletes the bundle.
    fn report_expired(&self, name: &str, error: &anyhow::Error) {
        let dir = self.spool.ready_dir().join(name);
        if let Ok(payload) = read_capture(&dir) {
            let _ = self.reports.send(CaptureEventPayload {
                outcome: Outcome::Failed,
                manifest_key: None,
                error: Some(format!("upload gave up: {error:#}")),
                ..payload
            });
        }
    }
}

/// Upload one bundle: core, then proc snapshot, then manifest. Returns the
/// capture's final event.
async fn upload_bundle(
    store: &Arc<dyn ObjectStore>,
    dir: &Path,
    deadline_at: Option<Instant>,
) -> Result<CaptureEventPayload> {
    let payload = read_capture(dir)?;
    let manifest_json = tokio::fs::read(dir.join(spool::MANIFEST_FILE))
        .await
        .context("reading the bundle manifest")?;
    let manifest: Manifest =
        serde_json::from_slice(&manifest_json).context("parsing the bundle manifest")?;
    let Some(manifest_key) = payload.manifest_key.clone() else {
        bail!("bundle capture event has no manifest key");
    };

    if let Some(core_key) = &manifest.core.object_key {
        upload::upload_file(store, &dir.join(spool::CORE_FILE), core_key, deadline_at).await?;
    }
    if let Some(snapshot) = &manifest.proc_snapshot {
        upload::upload_file(
            store,
            &dir.join(spool::SNAPSHOT_FILE),
            &snapshot.object_key,
            deadline_at,
        )
        .await?;
    }
    let put = upload::put_object(store, &manifest_key, manifest_json);
    match deadline_at {
        Some(dl) => tokio::time::timeout_at(dl, put)
            .await
            .map_err(|_| anyhow!("upload deadline passed while putting {manifest_key}"))??,
        None => put.await?,
    }

    Ok(CaptureEventPayload {
        outcome: match payload.outcome {
            Outcome::Spooled => Outcome::Uploaded,
            other => other,
        },
        ..payload
    })
}

fn read_capture(dir: &Path) -> Result<CaptureEventPayload> {
    let bytes =
        std::fs::read(dir.join(spool::CAPTURE_FILE)).context("reading the bundle capture event")?;
    serde_json::from_slice(&bytes).context("parsing the bundle capture event")
}

/// The bundle names in `dir`. Empty when it cannot be read.
fn list_names(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok()?.file_name().into_string().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Delete bundles in `incoming/` older than [`INCOMING_MAX_AGE`].
fn sweep_incoming(dir: &Path) {
    for name in list_names(dir) {
        let path = dir.join(&name);
        let stale = newest_mtime(&path)
            .and_then(|mtime| mtime.elapsed().ok())
            .is_some_and(|age| age > INCOMING_MAX_AGE);
        if stale {
            warn!(bundle = %name, "deleting a bundle a killed handler left in incoming/");
            if let Err(e) = std::fs::remove_dir_all(&path) {
                warn!(error = %e, dir = %path.display(), "removing a stale incoming bundle failed");
            }
        }
    }
}

/// The newest modification time of `bundle` and the files in it. A running
/// drain moves its core file's mtime, not the directory's.
fn newest_mtime(bundle: &Path) -> Option<std::time::SystemTime> {
    let dir = std::fs::metadata(bundle).and_then(|m| m.modified()).ok()?;
    let files = std::fs::read_dir(bundle)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok()?.metadata().ok()?.modified().ok());
    Some(files.fold(dir, std::cmp::max))
}

/// Age of the bundle `name` in `ready_dir`: the time since the crash, from
/// the timestamp its name starts with (see [`spool::bundle_name`]). A name
/// without one, which the handler never writes, is aged by its newest
/// mtime, so it cannot be retried forever.
fn bundle_age(ready_dir: &Path, name: &str) -> Option<Duration> {
    let crash = name
        .split('-')
        .next()
        .and_then(|ts| ts.parse::<u64>().ok())
        .map(|ts| UNIX_EPOCH + Duration::from_secs(ts));
    let born = crash.or_else(|| newest_mtime(&ready_dir.join(name)))?;
    Some(SystemTime::now().duration_since(born).unwrap_or_default())
}

fn backoff(base: Duration, attempts: u32) -> Duration {
    base.saturating_mul(2u32.saturating_pow(attempts.saturating_sub(1)))
        .min(MAX_BACKOFF)
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        let base = Duration::from_secs(10);
        assert_eq!(backoff(base, 1), Duration::from_secs(10));
        assert_eq!(backoff(base, 2), Duration::from_secs(20));
        assert_eq!(backoff(base, 4), Duration::from_secs(80));
        assert_eq!(backoff(base, 30), MAX_BACKOFF);
    }

    #[test]
    fn bundle_age_reads_the_crash_time_from_the_name() {
        let dir = Path::new("/no/such/spool/ready");
        let age = bundle_age(dir, "1000-4242-pod-cid").unwrap();
        assert!(age > Duration::from_secs(1_000_000_000));
        assert!(
            bundle_age(dir, "not-a-bundle").is_none(),
            "no timestamp and no directory to age by"
        );
    }
}
