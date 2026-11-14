//! The kernel-exec'd capture handler: `coredrop capture %P %s %t %E`.
//!
//! The kernel runs this in the *host* namespaces with the core on stdin. The
//! handler writes only to the node-local spool (see [`crate::spool`]); the
//! daemon uploads the spool later. Handler flow (top-to-bottom, ordered by
//! time-criticality):
//!
//! 1. Pre-reap snapshot - snapshot `/proc/<hostpid>` before the kernel
//!    reaps the PID; must complete while the kernel waits.
//! 2. Capture filter - drop a crash the filter (see [`crate::filter`]) does
//!    not keep. A filter on namespace, pod, container or image runs the
//!    crictl enrichment of step 5 here, before the drain.
//! 3. Core drain - stream stdin through zstd into the capture's spool bundle.
//!    A core that is not kept is not read. Then release the kernel's pipe:
//!    the kernel reaps the crashed process and frees its `core_pipe_limit`
//!    slot.
//! 4. Proc snapshot - write the small tar into the bundle.
//! 5. Crictl enrichment - best-effort: shell `crictl inspect` for
//!    human-readable identity (namespace, pod name, container name, image,
//!    restart count). Failure degrades to cgroup-only identity. Skipped when
//!    step 2 already ran it.
//! 6. Manifest - write the JSON sidecar into the bundle, then move the
//!    complete bundle to the spool's `ready/` directory.
//! 7. Capture event - fire-and-forget datagram to the daemon for every
//!    outcome, failures included. The daemon logs it (the handler itself has
//!    no stdout) and treats a spooled capture as a hint to scan the spool.
//!    Best-effort.

pub mod backend;
pub mod buildid;
pub mod crictl;
pub mod drain;
pub mod ratelimit;
pub mod redact;
pub mod snapshot;

use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, Result, anyhow, bail};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::{info, warn};

use crate::cgroup::{self, CgroupIdentity};
use crate::config::HandlerConfig;
use crate::events::{self, CaptureEventPayload, Outcome};
use crate::filter::Subject;
use crate::handler::backend::{CaptureBackend, CoreStats, DrainDeadline};
use crate::handler::drain::SpoolBackend;
use crate::handler::ratelimit::{RateDecision, RateLimiter};
use crate::handler::redact::Redactor;
use crate::handler::snapshot::ProcSnapshot;
use crate::keys::ObjectKeys;
use crate::manifest::{CoreRef, Manifest, ManifestIdentity, ProcSnapshotRef};
use crate::spool::{self, Bundle, Spool, SpoolBudget};

/// The positional args the kernel passes after the `capture` subcommand,
/// matching the `core_pattern` template `%P %s %t %E`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureArgs {
    /// Host (global) PID of the faulting process (`%P`).
    pub host_pid: i32,
    /// Terminating signal number (`%s`).
    pub signal: i32,
    /// Dump time, unix epoch seconds (`%t`).
    pub timestamp: i64,
    /// Slash-mangled executable path (`%E`).
    pub exe: String,
}

impl CaptureArgs {
    /// # Errors
    ///
    /// Fails when the argument count is not 4 or when pid/signal/timestamp
    /// are not parseable integers.
    ///
    /// Takes `OsString`s because `%E` is a file name the crashing workload
    /// chooses: it need not be UTF-8, and a panic here would lose the core.
    /// `exe` is converted lossily; only the numeric fields must be exact.
    pub fn parse(args: &[OsString]) -> Result<Self> {
        let [pid, sig, ts, exe] = args else {
            bail!(
                "capture expects 4 args (%P %s %t %E), got {}: {args:?}",
                args.len()
            );
        };
        Ok(Self {
            host_pid: parse_number(pid)
                .with_context(|| format!("bad host pid {}", pid.display()))?,
            signal: parse_number(sig).with_context(|| format!("bad signal {}", sig.display()))?,
            timestamp: parse_number(ts)
                .with_context(|| format!("bad timestamp {}", ts.display()))?,
            exe: exe.to_string_lossy().into_owned(),
        })
    }
}

fn parse_number<T: std::str::FromStr>(arg: &OsStr) -> Option<T> {
    arg.to_str()?.parse().ok()
}

/// The kernel's core pipe on stdin. Dropping it points fd 0 at `/dev/null`,
/// which closes the handler's end of the pipe: the kernel then reaps the
/// crashed process and frees its `core_pipe_limit` slot while the handler goes
/// on with crictl and the manifest. Dropping tokio's `Stdin` alone does not
/// close fd 0.
pub struct KernelPipe(tokio::io::Stdin);

impl KernelPipe {
    /// The core pipe the kernel connected to the handler's stdin.
    #[must_use]
    pub fn stdin() -> Self {
        Self(tokio::io::stdin())
    }
}

impl AsyncRead for KernelPipe {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl Drop for KernelPipe {
    fn drop(&mut self) {
        let released = std::fs::File::open("/dev/null")
            .and_then(|null| rustix::stdio::dup2_stdin(&null).map_err(std::io::Error::from));
        if let Err(e) = released {
            warn!(error = %e, "releasing the core pipe failed; the kernel waits for the handler to exit");
        }
    }
}

/// Where a capture's files go: the spool bundle and the object keys the
/// daemon will upload them to. Only a capture with a kubernetes identity and
/// a configured store has one.
struct Target<'a> {
    identity: &'a CgroupIdentity,
    keys: ObjectKeys,
    bundle: Bundle,
}

/// Run the handler: snapshot /proc, drain the core into the spool, enrich via
/// crictl, write the manifest, and hand the bundle to the daemon.
///
/// `core_in` is the core stream (normally [`KernelPipe::stdin`]; a byte slice
/// in tests). It is dropped right after the drain, which releases the kernel's
/// pipe.
///
/// # Errors
///
/// Fails when draining the core stream fails or the bundle cannot be written.
/// Best-effort stages (proc snapshot, crictl enrichment, rate-limit state IO)
/// degrade instead of erroring.
// Sequential capture pipeline; splitting it would only scatter the stages.
#[allow(clippy::too_many_lines)]
pub async fn run(
    args: CaptureArgs,
    config: &HandlerConfig,
    mut core_in: impl AsyncRead + Unpin + Send,
) -> Result<()> {
    info!(
        host_pid = args.host_pid,
        signal = args.signal,
        timestamp = args.timestamp,
        exe = %args.exe,
        "capture handler invoked"
    );

    let proc_root = Path::new(&config.proc_root);
    let redactor = if config.no_redact {
        Redactor::disabled()
    } else {
        Redactor::default()
    };

    // Step 1: Pre-reap snapshot (must complete while kernel waits).
    let snapshot = ProcSnapshot::capture(proc_root, args.host_pid, &redactor);
    info!(
        files = snapshot.files.len(),
        build_id = snapshot.build_id.as_deref().unwrap_or("<none>"),
        "pre-reap /proc snapshot captured"
    );

    let identity = read_cgroup_identity(proc_root, args.host_pid);
    if identity.is_none() {
        warn!(
            host_pid = args.host_pid,
            "no kubernetes cgroup identity resolved; nothing will be spooled"
        );
    }

    // Step 2: Capture filter. crictl runs here only when the filter needs its
    // fields; `early_info` is then `Some`, also when crictl failed, so the
    // enrichment below does not run it a second time.
    let exe_path = args.exe.replace('!', "/");
    let early_info = match &identity {
        Some(id) if config.filter.needs_crictl() => Some(
            crictl::inspect_with_timeout(
                &id.container_id,
                config,
                config.pre_drain_crictl_timeout_secs(),
            )
            .await,
        ),
        _ => None,
    };
    let filtered = identity.is_some() && {
        let info = early_info.as_ref().and_then(Option::as_ref);
        let subject = Subject {
            namespace: info.and_then(|i| i.namespace.as_deref()),
            pod: info.and_then(|i| i.pod_name.as_deref()),
            container: info.and_then(|i| i.container_name.as_deref()),
            image: info.and_then(|i| i.image.as_deref()),
            exe: Some(&exe_path),
        };
        !config.filter.keeps(&subject)
    };
    if filtered {
        info!(exe = %exe_path, "capture filter drops this crash; nothing will be spooled");
    } else if matches!(early_info, Some(None)) {
        warn!("crictl identity unknown; capture filter keeps this crash");
    }

    let cluster = config.cluster.as_str();
    let spool = Spool::new(&config.spool_dir);

    // A bundle needs an identity (for its object keys) and a store to go to.
    // When the bundle cannot be created the core is not read, and the
    // capture is reported as failed.
    let mut spool_error = None;
    let target = match &identity {
        Some(id) if config.store_configured && !filtered => {
            let name =
                spool::bundle_name(args.timestamp, args.host_pid, &id.pod_uid, &id.container_id);
            match spool.create_bundle(&name) {
                Ok(bundle) => Some(Target {
                    identity: id,
                    keys: ObjectKeys::new(
                        cluster,
                        &id.pod_uid,
                        &id.container_id,
                        args.timestamp,
                        args.host_pid,
                    ),
                    bundle,
                }),
                Err(e) => {
                    warn!(error = %e, spool = %config.spool_dir.display(), "creating the spool bundle failed; discarding core");
                    spool_error = Some(anyhow!(e).context("creating the spool bundle"));
                    None
                }
            }
        }
        _ => None,
    };

    // Rate limit: consult only when a core would be kept.
    let limiter = RateLimiter::new(&config.rate_state_path, config.max_cores_per_hour);
    // Set while this crash holds a rate-limit slot. A refund clears it, so a
    // slot is never given back twice: a second refund would remove another
    // crash's entry with the same timestamp.
    let slot_held = std::cell::Cell::new(false);
    let rate_suppressed = match &target {
        Some(t) => match limiter.check_and_record(&t.identity.pod_uid, args.timestamp) {
            RateDecision::Suppressed { recent } => {
                warn!(
                    pod_uid = %t.identity.pod_uid,
                    recent,
                    max_per_hour = config.max_cores_per_hour,
                    "per-pod core budget exhausted - discarding core, keeping snapshot + manifest"
                );
                true
            }
            RateDecision::Allowed => {
                slot_held.set(true);
                false
            }
        },
        None => false,
    };

    // Spool quota: a core that cannot fit is dropped, and gives its budget
    // slot back - nothing was kept.
    let core_budget = match &target {
        Some(_) if !rate_suppressed => {
            spool.core_budget(config.spool_max_bytes, config.spool_min_free_percent)
        }
        _ => 0,
    };
    let spool_full = target.is_some() && !rate_suppressed && core_budget == 0;
    if spool_full {
        warn!(
            spool = %config.spool_dir.display(),
            max_bytes = config.spool_max_bytes,
            min_free_percent = config.spool_min_free_percent,
            "spool is full - discarding core, keeping snapshot + manifest"
        );
    }
    let refund = |pod_uid: &str| {
        if slot_held.replace(false) {
            limiter.refund(pod_uid, args.timestamp);
        }
    };
    if spool_full && let Some(t) = &target {
        refund(&t.identity.pod_uid);
    }

    // Step 3: Drain a kept core, then release the kernel's pipe. A core that
    // is not kept is never read: the closed pipe stops the dump, so the
    // kernel reaps the crashed process sooner.
    let deadline = (config.drain_deadline_secs != 0)
        .then(|| std::time::Duration::from_secs(config.drain_deadline_secs));
    let drained = match &target {
        Some(t) if !rate_suppressed && !spool_full => {
            info!(bundle = %t.bundle.path("").display(), "spooling core");
            SpoolBackend::new(
                t.bundle.path(spool::CORE_FILE),
                config.max_core_bytes,
                core_budget,
                Some(SpoolBudget::new(
                    spool.clone(),
                    config.spool_max_bytes,
                    config.spool_min_free_percent,
                )),
            )
            .drain_core(&mut core_in, deadline)
            .await
        }
        _ => {
            info!("core is not kept; closing the core pipe unread");
            Ok(CoreStats::default())
        }
    };
    drop(core_in);
    // A kept core that passes the drain deadline is dropped. The capture
    // keeps its snapshot and manifest, like a rate-limited or spool-full one.
    let drain_deadline = match (&drained, &target) {
        (Err(e), Some(_)) => e.downcast_ref::<DrainDeadline>().copied(),
        _ => None,
    };
    let drained = match (drain_deadline, &target) {
        (Some(DrainDeadline { bytes }), Some(t)) => {
            warn!(
                bytes,
                "core drain exceeded the deadline - dropping core, keeping snapshot + manifest"
            );
            remove_partial_core(&t.bundle).map(|()| {
                refund(&t.identity.pod_uid);
                CoreStats {
                    bytes,
                    ..CoreStats::default()
                }
            })
        }
        _ => drained,
    };
    let drained =
        drained
            .context("draining core stream")
            .and_then(|stats| match spool_error.take() {
                Some(e) => Err(e),
                None => Ok(stats),
            });
    // fsync the stored core only now: the pipe is released, so a slow fsync
    // does not hold the kernel's `core_pipe_limit` slot.
    let drained = drained.and_then(|stats| match &target {
        Some(t) if stats.sha256.is_some() => t
            .bundle
            .sync_file(spool::CORE_FILE)
            .context("syncing the stored core")
            .map(|()| stats),
        _ => Ok(stats),
    });
    let stats = match drained {
        Ok(stats) => stats,
        Err(e) => {
            // Nothing was kept: give the budget slot back, or a transient
            // disk problem would exhaust the budget with zero cores kept.
            if let Some(t) = target {
                refund(&t.identity.pod_uid);
                t.bundle.discard();
            }
            // crictl has not run yet, so only the cgroup identity is known.
            report(
                config,
                &CaptureEventPayload {
                    pod_uid: identity.as_ref().map(|id| id.pod_uid.clone()),
                    container_id: identity.as_ref().map(|id| id.container_id.clone()),
                    error: Some(format!("{e:#}")),
                    ..base_payload(&args, Outcome::Failed)
                },
            );
            return Err(e);
        }
    };
    let stored = stats.sha256.is_some();
    info!(
        core_bytes = stats.bytes,
        stored_bytes = stats.stored_bytes,
        truncated = stats.truncated,
        stored,
        "core stream drained"
    );

    // Step 4: Proc snapshot into the bundle (small, buffered).
    let proc_snapshot_key = target.as_ref().and_then(|t| {
        let tar = match snapshot.to_tar() {
            Ok(tar) => tar,
            Err(e) => {
                warn!(error = %e, "rendering proc snapshot tar failed; ref omitted from manifest");
                return None;
            }
        };
        match t.bundle.write_file(spool::SNAPSHOT_FILE, &tar) {
            Ok(()) => {
                info!(bytes = tar.len(), "proc snapshot spooled");
                Some(t.keys.proc_snapshot())
            }
            Err(e) => {
                warn!(error = %e, "spooling proc snapshot failed; ref omitted from manifest");
                None
            }
        }
    });

    // Step 5: Crictl enrichment (after the pipe is released, so not
    // time-critical), unless the capture filter already ran it.
    let container_info = match (early_info, &identity) {
        (Some(info), _) => info,
        (None, Some(id)) => crictl::inspect(&id.container_id, config).await,
        (None, None) => None,
    };

    // Step 6: Assemble manifest.
    let (pod_uid, container_id) = identity
        .as_ref()
        .map(|id| (id.pod_uid.clone(), id.container_id.clone()))
        .unwrap_or_default();
    let info = container_info.unwrap_or_default();
    let manifest_identity = ManifestIdentity {
        pod_uid,
        container_id,
        namespace: info.namespace,
        pod_name: info.pod_name,
        container_name: info.container_name,
        image: info.image,
        image_digest: info.image_digest,
        restart_count: info.restart_count,
    };

    let any_snapshot_truncated = snapshot.files.iter().any(|f| f.truncated);
    let proc_ref = proc_snapshot_key.map(|key| ProcSnapshotRef {
        object_key: key,
        truncated: any_snapshot_truncated,
        file_count: snapshot.files.len(),
    });

    let skipped_reason = if rate_suppressed {
        Some("rate_limit")
    } else if spool_full {
        Some("spool_full")
    } else if drain_deadline.is_some() {
        Some("drain_deadline")
    } else {
        None
    };
    let manifest = Manifest {
        schema_version: 1,
        captured_at: format_timestamp(args.timestamp),
        cluster: cluster.to_string(),
        node: node_hostname(),
        signal: args.signal,
        signal_name: signal_name(args.signal).map(str::to_string),
        exe: args.exe.clone(),
        build_id: snapshot.build_id.clone(),
        identity: manifest_identity,
        core: CoreRef {
            present: stored,
            object_key: target.as_ref().map(|t| t.keys.core()).filter(|_| stored),
            sha256: stats.sha256.clone(),
            size_bytes: stored.then_some(stats.bytes),
            stored_bytes: stored.then_some(stats.stored_bytes),
            truncated: stats.truncated,
            truncated_reason: stats.truncated_reason.clone(),
            skipped_reason: skipped_reason.map(str::to_string),
            codec: "zstd".to_string(),
        },
        proc_snapshot: proc_ref,
    };

    let outcome = if identity.is_none() {
        Outcome::SkippedNonK8s
    } else if filtered {
        Outcome::Filtered
    } else if !config.store_configured {
        Outcome::NoStoreDiscard
    } else if rate_suppressed {
        Outcome::SuppressedRateLimit
    } else if spool_full {
        Outcome::SpoolFull
    } else if drain_deadline.is_some() {
        Outcome::DrainDeadline
    } else {
        Outcome::Spooled
    };
    let id = &manifest.identity;
    let payload = CaptureEventPayload {
        namespace: id.namespace.clone(),
        pod_name: id.pod_name.clone(),
        pod_uid: identity.as_ref().map(|i| i.pod_uid.clone()),
        container_id: identity.as_ref().map(|i| i.container_id.clone()),
        container_name: id.container_name.clone(),
        manifest_key: target.as_ref().map(|t| t.keys.manifest()),
        core_bytes: stats.bytes,
        stored_bytes: stored.then_some(stats.stored_bytes),
        truncated: stats.truncated,
        ..base_payload(&args, outcome)
    };

    // Write the manifest and the capture event into the bundle, then hand
    // the complete bundle to the daemon. A bundle that cannot be completed
    // is deleted: the daemon must never upload part of a capture.
    if let Some(t) = target {
        if let Err(e) = commit_bundle(t.bundle, &manifest, &payload) {
            refund(&t.identity.pod_uid);
            report(
                config,
                &CaptureEventPayload {
                    outcome: Outcome::Failed,
                    manifest_key: None,
                    error: Some(format!("{e:#}")),
                    ..payload
                },
            );
            return Err(e);
        }
        info!(
            manifest_key = payload.manifest_key.as_deref().unwrap_or("<none>"),
            "capture spooled"
        );
    }

    // Step 7: report the capture to the daemon.
    report(config, &payload);

    Ok(())
}

/// Delete the partial core of a drain that passed its deadline. Its zstd
/// stream is not finalized, so the file is not a valid core.
fn remove_partial_core(bundle: &Bundle) -> Result<()> {
    let path = bundle.path(spool::CORE_FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(anyhow!(e).context(format!("removing the partial core {}", path.display()))),
    }
}

/// Write the manifest and capture event into `bundle` and move it to
/// `ready/`. On failure the bundle is deleted.
fn commit_bundle(bundle: Bundle, manifest: &Manifest, payload: &CaptureEventPayload) -> Result<()> {
    let written = serde_json::to_vec_pretty(manifest)
        .context("serializing manifest")
        .and_then(|json| {
            bundle
                .write_file(spool::MANIFEST_FILE, &json)
                .context("spooling manifest")
        })
        .and_then(|()| serde_json::to_vec(payload).context("serializing capture event"))
        .and_then(|json| {
            bundle
                .write_file(spool::CAPTURE_FILE, &json)
                .context("spooling capture event")
        });
    if let Err(e) = written {
        bundle.discard();
        return Err(e);
    }
    bundle
        .commit()
        .context("moving the bundle to the spool's ready dir")?;
    Ok(())
}

/// The payload fields every capture has, whatever its outcome.
fn base_payload(args: &CaptureArgs, outcome: Outcome) -> CaptureEventPayload {
    CaptureEventPayload {
        namespace: None,
        pod_name: None,
        pod_uid: None,
        container_id: None,
        container_name: None,
        host_pid: args.host_pid,
        exe: args.exe.clone(),
        signal: args.signal,
        signal_name: signal_name(args.signal).map(str::to_string),
        outcome,
        manifest_key: None,
        core_bytes: 0,
        stored_bytes: None,
        truncated: false,
        error: None,
        timestamp: args.timestamp,
    }
}

/// One summary line per capture, on every path, then the same summary to the
/// daemon. The kernel gives the handler no stdout, so the daemon's copy of
/// this line is the one operators see and alert on.
fn report(config: &HandlerConfig, payload: &CaptureEventPayload) {
    info!(
        outcome = payload.outcome.as_str(),
        host_pid = payload.host_pid,
        container_id = payload.container_id.as_deref().unwrap_or("<none>"),
        manifest_key = payload.manifest_key.as_deref().unwrap_or("<none>"),
        core_bytes = payload.core_bytes,
        truncated = payload.truncated,
        error = payload.error.as_deref().unwrap_or(""),
        "capture complete"
    );
    events::send_capture_event(config.event_socket_path.as_deref(), payload);
}

/// The UTS hostname: the node name for the handler, which the kernel runs in
/// the host namespaces. The daemon pod prefers `NODE_NAME`. Reads
/// `/proc/sys/kernel/hostname`; falls back to `localhost`.
#[must_use]
pub fn node_hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

fn read_cgroup_identity(proc_root: &Path, pid: i32) -> Option<CgroupIdentity> {
    let path = proc_root.join(pid.to_string()).join("cgroup");
    let content = std::fs::read_to_string(&path).ok()?;
    cgroup::parse_cgroup(&content)
}

fn format_timestamp(epoch_secs: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(epoch_secs, 0)
        .map_or_else(|| epoch_secs.to_string(), |dt| dt.to_rfc3339())
}

fn signal_name(sig: i32) -> Option<&'static str> {
    match sig {
        1 => Some("SIGHUP"),
        2 => Some("SIGINT"),
        3 => Some("SIGQUIT"),
        4 => Some("SIGILL"),
        6 => Some("SIGABRT"),
        7 => Some("SIGBUS"),
        8 => Some("SIGFPE"),
        9 => Some("SIGKILL"),
        11 => Some("SIGSEGV"),
        13 => Some("SIGPIPE"),
        15 => Some("SIGTERM"),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<OsString> {
        v.iter().map(OsString::from).collect()
    }

    #[test]
    fn accepts_a_non_utf8_exe() {
        use std::os::unix::ffi::OsStringExt;
        let exe = OsString::from_vec(b"!usr!bin!app\xff\xfe".to_vec());
        let mut argv = args(&["4242", "11", "1749600000"]);
        argv.push(exe);
        let parsed = CaptureArgs::parse(&argv).unwrap();
        assert_eq!(parsed.exe, "!usr!bin!app\u{fffd}\u{fffd}");
        assert_eq!(parsed.host_pid, 4242);
    }

    #[test]
    fn rejects_a_non_utf8_number() {
        use std::os::unix::ffi::OsStringExt;
        let mut argv = vec![OsString::from_vec(b"42\xff".to_vec())];
        argv.extend(args(&["11", "0", "exe"]));
        assert!(CaptureArgs::parse(&argv).is_err());
    }

    #[test]
    fn parses_the_kernel_arg_template() {
        let parsed =
            CaptureArgs::parse(&args(&["4242", "11", "1749600000", "!usr!bin!app"])).unwrap();
        assert_eq!(
            parsed,
            CaptureArgs {
                host_pid: 4242,
                signal: 11,
                timestamp: 1_749_600_000,
                exe: "!usr!bin!app".into(),
            }
        );
    }

    #[test]
    fn rejects_wrong_arity() {
        assert!(CaptureArgs::parse(&args(&["1", "2", "3"])).is_err());
        assert!(CaptureArgs::parse(&args(&["1", "2", "3", "x", "extra"])).is_err());
    }

    #[test]
    fn rejects_non_numeric_fields() {
        assert!(CaptureArgs::parse(&args(&["notapid", "11", "0", "exe"])).is_err());
        assert!(CaptureArgs::parse(&args(&["1", "sig", "0", "exe"])).is_err());
    }

    #[test]
    fn signal_names_cover_common_faults() {
        assert_eq!(signal_name(11), Some("SIGSEGV"));
        assert_eq!(signal_name(6), Some("SIGABRT"));
        assert_eq!(signal_name(8), Some("SIGFPE"));
        assert_eq!(signal_name(4), Some("SIGILL"));
        assert_eq!(signal_name(7), Some("SIGBUS"));
        assert_eq!(signal_name(99), None);
    }

    #[test]
    fn format_timestamp_produces_rfc3339() {
        assert_eq!(format_timestamp(0), "1970-01-01T00:00:00+00:00");
        assert_eq!(format_timestamp(1_749_600_000), "2025-06-11T00:00:00+00:00");
    }
}
