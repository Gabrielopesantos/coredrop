//! The kernel-exec'd capture handler: `coredrop capture %P %s %t %E`.
//!
//! The kernel runs this in the *host* namespaces with the core on stdin.
//! Handler flow (top-to-bottom, ordered by time-criticality):
//!
//! 1. Pre-reap snapshot - snapshot `/proc/<hostpid>` before the kernel
//!    reaps the PID; must complete while the kernel waits.
//! 2. Core drain - stream stdin through zstd to the object store (or
//!    discard if no store). Releases the kernel's pipe.
//! 3. Proc snapshot upload - buffered PUT of the small tar.
//! 4. Crictl enrichment - post-drain, best-effort: shell `crictl inspect`
//!    for human-readable identity (namespace, pod name, container name, image,
//!    restart count). Failure degrades to cgroup-only identity.
//! 5. Manifest write - assemble and PUT the JSON sidecar next to the core.
//! 6. Capture event - fire-and-forget datagram to the daemon, which posts a
//!    k8s Event on the crashing pod. Best-effort; requires crictl-enriched
//!    identity (namespace + pod name).

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use object_store::ObjectStore;
use tokio::io::AsyncRead;
use tracing::{debug, info, warn};

use crate::backend::{CaptureBackend, CaptureBackendKind, DiscardBackend};
use crate::cgroup::{self, CgroupIdentity};
use crate::config::HandlerConfig;
use crate::crictl;
use crate::events::{self, CaptureEventPayload, Outcome};
use crate::manifest::{CoreRef, Manifest, ManifestIdentity, ProcSnapshotRef};
use crate::ratelimit::{RateDecision, RateLimiter};
use crate::redact::Redactor;
use crate::snapshot::ProcSnapshot;
use crate::systemd::{self, SystemdCoredumpBackend};
use crate::upload::{self, StandaloneBackend};

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
    pub fn parse(args: &[String]) -> Result<Self> {
        let [pid, sig, ts, exe] = args else {
            bail!(
                "capture expects 4 args (%P %s %t %E), got {}: {args:?}",
                args.len()
            );
        };
        Ok(Self {
            host_pid: pid
                .parse()
                .with_context(|| format!("bad host pid {pid:?}"))?,
            signal: sig.parse().with_context(|| format!("bad signal {sig:?}"))?,
            timestamp: ts
                .parse()
                .with_context(|| format!("bad timestamp {ts:?}"))?,
            exe: exe.clone(),
        })
    }
}

/// Run the handler: snapshot /proc, drain core, enrich via crictl, write manifest.
///
/// `core_in` is the core stream (normally `tokio::io::stdin()`; injected in
/// tests as a byte slice). `store_override` injects a pre-built `ObjectStore`
/// for tests; `None` falls back to the URL-derived store from `config`.
///
/// # Errors
///
/// Fails when no object store is configured or reachable, or when draining
/// the core stream or writing the manifest fails. Best-effort stages
/// (proc snapshot, crictl enrichment, rate-limit state IO) degrade instead
/// of erroring.
// Sequential capture pipeline; splitting it would only scatter the stages.
#[allow(clippy::too_many_lines)]
pub async fn run(
    args: CaptureArgs,
    config: &HandlerConfig,
    core_in: &mut (impl AsyncRead + Unpin + Send),
    store_override: Option<Arc<dyn ObjectStore>>,
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
            "no kubernetes cgroup identity resolved; uploads will be skipped"
        );
    }
    let cluster = config.cluster.as_str();
    let store = store_override.or_else(|| config.object_store());

    let core_key = identity
        .as_ref()
        .map(|id| upload::core_object_key(cluster, &id.pod_uid, &id.container_id, args.timestamp));

    // Rate limit: consult only when a core would actually upload (standalone
    // backend with identity + store). The systemd backend owns its own limits.
    let limiter = RateLimiter::new(&config.rate_state_path, config.max_cores_per_hour);
    let mut rate_recorded = false;
    let rate_suppressed = match (&identity, &store, config.backend_kind()) {
        (Some(id), Some(_), CaptureBackendKind::Standalone) => {
            match limiter.check_and_record(&id.container_id, args.timestamp) {
                RateDecision::Suppressed { recent } => {
                    warn!(
                        container_id = %id.container_id,
                        recent,
                        max_per_hour = config.max_cores_per_hour,
                        "per-container core budget exhausted - discarding core, keeping snapshot + manifest"
                    );
                    true
                }
                RateDecision::Allowed => {
                    rate_recorded = true;
                    false
                }
            }
        }
        _ => false,
    };

    // Step 2: Drain core (releases the kernel's pipe).
    let backend: Box<dyn CaptureBackend> = if rate_suppressed {
        Box::new(DiscardBackend)
    } else {
        build_backend(
            config,
            &snapshot,
            &args,
            core_key.as_deref(),
            store.as_ref(),
        )
    };
    let stats = match backend
        .drain_core(core_in)
        .await
        .context("draining core stream")
    {
        Ok(stats) => stats,
        Err(e) => {
            // Nothing was stored: give the budget slot back, or a transient
            // store outage would exhaust the budget with zero cores kept.
            if rate_recorded && let Some(id) = &identity {
                limiter.refund(&id.container_id, args.timestamp);
            }
            info!(
                outcome = Outcome::Failed.as_str(),
                host_pid = args.host_pid,
                error = %e,
                "capture complete"
            );
            return Err(e);
        }
    };
    let uploaded = stats.sha256.is_some();
    info!(
        core_bytes = stats.bytes,
        stored_bytes = stats.stored_bytes,
        truncated = stats.truncated,
        uploaded,
        "core stream drained"
    );

    // Step 3: Upload proc snapshot (small, buffered).
    let proc_snapshot_key = match (&identity, &store) {
        (Some(id), Some(store)) => {
            let key = upload::proc_snapshot_object_key(
                cluster,
                &id.pod_uid,
                &id.container_id,
                args.timestamp,
            );
            match snapshot.to_tar() {
                Ok(tar) => {
                    let bytes = tar.len();
                    match upload::put_object(store, &key, tar).await {
                        Ok(()) => {
                            info!(key = %key, bytes, "proc snapshot uploaded");
                            Some(key)
                        }
                        Err(e) => {
                            warn!(error = %e, "proc snapshot upload failed; ref omitted from manifest");
                            None
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "rendering proc snapshot tar failed; ref omitted from manifest");
                    None
                }
            }
        }
        _ => None,
    };

    // Step 4: Crictl enrichment (post-drain - no longer time-critical).
    let container_info = if let Some(id) = &identity {
        crictl::inspect(&id.container_id, config).await
    } else {
        None
    };

    // Step 5: Assemble manifest.
    let manifest_identity = ManifestIdentity {
        pod_uid: identity
            .as_ref()
            .map(|id| id.pod_uid.clone())
            .unwrap_or_default(),
        container_id: identity
            .as_ref()
            .map(|id| id.container_id.clone())
            .unwrap_or_default(),
        namespace: container_info.as_ref().and_then(|c| c.namespace.clone()),
        pod_name: container_info.as_ref().and_then(|c| c.pod_name.clone()),
        container_name: container_info
            .as_ref()
            .and_then(|c| c.container_name.clone()),
        image: container_info.as_ref().and_then(|c| c.image.clone()),
        image_digest: container_info.as_ref().and_then(|c| c.image_digest.clone()),
        restart_count: container_info.as_ref().and_then(|c| c.restart_count),
    };

    let any_snapshot_truncated = snapshot.files.iter().any(|f| f.truncated);
    let proc_ref = proc_snapshot_key.map(|key| ProcSnapshotRef {
        object_key: key,
        truncated: any_snapshot_truncated,
        file_count: snapshot.files.len(),
    });

    let manifest = Manifest {
        schema_version: 1,
        captured_at: format_timestamp(args.timestamp),
        cluster: cluster.to_string(),
        node: systemd::node_hostname(),
        signal: args.signal,
        signal_name: signal_name(args.signal).map(str::to_string),
        exe: args.exe.clone(),
        build_id: snapshot.build_id.clone(),
        identity: manifest_identity,
        core: CoreRef {
            present: uploaded,
            object_key: if uploaded { core_key.clone() } else { None },
            sha256: stats.sha256.clone(),
            size_bytes: if uploaded { Some(stats.bytes) } else { None },
            stored_bytes: if uploaded {
                Some(stats.stored_bytes)
            } else {
                None
            },
            truncated: stats.truncated,
            truncated_reason: stats.truncated_reason.clone(),
            skipped_reason: rate_suppressed.then(|| "rate_limit".to_string()),
            codec: "zstd".to_string(),
        },
        proc_snapshot: proc_ref,
    };

    // Write manifest to store (blob-first ordering: core → snapshot → manifest).
    let manifest_key = if let (Some(id), Some(store)) = (&identity, &store) {
        let key =
            upload::manifest_object_key(cluster, &id.pod_uid, &id.container_id, args.timestamp);
        match serde_json::to_vec_pretty(&manifest) {
            Ok(json) => match upload::put_object(store, &key, json).await {
                Ok(()) => {
                    info!(key = %key, "manifest written");
                    Some(key)
                }
                Err(e) => {
                    warn!(error = %e, key = %key, "manifest write failed");
                    None
                }
            },
            Err(e) => {
                warn!(error = %e, "manifest serialization failed");
                None
            }
        }
    } else {
        warn!("no store or no cgroup identity; manifest skipped");
        None
    };

    // One summary line per capture, whatever the path taken - the log
    // interface operators alert on.
    let outcome = if identity.is_none() {
        Outcome::SkippedNonK8s
    } else if rate_suppressed {
        Outcome::SuppressedRateLimit
    } else if config.backend_kind() == CaptureBackendKind::SystemdCoredump {
        Outcome::ForwardedSystemd
    } else if store.is_none() {
        Outcome::NoStoreDiscard
    } else if uploaded {
        Outcome::Uploaded
    } else {
        Outcome::Failed
    };

    // Step 6: report the capture to the daemon for k8s Event emission.
    // Requires namespace + pod name, i.e. only when crictl enrichment
    // succeeded - a cgroup-only identity can't target a pod object.
    if let (Some(namespace), Some(pod_name)) = (
        container_info.as_ref().and_then(|c| c.namespace.clone()),
        container_info.as_ref().and_then(|c| c.pod_name.clone()),
    ) {
        let payload = CaptureEventPayload {
            namespace,
            pod_name,
            pod_uid: identity
                .as_ref()
                .map(|id| id.pod_uid.clone())
                .unwrap_or_default(),
            container_name: container_info
                .as_ref()
                .and_then(|c| c.container_name.clone()),
            signal: args.signal,
            signal_name: signal_name(args.signal).map(str::to_string),
            outcome,
            manifest_key: manifest_key.clone(),
            stored_bytes: uploaded.then_some(stats.stored_bytes),
            timestamp: args.timestamp,
        };
        events::send_capture_event(config.event_socket_path.as_deref(), &payload);
    } else {
        debug!(
            "no namespace/pod name resolved (crictl enrichment unavailable); skipping capture event"
        );
    }

    info!(
        outcome = outcome.as_str(),
        host_pid = args.host_pid,
        container_id = identity
            .as_ref()
            .map_or("<none>", |id| id.container_id.as_str()),
        core_key = core_key.as_deref().unwrap_or("<none>"),
        core_bytes = stats.bytes,
        truncated = stats.truncated,
        "capture complete"
    );

    Ok(())
}

fn build_backend(
    config: &HandlerConfig,
    snapshot: &ProcSnapshot,
    args: &CaptureArgs,
    object_key: Option<&str>,
    store: Option<&Arc<dyn ObjectStore>>,
) -> Box<dyn CaptureBackend> {
    match config.backend_kind() {
        CaptureBackendKind::SystemdCoredump => {
            let program = config
                .systemd_coredump_path
                .clone()
                .unwrap_or_else(|| systemd::DEFAULT_SYSTEMD_COREDUMP_PATH.to_string());
            let uid = snapshot_file(snapshot, "status")
                .and_then(|s| systemd::status_first_field(s, "Uid:"))
                .unwrap_or_else(|| "0".to_string());
            let gid = snapshot_file(snapshot, "status")
                .and_then(|s| systemd::status_first_field(s, "Gid:"))
                .unwrap_or_else(|| "0".to_string());
            let core_limit = snapshot_file(snapshot, "limits")
                .and_then(systemd::parse_core_limit)
                .unwrap_or_else(|| u64::MAX.to_string());
            let hostname = systemd::node_hostname();
            info!(program = %program, "forwarding core to systemd-coredump (chaining backend)");
            Box::new(SystemdCoredumpBackend::new(
                program,
                systemd::build_forward_args(
                    args.host_pid,
                    &uid,
                    &gid,
                    args.signal,
                    args.timestamp,
                    &core_limit,
                    &hostname,
                ),
            ))
        }
        CaptureBackendKind::Standalone => {
            if let (Some(key), Some(store)) = (object_key, store) {
                info!(key = %key, "streaming core to object store (standalone backend)");
                Box::new(StandaloneBackend::new(
                    store.clone(),
                    key,
                    config.max_core_bytes,
                ))
            } else {
                info!("no object store configured or identity unresolved; discarding core");
                Box::new(DiscardBackend)
            }
        }
    }
}

fn snapshot_file<'a>(snapshot: &'a ProcSnapshot, name: &str) -> Option<&'a [u8]> {
    snapshot
        .files
        .iter()
        .find(|f| f.name == name)
        .map(|f| f.bytes.as_slice())
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

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(std::string::ToString::to_string).collect()
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
        let s = format_timestamp(0);
        assert!(s.contains("1970"), "epoch 0 should be 1970, got {s}");
        let s = format_timestamp(1_749_600_000);
        assert!(s.starts_with("2025") || s.starts_with("2026"), "got {s}");
    }
}
