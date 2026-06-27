//! The JSON manifest sidecar written next to each captured core.
//!
//! After core drain + `/proc` snapshot upload + crictl enrichment, the handler
//! assembles a `Manifest` and writes it to the object store at
//! `{cluster}/{podUID}/{containerID}/{ts}-manifest.json` - sibling to the core
//! at `…-core.zst`. The manifest is the "record": a manifest pointing at a
//! missing core is a real bug; an orphan core with no manifest is GC-able.
//!
//! Write ordering: core → proc snapshot → manifest.

use serde::{Deserialize, Serialize};

/// The top-level manifest written next to each captured core.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub schema_version: u32,
    /// RFC3339 timestamp of the crash (from the kernel's `%t` specifier).
    pub captured_at: String,
    pub cluster: String,
    /// Node hostname.
    pub node: String,
    pub signal: i32,
    pub signal_name: Option<String>,
    /// Slash-mangled executable path (the kernel's `%E` specifier).
    pub exe: String,
    /// GNU build-id of the main executable, if readable.
    pub build_id: Option<String>,
    /// Container identity (cgroup-derived + crictl-enriched).
    pub identity: ManifestIdentity,
    pub core: CoreRef,
    pub proc_snapshot: Option<ProcSnapshotRef>,
}

/// Container identity in the manifest. `pod_uid` and `container_id` are always
/// present (cgroup-derived); the remaining fields are best-effort from crictl.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestIdentity {
    pub pod_uid: String,
    pub container_id: String,
    pub namespace: Option<String>,
    pub pod_name: Option<String>,
    pub container_name: Option<String>,
    pub image: Option<String>,
    pub image_digest: Option<String>,
    /// Restart count analog from `crictl inspect` (`status.metadata.attempt`).
    /// Best-effort: `None` when crictl was unavailable or returned no attempt.
    pub restart_count: Option<u32>,
}

/// Reference to the captured core object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreRef {
    /// Whether a core was actually stored (`false` for systemd-coredump backend
    /// or when no store is configured).
    pub present: bool,
    pub object_key: Option<String>,
    pub sha256: Option<String>,
    /// Uncompressed size in bytes.
    pub size_bytes: Option<u64>,
    /// Compressed size in bytes (what's stored in the object store).
    pub stored_bytes: Option<u64>,
    pub truncated: bool,
    /// Compression codec applied to the stored object.
    pub codec: String,
}

/// Reference to the `/proc` snapshot tar uploaded alongside the core.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcSnapshotRef {
    pub object_key: String,
    pub truncated: bool,
    pub file_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Manifest {
        Manifest {
            schema_version: 1,
            captured_at: "2026-06-27T12:00:00+00:00".into(),
            cluster: "prod".into(),
            node: "node-a".into(),
            signal: 11,
            signal_name: Some("SIGSEGV".into()),
            exe: "!usr!bin!app".into(),
            build_id: Some("deadbeef0123".into()),
            identity: ManifestIdentity {
                pod_uid: "abc-123".into(),
                container_id: "def456".into(),
                namespace: Some("default".into()),
                pod_name: Some("my-pod".into()),
                container_name: Some("app".into()),
                image: Some("ubuntu:latest".into()),
                image_digest: Some("sha256:cafebabe".into()),
                restart_count: Some(2),
            },
            core: CoreRef {
                present: true,
                object_key: Some("prod/abc-123/def456/1234567890-core.zst".into()),
                sha256: Some("aaaa".into()),
                size_bytes: Some(1_000_000),
                stored_bytes: Some(200_000),
                truncated: false,
                codec: "zstd".into(),
            },
            proc_snapshot: Some(ProcSnapshotRef {
                object_key: "prod/abc-123/def456/1234567890-procsnapshot.tar".into(),
                truncated: false,
                file_count: 7,
            }),
        }
    }

    #[test]
    fn round_trips_through_json() {
        let m = sample();
        let json = serde_json::to_string(&m).unwrap();
        let back: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.schema_version, 1);
        assert_eq!(back.identity.namespace.as_deref(), Some("default"));
        assert_eq!(back.identity.restart_count, Some(2));
        assert!(back.core.present);
        assert_eq!(back.proc_snapshot.as_ref().unwrap().file_count, 7);
    }

    #[test]
    fn serializes_to_readable_json() {
        let json = serde_json::to_string_pretty(&sample()).unwrap();
        assert!(json.contains("\"schema_version\": 1"));
        assert!(json.contains("\"signal_name\": \"SIGSEGV\""));
        assert!(json.contains("\"codec\": \"zstd\""));
    }

    #[test]
    fn no_core_manifest_is_valid() {
        let mut m = sample();
        m.core.present = false;
        m.core.object_key = None;
        m.core.sha256 = None;
        m.core.size_bytes = None;
        m.core.stored_bytes = None;
        m.proc_snapshot = None;
        let json = serde_json::to_string(&m).unwrap();
        let back: Manifest = serde_json::from_str(&json).unwrap();
        assert!(!back.core.present);
        assert!(back.proc_snapshot.is_none());
    }
}
