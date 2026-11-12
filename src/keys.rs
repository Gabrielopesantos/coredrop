//! Object-store key scheme. The handler records the keys in the manifest;
//! the daemon uploads each object to its key.

/// Object keys for one capture. Every object of a capture shares the prefix
/// `{cluster}/{podUID}/{containerID}/{timestamp}-{hostPid}`; the suffix names
/// the object. Handler-derivable from the cgroup - no UUIDs; the manifest
/// carries the human-readable identity from crictl. The host PID
/// disambiguates two crashes in the same container within the same second
/// (possible before the rate limiter engages), which would otherwise
/// overwrite each other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectKeys {
    prefix: String,
}

impl ObjectKeys {
    /// Build the key prefix for one capture:
    /// `{cluster}/{pod_uid}/{container_id}/{timestamp}-{host_pid}`.
    #[must_use]
    pub fn new(
        cluster: &str,
        pod_uid: &str,
        container_id: &str,
        timestamp: i64,
        host_pid: i32,
    ) -> Self {
        Self {
            prefix: format!("{cluster}/{pod_uid}/{container_id}/{timestamp}-{host_pid}"),
        }
    }

    /// `<prefix>-core.zst`
    #[must_use]
    pub fn core(&self) -> String {
        format!("{}-core.zst", self.prefix)
    }

    /// `<prefix>-procsnapshot.tar`
    #[must_use]
    pub fn proc_snapshot(&self) -> String {
        format!("{}-procsnapshot.tar", self.prefix)
    }

    /// `<prefix>-manifest.json`, written last (blob-first ordering).
    #[must_use]
    pub fn manifest(&self) -> String {
        format!("{}-manifest.json", self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_object_keys() {
        assert_eq!(
            ObjectKeys::new("prod", "pod-uid-123", "abc123def", 1_749_600_000, 4242).core(),
            "prod/pod-uid-123/abc123def/1749600000-4242-core.zst"
        );
        assert_eq!(
            ObjectKeys::new("prod", "pod-uid-123", "abc123def", 1_749_600_000, 4242)
                .proc_snapshot(),
            "prod/pod-uid-123/abc123def/1749600000-4242-procsnapshot.tar"
        );
        assert_eq!(
            ObjectKeys::new("prod", "pod-uid-123", "abc123def", 1_749_600_000, 4242).manifest(),
            "prod/pod-uid-123/abc123def/1749600000-4242-manifest.json"
        );
    }

    #[test]
    fn distinct_pids_yield_distinct_keys_within_the_same_second() {
        let a = ObjectKeys::new("prod", "pod-uid-123", "abc123def", 1_749_600_000, 111).core();
        let b = ObjectKeys::new("prod", "pod-uid-123", "abc123def", 1_749_600_000, 222).core();
        assert_ne!(a, b);
    }
}
