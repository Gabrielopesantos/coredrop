//! Parse `/proc/<hostpid>/cgroup` -> `(podUID, containerID)`.
//!
//! The capture handler runs in the host PID namespace, so the faulting
//! process's cgroup path carries its Kubernetes identity. This pure parse
//! makes the object key handler-derivable before any enrichment.
//!
//! Two cgroup layouts appear in the wild, by the kubelet's cgroup driver:
//!   - cgroupfs: `…/kubepods/<qos>/pod<uid>/<containerID>`. `<uid>` keeps
//!     its dashes; `<containerID>` is the bare hex id.
//!   - systemd: `…/kubepods-<qos>-pod<uid>.slice/cri-containerd-<cid>.scope`.
//!     `<uid>` has its dashes rewritten to `_`; the container id wears a
//!     runtime prefix and a `.scope` suffix.
//!
//! cgroup v2 is a single `0::<path>` line; v1 has many `n:subsys:<path>`
//! lines. We scan every line's path tail and take the first that yields both ids.

/// Pod + container identity recovered from a process's cgroup path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CgroupIdentity {
    pub pod_uid: String,
    pub container_id: String,
}

/// Parse the contents of `/proc/<pid>/cgroup`. `None` for a process that is not
/// a Kubernetes container - the handler then cannot build a key and skips the
/// upload rather than guessing.
#[must_use]
pub fn parse_cgroup(content: &str) -> Option<CgroupIdentity> {
    content.lines().find_map(|line| {
        let path = line.rsplit(':').next().unwrap_or(line);
        parse_cgroup_path(path)
    })
}

/// Pull `(podUID, containerID)` out of one cgroup path.
#[must_use]
pub fn parse_cgroup_path(path: &str) -> Option<CgroupIdentity> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let pod_uid = segments.iter().find_map(|s| extract_pod_uid(s))?;
    let container_id = segments.last().and_then(|s| extract_container_id(s))?;
    Some(CgroupIdentity {
        pod_uid,
        container_id,
    })
}

/// Pull the podUID from a pod-level cgroup slice path - the directory whose
/// final segment names a pod but not a container.
#[must_use]
pub fn parse_pod_slice_path(path: &str) -> Option<String> {
    let last = path.split('/').rfind(|s| !s.is_empty())?;
    if extract_container_id(last).is_some() {
        return None;
    }
    extract_pod_uid(last)
}

fn extract_pod_uid(seg: &str) -> Option<String> {
    let s = seg.strip_suffix(".slice").unwrap_or(seg);
    let uid = if let Some(rest) = s.strip_prefix("pod") {
        rest
    } else {
        let idx = s.find("-pod")?;
        &s[idx + "-pod".len()..]
    };
    if uid.is_empty() {
        return None;
    }
    Some(uid.replace('_', "-"))
}

fn extract_container_id(seg: &str) -> Option<String> {
    let s = seg.strip_suffix(".scope").unwrap_or(seg);
    let s = s
        .strip_prefix("cri-containerd-")
        .or_else(|| s.strip_prefix("docker-"))
        .or_else(|| s.strip_prefix("crio-"))
        .unwrap_or(s);
    if s.len() >= 12 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(s.to_string())
    } else {
        None
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_cgroupfs_v2() {
        let content = "0::/kubepods/burstable/pod1234abcd-5678-90ef-ghij-klmnopqrstuv/\
             3b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c\n";
        let id = parse_cgroup(content).unwrap();
        assert_eq!(id.pod_uid, "1234abcd-5678-90ef-ghij-klmnopqrstuv");
        assert_eq!(
            id.container_id,
            "3b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c"
        );
    }

    #[test]
    fn parses_systemd_v2_with_underscored_uid_and_runtime_prefix() {
        let content = "0::/kubepods.slice/kubepods-burstable.slice/\
             kubepods-burstable-pod1234abcd_5678_90ef_ghij_klmnopqrstuv.slice/\
             cri-containerd-3b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c.scope\n";
        let id = parse_cgroup(content).unwrap();
        assert_eq!(id.pod_uid, "1234abcd-5678-90ef-ghij-klmnopqrstuv");
        assert_eq!(
            id.container_id,
            "3b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c"
        );
    }

    #[test]
    fn parses_cgroup_v1_multiline() {
        let content = "12:pids:/kubepods/besteffort/podaaaa1111-bbbb/abcdef0123456789abcdef\n\
             11:memory:/kubepods/besteffort/podaaaa1111-bbbb/abcdef0123456789abcdef\n\
             10:cpu,cpuacct:/kubepods/besteffort/podaaaa1111-bbbb/abcdef0123456789abcdef\n";
        let id = parse_cgroup(content).unwrap();
        assert_eq!(id.pod_uid, "aaaa1111-bbbb");
        assert_eq!(id.container_id, "abcdef0123456789abcdef");
    }

    #[test]
    fn parse_pod_slice_path_recognizes_pod_level_not_scope_or_qos() {
        let sd = "/kubepods.slice/kubepods-burstable.slice/\
             kubepods-burstable-pod1234abcd_5678_90ef_ghij_klmnopqrstuv.slice";
        assert_eq!(
            parse_pod_slice_path(sd).as_deref(),
            Some("1234abcd-5678-90ef-ghij-klmnopqrstuv")
        );
        let cf = "/kubepods/burstable/pod1234abcd-5678-90ef-ghij-klmnopqrstuv";
        assert_eq!(
            parse_pod_slice_path(cf).as_deref(),
            Some("1234abcd-5678-90ef-ghij-klmnopqrstuv")
        );
        let scope = "/kubepods.slice/kubepods-burstable-pod1234abcd_5678.slice/\
             cri-containerd-3b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c.scope";
        assert!(parse_pod_slice_path(scope).is_none());
        assert!(parse_pod_slice_path("/kubepods.slice/kubepods-burstable.slice").is_none());
        assert!(parse_pod_slice_path("/system.slice/sshd.service").is_none());
    }

    #[test]
    fn non_kubernetes_cgroup_is_none() {
        assert!(parse_cgroup("0::/system.slice/sshd.service\n").is_none());
        assert!(parse_cgroup("0::/kubepods.slice\n").is_none());
    }
}
