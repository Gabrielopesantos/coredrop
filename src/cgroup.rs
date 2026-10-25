//! Parse `/proc/<hostpid>/cgroup` -> `(podUID, containerID)`.
//!
//! The capture handler runs in the host PID namespace, so the faulting
//! process's cgroup path carries its Kubernetes identity. This pure parse
//! makes the object key handler-derivable before any enrichment.
//!
//! There are two cgroup layouts, one for each kubelet cgroup driver:
//!   - cgroupfs: `.../kubepods/<qos>/pod<uid>/<containerID>`. `<uid>` keeps
//!     its dashes; `<containerID>` is the bare hex id.
//!   - systemd: `.../kubepods-<qos>-pod<uid>.slice/cri-containerd-<cid>.scope`.
//!     `<uid>` has its dashes rewritten to `_`; the container id wears a
//!     runtime prefix and a `.scope` suffix.
//!
//! cgroup v2 is a single `0::<path>` line; v1 has many `n:subsys:<path>`
//! lines. We scan every line's path tail and take the first that yields both ids.
//!
//! The container id is the segment directly below the pod segment, not the
//! last one. A container can have its own cgroups below that (systemd in a
//! container makes `init.scope`), and a container with a writable cgroupfs
//! can name them as it likes, so the last segment can be missing or a lie.

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
    let mut segments = path.split('/').filter(|s| !s.is_empty());
    let pod_uid = segments.by_ref().find_map(extract_pod_uid)?;
    let container_id = extract_container_id(segments.next()?)?;
    Some(CgroupIdentity {
        pod_uid,
        container_id,
    })
}

/// Whether `s` has the characters of a pod UID: lowercase hex digits and `-`.
/// The UID goes into object keys and API request URLs.
#[must_use]
pub fn is_pod_uid(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MAX_POD_UID_LEN
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c) || c == b'-')
}

/// Upper bound on a pod UID. Kubernetes UIDs are 36-character UUIDs; this
/// bound only rejects absurd values.
pub const MAX_POD_UID_LEN: usize = 253;

fn extract_pod_uid(seg: &str) -> Option<String> {
    let s = seg.strip_suffix(".slice").unwrap_or(seg);
    let uid = if let Some(rest) = s.strip_prefix("pod") {
        rest
    } else {
        let idx = s.find("-pod")?;
        &s[idx + "-pod".len()..]
    };
    let uid = uid.replace('_', "-");
    is_pod_uid(&uid).then_some(uid)
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
        let content = "0::/kubepods/burstable/poded1e9c81-9a92-4f7e-be2c-8b26b56d3b98/\
             3b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c\n";
        let id = parse_cgroup(content).unwrap();
        assert_eq!(id.pod_uid, "ed1e9c81-9a92-4f7e-be2c-8b26b56d3b98");
        assert_eq!(
            id.container_id,
            "3b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c"
        );
    }

    #[test]
    fn parses_systemd_v2_with_underscored_uid_and_runtime_prefix() {
        let content = "0::/kubepods.slice/kubepods-burstable.slice/\
             kubepods-burstable-poded1e9c81_9a92_4f7e_be2c_8b26b56d3b98.slice/\
             cri-containerd-3b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c.scope\n";
        let id = parse_cgroup(content).unwrap();
        assert_eq!(id.pod_uid, "ed1e9c81-9a92-4f7e-be2c-8b26b56d3b98");
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
    fn parses_docker_and_crio_container_id_prefixes() {
        // `extract_container_id` also removes these runtime prefixes. Each
        // prefix must give the same bare hex id.
        for prefix in ["docker-", "crio-"] {
            let path = format!(
                "/kubepods.slice/kubepods-besteffort-podaaaa1111_bbbb.slice/\
                 {prefix}abcdef0123456789abcdef.scope"
            );
            let id = parse_cgroup_path(&path).unwrap();
            assert_eq!(id.pod_uid, "aaaa1111-bbbb", "{prefix}");
            assert_eq!(id.container_id, "abcdef0123456789abcdef", "{prefix}");
        }
    }

    #[test]
    fn non_kubernetes_cgroup_is_none() {
        assert!(parse_cgroup("0::/system.slice/sshd.service\n").is_none());
        assert!(parse_cgroup("0::/kubepods.slice\n").is_none());
    }

    const POD: &str = "poded1e9c81-9a92-4f7e-be2c-8b26b56d3b98";
    const CID: &str = "3b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c";

    #[test]
    fn nested_cgroup_below_the_container_keeps_the_container_id() {
        // systemd inside the container makes `init.scope` below the
        // container's own cgroup.
        let id = parse_cgroup_path(&format!("/kubepods/burstable/{POD}/{CID}/init.scope")).unwrap();
        assert_eq!(id.container_id, CID);
    }

    #[test]
    fn a_hex_segment_made_below_the_container_is_ignored() {
        // A container with a writable cgroupfs names its own sub-cgroup after
        // another container's id; the real id is still the one used.
        let other = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let id = parse_cgroup_path(&format!("/kubepods/burstable/{POD}/{CID}/x/{other}")).unwrap();
        assert_eq!(id.container_id, CID);
    }

    #[test]
    fn pod_cgroup_without_a_container_below_it_is_none() {
        assert!(parse_cgroup_path(&format!("/kubepods/burstable/{POD}")).is_none());
        assert!(parse_cgroup_path(&format!("/kubepods/burstable/{POD}/init.scope")).is_none());
    }

    #[test]
    fn non_hex_pod_uid_is_rejected() {
        for pod in ["podEVIL", "pod..", "pod", "podzz11-22"] {
            assert!(
                parse_cgroup_path(&format!("/kubepods/burstable/{pod}/{CID}")).is_none(),
                "{pod}"
            );
        }
    }

    #[test]
    fn is_pod_uid_accepts_uuids_only_in_lowercase_hex() {
        assert!(is_pod_uid("ed1e9c81-9a92-4f7e-be2c-8b26b56d3b98"));
        assert!(!is_pod_uid("ED1E9C81-9A92"));
        assert!(!is_pod_uid(""));
        assert!(!is_pod_uid(&"a".repeat(MAX_POD_UID_LEN + 1)));
    }
}
