//! The systemd-coredump chaining backend (coexistence mode).
//!
//! On nodes that already run systemd-coredump, coredrop stays the
//! `core_pattern` pipe target - so it can still take the irreplaceable
//! pre-reap `/proc` snapshot - then forwards the core stream on to
//! systemd-coredump, which keeps owning rotation/compression/size limits per
//! the node's config. The snapshot path is identical to the standalone backend;
//! only the core's destination differs.
//!
//! Because systemd-coredump owns core storage, this backend stores nothing in
//! our object store: [`CoreStats::stored_bytes`] is `0` and `sha256` is `None`,
//! so the manifest carries `core.present: false`.

use std::process::Stdio;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::Command;
use tracing::warn;

use crate::backend::{CaptureBackend, CoreStats};

const FORWARD_CHUNK: usize = 256 * 1024;

pub const DEFAULT_SYSTEMD_COREDUMP_PATH: &str = "/usr/lib/systemd/systemd-coredump";

/// The systemd-coredump chaining backend: spawn the node's systemd-coredump
/// and pipe the core to its stdin.
pub struct SystemdCoredumpBackend {
    program: String,
    args: Vec<String>,
}

impl SystemdCoredumpBackend {
    pub fn new(program: String, args: Vec<String>) -> Self {
        Self { program, args }
    }
}

#[async_trait]
impl CaptureBackend for SystemdCoredumpBackend {
    async fn drain_core(&self, reader: &mut (dyn AsyncRead + Unpin + Send)) -> Result<CoreStats> {
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("spawning systemd-coredump at {}", self.program))?;

        let mut stdin = child
            .stdin
            .take()
            .context("systemd-coredump child stdin missing")?;
        let (bytes, mut truncated) = forward(reader, &mut stdin).await;
        drop(stdin);

        match child.wait().await {
            Ok(status) if status.success() => {}
            Ok(status) => {
                warn!(%status, "systemd-coredump exited non-zero - marking core truncated");
                truncated = true;
            }
            Err(e) => {
                warn!(error = %e, "waiting on systemd-coredump failed - marking core truncated");
                truncated = true;
            }
        }

        Ok(CoreStats {
            bytes,
            stored_bytes: 0,
            sha256: None,
            truncated,
            truncated_reason: truncated.then(|| "forward_failed".to_string()),
        })
    }
}

async fn forward<R, W>(reader: &mut R, sink: &mut W) -> (u64, bool)
where
    R: AsyncRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    let mut buf = vec![0u8; FORWARD_CHUNK];
    let mut bytes = 0u64;
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                bytes += n as u64;
                if let Err(e) = sink.write_all(&buf[..n]).await {
                    warn!(error = %e, bytes, "writing core to systemd-coredump failed - marking truncated");
                    return (bytes, true);
                }
            }
            Err(e) => {
                warn!(error = %e, bytes, "core stream read error - marking truncated");
                return (bytes, true);
            }
        }
    }
    if let Err(e) = sink.flush().await {
        warn!(error = %e, bytes, "flushing core to systemd-coredump failed - marking truncated");
        return (bytes, true);
    }
    (bytes, false)
}

/// Build systemd-coredump's canonical `%P %u %g %s %t %c %h` positional
/// argument vector.
pub fn build_forward_args(
    host_pid: i32,
    uid: &str,
    gid: &str,
    signal: i32,
    timestamp: i64,
    core_limit: &str,
    hostname: &str,
) -> Vec<String> {
    vec![
        host_pid.to_string(),
        uid.to_string(),
        gid.to_string(),
        signal.to_string(),
        timestamp.to_string(),
        core_limit.to_string(),
        hostname.to_string(),
    ]
}

/// Read the first whitespace-delimited field of a `/proc/<pid>/status` line
/// whose label is `key` (e.g. `Uid:` / `Gid:`).
pub fn status_first_field(status: &[u8], key: &str) -> Option<String> {
    let text = String::from_utf8_lossy(status);
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            return rest.split_whitespace().next().map(|s| s.to_string());
        }
    }
    None
}

/// Recover systemd-coredump's `%c` (core-size soft limit) from a
/// `/proc/<pid>/limits` snapshot.
pub fn parse_core_limit(limits: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(limits);
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("Max core file size") {
            let soft = rest.split_whitespace().next()?;
            return Some(if soft.eq_ignore_ascii_case("unlimited") {
                u64::MAX.to_string()
            } else {
                soft.to_string()
            });
        }
    }
    None
}

/// The node hostname for systemd-coredump's `%h`. Reads
/// `/proc/sys/kernel/hostname`; falls back to `localhost`.
pub fn node_hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read as _;

    #[test]
    fn builds_systemd_coredumps_canonical_arg_vector() {
        let args = build_forward_args(4242, "1000", "1000", 11, 1_749_600_000, "0", "node-a");
        assert_eq!(
            args,
            vec!["4242", "1000", "1000", "11", "1749600000", "0", "node-a"]
        );
    }

    #[test]
    fn reads_real_uid_and_gid_from_status() {
        let status = b"Name:\tapp\nUid:\t1000\t1000\t1000\t1000\nGid:\t2000\t2000\t2000\t2000\n";
        assert_eq!(status_first_field(status, "Uid:").as_deref(), Some("1000"));
        assert_eq!(status_first_field(status, "Gid:").as_deref(), Some("2000"));
        assert_eq!(status_first_field(status, "Pid:"), None);
    }

    #[test]
    fn parses_core_limit_numeric_and_unlimited() {
        let limits = b"Limit                     Soft Limit           Hard Limit           Units\n\
                       Max cpu time              unlimited            unlimited            seconds\n\
                       Max core file size        0                    unlimited            bytes\n";
        assert_eq!(parse_core_limit(limits).as_deref(), Some("0"));

        let unlimited =
            b"Max core file size        unlimited            unlimited            bytes\n";
        assert_eq!(
            parse_core_limit(unlimited).as_deref(),
            Some(u64::MAX.to_string().as_str())
        );

        assert_eq!(parse_core_limit(b"Max open files 1024 4096 files"), None);
    }

    #[tokio::test]
    async fn forwards_the_core_to_a_child_process_stdin() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let out = std::env::temp_dir().join(format!("coredrop-systemd-{nanos}"));
        let core = vec![0x7Fu8; 300_000];

        let backend = SystemdCoredumpBackend::new(
            "sh".to_string(),
            vec!["-c".to_string(), format!("cat > {}", out.display())],
        );
        let mut reader: &[u8] = &core;
        let stats = backend.drain_core(&mut reader).await.unwrap();

        assert!(!stats.truncated);
        assert_eq!(stats.bytes, core.len() as u64);
        assert_eq!(stats.stored_bytes, 0);
        assert_eq!(stats.sha256, None);

        let mut written = Vec::new();
        std::fs::File::open(&out)
            .unwrap()
            .read_to_end(&mut written)
            .unwrap();
        assert_eq!(written, core);

        std::fs::remove_file(&out).ok();
    }

    #[tokio::test]
    async fn marks_truncated_when_the_child_fails() {
        let backend = SystemdCoredumpBackend::new(
            "sh".to_string(),
            vec!["-c".to_string(), "exit 3".to_string()],
        );
        let core = vec![0u8; 10_000];
        let mut reader: &[u8] = &core;
        let stats = backend.drain_core(&mut reader).await.unwrap();
        assert!(stats.truncated);
        assert_eq!(stats.stored_bytes, 0);
    }
}
