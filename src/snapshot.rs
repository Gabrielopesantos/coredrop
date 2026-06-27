//! The pre-reap `/proc/<hostpid>` snapshot - the irreplaceable, time-critical
//! capture work.
//!
//! The kernel holds the faulting process while the `core_pattern` handler runs
//! (with `core_pipe_limit` raised, see [`crate::core_pattern`]). The handler
//! must snapshot `/proc/<hostpid>` before that window closes and the PID is
//! reaped, because none of it is reconstructable afterwards. We grab the
//! forensic files (`maps`, `smaps`, `status`, `fd`, `limits`, `environ`,
//! `cmdline`, `stack`, `exe`), redact `environ`, and read the executable's
//! build-id.
//!
//! Everything here is bounded and buffered in memory - the snapshot is
//! small, and only the multi-GB core ever streams. The whole bundle renders to
//! an in-memory tar for upload to the object store.
//!
//! The proc root is injectable so the capture logic is testable against a
//! fixture tree instead of a live `/proc`.

use std::io::Read;
use std::path::Path;

use crate::buildid::build_id_from_path;
use crate::redact::Redactor;

// Cap snapshot files at 8 MBs
const MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;

// Which files to include in the snapshot
const SIMPLE_FILES: &[&str] = &["maps", "smaps", "status", "limits", "cmdline", "stack"];

/// One captured `/proc` file, named by its basename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotFile {
    pub name: String,
    pub bytes: Vec<u8>,
    /// The read hit the cap, so `bytes` is a partial prefix.
    pub truncated: bool,
}

/// The in-memory pre-reap snapshot of one faulting process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcSnapshot {
    pub host_pid: i32,
    pub build_id: Option<String>,
    pub files: Vec<SnapshotFile>,
}

impl ProcSnapshot {
    /// Snapshot `/proc/<pid>` under `proc_root`. Best-effort per file: an
    /// unreadable file is skipped, not fatal.
    pub fn capture(proc_root: &Path, pid: i32, redactor: &Redactor) -> Self {
        let base = proc_root.join(pid.to_string());
        let mut files = Vec::new();

        for name in SIMPLE_FILES {
            if let Some((bytes, truncated)) = read_capped(&base.join(name)) {
                files.push(SnapshotFile {
                    name: (*name).to_string(),
                    bytes,
                    truncated,
                });
            }
        }

        if let Some((raw, truncated)) = read_capped(&base.join("environ")) {
            files.push(SnapshotFile {
                name: "environ".to_string(),
                bytes: redactor.redact_environ(&raw),
                truncated,
            });
        }

        if let Some(listing) = read_fd_listing(&base.join("fd")) {
            files.push(SnapshotFile {
                name: "fd".to_string(),
                bytes: listing.into_bytes(),
                truncated: false,
            });
        }

        let exe_target = std::fs::read_link(base.join("exe")).ok();
        if let Some(target) = &exe_target {
            files.push(SnapshotFile {
                name: "exe".to_string(),
                bytes: target.to_string_lossy().into_owned().into_bytes(),
                truncated: false,
            });
        }
        let build_id = exe_target.as_deref().and_then(build_id_from_path);

        Self {
            host_pid: pid,
            build_id,
            files,
        }
    }

    /// Render the snapshot to an in-memory tar for object-store upload. Any
    /// file the cap truncated is named in a `TRUNCATED` manifest entry.
    pub fn to_tar(&self) -> std::io::Result<Vec<u8>> {
        let mut builder = tar::Builder::new(Vec::new());
        for f in &self.files {
            append_tar(&mut builder, &f.name, &f.bytes)?;
        }
        let truncated: Vec<&str> = self
            .files
            .iter()
            .filter(|f| f.truncated)
            .map(|f| f.name.as_str())
            .collect();
        if !truncated.is_empty() {
            append_tar(&mut builder, "TRUNCATED", truncated.join("\n").as_bytes())?;
        }
        builder.into_inner()
    }
}

fn append_tar(
    builder: &mut tar::Builder<Vec<u8>>,
    name: &str,
    bytes: &[u8],
) -> std::io::Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o600);
    header.set_mtime(0);
    header.set_cksum();
    builder.append_data(&mut header, name, bytes)
}

fn read_capped(path: &Path) -> Option<(Vec<u8>, bool)> {
    let file = std::fs::File::open(path).ok()?;
    let mut buf = Vec::new();
    // Read one byte past the cap: a file of exactly MAX_FILE_BYTES is then
    // distinguishable from a longer one (buf.len() > cap means content continued).
    file.take(MAX_FILE_BYTES + 1).read_to_end(&mut buf).ok()?;
    let truncated = buf.len() as u64 > MAX_FILE_BYTES;
    if truncated {
        buf.truncate(MAX_FILE_BYTES as usize);
    }
    Some((buf, truncated))
}

fn read_fd_listing(fd_dir: &Path) -> Option<String> {
    let mut entries: Vec<(u64, String)> = Vec::new();
    for dirent in std::fs::read_dir(fd_dir).ok()? {
        let Ok(dirent) = dirent else { continue };
        let name = dirent.file_name().to_string_lossy().into_owned();
        let num: u64 = name.parse().unwrap_or(u64::MAX);
        let target = std::fs::read_link(dirent.path())
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "<unreadable>".to_string());
        entries.push((num, format!("{name} -> {target}")));
    }
    entries.sort_by_key(|(n, _)| *n);
    Some(
        entries
            .into_iter()
            .map(|(_, line)| line)
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buildid::tests::synthetic_elf_with_build_id;
    use std::collections::BTreeMap;

    fn fixture(pid: i32, files: &[(&str, &[u8])], build_id: &[u8]) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("coredrop-proc-{}-{nanos}", std::process::id()));
        let proc_pid = root.join(pid.to_string());
        std::fs::create_dir_all(&proc_pid).unwrap();

        for (name, bytes) in files {
            std::fs::write(proc_pid.join(name), bytes).unwrap();
        }

        let elf_path = root.join("the-binary");
        std::fs::write(&elf_path, synthetic_elf_with_build_id(build_id)).unwrap();
        std::os::unix::fs::symlink(&elf_path, proc_pid.join("exe")).unwrap();

        let fd_dir = proc_pid.join("fd");
        std::fs::create_dir_all(&fd_dir).unwrap();
        std::os::unix::fs::symlink(&elf_path, fd_dir.join("3")).unwrap();

        root
    }

    fn by_name(snap: &ProcSnapshot) -> BTreeMap<&str, &[u8]> {
        snap.files
            .iter()
            .map(|f| (f.name.as_str(), f.bytes.as_slice()))
            .collect()
    }

    #[test]
    fn captures_proc_files_redacts_environ_and_reads_build_id() {
        let root = fixture(
            4242,
            &[
                ("maps", b"00400000-0040b000 r-xp 00000000 fd:00 12 /bin/app"),
                ("status", b"Name:\tapp\nState:\tZ (zombie)\n"),
                ("cmdline", b"app\0--flag\0"),
                ("environ", b"DB_PASSWORD=hunter2\0LANG=en_US.UTF-8\0"),
            ],
            &[0xab, 0xcd, 0xef],
        );

        let snap = ProcSnapshot::capture(&root, 4242, &Redactor::default());
        let files = by_name(&snap);

        assert_eq!(
            files.get("maps").unwrap(),
            b"00400000-0040b000 r-xp 00000000 fd:00 12 /bin/app"
        );
        assert_eq!(
            files.get("environ").unwrap(),
            b"DB_PASSWORD=<redacted>\0LANG=en_US.UTF-8\0"
        );
        let fd = std::str::from_utf8(files.get("fd").unwrap()).unwrap();
        assert!(fd.starts_with("3 -> "), "fd listing: {fd}");
        assert!(files.contains_key("exe"));
        assert_eq!(snap.build_id.as_deref(), Some("abcdef"));

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn skips_absent_files_without_failing() {
        let root = fixture(7, &[("maps", b"x")], &[0x01]);
        let snap = ProcSnapshot::capture(&root, 7, &Redactor::default());
        let names: Vec<&str> = snap.files.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"maps"));
        assert!(!names.contains(&"smaps"));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn caps_large_file_and_flags_truncation() {
        let big = vec![b'x'; MAX_FILE_BYTES as usize + 100];
        let root = fixture(11, &[("smaps", &big)], &[0x03]);
        let snap = ProcSnapshot::capture(&root, 11, &Redactor::default());

        let smaps = snap.files.iter().find(|f| f.name == "smaps").unwrap();
        assert_eq!(smaps.bytes.len() as u64, MAX_FILE_BYTES);
        assert!(smaps.truncated, "over-cap file must be flagged truncated");

        let tar_bytes = snap.to_tar().unwrap();
        let mut archive = tar::Archive::new(tar_bytes.as_slice());
        let mut manifest = None;
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            if entry.path().unwrap().to_string_lossy() == "TRUNCATED" {
                let mut s = String::new();
                entry.read_to_string(&mut s).unwrap();
                manifest = Some(s);
            }
        }
        assert_eq!(manifest.as_deref(), Some("smaps"));

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn renders_a_readable_tar_bundle() {
        let root = fixture(9, &[("maps", b"map-bytes"), ("environ", b"A=1\0")], &[0x02]);
        let snap = ProcSnapshot::capture(&root, 9, &Redactor::default());
        let tar_bytes = snap.to_tar().unwrap();

        let mut archive = tar::Archive::new(tar_bytes.as_slice());
        let mut seen: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().into_owned();
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).unwrap();
            seen.insert(path, bytes);
        }
        assert_eq!(
            seen.get("maps").map(|v| v.as_slice()),
            Some(&b"map-bytes"[..])
        );
        assert_eq!(seen.len(), snap.files.len());

        std::fs::remove_dir_all(&root).ok();
    }
}
