//! GNU build-id capture: read the faulting executable's `NT_GNU_BUILD_ID` at
//! crash time so symbols can be resolved later against a symbol store /
//! debuginfod. Never blocks capture - any parse difficulty yields `None`.
//!
//! We parse only what the build-id needs: the ELF header, the program headers,
//! and the `PT_NOTE` segments. No section headers, no external ELF crate - a
//! crashed process's `/proc/<pid>/exe` is best-effort input, so a compact,
//! bounds-checked, dependency-free reader that fails soft is the right tool.

use std::io::Read;
use std::path::Path;

const NT_GNU_BUILD_ID: u32 = 3;
const PT_NOTE: u32 = 4;
const MAX_ELF_PREFIX: u64 = 2 * 1024 * 1024;

/// Read the GNU build-id from an executable path (e.g. `/proc/<pid>/exe`).
/// Returns `None` on any error - build-id is best-effort.
#[must_use]
pub fn build_id_from_path(path: &Path) -> Option<String> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) => {
            tracing::debug!(error = %e, path = %path.display(), "opening exe for build-id failed; omitted");
            return None;
        }
    };
    let mut buf = Vec::new();
    if let Err(e) = file.take(MAX_ELF_PREFIX).read_to_end(&mut buf) {
        tracing::debug!(error = %e, path = %path.display(), "reading exe for build-id failed; omitted");
        return None;
    }
    build_id_from_bytes(&buf)
}

/// Parse the GNU build-id out of an ELF image prefix, hex-encoded. `None` if
/// the bytes are not an ELF we can parse or carry no build-id note.
#[must_use]
pub fn build_id_from_bytes(elf: &[u8]) -> Option<String> {
    if elf.len() < 64 || &elf[0..4] != b"\x7fELF" {
        return None;
    }
    let is64 = match elf[4] {
        1 => false,
        2 => true,
        _ => return None,
    };
    let le = match elf[5] {
        1 => true,
        2 => false,
        _ => return None,
    };

    let (phoff, phentsize, phnum) = if is64 {
        (
            read_u64(elf, 0x20, le)?,
            u64::from(read_u16(elf, 0x36, le)?),
            u64::from(read_u16(elf, 0x38, le)?),
        )
    } else {
        (
            u64::from(read_u32(elf, 0x1C, le)?),
            u64::from(read_u16(elf, 0x2A, le)?),
            u64::from(read_u16(elf, 0x2C, le)?),
        )
    };

    for i in 0..phnum {
        let off = usize::try_from(phoff.checked_add(i.checked_mul(phentsize)?)?).ok()?;
        if read_u32(elf, off, le)? != PT_NOTE {
            continue;
        }
        let (p_offset, p_filesz) = if is64 {
            (
                usize::try_from(read_u64(elf, off + 8, le)?).ok()?,
                usize::try_from(read_u64(elf, off + 32, le)?).ok()?,
            )
        } else {
            (
                read_u32(elf, off + 4, le)? as usize,
                read_u32(elf, off + 16, le)? as usize,
            )
        };
        let notes = elf.get(p_offset..p_offset.checked_add(p_filesz)?)?;
        if let Some(id) = scan_notes(notes, le) {
            return Some(id);
        }
    }
    None
}

fn scan_notes(notes: &[u8], le: bool) -> Option<String> {
    let mut pos = 0usize;
    while pos + 12 <= notes.len() {
        let namesz = read_u32(notes, pos, le)? as usize;
        let descsz = read_u32(notes, pos + 4, le)? as usize;
        let ntype = read_u32(notes, pos + 8, le)?;

        let name_start = pos + 12;
        let name_end = name_start.checked_add(namesz)?;
        let desc_start = name_start + ((namesz + 3) & !3);
        let desc_end = desc_start.checked_add(descsz)?;
        if desc_end > notes.len() {
            break;
        }

        if ntype == NT_GNU_BUILD_ID && notes.get(name_start..name_end)?.starts_with(b"GNU") {
            return Some(hex(&notes[desc_start..desc_end]));
        }
        pos = desc_start + ((descsz + 3) & !3);
    }
    None
}

fn read_u16(b: &[u8], o: usize, le: bool) -> Option<u16> {
    let s: [u8; 2] = b.get(o..o + 2)?.try_into().ok()?;
    Some(if le {
        u16::from_le_bytes(s)
    } else {
        u16::from_be_bytes(s)
    })
}

fn read_u32(b: &[u8], o: usize, le: bool) -> Option<u32> {
    let s: [u8; 4] = b.get(o..o + 4)?.try_into().ok()?;
    Some(if le {
        u32::from_le_bytes(s)
    } else {
        u32::from_be_bytes(s)
    })
}

fn read_u64(b: &[u8], o: usize, le: bool) -> Option<u64> {
    let s: [u8; 8] = b.get(o..o + 8)?.try_into().ok()?;
    Some(if le {
        u64::from_le_bytes(s)
    } else {
        u64::from_be_bytes(s)
    })
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from(HEX[usize::from(b >> 4)]));
        s.push(char::from(HEX[usize::from(b & 0xf)]));
    }
    s
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn synthetic_elf_with_build_id(desc: &[u8]) -> Vec<u8> {
        let mut note = Vec::new();
        note.extend_from_slice(&4u32.to_le_bytes());
        note.extend_from_slice(&(desc.len() as u32).to_le_bytes());
        note.extend_from_slice(&NT_GNU_BUILD_ID.to_le_bytes());
        note.extend_from_slice(b"GNU\0");
        note.extend_from_slice(desc);
        while note.len() % 4 != 0 {
            note.push(0);
        }

        let ehdr_size = 64usize;
        let phdr_size = 56usize;
        let note_off = ehdr_size + phdr_size;

        let mut elf = vec![0u8; note_off];
        elf[0..4].copy_from_slice(b"\x7fELF");
        elf[4] = 2;
        elf[5] = 1;
        elf[6] = 1;
        elf[16..18].copy_from_slice(&2u16.to_le_bytes());
        elf[18..20].copy_from_slice(&62u16.to_le_bytes());
        elf[20..24].copy_from_slice(&1u32.to_le_bytes());
        elf[0x20..0x28].copy_from_slice(&(ehdr_size as u64).to_le_bytes());
        elf[52..54].copy_from_slice(&(ehdr_size as u16).to_le_bytes());
        elf[0x36..0x38].copy_from_slice(&(phdr_size as u16).to_le_bytes());
        elf[0x38..0x3A].copy_from_slice(&1u16.to_le_bytes());

        let p = ehdr_size;
        elf[p..p + 4].copy_from_slice(&PT_NOTE.to_le_bytes());
        elf[p + 4..p + 8].copy_from_slice(&4u32.to_le_bytes());
        elf[p + 8..p + 16].copy_from_slice(&(note_off as u64).to_le_bytes());
        elf[p + 32..p + 40].copy_from_slice(&(note.len() as u64).to_le_bytes());

        elf.extend_from_slice(&note);
        elf
    }

    #[test]
    fn extracts_build_id_from_a_synthetic_elf() {
        let desc = [0xde, 0xad, 0xbe, 0xef, 0x01, 0x23];
        let elf = synthetic_elf_with_build_id(&desc);
        assert_eq!(build_id_from_bytes(&elf).as_deref(), Some("deadbeef0123"));
    }

    #[test]
    fn returns_none_for_non_elf_input() {
        assert_eq!(build_id_from_bytes(b"not an elf at all, really"), None);
        assert_eq!(build_id_from_bytes(&[]), None);
    }

    #[test]
    fn returns_none_when_no_build_id_note_present() {
        let mut elf = synthetic_elf_with_build_id(&[1, 2, 3, 4]);
        elf.truncate(64 + 56);
        assert_eq!(build_id_from_bytes(&elf), None);
    }
}
