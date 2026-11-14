//! Host facts and files that the handler and the daemon both use.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// The UTS hostname: the node name for the handler, which the kernel runs in
/// the host namespaces. The daemon pod prefers `--node-name`. Reads
/// `/proc/sys/kernel/hostname`; falls back to `localhost`.
#[must_use]
pub fn node_hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

/// Create `dir` (and missing parents, with the umask mode) and set `dir`
/// itself to mode `0700`, also when it exists: `create_dir_all` does not
/// touch the mode of an existing dir.
///
/// # Errors
///
/// Fails when the directory cannot be created or its mode cannot be set.
pub(crate) fn ensure_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
}
