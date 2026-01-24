use std::path::Path;

use anyhow::{Context, Result};

/// Delete the `download/` directory (and all its contents) under `out_dir`.
pub(crate) fn cleanup_download_dir(pack_dir: &Path, verbose: u8) -> Result<()> {
    let download_dir = pack_dir.join("download");

    if !download_dir.exists() {
        if verbose > 0 { eprintln!("[cleanup] nothing to remove at {}", download_dir.display()) }
        return Ok(());
    }

    if verbose > 0 { eprintln!("[cleanup] removing {}", download_dir.display()) }
    std::fs::remove_dir_all(&download_dir)
        .with_context(|| format!("failed to remove {}", download_dir.display()))
}
