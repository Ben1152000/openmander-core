use std::{fs::{File, create_dir_all, remove_file}, path::Path};

use anyhow::{anyhow, bail, Context, Result};
use zip::ZipArchive;

/// Create the directory if it doesn’t exist; error if a non-directory exists there.
pub fn ensure_dir_exists(path: &Path) -> Result<()> {
    if path.exists() {
        if !path.is_dir() { bail!("Path exists but is not a directory: {}", path.display()); }
    } else {
        create_dir_all(path)
            .with_context(|| format!("Failed to create directory {}", path.display()))?;
    }
    Ok(())
}

/// Error unless the directory already exists.
pub fn require_dir_exists(path: &Path) -> Result<()> {
    if !path.exists() { bail!("Directory does not exist: {}", path.display()); }
    if !path.is_dir() { bail!("Path exists but is not a directory: {}", path.display()); }
    Ok(())
}

/// Extracts the given `.zip` file to the target directory.
/// If `delete_after` is `true`, removes the `.zip` file after a successful extraction.
pub fn extract_zip(zip_path: &Path, dest_dir: &Path, delete_after: bool) -> anyhow::Result<()> {
    let file = File::open(zip_path)
        .map_err(|e| anyhow!("failed to open {:?}: {}", zip_path, e))?;
    let mut archive = ZipArchive::new(file)
        .map_err(|e| anyhow!("failed to read zip archive {:?}: {}", zip_path, e))?;

    archive
        .extract(dest_dir)
        .map_err(|e| anyhow!("failed to extract {:?} to {:?}: {}", zip_path, dest_dir, e))?;

    if delete_after {
        remove_file(zip_path)
            .map_err(|e| anyhow!("failed to delete {:?}: {}", zip_path, e))?;
    }

    Ok(())
}
