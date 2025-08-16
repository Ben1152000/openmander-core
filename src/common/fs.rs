use std::{fs::{File, create_dir_all, remove_file}, io::Read, path::Path};

use anyhow::{anyhow, bail, Context, Result};
use zip::ZipArchive;
use sha2::{Digest, Sha256};

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

pub fn ensure_dirs(base: &Path, dirs: &[&str]) -> Result<()> {
    for &dir in dirs {
        ensure_dir_exists(&base.join(dir))?;
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

pub fn sha256_file(rel_path: &str, root: &Path) -> Result<(String, String)> {
    let full = root.join(rel_path);
    let mut file = File::open(&full)
        .with_context(|| format!("open for hash {}", full.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 1 << 16];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let hex = hex::encode(hasher.finalize());
    Ok((rel_path.to_string(), hex))
}
