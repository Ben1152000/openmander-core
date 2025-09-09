use std::{fs::File, io::{Seek, Write}, path::{Path, PathBuf}};

use anyhow::{anyhow, bail, Context, Result};
use tempfile::NamedTempFile;
use zip::ZipArchive;

/// Create the directory if it doesn’t exist; error if a non-directory exists there.
pub fn ensure_dir_exists(path: &Path) -> Result<()> {
    if path.exists() {
        if !path.is_dir() { bail!("Path exists but is not a directory: {}", path.display()); }
    } else {
        std::fs::create_dir_all(path)
            .with_context(|| format!("Failed to create directory {}", path.display()))?;
    }
    Ok(())
}

/// Error unless the directory already exists.
pub fn _require_dir_exists(path: &Path) -> Result<()> {
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
        std::fs::remove_file(zip_path)
            .map_err(|e| anyhow!("failed to delete {:?}: {}", zip_path, e))?;
    }

    Ok(())
}

/// Two-letter postal code → full state name
pub fn state_abbr_to_name(state: &str) -> Option<&'static str> {
    match state {
        "AL" => Some("Alabama"),
        "AK" => Some("Alaska"),
        "AZ" => Some("Arizona"),
        "AR" => Some("Arkansas"),
        "CA" => Some("California"),
        "CO" => Some("Colorado"),
        "CT" => Some("Connecticut"),
        "DE" => Some("Delaware"),
        "FL" => Some("Florida"),
        "GA" => Some("Georgia"),
        "HI" => Some("Hawaii"),
        "ID" => Some("Idaho"),
        "IL" => Some("Illinois"),
        "IN" => Some("Indiana"),
        "IA" => Some("Iowa"),
        "KS" => Some("Kansas"),
        "KY" => Some("Kentucky"),
        "LA" => Some("Louisiana"),
        "ME" => Some("Maine"),
        "MD" => Some("Maryland"),
        "MA" => Some("Massachusetts"),
        "MI" => Some("Michigan"),
        "MN" => Some("Minnesota"),
        "MS" => Some("Mississippi"),
        "MO" => Some("Missouri"),
        "MT" => Some("Montana"),
        "NE" => Some("Nebraska"),
        "NV" => Some("Nevada"),
        "NH" => Some("New Hampshire"),
        "NJ" => Some("New Jersey"),
        "NM" => Some("New Mexico"),
        "NY" => Some("New York"),
        "NC" => Some("North Carolina"),
        "ND" => Some("North Dakota"),
        "OH" => Some("Ohio"),
        "OK" => Some("Oklahoma"),
        "OR" => Some("Oregon"),
        "PA" => Some("Pennsylvania"),
        "RI" => Some("Rhode Island"),
        "SC" => Some("South Carolina"),
        "SD" => Some("South Dakota"),
        "TN" => Some("Tennessee"),
        "TX" => Some("Texas"),
        "UT" => Some("Utah"),
        "VT" => Some("Vermont"),
        "VA" => Some("Virginia"),
        "WA" => Some("Washington"),
        "WV" => Some("West Virginia"),
        "WI" => Some("Wisconsin"),
        "WY" => Some("Wyoming"),
        "DC" => Some("District of Columbia"),
        "PR" => Some("Puerto Rico"),
        _ => None,
    }
}

/// Two-letter postal code → FIPS code
pub fn state_abbr_to_fips(state: &str) -> Option<&'static str> {
    match state {
        "AL" => Some("01"),
        "AK" => Some("02"),
        "AZ" => Some("04"),
        "AR" => Some("05"),
        "CA" => Some("06"),
        "CO" => Some("08"),
        "CT" => Some("09"),
        "DE" => Some("10"),
        "DC" => Some("11"),
        "FL" => Some("12"),
        "GA" => Some("13"),
        "HI" => Some("15"),
        "ID" => Some("16"),
        "IL" => Some("17"),
        "IN" => Some("18"),
        "IA" => Some("19"),
        "KS" => Some("20"),
        "KY" => Some("21"),
        "LA" => Some("22"),
        "ME" => Some("23"),
        "MD" => Some("24"),
        "MA" => Some("25"),
        "MI" => Some("26"),
        "MN" => Some("27"),
        "MS" => Some("28"),
        "MO" => Some("29"),
        "MT" => Some("30"),
        "NE" => Some("31"),
        "NV" => Some("32"),
        "NH" => Some("33"),
        "NJ" => Some("34"),
        "NM" => Some("35"),
        "NY" => Some("36"),
        "NC" => Some("37"),
        "ND" => Some("38"),
        "OH" => Some("39"),
        "OK" => Some("40"),
        "OR" => Some("41"),
        "PA" => Some("42"),
        "RI" => Some("44"),
        "SC" => Some("45"),
        "SD" => Some("46"),
        "TN" => Some("47"),
        "TX" => Some("48"),
        "UT" => Some("49"),
        "VT" => Some("50"),
        "VA" => Some("51"),
        "WA" => Some("53"),
        "WV" => Some("54"),
        "WI" => Some("55"),
        "WY" => Some("56"),
        "PR" => Some("72"),
        _ => None,
    }
}

/// Write-then-rename wrapper for atomic big-file outputs
struct PendingWrite {
    target: PathBuf,
    tmp: Option<(NamedTempFile, bool)>, // (file, need_fsync_dir)
}

fn open_for_big_write(target: &Path, force: bool) -> Result<PendingWrite> {
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create dir {}", parent.display()))?;
    }
    if !force && target.exists() {
        bail!("Refusing to overwrite existing file: {} (use --force)", target.display());
    }
    let need_fsync_dir = target.parent().is_some();
    let tmp = NamedTempFile::new_in(target.parent().unwrap_or(Path::new(".")))
        .context("create temp file")?;

    Ok(PendingWrite { target: target.to_path_buf(), tmp: Some((tmp, need_fsync_dir)) })
}

impl Write for PendingWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.tmp.as_mut().unwrap().0.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.tmp.as_mut().unwrap().0.flush()
    }
}

impl Seek for PendingWrite {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.tmp.as_mut().unwrap().0.as_file_mut().seek(pos)
    }
}

fn finalize_big_write(mut pending: PendingWrite) -> Result<()> {
    let (tmp, need_fsync_dir) = pending.tmp.take().expect("not finalized");
    tmp.as_file().sync_all().ok(); // best-effort fsync file
    tmp.persist(&pending.target)
        .with_context(|| format!("rename to {}", pending.target.display()))?;
    if need_fsync_dir {
        if let Some(dir) = pending.target.parent() {
            let _ = File::open(dir).and_then(|f| f.sync_all());
        }
    }
    Ok(())
}

/// Download a large file from `file_url` to `out_path`.
pub fn download_big_file(file_url: String, out_path: &PathBuf, force: bool) -> Result<()> {
    // Safe big-file write (tempfile -> atomic rename), no accidental overwrite unless --force
    let mut sink = open_for_big_write(&out_path, force)?;

    let mut resp = reqwest::blocking::get(&file_url)
        .with_context(|| format!("GET {file_url}"))?
        .error_for_status()
        .with_context(|| format!("GET {file_url} returned error status"))?;

    std::io::copy(&mut resp, &mut sink).with_context(|| format!("write {}", out_path.display()))?;

    finalize_big_write(sink)?;
    Ok(())
}
