use std::{fs::File, io::{Seek, Write}, path::{Path, PathBuf}};

use anyhow::{Context, Result, ensure};
use tempfile::NamedTempFile;

use crate::common;

/// Write-then-rename wrapper for atomic big-file outputs
struct PendingWrite {
    target: PathBuf,
    tmp: Option<(NamedTempFile, bool)>, // (file, need_fsync_dir)
}

impl PendingWrite {
    /// Open a file for a big write.
    fn open(target: &Path, force: bool) -> Result<Self> {
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create dir {}", parent.display()))?;
        }
        ensure!(force || !target.exists(), "Refusing to overwrite existing file: {} (use --force)", target.display());
        let need_fsync_dir = target.parent().is_some();
        let tmp = NamedTempFile::new_in(target.parent().unwrap_or(Path::new(".")))
            .context("create temp file")?;

        Ok(Self { target: target.to_path_buf(), tmp: Some((tmp, need_fsync_dir)) })
    }

    /// Finalize the big write.
    fn finalize(&mut self) -> Result<()> {
        let (tmp, need_fsync_dir) = self.tmp.take().expect("not finalized");
        tmp.as_file().sync_all().ok(); // best-effort fsync file
        tmp.persist(&self.target)
            .with_context(|| format!("rename to {}", self.target.display()))?;
        if need_fsync_dir {
            if let Some(dir) = self.target.parent() {
                File::open(dir).and_then(|f| f.sync_all())?;
            }
        }
        Ok(())
    }
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

/// Download a large file from `file_url` to `out_path`.
pub(crate) fn download_big_file(file_url: String, out_path: &PathBuf, force: bool) -> Result<()> {
    // Safe big-file write (tempfile -> atomic rename), no accidental overwrite unless --force
    let mut sink = PendingWrite::open(&out_path, force)?;

    let mut resp = reqwest::blocking::get(&file_url)
        .with_context(|| format!("GET {file_url}"))?
        .error_for_status()
        .with_context(|| format!("GET {file_url} returned error status"))?;

    std::io::copy(&mut resp, &mut sink).with_context(|| format!("write {}", out_path.display()))?;

    sink.finalize()?;
    Ok(())
}

/// Delete the `download/` directory (and all its contents) under `pack_dir`.
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

/// Download demographic data from Dave's redistricting
fn download_daves_demographics(out_dir: &PathBuf, state: &str, verbose: u8) -> Result<()> {
    let file_url = format!("https://data.dra2020.net/file/dra-block-data/Demographic_Data_Block_{state}.v06.zip");
    let zip_path = out_dir.join(format!("Demographic_Data_Block_{state}.v06.zip"));
    let out_path = out_dir.join(format!("Demographic_Data_Block_{state}"));

    if verbose > 0 { eprintln!("[download] downloading {file_url}"); }
    download_big_file(file_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[download] extracting {}", zip_path.display()); }
    common::extract_zip(&zip_path, &out_path, true)?;

    Ok(())
}

/// Download election data from Dave's redistricting
fn download_daves_elections(out_dir: &PathBuf, state: &str, verbose: u8) -> Result<()> {
    let file_url = format!("https://data.dra2020.net/file/dra-block-data/Election_Data_Block_{state}.v06.zip");
    let zip_path = out_dir.join(format!("Election_Data_Block_{state}.v06.zip"));
    let out_path = out_dir.join(format!("Election_Data_Block_{state}"));

    if verbose > 0 { eprintln!("[download] downloading {file_url}"); }
    download_big_file(file_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[download] extracting {}", zip_path.display()); }
    common::extract_zip(&zip_path, &out_path, true)?;

    Ok(())
}

/// Download geometry data from US Census TIGER 2020 PL directory
/// Example URL: "NE" -> "https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/31_NEBRASKA/31/"
fn download_tiger_geometries(out_dir: &PathBuf, state: &str, has_vtd: bool, verbose: u8) -> Result<()> {
    let fips = common::state_abbr_to_fips(&state)
        .with_context(|| format!("Unknown state/territory postal code: {state}"))?;
    let name = common::state_abbr_to_name(&state)
        .with_context(|| format!("Unknown state/territory postal code: {state}"))?
        .to_ascii_uppercase().replace(' ', "_");

    let base = format!("https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/{fips}_{name}/{fips}/");

    // Filenames we need for TIGER 2020 (state/county/tract/bg/vtd/block)
    let files = ["state20", "county20", "tract20", "bg20", "vtd20", "tabblock20"];

    for name in files {
        // Skip if the vtd data isn't available (CA, ME, OR, WY)
        if !has_vtd && name == "vtd20" { continue }

        let file_url = format!("{base}tl_2020_{fips}_{name}.zip");
        let zip_path = out_dir.join(format!("tl_2020_{fips}_{name}.zip"));
        let out_path = out_dir.join(format!("tl_2020_{fips}_{name}"));

        if verbose > 0 { eprintln!("[download] downloading {file_url}"); }
        download_big_file(file_url, &zip_path, true)?;

        if verbose > 0 { eprintln!("[download] extracting {}", zip_path.display()); }
        common::extract_zip(&zip_path, &out_path, true)?;
    }

    Ok(())
}

/// Download block-level crosswalks from the US Census website
/// Example URL: "NE" -> "https://www2.census.gov/geo/docs/maps-data/data/baf2020/BlockAssign_ST31_NE.zip"
fn download_census_crosswalks(out_dir: &PathBuf, state: &str, verbose: u8) -> Result<()> {
    let fips = common::state_abbr_to_fips(&state)
        .with_context(|| format!("Unknown state/territory postal code: {state}"))?;

    let file_url = format!("https://www2.census.gov/geo/docs/maps-data/data/baf2020/BlockAssign_ST{fips}_{state}.zip");

    let zip_path = out_dir.join(format!("BlockAssign_ST{fips}_{state}.zip"));
    let out_path = out_dir.join(format!("BlockAssign_ST{fips}_{state}"));

    if verbose > 0 { eprintln!("[download] downloading {file_url}"); }
    download_big_file(file_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[download] extracting {}", zip_path.display()); }
    common::extract_zip(&zip_path, &out_path, true)?;

    Ok(())
}

/// Download all map files for the given state into the `download/` directory under `pack_dir`.
/// Returns the path to the `download/` directory.
pub(crate) fn download_data(state: &str, pack_dir: &PathBuf, has_vtd: bool, verbose: u8) -> Result<PathBuf> {
    common::require_dir_exists(&pack_dir)?;

    let download_dir = pack_dir.join("download");
    common::ensure_dir_exists(&download_dir)?;

    if verbose > 0 { eprintln!("[download] state={state} -> dir {}", download_dir.display()); }

    download_tiger_geometries(&download_dir, state, has_vtd, verbose)?;
    download_daves_demographics(&download_dir, state, verbose)?;
    download_daves_elections(&download_dir, state, verbose)?;
    download_census_crosswalks(&download_dir, state, verbose)?;

    Ok(download_dir)
}
