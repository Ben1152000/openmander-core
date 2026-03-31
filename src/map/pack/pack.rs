use std::path::{Path, PathBuf};

use anyhow::Result;

#[cfg(feature = "download")]
use std::time::Duration;
#[cfg(feature = "download")]
use anyhow::{Context, anyhow};
#[cfg(feature = "download")]
use crate::map::{Map, util};

#[cfg(feature = "download")]
use super::download::{cleanup_download_dir, download_data, download_big_file};

/// Lightweight existence check for a remote file.
/// Returns Ok(true) if it exists, Ok(false) if it's 404/410, Err(_) otherwise.
#[cfg(feature = "download")]
pub(crate) fn remote_file_exists(url: &str) -> Result<bool> {
    use reqwest::{blocking::Client, redirect::Policy, StatusCode};

    let client = Client::builder()
        .user_agent("openmander/0.1 (+https://github.com/Ben1152000/openmander-core)")
        .redirect(Policy::limited(10))
        .timeout(Duration::from_secs(10))
        .build()?;

    // Try HEAD first
    if let Ok(resp) = client.head(url).send() {
        match resp.status() {
            StatusCode::OK => return Ok(true),
            StatusCode::NOT_FOUND | StatusCode::GONE => return Ok(false),
            // Some servers don’t like HEAD; fall through to range GET.
            _ => {}
        }
    }

    // Fallback: GET first byte only
    let resp = client
        .get(url)
        .header(reqwest::header::RANGE, "bytes=0-0")
        .send()?;

    match resp.status() {
        StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(true),
        StatusCode::NOT_FOUND | StatusCode::GONE => Ok(false),
        s => Err(anyhow!("unexpected status {} probing {}", s, url)),
    }
}

/// Download data files for a state, build the map pack, and write it to a new directory in `path`.
/// Returns the path to the new pack directory.
#[cfg(feature = "download")]
pub fn build_pack(state_code: &str, path: &Path, has_vtd: bool, verbose: u8) -> Result<PathBuf> {
    let state_code = state_code.to_ascii_uppercase();
    util::require_dir_exists(path)?;

    let pack_dir = path.join(format!("{state_code}_2020_pack"));
    util::ensure_dir_exists(&pack_dir)?;

    let download_dir = download_data(&state_code, &pack_dir, has_vtd, verbose)?;
    if verbose > 0 { eprintln!("Downloaded files for {} into {}", state_code, pack_dir.display()); }

    let fips = util::state_abbr_to_fips(&state_code)
        .with_context(|| format!("Unknown state/territory postal code: {state_code}"))?;

    let map = Map::build_pack(&download_dir, &state_code, fips, has_vtd, verbose)?;
    if verbose > 0 { eprintln!("Built pack for {state_code}"); }
    map.write_to_pack( &pack_dir)?;
    if verbose > 0 { eprintln!("Wrote pack to {}", pack_dir.display()); }

    cleanup_download_dir(&pack_dir, verbose)?;

    Ok(pack_dir)
}

/// Download the full map pack for a given state into `path`.
/// Falls back to building the pack locally if no prebuilt pack is available.
/// `include_geoms` controls whether geometries are included in the download.
/// Returns the path to the downloaded pack directory.
#[cfg(feature = "download")]
pub fn download_pack(state_code: &str, path: &Path, verbose: u8) -> Result<PathBuf> {
    let state_code = state_code.to_ascii_uppercase();
    util::require_dir_exists(path)?;

    let pack_name = format!("{state_code}_2020_pack");
    let pack_url = format!("https://media.githubusercontent.com/media/Ben1152000/openmander-data/master/packs/{state_code}/{pack_name}.zip");
    if !remote_file_exists(&pack_url)? {
        if verbose > 0 { eprintln!("No prebuilt pack found for {state_code}, building locally..."); }
        return build_pack(&state_code, path, true, verbose)
    }

    let zip_path = path.join(format!("{pack_name}.zip"));
    let pack_dir = path.join(pack_name);

    if verbose > 0 { eprintln!("[download] downloading {pack_url}"); }
    download_big_file(pack_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[download] extracting {}", zip_path.display()); }
    util::extract_zip(&zip_path, path, true)?;

    if verbose > 0 { eprintln!("Downloaded pack to {}", pack_dir.display()); }

    Ok(pack_dir)
}

/// Validate the contents of a map pack at `pack_path`.
#[allow(dead_code, unused_variables)]
pub fn validate_pack(pack_path: &Path, verbose: u8) -> Result<()> { todo!()}
