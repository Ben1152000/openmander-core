use std::path::PathBuf;

use anyhow::{Context, Result};
use openmander_map::Map;

use crate::{clean::cleanup_download_dir, common::*, download::download_data};

/// Download all data files for a state, build the map pack, and write it to `out_dir`.
pub fn build_pack(out_dir: &PathBuf, state_code: &str, verbose: u8) -> Result<()> {
    ensure_dir_exists(out_dir)?;

    let download_dir = &out_dir.join("download");
    ensure_dir_exists(&download_dir)?;
    download_data(download_dir, state_code, verbose)?;
    if verbose > 0 { eprintln!("Downloaded files for {} into {}", state_code, out_dir.display()); }

    let fips = state_abbr_to_fips(&state_code)
        .with_context(|| format!("Unknown state/territory postal code: {state_code}"))?;

    let map = Map::build_pack(download_dir, state_code, fips, verbose)?;
    if verbose > 0 { eprintln!("Built pack for {state_code}"); }

    map.write_to_pack( out_dir)?;
    if verbose > 0 { eprintln!("Wrote pack to {}", out_dir.display()); }

    cleanup_download_dir(out_dir, verbose)?;

    Ok(())
}

/// Download the full map pack for a given state into `out_dir`.
#[allow(dead_code, unused_variables)]
pub fn download_pack(out_dir: &PathBuf, state_code: &str, verbose: u8) -> Result<()> { todo!() }

/// Download the map pack without geometries for a given state into `out_dir`.
#[allow(dead_code, unused_variables)]
pub fn download_pack_without_geoms(out_dir: &PathBuf, state_code: &str, verbose: u8) -> Result<()> { todo!() }

/// Validate the contents of a map pack at `pack_path`.
#[allow(dead_code, unused_variables)]
pub fn validate_pack(pack_path: &PathBuf, verbose: u8) -> Result<()> { todo!()}
