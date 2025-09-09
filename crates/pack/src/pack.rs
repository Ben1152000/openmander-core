use std::path::PathBuf;

use anyhow::{Context, Result};
use openmander_map::Map;

use crate::{clean::cleanup_download_dir, common::*, download::download_data};

/// Download all data files for a state, build the state pack, and write it to `out_dir`.
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

/// Download the full state pack for a given state into `out_dir`.
pub fn download_pack(out_dir: &PathBuf, state_code: &str, verbose: u8) -> Result<()> { todo!() }

/// Download the state pack without geometries for a given state into `out_dir`.
pub fn download_pack_without_geoms(out_dir: &PathBuf, state_code: &str, verbose: u8) -> Result<()> { todo!() }
