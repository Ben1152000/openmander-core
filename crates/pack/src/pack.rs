use std::path::PathBuf;

use anyhow::{Context, Result};
use openmander_map::Map;

use crate::{clean::cleanup_download_dir, common::*, download::download_data};

/// Download data files for a state, build the map pack, and write it to a new directory in `path`.
/// Returns the path to the new pack directory.
pub fn build_pack(state_code: &str, path: &PathBuf, verbose: u8) -> Result<PathBuf> {
    let state_code = state_code.to_ascii_uppercase();

    require_dir_exists(&path)?;

    let pack_dir = path.join(format!("{state_code}_2020_pack"));
    ensure_dir_exists(&pack_dir)?;

    let download_dir = download_data(&state_code, &pack_dir, verbose)?;
    if verbose > 0 { eprintln!("Downloaded files for {} into {}", state_code, pack_dir.display()); }

    let fips = state_abbr_to_fips(&state_code)
        .with_context(|| format!("Unknown state/territory postal code: {state_code}"))?;

    let map = Map::build_pack(&download_dir, &state_code, fips, verbose)?;
    if verbose > 0 { eprintln!("Built pack for {state_code}"); }

    map.write_to_pack( &pack_dir)?;
    if verbose > 0 { eprintln!("Wrote pack to {}", pack_dir.display()); }

    cleanup_download_dir(&pack_dir, verbose)?;

    Ok(pack_dir)
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
