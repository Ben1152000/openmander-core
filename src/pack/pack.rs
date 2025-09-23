use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::{common, map::Map, pack::{clean::cleanup_download_dir, download::download_data}};

/// Download data files for a state, build the map pack, and write it to a new directory in `path`.
/// Returns the path to the new pack directory.
pub fn build_pack(state_code: &str, path: &PathBuf, verbose: u8) -> Result<PathBuf> {
    let state_code = state_code.to_ascii_uppercase();
    common::require_dir_exists(&path)?;

    let pack_dir = path.join(format!("{state_code}_2020_pack"));
    common::ensure_dir_exists(&pack_dir)?;

    let download_dir = download_data(&state_code, &pack_dir, verbose)?;
    if verbose > 0 { eprintln!("Downloaded files for {} into {}", state_code, pack_dir.display()); }

    let fips = common::state_abbr_to_fips(&state_code)
        .with_context(|| format!("Unknown state/territory postal code: {state_code}"))?;

    let map = Map::build_pack(&download_dir, &state_code, fips, verbose)?;
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
pub fn download_pack(state_code: &str, path: &PathBuf, verbose: u8) -> Result<PathBuf> {
    let state_code = state_code.to_ascii_uppercase();
    common::require_dir_exists(&path)?;

    let pack_name = format!("{state_code}_2020_pack");
    let pack_url = format!("https://media.githubusercontent.com/media/Ben1152000/openmander-data/master/packs/{state_code}/{pack_name}.zip");
    if !common::remote_file_exists(&pack_url)? {
        if verbose > 0 { eprintln!("No prebuilt pack found for {state_code}, building locally..."); }
        return build_pack(&state_code, path, verbose)
    }

    let zip_path = path.join(format!("{pack_name}.zip"));
    let pack_dir = path.join(pack_name);

    if verbose > 0 { eprintln!("[download] downloading {pack_url}"); }
    common::download_big_file(pack_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[download] extracting {}", zip_path.display()); }
    common::extract_zip(&zip_path, &pack_dir, true)?;

    if verbose > 0 { eprintln!("Downloaded pack to {}", pack_dir.display()); }

    Ok(pack_dir)
}

/// Validate the contents of a map pack at `pack_path`.
#[allow(dead_code, unused_variables)]
pub fn validate_pack(pack_path: &PathBuf, verbose: u8) -> Result<()> { todo!()}
