use std::path::{PathBuf};

use anyhow::{Context, Result};
use openmander_common as common;

/// Download demographic data from Dave's redistricting
fn download_daves_demographics(out_dir: &PathBuf, state: &str, verbose: u8) -> Result<()> {
    let file_url = format!("https://data.dra2020.net/file/dra-block-data/Demographic_Data_Block_{state}.v06.zip");
    let zip_path = out_dir.join(format!("Demographic_Data_Block_{state}.v06.zip"));
    let out_path = out_dir.join(format!("Demographic_Data_Block_{state}"));

    if verbose > 0 { eprintln!("[download] downloading {file_url}"); }
    common::download_big_file(file_url, &zip_path, true)?;

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
    common::download_big_file(file_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[download] extracting {}", zip_path.display()); }
    common::extract_zip(&zip_path, &out_path, true)?;

    Ok(())
}

/// Download geometry data from US Census TIGER 2020 PL directory
/// Example URL: "NE" -> "https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/31_NEBRASKA/31/"
fn download_tiger_geometries(out_dir: &PathBuf, state: &str, verbose: u8) -> Result<()> {
    let fips = common::state_abbr_to_fips(&state)
        .with_context(|| format!("Unknown state/territory postal code: {state}"))?;
    let name = common::state_abbr_to_name(&state)
        .with_context(|| format!("Unknown state/territory postal code: {state}"))?
        .to_ascii_uppercase().replace(' ', "_");

    let base = format!("https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/{fips}_{name}/{fips}/");

    // Filenames we need for TIGER 2020 (state/county/tract/bg/vtd/block)
    let files = ["state20", "county20", "tract20", "bg20", "vtd20", "tabblock20"];

    for name in files {
        let file_url = format!("{base}tl_2020_{fips}_{name}.zip");
        let zip_path = out_dir.join(format!("tl_2020_{fips}_{name}.zip"));
        let out_path = out_dir.join(format!("tl_2020_{fips}_{name}"));

        if verbose > 0 { eprintln!("[download] downloading {file_url}"); }
        common::download_big_file(file_url, &zip_path, true)?;

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
    common::download_big_file(file_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[download] extracting {}", zip_path.display()); }
    common::extract_zip(&zip_path, &out_path, true)?;

    Ok(())
}

/// Download all map files for the given state into the `download/` directory under `pack_dir`.
/// Returns the path to the `download/` directory.
pub fn download_data(state: &str, pack_dir: &PathBuf, verbose: u8) -> Result<PathBuf> {
    common::require_dir_exists(&pack_dir)?;

    let download_dir = pack_dir.join("download");
    common::ensure_dir_exists(&download_dir)?;

    if verbose > 0 { eprintln!("[download] state={state} -> dir {}", download_dir.display()); }

    download_tiger_geometries(&download_dir, state, verbose)?;
    download_daves_demographics(&download_dir, state, verbose)?;
    download_daves_elections(&download_dir, state, verbose)?;
    download_census_crosswalks(&download_dir, state, verbose)?;

    Ok(download_dir)
}
