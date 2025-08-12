use anyhow::{Context, Result};
use std::path::PathBuf;

use crate::common::{
    fs::extract_zip, 
    fs::ensure_dir_exists, 
    geography::{state_abbr_to_fips, state_abbr_to_name}, 
    io::download_big_file
};

/// Download demographic data from Dave's redistricting
pub fn download_daves_demographics(out_dir: &PathBuf, state: &String, verbose: u8) -> Result<()> {
    let code = state.to_ascii_uppercase();

    let file_url = format!("https://data.dra2020.net/file/dra-block-data/Demographic_Data_Block_{code}.v06.zip");
    let zip_path = out_dir.join(format!("Demographic_Data_Block_{code}.v06.zip"));
    let out_path = out_dir.join(format!("Demographic_Data_Block_{code}"));


    if verbose > 0 { eprintln!("[download] {file_url} -> {}", zip_path.display()); }
    download_big_file(file_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[extract] {} -> {}", zip_path.display(), out_path.display()); }
    extract_zip(&zip_path, &out_path, true)?;

    Ok(())
}

/// Download election data from Dave's redistricting
pub fn download_daves_elections(out_dir: &PathBuf, state: &String, verbose: u8) -> Result<()> {
    let code = state.to_ascii_uppercase();

    let file_url = format!("https://data.dra2020.net/file/dra-block-data/Election_Data_Block_{code}.v06.zip");
    let zip_path = out_dir.join(format!("Election_Data_Block_{code}.v06.zip"));
    let out_path = out_dir.join(format!("Election_Data_Block_{code}"));

    if verbose > 0 { eprintln!("[download] {file_url} -> {}", zip_path.display()); }
    download_big_file(file_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[extract] {} -> {}", zip_path.display(), out_path.display()); }
    extract_zip(&zip_path, &out_path, true)?;

    Ok(())
}

/// Download geometry data from US Census website
pub fn download_tiger_geometries(out_dir: &PathBuf, state: &String, verbose: u8) -> Result<()> {
    // Build the Census TIGER 2020 PL directory URL for a given postal code.
    // Example: "NE" -> "https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/31_NEBRASKA/31/"
    let code = state.to_ascii_uppercase();

    let name = state_abbr_to_name(&code)
        .with_context(|| format!("Unknown state/territory postal code: {code}"))?
        .to_ascii_uppercase();
    let fips = state_abbr_to_fips(&code)
        .with_context(|| format!("Unknown state/territory postal code: {code}"))?;

    let base = format!("https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/{}_{}/{}/", fips, name, fips);

    // Filenames we need for TIGER 2020 (state/county/tract/bg/vtd/block)
    let files = ["state20", "county20", "tract20", "bg20", "vtd20", "tabblock20"];

    for name in files {
        let file_url = format!("{base}tl_2020_{fips}_{name}.zip");
        let zip_path = out_dir.join(format!("tl_2020_{fips}_{name}.zip"));
        let out_path = out_dir.join(format!("tl_2020_{fips}_{name}"));

        if verbose > 0 { eprintln!("[download] {file_url} -> {}", zip_path.display()); }
        download_big_file(file_url, &zip_path, true)?;

        if verbose > 0 { eprintln!("[extract] {} -> {}", zip_path.display(), out_path.display()); }
        extract_zip(&zip_path, &out_path, true)?;
    }

    Ok(())
}

/// Download all map files for the given state (specificied by state_code) into the output directory
pub fn download_all_files(out_dir: &PathBuf, state_code: &String, verbose: u8) -> Result<()> {
    ensure_dir_exists(out_dir)?;

    if verbose > 0 {
        eprintln!("[download] state={}", state_code);
        eprintln!("[download] -> dir {}", out_dir.display());
    }

    download_tiger_geometries(out_dir, state_code, verbose)?;

    download_daves_demographics(out_dir, state_code, verbose)?;

    download_daves_elections(out_dir, state_code, verbose)?;

    if verbose > 0 {
        println!("Downloaded files for {} into {}", state_code, out_dir.display());
    }

    Ok(())
}
