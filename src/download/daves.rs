use crate::io::{finalize_big_write, open_for_big_write};
use anyhow::{Context, Result};
use std::path::PathBuf;

/// Download demographic data from Dave's redistricting
pub fn download_demographic_data(out_dir: &PathBuf, state: &String, verbose: u8) -> Result<()> {
    let file_url = format!(
        "https://data.dra2020.net/file/dra-block-data/Demographic_Data_Block_{}.v06.zip",
        state.to_ascii_uppercase()
    );

    let out_path = out_dir.join(format!(
        "Demographic_Data_2020_Block_{}.v06.zip",
        state.to_ascii_uppercase()
    ));

    if verbose > 0 {
        eprintln!("[download:demographics] {file_url} -> {}", out_path.display());
    }

    // Safe big-file write (tempfile -> atomic rename), no accidental overwrite unless --force
    let mut sink = open_for_big_write(&out_path, true)?;

    let mut resp = reqwest::blocking::get(&file_url)
        .with_context(|| format!("GET {file_url}"))?
        .error_for_status()
        .with_context(|| format!("GET {file_url} returned error status"))?;

    std::io::copy(&mut resp, &mut sink).with_context(|| format!("write {}", out_path.display()))?;

    finalize_big_write(sink)?;

    return Ok(());
}

/// Download election data from Dave's redistricting
pub fn download_election_data(out_dir: &PathBuf, state: &String, verbose: u8) -> Result<()> {
    // Build base URL dir and derive FIPS (e.g., "31")

    let file_url = format!(
        "https://data.dra2020.net/file/dra-block-data/Election_Data_Block_{}.v06.zip",
        state.to_ascii_uppercase()
    );

    let out_path = out_dir.join(format!(
        "Election_Data_2020_Block_{}.v06.zip",
        state.to_ascii_uppercase()
    ));

    if verbose > 0 {
        eprintln!("[download:elections] {file_url} -> {}", out_path.display());
    }

    // Safe big-file write (tempfile -> atomic rename), no accidental overwrite unless --force
    let mut sink = open_for_big_write(&out_path, true)?;

    let mut resp = reqwest::blocking::get(&file_url)
        .with_context(|| format!("GET {file_url}"))?
        .error_for_status()
        .with_context(|| format!("GET {file_url} returned error status"))?;

    std::io::copy(&mut resp, &mut sink).with_context(|| format!("write {}", out_path.display()))?;

    finalize_big_write(sink)?;

    return Ok(());
}
