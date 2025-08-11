use crate::io::{download_big_file};
use anyhow::Result;
use std::path::PathBuf;

/// Download demographic data from Dave's redistricting
pub fn download_daves_demographics(out_dir: &PathBuf, state: &String, verbose: u8) -> Result<()> {
    let code = state.to_ascii_uppercase();

    let file_url = format!("https://data.dra2020.net/file/dra-block-data/Demographic_Data_Block_{code}.v06.zip");
    let out_path = out_dir.join(format!("daves_demographic_v06_{code}_2020.zip"));

    if verbose > 0 { eprintln!("[download:demographics] {file_url} -> {}", out_path.display()); }

    download_big_file(file_url, &out_path, true)?;
    Ok(())
}

/// Download election data from Dave's redistricting
pub fn download_daves_elections(out_dir: &PathBuf, state: &String, verbose: u8) -> Result<()> {
    let code = state.to_ascii_uppercase();

    let file_url = format!("https://data.dra2020.net/file/dra-block-data/Election_Data_Block_{code}.v06.zip");
    let out_path = out_dir.join(format!("Election_Data_2020_Block_{code}.v06.zip"));

    if verbose > 0 { eprintln!("[download:elections] {file_url} -> {}", out_path.display()); }

    download_big_file(file_url, &out_path, true)?;
    Ok(())
}
