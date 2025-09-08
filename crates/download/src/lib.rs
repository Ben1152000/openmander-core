use std::path::PathBuf;

use anyhow::Result;

pub(crate) mod common;
mod download;
mod clean;
mod pack;

/// Download all data files for a state, build the state pack, and write it to `out_dir`.
pub fn build_pack(out_dir: &PathBuf, state_code: &str, verbose: u8) -> Result<()> {
    common::ensure_dir_exists(out_dir)?;

    let download_dir = &out_dir.join("download");
    common::ensure_dir_exists(&download_dir)?;
    download::download_data(download_dir, state_code, verbose)?;
    if verbose > 0 { eprintln!("Downloaded files for {} into {}", state_code, out_dir.display()); }

    let data = pack::build_pack_from_data(download_dir, out_dir, state_code, verbose)?;
    if verbose > 0 { eprintln!("Built pack for {state_code}"); }

    data.write_to_pack( out_dir)?;
    if verbose > 0 { eprintln!("Wrote pack to {}", out_dir.display()); }

    clean::cleanup_download_dir(out_dir, verbose)?;

    Ok(())
}

/// Download the full state pack for a given state into `out_dir`.
pub fn download_pack(out_dir: &PathBuf, state_code: &str, verbose: u8) -> Result<()> { todo!() }

/// Download the state pack without geometries for a given state into `out_dir`.
pub fn download_pack_without_geoms(out_dir: &PathBuf, state_code: &str, verbose: u8) -> Result<()> { todo!() }
