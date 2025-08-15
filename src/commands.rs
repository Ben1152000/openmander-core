use anyhow::{Result};

use crate::cli::{DownloadArgs, RedistrictArgs};
use crate::common::fs::ensure_dir_exists;
use crate::download::*;
use crate::preprocess::build_pack;

pub fn download(cli: &crate::cli::Cli, args: &DownloadArgs) -> Result<()> {

    let state_code = &args.state;
    let out_dir = &args.out.join(format!("{state_code}_2020_pack"));
    ensure_dir_exists(out_dir)?;

    let download_dir = &out_dir.join("download");
    ensure_dir_exists(&download_dir)?;

    if false { // remove when done testing
        // Download all map files for the given state (specificied by state_code) into the output directory
        if cli.verbose > 0 {
            eprintln!("[download] state={}", state_code);
            eprintln!("[download] -> dir {}", download_dir.display());
        }

        download_tiger_geometries(download_dir, state_code, cli.verbose)?;

        download_daves_demographics(download_dir, state_code, cli.verbose)?;

        download_daves_elections(download_dir, state_code, cli.verbose)?;

        download_census_crosswalks(download_dir, state_code, cli.verbose)?;

        if cli.verbose > 0 {
            println!("Downloaded files for {} into {}", state_code, download_dir.display());
        }
    }

    build_pack(download_dir, out_dir, cli.verbose)?;

    return Ok(());
}

pub fn redistrict(cli: &crate::cli::Cli, args: &RedistrictArgs) -> Result<()> {

    if cli.verbose > 0 {
        eprintln!(
            "[redistrict] districts={} data={} -> {}",
            args.districts.display(),
            args.data.display(),
            args.output.display()
        );
    }

    Ok(())
}
