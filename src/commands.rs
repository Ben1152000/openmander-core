use anyhow::{Result};

use crate::cli::{DownloadArgs, RedistrictArgs};
use crate::common::fs::ensure_dir_exists;
use crate::download::download_all_files;
use crate::packbuilder::build_pack;

pub fn download(cli: &crate::cli::Cli, args: &DownloadArgs) -> Result<()> {

    let state_code = &args.state;
    let out_dir = &args.out.join(format!("{state_code}_2020_pack"));
    ensure_dir_exists(out_dir)?;

    let download_dir = &out_dir.join("download");
    ensure_dir_exists(&download_dir)?;

    download_all_files(download_dir, state_code, cli.verbose)?;

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
