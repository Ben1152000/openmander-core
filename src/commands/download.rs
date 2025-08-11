use anyhow::{bail, Context, Result};
use crate::cli::{DownloadArgs};
use crate::download::{tiger::download_tiger_geometries, daves::download_daves_demographics, daves::download_daves_elections};
use std::path::Path;

pub fn run(cli: &crate::cli::Cli, args: &DownloadArgs) -> Result<()> {

    // Assert output path is not stdout
    if args.out == Path::new("-") { bail!("stdout is not supported."); }

    // If the directory doesn't exist, create it
    if args.out.extension().is_none() && !args.out.exists() {
        std::fs::create_dir_all(&args.out)
            .with_context(|| format!("create dir {}", args.out.display()))?;
    }

    if !args.out.is_dir() { bail!("output path must be a directory."); }


    if cli.verbose > 0 {
        eprintln!("[download] state={}", &args.state);
        eprintln!("[download] -> dir {}", &args.out.display());
    }

    download_tiger_geometries(&args.out, &args.state, cli.verbose)?;

    download_daves_demographics(&args.out, &args.state, cli.verbose)?;

    download_daves_elections(&args.out, &args.state, cli.verbose)?;

    if cli.verbose > 0 {
        println!("Downloaded files for {} into {}", &args.state, &args.out.display());
    }

    return Ok(());
}
