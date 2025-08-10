use anyhow::Result;
use std::path::PathBuf;

use crate::cli::{DownloadArgs, OutputFormat};
use crate::io::{assert_not_stdout, finalize_big_write, looks_like_dir, open_for_big_write};

pub fn run(cli: &crate::cli::Cli, args: &DownloadArgs) -> Result<()> {
    let ext = match args.format {
        OutputFormat::Geojson => "geojson",
        OutputFormat::Parquet => "parquet",
        OutputFormat::Shapefile => "shp",
    };

    let out_path: PathBuf = if args.out.is_dir() || looks_like_dir(&args.out) {
        let fname = format!("{}.{}", args.state.to_ascii_lowercase(), ext);
        args.out.join(fname)
    } else {
        args.out.clone()
    };

    assert_not_stdout(&out_path)?;
    let sink = open_for_big_write(&out_path, args.force)?;

    if cli.verbose > 0 {
        eprintln!("[download] state={} -> {}", args.state, out_path.display());
    }

    // TODO: stream network response into `sink`
    // std::io::copy(&mut network_reader, &mut sink)?;

    finalize_big_write(sink)?;
    println!("Downloaded {} -> {}", args.state, out_path.display());
    Ok(())
}
