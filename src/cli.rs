use std::path::PathBuf;

use anyhow::{Result};
use clap::{Args, Parser, Subcommand, ValueHint};

use crate::{common::fs::*, download::*, pack::*, preprocess::*, map::Map};

/// Redistricting CLI (argument schema only)
#[derive(Parser, Debug)]
#[command(name = "districtor", version, about, propagate_version = true)]
pub struct Cli {
    /// Increase output verbosity (-v, -vv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Download source data for a state (forbids stdout)
    Download(DownloadArgs),

    /// Build a redistricted plan (forbids stdout)
    Redistrict(RedistrictArgs),
}

#[derive(Args, Debug)]
pub struct DownloadArgs {
    /// Two/three-letter code, e.g. IL, CA, PR
    pub state: String,

    /// Output location (directory).
    #[arg(value_hint = ValueHint::DirPath)]
    pub out: PathBuf,
}

#[derive(Args, Debug)]
pub struct RedistrictArgs {
    /// Input tabular data file (attributes, demographics, etc.)
    #[arg(value_hint = ValueHint::DirPath)]
    pub pack: PathBuf,

    // /// Input district block assignment
    // #[arg(value_hint = ValueHint::FilePath)]
    // pub input: PathBuf,

    // /// Output plan file (must be a file path; "-" is rejected)
    // #[arg(short, long, value_hint = ValueHint::FilePath)]
    // pub output: PathBuf,
}

fn download(cli: &crate::cli::Cli, args: &DownloadArgs) -> Result<()> {
    let state_code = &args.state.to_ascii_uppercase();
    let out_dir = &args.out.join(format!("{state_code}_2020_pack"));
    ensure_dir_exists(out_dir)?;

    let download_dir = &out_dir.join("download");
    ensure_dir_exists(&download_dir)?;

    download_files(download_dir, &state_code, cli.verbose)?;
    if cli.verbose > 0 { eprintln!("Downloaded files for {} into {}", state_code, out_dir.display()); }

    let data = build_pack(download_dir, out_dir, &state_code, cli.verbose)?;
    data.write_to_pack( out_dir)?;
    if cli.verbose > 0 { eprintln!("Wrote pack to {}", out_dir.display()); }

    Ok(())
}

fn redistrict(cli: &crate::cli::Cli, args: &RedistrictArgs) -> Result<()> {
    let map_data = Map::read_from_pack(&args.pack)?;

    println!("{:?}", map_data);

    // if cli.verbose > 0 { eprintln!("[redistrict] districts={} data={} -> {}", args.districts.display(), args.data.display(), args.output.display()); }

    Ok(())
}

pub fn entry() -> Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::Download(args) => download(&cli, args),
        Commands::Redistrict(args) => redistrict(&cli, args),
    }
}
