use clap::{Args, Parser, Subcommand, ValueEnum, ValueHint};
use std::path::PathBuf;

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

    /// Overwrite if the directory already exists (off by default)
    #[arg(long)]
    pub force: bool,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, ValueEnum)]
pub enum OutputFormat { Geojson, Parquet, Shapefile }

#[derive(Args, Debug)]
pub struct RedistrictArgs {
    /// Input district geometry file
    #[arg(value_hint = ValueHint::FilePath)]
    pub districts: PathBuf,

    /// Input tabular data file (attributes, demographics, etc.)
    #[arg(value_hint = ValueHint::FilePath)]
    pub data: PathBuf,

    /// Output plan file (must be a file path; "-" is rejected)
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    pub output: PathBuf,

    /// Overwrite if the file exists
    #[arg(long)]
    pub force: bool,
}
