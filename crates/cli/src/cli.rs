use std::path::PathBuf;

/// Redistricting CLI (argument schema only)
#[derive(clap::Parser, Debug)]
#[command(name = "districtor", version, about, propagate_version = true)]
pub struct Cli {
    /// Increase output verbosity (-v, -vv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(clap::Subcommand, Debug)]
pub enum Commands {
    /// Download source data for a state (forbids stdout)
    Download(DownloadArgs),

    /// Build a redistricted plan (forbids stdout)
    Redistrict(RedistrictArgs),
}

#[derive(clap::Args, Debug)]
pub struct DownloadArgs {
    /// Two/three-letter code, e.g. IL, CA, PR
    pub state: String,

    /// Output location (directory).
    #[arg(value_hint = clap::ValueHint::DirPath)]
    pub out: PathBuf,
}

#[derive(clap::Args, Debug)]
pub struct RedistrictArgs {
    /// Input tabular data file (attributes, demographics, etc.)
    #[arg(value_hint = clap::ValueHint::DirPath)]
    pub pack: PathBuf,

    /// Output plan file (must be a file path; "-" is rejected)
    #[arg(short, long, value_hint = clap::ValueHint::FilePath)]
    pub output: PathBuf,

    // /// Input district block assignment
    // #[arg(value_hint = ValueHint::FilePath)]
    // pub input: PathBuf,
}
