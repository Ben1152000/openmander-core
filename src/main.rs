use anyhow::Result;
use clap::Parser;

use openmander_core::cli::{Cli, Commands};
use openmander_core::commands::{download, redistrict};

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::Download(args) => download(&cli, args),
        Commands::Redistrict(args) => redistrict(&cli, args),
    }
}
