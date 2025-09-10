
mod cli;
mod commands;

use cli::{Cli, Commands};
use commands::{download, redistrict};

pub fn run() -> anyhow::Result<()> {
    use clap::Parser;

    let cli = Cli::parse();
    match &cli.command {
        Commands::Download(args) => download::run(&cli, args),
        Commands::Redistrict(args) => redistrict::run(&cli, args),
    }
}

fn main() -> anyhow::Result<()> { run() }
