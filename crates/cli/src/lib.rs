mod cli;
pub mod commands;
mod common;

pub use cli::{Cli, Commands};
pub use commands::{download, redistrict};

pub fn run() -> anyhow::Result<()> {
    use clap::Parser;

    let cli = Cli::parse();
    match &cli.command {
        Commands::Download(args) => download::run(&cli, args),
        Commands::Redistrict(args) => redistrict::run(&cli, args),
    }
}
