use anyhow::{Ok, Result};
use openmander::build_pack;

pub fn run(cli: &crate::cli::Cli, args: &crate::cli::DownloadArgs) -> Result<()> {
    let state_code = &args.state.to_ascii_uppercase();
    let out_dir = &args.output.clone().unwrap_or(".".into());

    build_pack(state_code, out_dir, cli.verbose)?;

    Ok(())
}
