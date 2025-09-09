use anyhow::Result;
use openmander_pack::build_pack;

pub fn run(cli: &crate::cli::Cli, args: &crate::cli::DownloadArgs) -> Result<()> {
    let state_code = &args.state.to_ascii_uppercase();
    let out_dir = &args.output.clone().unwrap_or(".".into())
        .join(format!("{state_code}_2020_pack"));

    build_pack(out_dir, state_code, cli.verbose)
}
