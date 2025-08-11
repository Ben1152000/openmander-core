use anyhow::{bail, Result};
use crate::cli::RedistrictArgs;
use std::path::{Path};


pub fn run(cli: &crate::cli::Cli, args: &RedistrictArgs) -> Result<()> {

    // Assert output path is not stdout
    if args.output == Path::new("-") { bail!("stdout is not supported."); }

    if cli.verbose > 0 {
        eprintln!(
            "[redistrict] districts={} data={} -> {}",
            args.districts.display(),
            args.data.display(),
            args.output.display()
        );
    }

    // let mut sink = open_for_big_write(&args.output, args.force)?;

    // // TODO: compute plan from args.districts + args.data
    // // Example placeholder write:
    // writeln!(sink, "{{\"status\":\"ok\"}}")?;

    // finalize_big_write(sink)?;
    // println!("Wrote plan -> {}", args.output.display());
    Ok(())
}
