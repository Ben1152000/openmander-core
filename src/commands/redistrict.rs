use anyhow::Result;
use crate::cli::RedistrictArgs;
use crate::io::{assert_not_stdout, finalize_big_write, open_for_big_write};
use std::io::Write;

pub fn run(cli: &crate::cli::Cli, args: &RedistrictArgs) -> Result<()> {
    assert_not_stdout(&args.output)?;
    let mut sink = open_for_big_write(&args.output, args.force)?;

    if cli.verbose > 0 {
        eprintln!(
            "[redistrict] districts={} data={} -> {}",
            args.districts.display(),
            args.data.display(),
            args.output.display()
        );
    }

    // TODO: compute plan from args.districts + args.data
    // Example placeholder write:
    writeln!(sink, "{{\"status\":\"ok\"}}")?;

    finalize_big_write(sink)?;
    println!("Wrote plan -> {}", args.output.display());
    Ok(())
}
