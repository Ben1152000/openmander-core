use anyhow::{Result};

use openmander_map::Map;

use crate::cli::RedistrictArgs;

pub fn run(cli: &crate::cli::Cli, args: &RedistrictArgs) -> Result<()> {
    let map_data = Map::read_from_pack(&args.pack)?;

    println!("{:?}", map_data);

    // if cli.verbose > 0 { eprintln!("[redistrict] districts={} data={} -> {}", args.districts.display(), args.data.display(), args.output.display()); }

    Ok(())
}
