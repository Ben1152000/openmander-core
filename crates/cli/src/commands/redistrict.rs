use anyhow::{Result};

use openmander_map::Map;

use crate::cli::RedistrictArgs;

pub fn run(_cli: &crate::cli::Cli, args: &RedistrictArgs) -> Result<()> {
    let map_data = Map::read_from_pack(&args.pack)?;

    println!("{:?}", map_data.blocks.data);

    // Get the schema of the DataFrame
    let schema = map_data.blocks.data.schema();

    // Iterate through the schema to print column names and types
    for (name, dtype) in schema.iter() {
        println!("Column: {}, Type: {:?}", name, dtype);
    }

    // if cli.verbose > 0 { eprintln!("[redistrict] districts={} data={} -> {}", args.districts.display(), args.data.display(), args.output.display()); }

    Ok(())
}
