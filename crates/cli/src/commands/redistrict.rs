use std::path::Path;

use anyhow::{Result};

use openmander_map::Map;
use openmander_redistrict::Plan;

use crate::cli::RedistrictArgs;

pub fn run(_cli: &crate::cli::Cli, args: &RedistrictArgs) -> Result<()> {
    let map = Map::read_from_pack(&args.pack)?;

    // map.blocks.to_svg(Path::new("/Users/benjamin/Desktop/openmander/datasets/out.svg"));
    // let mut plan = Plan::from_csv(&map, 2, Path::new("/Users/benjamin/Desktop/openmander/datasets/RI_block-assignments.csv"))?;

    // let mut plan = Plan::new(&map, 2);
    // plan.partition.randomize();
    // plan.partition.anneal_balance_two("T_20_CENS_Total", 1, 2, 0.1, 100000);
    // plan.to_csv(Path::new("/Users/benjamin/Desktop/openmander/datasets/RI_block-assignments_NEW.csv"))?;

    let mut plan = Plan::new(&map, 38);
    plan.partition.randomize();
    plan.partition.equalize("T_20_CENS_Total", 0.005, 2000);
    plan.to_csv(Path::new("/Users/benjamin/Desktop/openmander/datasets/TX_block-assignments_NEW.csv"))?;

    // println!("{:?}", plan.partition.assignments);
    // println!("{:?}", plan.partition.part_weights);

    // if cli.verbose > 0 { eprintln!("[redistrict] districts={} data={} -> {}", args.districts.display(), args.data.display(), args.output.display()); }

    Ok(())
}
