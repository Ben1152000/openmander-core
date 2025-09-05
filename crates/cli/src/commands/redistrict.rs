use std::path::Path;

use anyhow::{Result};

use openmander_map::Map;
use openmander_redistrict::Plan;

use crate::cli::RedistrictArgs;

pub fn run(_cli: &crate::cli::Cli, args: &RedistrictArgs) -> Result<()> {
    println!("[redistrict] loading map from {}", args.pack.display());
    let map = Map::read_from_pack(&args.pack)?;

    let mut plan = Plan::new(&map, 40);
    println!("[redistrict] generating random plan with {} districts", plan.partition.num_parts - 1);
    plan.partition.randomize();

    println!("[redistrict] equalizing plan with tolerance 0.5% for 2000 iterations");
    plan.partition.equalize("T_20_CENS_Total", 0.0025, 10000);

    println!("[redistrict] writing plan to {}", args.output.display());
    plan.to_csv(Path::new(&args.output))?;

    // print population for each district:
    for part in 1..plan.partition.num_parts {
        let pop = plan.partition.part_weights.get_as_f64("T_20_CENS_Total", part as usize).unwrap();
        println!("  Part {:2}: Population {:.0}", part, pop);
    }

    // plan.partition.anneal_balance_two("T_20_CENS_Total", 1, 2, 0.1, 100000);

    // if cli.verbose > 0 { eprintln!("[redistrict] districts={} data={} -> {}", args.districts.display(), args.data.display(), args.output.display()); }

    Ok(())
}
