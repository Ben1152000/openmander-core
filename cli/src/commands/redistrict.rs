use std::{path::Path, sync::Arc};

use anyhow::Result;
use openmander::{Map, Plan};

pub fn run(_cli: &crate::cli::Cli, args: &crate::cli::RedistrictArgs) -> Result<()> {
    let pack_path = &args.pack;
    let out_path = &args.output.clone().unwrap_or("./plan.csv".into());
    let num_districts = args.districts;

    println!("[redistrict] loading map from {}", pack_path.display());
    let map = Arc::new(Map::read_from_pack(&pack_path)?);

    let mut plan = Plan::new(map.clone(), num_districts as u32);
    println!("[redistrict] generating random plan with {} districts", plan.num_districts());
    plan.randomize()?;

    println!("[redistrict] equalizing plan with tolerance 0.02% for 2000 iterations");
    plan.equalize("T_20_CENS_Total", 0.0002, 10000)?;
    // plan.partition.anneal_balance_two("T_20_CENS_Total", 1, 2, 0.1, 20000);

    println!("[redistrict] writing plan to {}", out_path.display());
    plan.to_csv(Path::new(&out_path))?;

    // if cli.verbose > 0 { eprintln!("[redistrict] districts={} data={} -> {}", args.districts.display(), args.data.display(), args.output.display()); }

    Ok(())
}
