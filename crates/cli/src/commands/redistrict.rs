use std::path::Path;

use anyhow::Result;

use openmander_map::Map;
use openmander_redistrict::Plan;

pub fn run(_cli: &crate::cli::Cli, args: &crate::cli::RedistrictArgs) -> Result<()> {
    let pack_path = &args.pack;
    let out_path = &args.output.clone().unwrap_or("./plan.csv".into());
    let num_districts = args.districts;

    println!("[redistrict] loading map from {}", pack_path.display());
    let map = Map::read_from_pack(&pack_path)?;

    let mut plan = Plan::new(&map, num_districts as u32);
    println!("[redistrict] generating random plan with {} districts", plan.partition.num_parts - 1);
    plan.partition.randomize();

    println!("[redistrict] equalizing plan with tolerance 0.1% for 2000 iterations");
    plan.partition.equalize("T_20_CENS_Total", 0.001, 10000);
    // plan.partition.anneal_balance_two("T_20_CENS_Total", 1, 2, 0.1, 20000);

    println!("[redistrict] writing plan to {}", out_path.display());
    plan.to_csv(Path::new(&out_path))?;

    // print population for each district:
    for part in 1..plan.partition.num_parts {
        let pop = plan.partition.part_weights.get_as_f64("T_20_CENS_Total", part as usize).unwrap();
        println!("  Part {:2}: Population {:.0}", part, pop);
    }

    // if cli.verbose > 0 { eprintln!("[redistrict] districts={} data={} -> {}", args.districts.display(), args.data.display(), args.output.display()); }

    Ok(())
}
