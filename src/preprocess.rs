use std::{path::Path};

use anyhow::Result;

use crate::common::{
    data::{debug_print_shapefile, read_from_csv, read_shapefile, write_to_parquet}, 
    fs::{ensure_dir_exists, require_dir_exists}
};


// pub fn load_tiger(dir: &Path) -> Result<TigerLayers> {
    

//     let state   = read_layer(dir, &["state20.zip"], map_state)?;
//     let counties= read_layer(dir, &["county20.zip"], map_county)?;
//     let tracts  = read_layer(dir, &["tract20.zip"], map_tract)?;
//     let groups  = read_layer(dir, &["bg20.zip"], map_bg)?;
//     let vtds    = read_layer(dir, &["vtd20.zip", "vtd.zip"], map_vtd)?;
//     let blocks  = read_layer(dir, &["tabblock20.zip"], map_block)?;

//     Ok(TigerLayers { state, counties, tracts, groups, vtds, blocks })
// }


/// End-to-end: read raw downloads → write pack files.
/// Keep this thin; all work lives in submodules.
pub fn build_pack(input_dir: &Path, out_dir: &Path, _verbose: u8) -> Result<()> {
    require_dir_exists(input_dir)?;
    ensure_dir_exists(out_dir)?;

    let election_data = read_from_csv(&input_dir.join("Election_Data_Block_NE/election_data_block_NE.v06.csv"))?;

    write_to_parquet(election_data, &out_dir.join("test.parquet"))?;

    let items = read_shapefile(&input_dir.join("tl_2020_31_county20/tl_2020_31_county20.shp"))?;
    debug_print_shapefile(&items);

    // 1) Load all shapefiles
    // let _tiger_state = ingest::load_tiger(&input_dir.join("tiger_state.zip"))?;
    // let _tiger_county = ingest::load_tiger(&input_dir.join("tiger_county.zip"))?;
    // let _tiger_tract = ingest::load_tiger(&input_dir.join("tiger_tract.zip"))?;
    // let _tiger_group = ingest::load_tiger(&input_dir.join("tiger_group.zip"))?;
    // let _tiger_vtd = ingest::load_tiger(&input_dir.join("tiger_vtd.zip"))?;
    // let _tiger_block = ingest::load_tiger(&input_dir.join("tiger_block.zip"))?;

    // 2) Crosswalks
    // let cw = xwalk::build_crosswalks(&tig)?;                  // edges w/ weights

    // 3) Entities (attributes table shared by all levels)
    // let entities_df = entities::build_entities(&tig, &cw)?;
    // entities::write_entities_parquet(out_dir, &entities_df)?;

    // 4) Graph (blocks)
    // let block_index = entities::dense_block_index(&entities_df)?;  // (block_geoid, id)
    // let csr = graph::build_block_csr(&tig, &block_index)?;
    // graph::write_csr(out_dir, &csr)?;

    // 5) Geometry layers (FGB)
    // geom::write_state(out_dir, &tig)?;
    // geom::write_counties(out_dir, &tig)?;
    // geom::write_tracts(out_dir, &tig)?;
    // geom::write_groups(out_dir, &tig)?;
    // geom::write_vtds(out_dir, &tig)?;
    // geom::write_blocks(out_dir, &tig)?; // consider sharding

    // 6) Convert elections & demographics data to parquet
    // let _election_df = ingest::load_daves_elections(&input_dir.join("daves_election_v06.zip"))?;
    // let _demographic_df  = ingest::load_daves_pop(&input_dir.join("daves_demographic_v06.zip"))?;
    // elections::write_parquet(out_dir, &elections_df)?;
    // populations::write_parquet(out_dir, &populations_df)?;

    // 7) Compute aggregate data

    // 8) Validate & Metadata
    // validate::check(&entities_df, &csr)?;
    // meta::write_metadata(out_dir, &entities_df, &csr)?;

    Ok(())
}