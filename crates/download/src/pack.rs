use std::{collections::HashMap, path::Path};

use anyhow::{Context, Result};
use openmander_map::{GeoId, GeoType, Map, MapLayer};
use polars::{frame::DataFrame, prelude::{DataType, StringChunked}};

use crate::common::*;

/// Build a map pack from the downloaded files
pub fn build_pack_from_data(input_dir: &Path, out_dir: &Path, state: &str, verbose: u8) -> Result<Map> {
    let code = state.to_ascii_uppercase();
    let fips = state_abbr_to_fips(&code)
        .with_context(|| format!("Unknown state/territory postal code: {code}"))?;

    let state_shapes_path = input_dir.join(format!("tl_2020_{fips}_state20/tl_2020_{fips}_state20.shp"));
    let county_shapes_path = input_dir.join(format!("tl_2020_{fips}_county20/tl_2020_{fips}_county20.shp"));
    let tract_shapes_path = input_dir.join(format!("tl_2020_{fips}_tract20/tl_2020_{fips}_tract20.shp"));
    let group_shapes_path = input_dir.join(format!("tl_2020_{fips}_bg20/tl_2020_{fips}_bg20.shp"));
    let vtd_shapes_path = input_dir.join(format!("tl_2020_{fips}_vtd20/tl_2020_{fips}_vtd20.shp"));
    let block_shapes_path = input_dir.join(format!("tl_2020_{fips}_tabblock20/tl_2020_{fips}_tabblock20.shp"));
    let block_assign_path = input_dir.join(format!("BlockAssign_ST{fips}_{code}/BlockAssign_ST{fips}_{code}_VTD.txt"));
    let demo_data_path = input_dir.join(format!("Demographic_Data_Block_{code}/demographic_data_block_{code}.v06.csv"));
    let elec_data_path = input_dir.join(format!("Election_Data_Block_{code}/election_data_block_{code}.v06.csv"));

    require_dir_exists(input_dir)?;
    ensure_dir_exists(out_dir)?;

    let mut map_data = Map::default();

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", state_shapes_path); }
    map_data.set_layer(MapLayer::from_tiger_shapefile(GeoType::State, &state_shapes_path)?);

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", county_shapes_path); }
    map_data.set_layer(MapLayer::from_tiger_shapefile(GeoType::County, &county_shapes_path)?);

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", tract_shapes_path); }
    map_data.set_layer(MapLayer::from_tiger_shapefile(GeoType::Tract, &tract_shapes_path)?);

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", group_shapes_path); }
    map_data.set_layer(MapLayer::from_tiger_shapefile(GeoType::Group, &group_shapes_path)?);

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", vtd_shapes_path); }
    map_data.set_layer(MapLayer::from_tiger_shapefile(GeoType::VTD, &vtd_shapes_path)?);

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", block_shapes_path); }
    map_data.set_layer(MapLayer::from_tiger_shapefile(GeoType::Block, &block_shapes_path)?);

    if verbose > 0 { eprintln!("[preprocess] computing crosswalks"); }
    map_data.assign_parents_from_geoids();

    /// Convert a crosswalk DataFrame to a map of GeoIds
    fn get_map_from_crosswalk_df(df: &DataFrame, geo_types: (GeoType, GeoType), col_names: (&str, &str)) -> Result<HashMap<GeoId, GeoId>> {
        Ok(df.column(col_names.0.into())?.str()?
            .into_iter()
            .zip(df.column(col_names.1.into())?.str()?)
            .filter_map(|(b, d)| Some((
                GeoId::new(geo_types.0, b?),
                GeoId::new(geo_types.1, &format!("{}{}", &b?[..5], d?)),
            )))
            .collect())
    }

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", block_assign_path); }
    map_data.get_layer_mut(GeoType::Block).assign_parents_from_map(
        GeoType::VTD,
        get_map_from_crosswalk_df(
            &read_from_pipe_delimited_txt(&block_assign_path)?, 
            (GeoType::Block, GeoType::VTD), 
            ("BLOCKID", "DISTRICT")
        )?
    )?;

    /// Convert GEOID column from i64 to String type
    fn ensure_geoid_is_str(mut df: DataFrame) -> Result<DataFrame> {
        if *df.column("GEOID")?.dtype() != DataType::String {
            let geoid_str = df.column("GEOID")?.i64()?.into_iter()
                .map(|opt| opt.map(|v| format!("{:015}", v)))
                .collect::<StringChunked>();
            df.replace("GEOID", geoid_str)?;
        }
        Ok(df)
    }

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", demo_data_path); }
    map_data.merge_block_data(ensure_geoid_is_str(read_from_csv(&demo_data_path)?)?, "GEOID")?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", elec_data_path); }
    map_data.merge_block_data(ensure_geoid_is_str(read_from_csv(&elec_data_path)?)?, "GEOID")?;

    // println!("{:?}", map_data.counties.geoms.unwrap().find_overlaps(1e-8));

    if verbose > 0 { eprintln!("[preprocess] computing adjacencies"); }
    map_data.compute_adjacencies()?;

    if verbose > 0 { eprintln!("[preprocess] computing shared perimeters"); }
    map_data.compute_shared_perimeters()?;

    Ok(map_data)
}
