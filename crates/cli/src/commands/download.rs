use std::{collections::HashMap, path::{Path, PathBuf}, sync::Arc};

use anyhow::{Context, Ok, Result};
use openmander_map::{GeoId, GeoType, Map, MapLayer};
use polars::{frame::DataFrame, prelude::*};

use crate::{cli::DownloadArgs, common::{fs::*, geo::*, io::*, data::*}};

/// Download demographic data from Dave's redistricting
pub fn download_daves_demographics(out_dir: &PathBuf, state: &str, verbose: u8) -> Result<()> {
    let file_url = format!("https://data.dra2020.net/file/dra-block-data/Demographic_Data_Block_{state}.v06.zip");
    let zip_path = out_dir.join(format!("Demographic_Data_Block_{state}.v06.zip"));
    let out_path = out_dir.join(format!("Demographic_Data_Block_{state}"));

    if verbose > 0 { eprintln!("[download] {file_url} -> {}", zip_path.display()); }
    download_big_file(file_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[extract] {} -> {}", zip_path.display(), out_path.display()); }
    extract_zip(&zip_path, &out_path, true)?;

    Ok(())
}

/// Download election data from Dave's redistricting
pub fn download_daves_elections(out_dir: &PathBuf, state: &str, verbose: u8) -> Result<()> {
    let file_url = format!("https://data.dra2020.net/file/dra-block-data/Election_Data_Block_{state}.v06.zip");
    let zip_path = out_dir.join(format!("Election_Data_Block_{state}.v06.zip"));
    let out_path = out_dir.join(format!("Election_Data_Block_{state}"));

    if verbose > 0 { eprintln!("[download] {file_url} -> {}", zip_path.display()); }
    download_big_file(file_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[extract] {} -> {}", zip_path.display(), out_path.display()); }
    extract_zip(&zip_path, &out_path, true)?;

    Ok(())
}

/// Download geometry data from US Census TIGER 2020 PL directory
/// Example URL: "NE" -> "https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/31_NEBRASKA/31/"
pub fn download_tiger_geometries(out_dir: &PathBuf, state: &str, verbose: u8) -> Result<()> {
    let fips = state_abbr_to_fips(&state)
        .with_context(|| format!("Unknown state/territory postal code: {state}"))?;
    let name = state_abbr_to_name(&state)
        .with_context(|| format!("Unknown state/territory postal code: {state}"))?
        .to_ascii_uppercase().replace(' ', "_");

    let base = format!("https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/{fips}_{name}/{fips}/");

    // Filenames we need for TIGER 2020 (state/county/tract/bg/vtd/block)
    let files = ["state20", "county20", "tract20", "bg20", "vtd20", "tabblock20"];

    for name in files {
        let file_url = format!("{base}tl_2020_{fips}_{name}.zip");
        let zip_path = out_dir.join(format!("tl_2020_{fips}_{name}.zip"));
        let out_path = out_dir.join(format!("tl_2020_{fips}_{name}"));

        if verbose > 0 { eprintln!("[download] {file_url} -> {}", zip_path.display()); }
        download_big_file(file_url, &zip_path, true)?;

        if verbose > 0 { eprintln!("[extract] {} -> {}", zip_path.display(), out_path.display()); }
        extract_zip(&zip_path, &out_path, true)?;
    }

    Ok(())
}

/// Download block-level crosswalks from the US Census website
/// Example URL: "NE" -> "https://www2.census.gov/geo/docs/maps-data/data/baf2020/BlockAssign_ST31_NE.zip"
pub fn download_census_crosswalks(out_dir: &PathBuf, state: &str, verbose: u8) -> Result<()> {
    let fips = state_abbr_to_fips(&state)
        .with_context(|| format!("Unknown state/territory postal code: {state}"))?;

    let file_url = format!("https://www2.census.gov/geo/docs/maps-data/data/baf2020/BlockAssign_ST{fips}_{state}.zip");

    let zip_path = out_dir.join(format!("BlockAssign_ST{fips}_{state}.zip"));
    let out_path = out_dir.join(format!("BlockAssign_ST{fips}_{state}"));

    if verbose > 0 { eprintln!("[download] {file_url} -> {}", zip_path.display()); }
    download_big_file(file_url, &zip_path, true)?;

    if verbose > 0 { eprintln!("[extract] {} -> {}", zip_path.display(), out_path.display()); }
    extract_zip(&zip_path, &out_path, true)?;

    Ok(())
}

/// Download all map files for the given state (specificied by state_code) into the output directory
pub fn download_files(out_dir: &PathBuf, state: &str, verbose: u8) -> Result<()> {
    if verbose > 0 { eprintln!("[download] state={}", state); }
    if verbose > 0 { eprintln!("[download] -> dir {}", out_dir.display()); }

    download_tiger_geometries(out_dir, state, verbose)?;
    download_daves_demographics(out_dir, state, verbose)?;
    download_daves_elections(out_dir, state, verbose)?;
    download_census_crosswalks(out_dir, state, verbose)?;

    Ok(())
}

/// Build a map pack from the downloaded files
pub fn build_pack(input_dir: &Path, out_dir: &Path, state: &str, verbose: u8) -> Result<Map> {
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
    map_data.states = MapLayer::from_tiger_shapefile(GeoType::State, &state_shapes_path)?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", county_shapes_path); }
    map_data.counties = MapLayer::from_tiger_shapefile(GeoType::County, &county_shapes_path)?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", tract_shapes_path); }
    map_data.tracts = MapLayer::from_tiger_shapefile(GeoType::Tract, &tract_shapes_path)?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", group_shapes_path); }
    map_data.groups = MapLayer::from_tiger_shapefile(GeoType::Group, &group_shapes_path)?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", vtd_shapes_path); }
    map_data.vtds = MapLayer::from_tiger_shapefile(GeoType::VTD, &vtd_shapes_path)?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", block_shapes_path); }
    map_data.blocks = MapLayer::from_tiger_shapefile(GeoType::Block, &block_shapes_path)?;

    if verbose > 0 { eprintln!("[preprocess] computing crosswalks"); }
    map_data.compute_parents()?;

    /// Convert a crosswalk DataFrame to a map of GeoIds
    fn get_map_from_crosswalk_df(df: &DataFrame, geo_types: (GeoType, GeoType), col_names: (&str, &str)) -> Result<HashMap<GeoId, GeoId>> {
        Ok(df.column(col_names.0.into())?.str()?
            .into_iter()
            .zip(df.column(col_names.1.into())?.str()?)
            .filter_map(|(b, d)| Some((
                GeoId { ty: geo_types.0, id: Arc::from(b?) },
                GeoId { ty: geo_types.1, id: Arc::from(format!("{}{}", &b?[..5], d?)) },
            )))
            .collect())
    }

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", block_assign_path); }
    map_data.blocks.assign_parents_from_map(
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
            let geoid_str: StringChunked = df
                .column("GEOID")?
                .i64()?
                .into_iter()
                .map(|opt| opt.map(|v| format!("{:015}", v)))
                .collect();
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

    if verbose > 0 { eprintln!("Built pack for {state}"); }

    Ok(map_data)
}

pub fn run(cli: &crate::cli::Cli, args: &DownloadArgs) -> Result<()> {
    let state_code = &args.state.to_ascii_uppercase();
    let out_dir = &args.out.join(format!("{state_code}_2020_pack"));
    ensure_dir_exists(out_dir)?;

    let download_dir = &out_dir.join("download");
    ensure_dir_exists(&download_dir)?;

    download_files(download_dir, &state_code, cli.verbose)?;
    if cli.verbose > 0 { eprintln!("Downloaded files for {} into {}", state_code, out_dir.display()); }

    let data = build_pack(download_dir, out_dir, &state_code, cli.verbose)?;
    data.write_to_pack( out_dir)?;
    if cli.verbose > 0 { eprintln!("Wrote pack to {}", out_dir.display()); }

    Ok(())
}
