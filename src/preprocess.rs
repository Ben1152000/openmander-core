use std::{collections::HashMap, hash::Hash, path::Path, str::FromStr, sync::Arc};

use anyhow::{anyhow, bail, Ok, Result};
use geo::{MultiPolygon, Point};
use polars::{frame::DataFrame, prelude::*, series::{IntoSeries, Series}};
use shapefile::{Shape, dbase::{Record, FieldValue}};

use crate::{common::{data::*, fs::*, polygon::shp_to_geo}, geometry::PlanarPartition, types::*};

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

/// Create __idx column and sort DataFrame by __idx
fn sort_df_by_index(mut df: DataFrame, index: &HashMap<GeoId, u32>) -> Result<DataFrame> {
    // Build the index values for each row
    let idx_values: Vec<u32> = df.column("GEOID")?
        .str()?
        .into_iter()
        .map(|opt| match opt {
            Some(s) => match index.get(&GeoId { ty: GeoType::Block, id: Arc::from(s) }).copied() {
                Some(i) => Ok(i),
                None => bail!("GEOID {} not found in index", s)
            },
            None => bail!("Null GEOID in DataFrame"),
        })
        .collect::<Result<_>>()?;

    // Add the index column (will stay in the DataFrame) and sort by index
    Ok(
        df.insert_column(0, Series::from_vec("__idx".into(), idx_values))?
            .sort(["__idx"], SortMultipleOptions::default())?
    )
}

/// Aggregate values in DataFrame by parent layer
fn aggregate_df_to_layer(df: &DataFrame, parents: &Vec<ParentRefs>, layer: &MapLayer) -> Result<DataFrame> {
    // 1) Compute parent index per row (new __idx)
    let idxs: Vec<u32> = df.column("__idx")?.u32()?
        .into_no_null_iter()
        .map(|i| {
            let geo_id = parents
                .get(i as usize)
                .ok_or_else(|| anyhow!("row {} out of bounds (parents len = {})", i, parents.len()))?
                .get(layer.ty)?
                .clone()
                .ok_or_else(|| anyhow!("parent reference {:?} not defined at row {}", layer.ty, i))?;
            layer.index
                .get(&geo_id)
                .copied()
                .ok_or_else(|| anyhow!("geoid {:?} not found in index", geo_id))
        })
        .collect::<Result<Vec<_>>>()?;

    // 2) Names of numeric columns (everything after "__idx", "GEOID")
    let cols: Vec<&str> = df.get_column_names()
        .iter()
        .skip(2)
        .map(|s| s.as_str())
        .collect();

    // 3) Lazy replace __idx with parent_idx and aggregate numerics by parent __idx
    let series = Series::from_vec("__idx".into(), idxs);
    let mut out = df.clone()
        .lazy()
        .with_columns([lit(series).alias("__idx")]) // replace, not append
        .group_by([col("__idx")])
        .agg(cols.iter()
            .map(|&c| col(c).sum().alias(c)) // keep original names
            .collect::<Vec<_>>(),
        )
        .collect()?;

    // 4) Add parent GEOID only for parent groups
    let geoids: Vec<&str> = out.column("__idx".into())?.u32()?
        .into_no_null_iter()
        .map(|i| Ok(layer.entities.get(i as usize)
            .ok_or_else(|| anyhow!("parent idx {} out of bounds (entities len = {})", i, layer.entities.len()))?
            .geo_id.id.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    Ok(
        out.insert_column(1, StringChunked::from_iter_values("GEOID".into(), geoids.into_iter()).into_series())?
            .sort(["__idx"], SortMultipleOptions::default())?
    )
}

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

impl Entity {
    /// Convert a single (Shape, Record) into an Entity.
    pub fn from_record(record: &Record, geo_type: GeoType) -> Self {
        /// Get the value of a character field from a Record
        fn get_character_field(record: &Record, field: &str) -> Option<String> {
            match record.get(field) {
                Some(FieldValue::Character(Some(s))) => Some(s.clone()),
                _ => None
            }
        }

        /// Get the value of a numeric field from a Record
        fn get_numeric_field(record: &Record, field: &str) -> Option<f64> {
            match record.get(field) {
                Some(FieldValue::Numeric(Some(n))) => Some(*n),
                _ => None
            }
        }

        Self {
            geo_id: GeoId {
                ty: geo_type,
                id: Arc::from(get_character_field(record, "GEOID20").unwrap()),
            },
            name: match geo_type {
                GeoType::County | GeoType::Group => Some(Arc::from(get_character_field(record, "NAMELSAD20").unwrap())),
                _ => Some(Arc::from(get_character_field(record, "NAME20").unwrap())),
            },
            area_m2: match (
                get_numeric_field(record, "ALAND20"),
                get_numeric_field(record, "AWATER20"),
            ) {
                (Some(x), Some(y)) => Some (x + y),
                _ => None
            },
            centroid: match (
                get_character_field(record, "INTPTLON20"),
                get_character_field(record, "INTPTLAT20"),
            ) {
                (Some(lat), Some(lon)) => Some(Point::new(
                    f64::from_str(&lon).unwrap(),
                    f64::from_str(&lat).unwrap(),
                )),
                _ => None
            },
        }
    }

}

impl MapLayer {
    fn insert_shapes(&mut self, shapes: Vec<(Shape, Record)>) -> Result<()> {
        /// Coerce a generic shape into an owned polygon, raising error if different shape
        fn expect_polygon(shape: Shape) -> Result<shapefile::Polygon> {
            match shape {
                Shape::Polygon(polygon) => Ok(polygon),
                other => bail!("found non-Polygon shape in layer: {:?}", other.shapetype())
            }
        }

        self.entities = shapes.iter()
            .map(|(_, record)| Entity::from_record(record, self.ty))
            .collect();

        self.parents.resize(shapes.len(), ParentRefs::default());

        let polygons: Vec<MultiPolygon> = shapes
            .into_iter()
            .map(|(shape, _)| Ok(shp_to_geo(&expect_polygon(shape)?)))
            .collect::<Result<Vec<_>>>()?;

        self.geoms = Some(PlanarPartition::new(polygons));

        self.index = self.entities.iter().enumerate()
            .map(|(i, entity)| (entity.geo_id.clone(), i as u32))
            .collect();

        Ok(())
    }

    fn assign_parents(&mut self, parent_ty: GeoType) -> Result<()> {
        self.entities.iter()
            .enumerate()
            .map(|(i, e)| self.parents[i].set(parent_ty,Some(e.geo_id.to_parent(parent_ty))))
            .collect()
    }

    fn assign_parents_from_map(&mut self, parent_ty: GeoType, parent_map: HashMap<GeoId, GeoId>) -> Result<()> {
        self.entities.iter()
            .enumerate()
            .map(|(i, e)| parent_map
                .get(&e.geo_id)
                .ok_or_else(|| anyhow!("No parent found for entity with geo_id: {:?}", e.geo_id))
                .map(|p| self.parents[i].set(parent_ty, Some(p.clone()))))
            .collect::<Result<_>>()?
    }
}

impl MapData {
    fn compute_parents(&mut self) -> Result<()> {
        self.counties.assign_parents(GeoType::State)?;
        self.tracts.assign_parents(GeoType::County)?;
        self.tracts.assign_parents(GeoType::State)?;
        self.groups.assign_parents(GeoType::Tract)?;
        self.groups.assign_parents(GeoType::County)?;
        self.groups.assign_parents(GeoType::State)?;
        self.vtds.assign_parents(GeoType::County)?;
        self.vtds.assign_parents(GeoType::State)?;
        self.blocks.assign_parents(GeoType::Group)?;
        self.blocks.assign_parents(GeoType::Tract)?;
        self.blocks.assign_parents(GeoType::County)?;
        self.blocks.assign_parents(GeoType::State)?;
        Ok(())
    }

    fn insert_demo_data(&mut self, block_demo_df: DataFrame) -> Result<()> {
        let df = sort_df_by_index(ensure_geoid_is_str(block_demo_df)?, &self.blocks.index)?;
        self.states.demo_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.states)?);
        self.counties.demo_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.counties)?);
        self.tracts.demo_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.tracts)?);
        self.groups.demo_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.groups)?);
        self.vtds.demo_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.vtds)?);
        self.blocks.demo_data = Some(df);
        Ok(())
    }

    fn insert_elec_data(&mut self, block_elec_df: DataFrame) -> Result<()> {
        let df = sort_df_by_index(ensure_geoid_is_str(block_elec_df)?, &self.blocks.index)?;
        self.states.elec_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.states)?);
        self.counties.demo_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.counties)?);
        self.tracts.elec_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.tracts)?);
        self.groups.elec_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.groups)?);
        self.vtds.elec_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.vtds)?);
        self.blocks.elec_data = Some(df);
        Ok(())
    }
}

/// End-to-end: read raw downloads → write pack files.
/// Keep this thin; all work lives in submodules.
pub fn build_pack(input_dir: &Path, out_dir: &Path, verbose: u8) -> Result<()> {
    require_dir_exists(input_dir)?;
    ensure_dir_exists(out_dir)?;

    let mut map_data = MapData::default();

    if verbose > 0 { eprintln!("Loading shapefiles"); }
    let state_shapes_path = "tl_2020_31_state20/tl_2020_31_state20.shp";
    let county_shapes_path = "tl_2020_31_county20/tl_2020_31_county20.shp";
    let tract_shapes_path = "tl_2020_31_tract20/tl_2020_31_tract20.shp";
    let group_shapes_path = "tl_2020_31_bg20/tl_2020_31_bg20.shp";
    let vtd_shapes_path = "tl_2020_31_vtd20/tl_2020_31_vtd20.shp";
    let block_shapes_path = "tl_2020_31_tabblock20/tl_2020_31_tabblock20.shp";

    if verbose > 0 { eprintln!("[preprocess] loading {state_shapes_path}"); }
    map_data.states.insert_shapes(read_shapefile(&input_dir.join(state_shapes_path))?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {county_shapes_path}"); }
    map_data.counties.insert_shapes(read_shapefile(&input_dir.join(county_shapes_path))?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {tract_shapes_path}"); }
    map_data.tracts.insert_shapes(read_shapefile(&input_dir.join(tract_shapes_path))?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {group_shapes_path}"); }
    map_data.groups.insert_shapes(read_shapefile(&input_dir.join(group_shapes_path))?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {vtd_shapes_path}"); }
    map_data.vtds.insert_shapes(read_shapefile(&input_dir.join(vtd_shapes_path))?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {block_shapes_path}"); }
    map_data.blocks.insert_shapes(read_shapefile(&input_dir.join(block_shapes_path))?)?;

    if verbose > 0 { eprintln!("Computing crosswalks"); }
    let block_assign_path = "BlockAssign_ST31_NE/BlockAssign_ST31_NE_VTD.txt";
    map_data.compute_parents()?;

    if verbose > 0 { eprintln!("[preprocess] loading {block_assign_path}"); }
    map_data.blocks.assign_parents_from_map(
        GeoType::VTD,
        get_map_from_crosswalk_df(
            &read_from_pipe_delimited_txt(&input_dir.join(block_assign_path))?, 
            (GeoType::Block, GeoType::VTD), 
            ("BLOCKID", "DISTRICT")
        )?
    )?;

    if verbose > 0 { eprintln!("Loading datasets"); }
    let demo_data_path = "Demographic_Data_Block_NE/demographic_data_block_NE.v06.csv";
    let elec_data_path: &'static str = "Election_Data_Block_NE/election_data_block_NE.v06.csv";

    if verbose > 0 { eprintln!("[preprocess] loading {demo_data_path}"); }
    map_data.insert_demo_data(read_from_csv(&input_dir.join(demo_data_path))?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {elec_data_path}"); }
    map_data.insert_elec_data(read_from_csv(&input_dir.join(elec_data_path))?)?;

    // println!("{:?}", map_data.counties.geoms.unwrap().find_overlaps(1e-8));

    if verbose > 0 { eprintln!("Computing adjacencies"); }
    map_data.blocks.compute_adjacencies()?;

    // Compute adjacency matrices for each level
    // Compute perimeter & edge weights using geometry

    // Write data files
    // Validate & Write Metadata

    /*
    NE_2020_pack/
      download/ (temp dir)
      entities/
        state.parquet
        county.parquet
        tract.parquet
        group.parquet
        vtd.parquet
        block.parquet
      elections/
        state.parquet
        county.parquet
        tract.parquet
        group.parquet
        vtd.parquet
        block.parquet
      demographics/
        state.parquet
        county.parquet
        tract.parquet
        group.parquet
        vtd.parquet
        block.parquet
      relations/
        county.csr.bin
        tract.csr.bin
        group.csr.bin
        vtd.csr.bin
        block.csr.bin
        block_to_vtd.parquet
      geometries/
        state.fgb
        counties.fgb
        tracts.fgb
        groups.fgb
        vtds.fgb
        blocks.fgb
      meta/
        manifest.json
            { 
                "pack_id":"NE-2020", 
                "version":"1", 
                "crs":"EPSG:4269", 
                "levels":["state","county","tract","blockgroup","vtd","block"], 
                "counts":{"block":123456}, 
                "files":{"geometries/blocks.fgb":{"sha256":"…"}}
            }
    */

    // 1. Load VTD shapefile → build dense index (VTD) → write geometry (FGB)
    //   - Read VTDs first (much fewer than blocks).
    //   - While streaming features:
    //       assign vtd_idx (dense u32),
    //       compute/store cheap per‑feature stats (area, bbox) if helpful,
    //       append geometry directly to an .fgb writer (no need to keep all geoms in RAM).
    //   - Persist: vtd_idx ↔ vtd_geoid (parquet/csv), VTD .fgb

    // 2. Load County, State shapefiles (each) → build dense index → write geometry (FGB)
    //   - Same per‑level streaming pattern as VTDs.
    //   - Persist: county_idx map, state_idx map, and their .fgb

    // 3. Load Tract and Block Group shapefiles (each) → build dense index → write geometry (FGB)
    //   - Still modest sizes—stream and flush.
    //   - Persist: tract_idx, bg_idx, .fgb files

    // 4. Load Block shapefile (streamed in chunks) → build dense index (Block)
    //   - Don’t keep all blocks in memory.
    //   - If you want a single “entities df”, only store columns you actually need (ids + parent refs + names); postpone geometry.
    //   - Persist: block_idx map (dense u32 ↔ GEOID15). (You can also spill a minimal “entities df” per level now, if you want.)
    
    /*
    5. Build Block → VTD relation (geometry)
      - Build an in‑RAM spatial index for VTDs only (already read in step 1). Keep VTD geometries in memory (small), or memory‑map the .fgb and build an R‑tree of their bboxes.
      - Stream Blocks in chunks:
          read a chunk of block geometries,
          query VTD index → exact polygon overlay to pick containing/intersecting VTD,
          emit (block_idx, vtd_idx) pairs and flush to disk (parquet/arrow) in batches.
      - Free block chunk before loading the next one; never hold all blocks.

    6. Compute per‑level adjacency (CSR) + edge weights (shared boundary)
      - Do this per level, one at a time, using its .fgb:
          Load geometries level‑by‑level (VTD → County → Tract → BG → Block (chunked if necessary)).
          Build an R‑tree on bboxes for the level.
          For each feature, candidate‑probe neighbors via bbox, then boundary‑intersect to confirm adjacency and accumulate shared‑edge length in the same pass.
          Emit CSR (indptr, indices) and optional weights (f32/f64 for shared meters) to disk immediately for that level.
      - Tip: For Blocks, do adjacency in tiles/chunks to avoid holding the entire level; build a temporary tile index.

    7. Compute simple per‑feature metrics (perimeter)
      - If you didn’t already compute perimeter during adjacency, do a single streaming pass per level’s .fgb to compute perimeter (and any other per‑feature metrics).
      - Persist as a slim per‑level table: (idx, perimeter_m, area_m2, …).

    8. Load block‑level elections & demographics
      - Read once, normalize keys to block_idx.
      - If large, stream via Polars scan and left‑join to block_idx map to replace GEOID15 with u32 indices.

    9. Aggregate to higher levels
      - You now have (block_idx → bg_idx/tract_idx/county_idx/state_idx) and (block_idx → vtd_idx) via step 5 and parent maps.
      - Do groupby‑reduce by each parent index to produce per‑level aggregates (VTD, BG, Tract, County, State).
      - Stream each aggregation separately (don’t hold all output levels at once).

    10. Write outputs
      - CSR per level (already written in step 6).
      - Geometry layers (.fgb) per level (already written in steps 1–4).
      - Attributes:
          Block: write cleaned/normalized block attributes (elections + demo) as parquet keyed by block_idx.
          Higher levels: write aggregated parquet per level keyed by dense index.
      - Entities df / dictionaries: write compact lookup tables (dense index ↔ GEOID ↔ names).

    11. Validate & write metadata
      - Sanity checks: counts per level, sum of areas, CSR symmetry for undirected levels, shared‑edge totals non‑negative, parent coverage (every block has VTD, BG, etc.).
      - Emit a small JSON/TOML manifest: file paths, counts, CRS, build hashes, schema versions.
    */

    Ok(())
}