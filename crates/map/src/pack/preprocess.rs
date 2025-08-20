use std::{collections::{HashMap, HashSet}, path::Path, str::FromStr, sync::Arc};

use anyhow::{anyhow, bail, Context, Ok, Result};
use geo::{MultiPolygon, Point};
use polars::{frame::DataFrame, prelude::*, series::{IntoSeries, Series}};
use shapefile::{Shape, dbase::{Record, FieldValue}};

use openmander_geometry::PlanarPartition;

use crate::{common::{data::*, fs::*, geo::*, polygon::*}, types::*};

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

    fn aggregate_adjacencies(&self, parent_layer: &mut MapLayer) -> Result<()> {
        let get_parent_index = |i: usize| -> Result<&u32> {
            let geoid = self.parents.get(i)
                .ok_or_else(|| anyhow!("Index {i} out of bounds in parents"))?
                .get(parent_layer.ty)?.as_ref()
                .ok_or_else(|| anyhow!("Parent with type {:?} is not defined", parent_layer.ty))?;
            parent_layer.index.get(&geoid)
                .ok_or_else(|| anyhow!("Index does not contain {:?}", geoid.id))
        };
    
        // Child adjacency (must exist)
        let child_adj = &self.geoms
            .as_ref()
            .ok_or_else(|| anyhow!("Cannot compute adjacencies on empty geometry!"))?
            .adj_list;
    
        // Prepare per-parent neighbor sets (for dedup)
        let n_parents = parent_layer.entities.len();
        let mut parent_sets: Vec<HashSet<u32>> =
            (0..n_parents).map(|_| HashSet::new()).collect();
    
        // Aggregate child edges -> parent edges
        child_adj.iter().enumerate()
            .map(|(i, nbrs)| -> Result<()> {
                let pi = *get_parent_index(i)? as u32;
                for &j in nbrs {
                    let pj = *get_parent_index(j as usize)? as u32;
                    if pi != pj {
                        parent_sets[pi as usize].insert(pj);
                        parent_sets[pj as usize].insert(pi);
                    }
                }
                Ok(())
            })
            .collect::<Result<()>>()?;
    
        // Write back into parent's adjacency list
        let parent_geoms = parent_layer.geoms
            .as_mut()
            .ok_or_else(|| anyhow!("Parent layer has no geometry store to receive adjacencies"))?;
    
        if parent_geoms.adj_list.len() != n_parents {
            parent_geoms.adj_list = vec![Vec::new(); n_parents];
        }
        for (p, set) in parent_sets.into_iter().enumerate() {
            let mut v: Vec<u32> = set.into_iter().collect();
            v.sort_unstable(); // deterministic order
            parent_geoms.adj_list[p] = v;
        }
    
        Ok(())
    }
}

impl Map {
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
        self.counties.elec_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.counties)?);
        self.tracts.elec_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.tracts)?);
        self.groups.elec_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.groups)?);
        self.vtds.elec_data = Some(aggregate_df_to_layer(&df, &self.blocks.parents, &self.vtds)?);
        self.blocks.elec_data = Some(df);
        Ok(())
    }
}

/// End-to-end: read raw downloads → write pack files.
/// Keep this thin; all work lives in submodules.
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
    map_data.states.insert_shapes(read_shapefile(&state_shapes_path)?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", county_shapes_path); }
    map_data.counties.insert_shapes(read_shapefile(&county_shapes_path)?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", tract_shapes_path); }
    map_data.tracts.insert_shapes(read_shapefile(&tract_shapes_path)?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", group_shapes_path); }
    map_data.groups.insert_shapes(read_shapefile(&group_shapes_path)?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", vtd_shapes_path); }
    map_data.vtds.insert_shapes(read_shapefile(&vtd_shapes_path)?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", block_shapes_path); }
    map_data.blocks.insert_shapes(read_shapefile(&block_shapes_path)?)?;

    if verbose > 0 { eprintln!("[preprocess] computing crosswalks"); }
    map_data.compute_parents()?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", block_assign_path); }
    map_data.blocks.assign_parents_from_map(
        GeoType::VTD,
        get_map_from_crosswalk_df(
            &read_from_pipe_delimited_txt(&block_assign_path)?, 
            (GeoType::Block, GeoType::VTD), 
            ("BLOCKID", "DISTRICT")
        )?
    )?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", demo_data_path); }
    map_data.insert_demo_data(read_from_csv(&demo_data_path)?)?;

    if verbose > 0 { eprintln!("[preprocess] loading {:?}", elec_data_path); }
    map_data.insert_elec_data(read_from_csv(&elec_data_path)?)?;

    // println!("{:?}", map_data.counties.geoms.unwrap().find_overlaps(1e-8));

    if verbose > 0 { eprintln!("[preprocess] computing adjacencies"); }
    map_data.blocks.compute_adjacencies()?;
    map_data.blocks.aggregate_adjacencies(&mut map_data.states)?;
    map_data.blocks.aggregate_adjacencies(&mut map_data.counties)?;
    map_data.blocks.aggregate_adjacencies(&mut map_data.tracts)?;
    map_data.blocks.aggregate_adjacencies(&mut map_data.groups)?;
    map_data.blocks.aggregate_adjacencies(&mut map_data.vtds)?;

    if verbose > 0 { eprintln!("Built pack for {state}"); }

    Ok(map_data)

    // Compute perimeter & edge weights using geometry

    // 7. Compute simple per‑feature metrics (perimeter)
    //   - If you didn’t already compute perimeter during adjacency, do a single streaming pass per level’s .fgb to compute perimeter (and any other per‑feature metrics).
    //   - Persist as a slim per‑level table: (idx, perimeter_m, area_m2, …).

    // Write data files
    // Validate & Write Metadata
}
