use std::{path::Path, str::FromStr, sync::Arc};

use anyhow::{bail, Context, Ok, Result};
use geo::{Point};
use shapefile::{Shape, dbase::{Record, FieldValue}};

use crate::{common::{data::*, fs::*}, types::*};

impl Entity {
    /// Convert a single (Shape, Record) into an Entity.
    pub fn from_record(record: &Record, entity_type: EntityType) -> Self {
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

        let geo_id: Arc<str> = Arc::from(get_character_field(record, "GEOID20").unwrap());

        Self {
            key: EntityKey {
                ty: entity_type,
                id: geo_id.clone(),
            },
            parents: ParentRefs {
                state: match entity_type {
                    EntityType::State => None,
                    _ => Some(EntityKey {
                        ty: EntityType::State,
                        id: Arc::from(&geo_id[..2]),
                    }),
                },
                county: match entity_type {
                    EntityType::State | EntityType::County => None,
                    _ => Some(EntityKey {
                        ty: EntityType::County,
                        id: Arc::from(&geo_id[..5]),
                    }),
                },
                tract: match entity_type {
                    EntityType::Group | EntityType::Block => Some(EntityKey {
                        ty: EntityType::Tract,
                        id: Arc::from(&geo_id[..11]),
                    }),
                    _ => None
                },
                group: match entity_type {
                    EntityType::Block => Some(EntityKey {
                        ty: EntityType::Group,
                        id: Arc::from(&geo_id[..12]),
                    }),
                    _ => None,
                },
                vtd: None,
            },
            name: match entity_type {
                EntityType::County | EntityType::Group => Some(Arc::from(get_character_field(record, "NAMELSAD20").unwrap())),
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
    fn insert(&mut self, shapes: Vec<(Shape, Record)>) -> Result<()> {
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

        self.geoms = shapes.into_iter()
            .map(|(shape, _)| expect_polygon(shape))
            .collect::<Result<_>>()?;

        self.index = self.entities.iter().enumerate()
            .map(|(i, entity)| (entity.key.clone(), i as u32))
            .collect();

        Ok(())
    }
}

/// End-to-end: read raw downloads → write pack files.
/// Keep this thin; all work lives in submodules.
pub fn build_pack(input_dir: &Path, out_dir: &Path, _verbose: u8) -> Result<()> {
    require_dir_exists(input_dir)?;
    ensure_dir_exists(out_dir)?;

    let mut map_data = MapData::default();

    map_data.states.insert(read_shapefile(&input_dir.join("tl_2020_31_state20/tl_2020_31_state20.shp"))?)?;
    map_data.counties.insert(read_shapefile(&input_dir.join("tl_2020_31_county20/tl_2020_31_county20.shp"))?)?;
    map_data.tracts.insert(read_shapefile(&input_dir.join("tl_2020_31_tract20/tl_2020_31_tract20.shp"))?)?;
    map_data.groups.insert(read_shapefile(&input_dir.join("tl_2020_31_bg20/tl_2020_31_bg20.shp"))?)?;
    map_data.vtds.insert(read_shapefile(&input_dir.join("tl_2020_31_vtd20/tl_2020_31_vtd20.shp"))?)?;

    let election_data = read_from_csv(&input_dir.join("Election_Data_Block_NE/election_data_block_NE.v06.csv"))?;

    let demographic_data = read_from_csv(&input_dir.join("Demographic_Data_Block_NE/demographic_data_block_NE.v06.csv"))?;

    println!("{:?}", election_data);

    println!("{:?}", demographic_data);


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

    // write_to_parquet(election_data, &out_dir.join("test.parquet"))?;

    // Load all shapefiles (one from each level)
    // Compute dense index for each level
    // Construct entities df from records
    // Compute adjacency matrices for each level
    // Compute perimeter & edge weights using geometry
    // Compute block -> vtd relation using geometry
    // Write csr adjacency matrices for each level
    // Write geometry layers (fgb) for each level
    // Load elections & demographic data (block level)
    // Calculate aggregate data for higher levels
    // Write elections & demographic data (parquet)
    // Validate & Write Metadata


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