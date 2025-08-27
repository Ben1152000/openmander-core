use std::{collections::{HashMap, HashSet}, path::Path, sync::Arc};

use anyhow::{anyhow, bail, Context, Ok, Result};
use geo::{MultiPolygon};
use polars::{frame::DataFrame, prelude::{col, Column, DataFrameJoinOps, IntoLazy, NamedFrom, SortMultipleOptions}, series::Series};
use shapefile::{dbase::{FieldValue, Record}, Reader, Shape};

use crate::{common::geom::*, types::*};

impl MapLayer {
    /// Loads layer geometries and data from a given .shp file path.
    pub fn from_tiger_shapefile(ty: GeoType, path: &Path) -> Result<Self> {
        /// Coerce a generic shape into an owned multipolygon, raising error if different shape
        fn shape_to_multipolygon(shape: Shape) -> Result<MultiPolygon<f64>> {
            match shape {
                Shape::Polygon(polygon) => Ok(shp_to_geo(&polygon)),
                other => bail!("found non-Polygon shape in layer: {:?}", other.shapetype())
            }
        }

        /// Convert a vector of records to a DataFrame (using TIGER/PL census format)
        fn records_to_dataframe(records: Vec<Record>, ty: GeoType) -> Result<DataFrame> {
            /// Get the value of a character field from a Record
            fn get_character_field(record: &Record, field: &str) -> Result<String> {
                match record.get(field) {
                    Some(FieldValue::Character(Some(s))) => Ok(s.trim().to_string()),
                    _ => bail!("missing or invalid character field: {}", field)
                }
            }

            /// Get the value of a numeric field from a Record
            fn get_numeric_field(record: &Record, field: &str) -> Result<f64> {
                match record.get(field) {
                    Some(FieldValue::Numeric(Some(n))) => Ok(*n),
                    _ => bail!("missing or invalid numeric field: {}", field)
                }
            }

            Ok(DataFrame::new(vec![
                Column::new(
                    "geo_id".into(),
                    records.iter()
                        .map(|record| get_character_field(record, "GEOID20"))
                        .collect::<Result<Vec<_>>>()?,
                ),
                Column::new(
                    "name".into(),
                    records.iter()
                        .map(|record| match ty {
                            GeoType::County | GeoType::Group => get_character_field(record, "NAMELSAD20"),
                            _ => get_character_field(record, "NAME20"),
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
                Column::new(
                    "centroid_lon".into(),
                    records.iter()
                        .map(|record| {
                            let s = get_character_field(record, "INTPTLON20")?;
                            Ok::<f64>(s.trim().parse()?)
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
                Column::new(
                    "centroid_lat".into(),
                    records.iter()
                        .map(|record| {
                            let s = get_character_field(record, "INTPTLAT20")?;
                            Ok::<f64>(s.trim().parse()?)
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
                Column::new(
                    "area_m2".into(),
                    records.iter()
                        .map(|record| Ok::<f64>(
                            get_numeric_field(record, "ALAND20")? + get_numeric_field(record, "AWATER20")?
                        ))
                        .collect::<Result<Vec<_>>>()?,
                ),
                Column::new(
                    "land_m2".into(),
                    records.iter()
                        .map(|record| get_numeric_field(record, "ALAND20"))
                        .collect::<Result<Vec<_>>>()?,
                ),
                Column::new(
                    "water_m2".into(),
                    records.iter()
                        .map(|record| get_numeric_field(record, "AWATER20"))
                        .collect::<Result<Vec<_>>>()?,
                ),
            ])?)
        }

        let mut reader = Reader::from_path(path)
            .with_context(|| format!("Failed to open shapefile: {}", path.display()))?;

        let size = reader.shape_count()?;

        let mut shapes: Vec<geo::MultiPolygon<f64>> = Vec::with_capacity(size);
        let mut records: Vec<Record> = Vec::with_capacity(size);
        for result in reader.iter_shapes_and_records() {
            let (shape, record) = result.context("Error reading shape+record")?;
            shapes.push(shape_to_multipolygon(shape)?);
            records.push(record);
        }

        let data = records_to_dataframe(records, ty)?
            .with_row_index("idx".into(), None)?;

        let geo_ids: Vec<GeoId> = data.column("geo_id")?.str()?
            .into_no_null_iter()
            .map(|val| GeoId { ty, id: Arc::from(val) })
            .collect();

        let index: HashMap<GeoId, u32> = geo_ids.iter().enumerate()
            .map(|(i, geo_id)| (geo_id.clone(), i as u32))
            .collect();

        Ok(Self {
            ty: ty,
            geo_ids,
            index: index,
            parents: vec![ParentRefs::default(); size],
            data: data,
            adjacencies: vec![Vec::new(); size],
            shared_perimeters: vec![Vec::new(); size],
            geoms: Some(Geometries::new(shapes)),
        })
    }

    /// Initialize layer with a list of geo_ids, replacing existing data.
    pub fn set_data(&mut self, geo_ids: Vec<&str>) -> Result<()> {
        self.parents.resize(geo_ids.len(), ParentRefs::default());

        self.data = DataFrame::new(vec![Column::new("geo_id".into(), &geo_ids)])?;
        self.data = self.data.with_row_index("idx".into(), None)?;

        self.geo_ids = geo_ids.iter()
            .map(|&val| GeoId { ty: self.ty, id: Arc::from(val) })
            .collect();

        self.index = self.geo_ids.iter().enumerate()
            .map(|(i, geo_id)| (geo_id.clone(), i as u32))
            .collect();

        Ok(())
    }

    /// Merge new dataframe into self.data, preserving geo_id
    pub fn merge_data(&mut self, df: DataFrame, id_col: &str) -> Result<()> {
        // Assert size of dataframe matches self.data
        if df.height() != self.data.height() {
            bail!("insert_data: size of dataframe ({:?}) does not match expected size: {:?}.", df.height(), self.data.height());
        }

        // Assert id_col exists and has type String
        df.column(id_col)
            .with_context(|| format!("insert_data: missing id column {:?}", id_col))?
            .str().with_context(|| format!("insert_data: id_col {:?} must be of type String", id_col))?;

        self.data = self.data.inner_join(&df, ["geo_id"], [id_col])?
            .sort(["idx"], SortMultipleOptions::default())?;

        Ok(())
    }

    /// Assign parent references for each entity in the layer, based on their truncated geo_id.
    pub fn assign_parents(&mut self, parent_ty: GeoType) {
        self.geo_ids.iter().enumerate()
            .map(|(i, geo_id)| self.parents[i].set(parent_ty,Some(geo_id.to_parent(parent_ty))))
            .collect()
    }

    /// Assign parent references for each entity in the layer, based on a provided map of geo_id to parent geo_id.
    pub fn assign_parents_from_map(&mut self, parent_ty: GeoType, parent_map: HashMap<GeoId, GeoId>) -> Result<()> {
        self.geo_ids.iter().enumerate()
            .map(|(i, geo_id)| Ok(parent_map.get(geo_id)
                .ok_or_else(|| anyhow!("No parent found for entity with geo_id: {:?}", geo_id))
                .map(|geo_id| self.parents[i].set(parent_ty, Some(geo_id.clone())))))
            .collect::<Result<_>>()?
    }

    /// Compute adjacencies for the layer geometries, if it exists.
    pub fn compute_adjacencies(&mut self) -> Result<()> {
        let geoms = self.geoms.as_mut()
            .ok_or_else(|| anyhow!("Cannot compute adjacencies on empty geometry!"))?;
        self.adjacencies = geoms.compute_adjacencies_fast(1e8)?;
        Ok(())
    }

    /// Compute shared perimeters for the layer geometries, if it exists.
    pub fn compute_shared_perimeters(&mut self) -> Result<()> {
        let geoms = self.geoms.as_mut()
            .ok_or_else(|| anyhow!("Cannot compute perimeters on empty geometry!"))?;
        self.shared_perimeters = geoms.compute_shared_perimeters_fast(&self.adjacencies, 1e8);
        Ok(())
    }
}

impl Map {
    /// Compute parent references for all layers based on truncated geo_id.
    pub fn compute_parents(&mut self) {
        self.counties.assign_parents(GeoType::State);

        self.tracts.assign_parents(GeoType::County);
        self.tracts.assign_parents(GeoType::State);

        self.groups.assign_parents(GeoType::Tract);
        self.groups.assign_parents(GeoType::County);
        self.groups.assign_parents(GeoType::State);

        self.vtds.assign_parents(GeoType::County);
        self.vtds.assign_parents(GeoType::State);

        self.blocks.assign_parents(GeoType::Group);
        self.blocks.assign_parents(GeoType::Tract);
        self.blocks.assign_parents(GeoType::County);
        self.blocks.assign_parents(GeoType::State);
    }

    /// Aggregate a DataFrame from a child layer to a parent layer.
    fn aggregate_data(&self, df: &DataFrame, id_col: &str, ty: GeoType, parent_ty: GeoType) -> Result<DataFrame> {
        // Convert id_col in df to parent id using index and parents
        let parent_ids = df.column(id_col)?.str()?.into_no_null_iter()
            .map(|id| {
                let &i = self.get_layer(ty).index
                    .get(&GeoId { ty, id: Arc::from(id) })
                    .ok_or_else(|| anyhow!("geoid {:?} not found in index", id))?;
                Ok(self.get_layer(ty).parents
                    .get(i as usize)
                    .ok_or_else(|| anyhow!("row {} out of bounds (parents len = {})", i, self.get_layer(ty).parents.len()))?
                    .get(parent_ty)
                    .ok_or_else(|| anyhow!("parent reference {:?} not defined at row {}", parent_ty, i))?
                    .id.to_string())
            })
            .collect::<Result<Vec<_>>>()?;

        // Replace id column and aggregate all other columns
        let mut new_df = df.clone();
        new_df.replace(id_col, Series::new(id_col.into(), parent_ids))?;

        Ok(new_df.lazy()
            .group_by([col(id_col)])
            .agg(df.get_column_names().iter()
                .filter(|&&c| c != id_col)
                .map(|&c| col(c.as_str()).sum().alias(c.as_str())) // keep original names
                .collect::<Vec<_>>(),
            )
            .collect()?)
    }

    /// Merge block-level data into a given dataframe, aggregating on id_col.
    pub fn merge_block_data(&mut self, df: DataFrame, id_col: &str) -> Result<()> {
        for ty in GeoType::order() {
            if ty != GeoType::Block {
                let aggregated = self.aggregate_data(&df, id_col, GeoType::Block, ty)?;
                self.get_layer_mut(ty).merge_data(aggregated, id_col)?;
            }
        }
        self.blocks.merge_data(df, "GEOID")?;

        Ok(())
    }

    /// Aggregate adjacencies from a child layer to a parent layer.
    pub fn aggregate_adjacencies(&mut self, ty: GeoType, parent_ty: GeoType) -> Result<()> {
        // Build parent edge sets from child graph
        let (parent_sets, n_parents) = {
            let child_layer = self.get_layer(ty);
            let parent_layer_ro = self.get_layer(parent_ty);

            // Child adjacency (must exist)
            let child_adj = &child_layer.adjacencies;

            // Precompute parent index for each child node
            let parent_index = &parent_layer_ro.index;
            let n_parents = parent_layer_ro.geo_ids.len();

            let parent_of_child: Vec<u32> = (0..child_adj.len())
                .map(|i| {
                    let geoid = child_layer
                        .parents
                        .get(i)
                        .ok_or_else(|| anyhow!("Index {i} out of bounds in child parents"))?
                        .get(parent_ty)
                        .ok_or_else(|| anyhow!("Parent with type {:?} is not defined for child[{i}]", parent_ty))?;
                    parent_index
                        .get(geoid)
                        .copied()
                        .ok_or_else(|| anyhow!("Parent index does not contain {:?}", geoid.id))
                })
                .collect::<Result<_>>()?;

            // Aggregate child edges -> parent edges with dedup
            let mut parent_sets: Vec<HashSet<u32>> =
                (0..n_parents).map(|_| HashSet::new()).collect();

            for (i, nbrs) in child_adj.iter().enumerate() {
                let pi = parent_of_child[i];
                for &j in nbrs {
                    let pj = parent_of_child[j as usize];
                    if pi != pj {
                        parent_sets[pi as usize].insert(pj);
                        parent_sets[pj as usize].insert(pi);
                    }
                }
            }

            (parent_sets, n_parents)
        };

        // Write back into parent's adjacency list
        let parent_layer = self.get_layer_mut(parent_ty);
        if parent_layer.adjacencies.len() != n_parents {
            parent_layer.adjacencies = vec![Vec::new(); n_parents];
        }
        for (p, set) in parent_sets.into_iter().enumerate() {
            let mut v: Vec<u32> = set.into_iter().collect();
            v.sort_unstable(); // deterministic
            parent_layer.adjacencies[p] = v;
        }

        Ok(())
    }

    /// Compute adjacencies for all layers, aggregating from blocks up to states.
    pub fn compute_adjacencies(&mut self) -> Result<()> {
        self.blocks.compute_adjacencies()?;
        self.patch_adjacencies();
        self.aggregate_adjacencies(GeoType::Block, GeoType::VTD)?;
        self.aggregate_adjacencies(GeoType::Block, GeoType::Group)?;
        self.aggregate_adjacencies(GeoType::Group, GeoType::Tract)?;
        self.aggregate_adjacencies(GeoType::Tract, GeoType::County)?;
        self.aggregate_adjacencies(GeoType::County, GeoType::State)?;

        Ok(())
    }

    /// Compute shared perimeters for all layers, if geometries exist.
    pub fn compute_shared_perimeters(&mut self) -> Result<()> {
        for ty in GeoType::order() {
            self.get_layer_mut(ty).compute_shared_perimeters()?;
        }
        Ok(())
    }
}
