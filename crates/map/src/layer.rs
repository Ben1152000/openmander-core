use std::{collections::HashMap, fmt, sync::Arc};

use openmander_graph::Graph;
use polars::{frame::DataFrame, prelude::DataType};

use crate::{GeoId, GeoType, Geometries, ParentRefs};

/// A single planar partition Layer of the map, containing entities and their relationships.
pub struct MapLayer {
    pub ty: GeoType,
    pub geo_ids: Vec<GeoId>,
    pub index: HashMap<GeoId, u32>, // Map between geo_ids and per-level contiguous indices
    pub parents: Vec<ParentRefs>, // References to parent entities (higher level types)
    pub data: DataFrame, // Entity data (incl. name, centroid, geographic data, election data)
    pub adjacencies: Vec<Vec<u32>>,
    pub shared_perimeters: Vec<Vec<f64>>,
    pub graph: Arc<Graph>, // Graph representation of layer used for partitioning
    pub geoms: Option<Geometries>, // Per-level geometry store, indexed by entities
}

impl MapLayer {
    pub fn new(ty: GeoType) -> Self {
        Self {
            ty,
            geo_ids: Vec::new(),
            index: HashMap::new(),
            parents: Vec::new(),
            data: DataFrame::empty(),
            adjacencies: Vec::new(),
            shared_perimeters: Vec::new(),
            graph: Arc::new(Graph::default()),
            geoms: None,
        }
    }

    /// Get the number of entities in this layer.
    #[inline] pub fn len(&self) -> usize { self.geo_ids.len() }

    /// Construct a graph representation of the layer for partitioning.
    /// Requires data, adjacencies, and shared_perimeters to be computed first.
    pub fn construct_graph(&mut self) {
        self.graph = Arc::new(Graph::new(
            self.len(),
            &self.adjacencies,
            &self.shared_perimeters,
            self.data.get_columns().iter()
                .filter(|&column| column.name() != "idx")
                .map(|column| (column.name().to_string(), column.as_series().unwrap()))
                .filter_map(|(name, series)| match series.dtype() {
                    DataType::Int64  => Some((name, series.i64().unwrap().into_no_null_iter().collect())),
                    DataType::Int32  => Some((name, series.i32().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                    DataType::Int16  => Some((name, series.i16().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                    DataType::Int8   => Some((name, series.i8().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                    DataType::UInt64 => Some((name, series.u64().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                    DataType::UInt32 => Some((name, series.u32().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                    DataType::UInt16 => Some((name, series.u16().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                    DataType::UInt8  => Some((name, series.u8().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                    _ => None,
                }).collect(),
            self.data.get_columns().iter()
                .map(|column| (column.name().to_string(), column.as_series().unwrap()))
                .filter_map(|(name, series)| match series.dtype() {
                    DataType::Float64 => Some((name, series.f64().unwrap().into_no_null_iter().collect())),
                    DataType::Float32 => Some((name, series.f32().unwrap().into_no_null_iter().map(|v| v as f64).collect())),
                    _ => None,
                }).collect(),
        ));
    }
}

impl fmt::Debug for MapLayer {
    /// Custom Debug implementation to summarize key layer stats.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n_entities = self.geo_ids.len();
        let n_index    = self.index.len();
        let n_parents  = self.parents.len();

        // DataFrame stats
        let df_rows = self.data.height();
        let df_cols = self.data.width();
        let col_names = self.data.get_column_names();
        let dtypes = self.data.dtypes();
        let cols_fmt: Vec<String> = col_names
            .iter()
            .zip(dtypes.iter())
            .map(|(n, dt)| format!("{n}: {:?}", dt))
            .collect();

        // Show up to 16 column descriptors unless pretty-printed with {:#?}
        let show_all = f.alternate();
        let max_cols = if show_all { cols_fmt.len() } else { cols_fmt.len().min(16) };
        let cols_preview = &cols_fmt[..max_cols];
        let cols_more = cols_fmt.len().saturating_sub(max_cols);

        // Adjacency stats
        let (deg_sum, max_deg) = self.adjacencies.iter().fold((0usize, 0usize), |(sum, mx), row| {
            let d = row.len();
            (sum + d, mx.max(d))
        });
        let avg_deg = if n_entities > 0 { (deg_sum as f64) / (n_entities as f64) } else { 0.0 };

        // Shared-perimeter stats
        let mut sp_nnz = 0usize;
        let mut sp_sum = 0.0_f64;
        let mut sp_max = 0.0_f64;
        for row in &self.shared_perimeters {
            sp_nnz += row.len();
            for &w in row {
                sp_sum += w;
                if w > sp_max { sp_max = w; }
            }
        }

        // Parent coverage
        let mut cnt_state = 0usize;
        let mut cnt_county = 0usize;
        let mut cnt_tract = 0usize;
        let mut cnt_group = 0usize;
        let mut cnt_vtd = 0usize;
        for p in &self.parents {
            if p.get(GeoType::State).is_some()  { cnt_state += 1; }
            if p.get(GeoType::County).is_some() { cnt_county += 1; }
            if p.get(GeoType::Tract).is_some()  { cnt_tract += 1; }
            if p.get(GeoType::Group).is_some()  { cnt_group += 1; }
            if p.get(GeoType::VTD).is_some()    { cnt_vtd += 1; }
        }

        // Geometry presence (don’t assume internals of Geometries)
        let geoms_present = self.geoms.is_some();

        // Optional small preview of first few IDs in pretty mode
        let geo_id_preview: Vec<&str> = if show_all {
            self.geo_ids.iter().take(5).map(|g| g.id.as_ref()).collect()
        } else {
            Vec::new()
        };

        let mut dbg = f.debug_struct("MapLayer");
        dbg.field("ty", &self.ty)
            .field("entities", &n_entities)
            .field("index_size", &n_index)
            .field("parents_rows", &n_parents)
            .field("data_rows", &df_rows)
            .field("data_cols", &df_cols)
            .field("data_cols_preview", &cols_preview)
            .field("data_cols_more", &cols_more)
            .field("adjacency_nnz", &deg_sum)
            .field("avg_degree", &avg_deg)
            .field("max_degree", &max_deg)
            .field("shared_perimeters_nnz", &sp_nnz)
            .field("shared_perimeters_sum", &sp_sum)
            .field("shared_perimeters_max", &sp_max)
            .field("parents_coverage", &format_args!(
                "state {}/{}; county {}/{}; tract {}/{}; group {}/{}; vtd {}/{}",
                cnt_state, n_entities, cnt_county, n_entities, cnt_tract, n_entities,
                cnt_group, n_entities, cnt_vtd, n_entities,
            ))
            .field("geoms_present", &geoms_present);

        if show_all {
            dbg.field("geo_id_preview", &geo_id_preview);
        }

        dbg.finish()
    }
}
