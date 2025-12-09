use std::{collections::HashMap, fmt, sync::Arc};

use anyhow::Result;
use geo::{MultiPolygon, Point, Polygon, Rect};
use polars::{frame::DataFrame, prelude::DataType};

use crate::{geom::Geometries, graph::{Graph, WeightMatrix}, map::{GeoId, GeoType, ParentRefs}};

/// A single planar partition Layer of the map, containing entities and their relationships.
#[derive(Clone)]
pub struct MapLayer {
    ty: GeoType,
    pub(super) geo_ids: Vec<GeoId>,
    pub(super) index: HashMap<GeoId, u32>,        // Map between geo_ids and per-level contiguous indices
    pub(super) parents: Vec<ParentRefs>,          // References to parent entities (higher level types)
    pub(super) data: DataFrame,                   // Entity data (incl. name, centroid, geographic data, election data)
    pub(super) adjacencies: Vec<Vec<u32>>,        // Adjacency list of contiguous indices
    pub(super) edge_lengths: Vec<Vec<f64>>,       // Shared perimeter lengths for adjacencies
    pub(super) graph: Arc<Graph>,                 // Graph representation of layer used for partitioning
    pub(super) geoms: Option<Geometries>,         // Per-level geometry store, indexed by entities
    pub(super) hulls: Option<Vec<Polygon<f64>>>,  // Approximate hulls for each entity (todo: remove option)
}

impl MapLayer {
    pub(super) fn new(ty: GeoType) -> Self {
        Self {
            ty,
            geo_ids: Vec::new(),
            index: HashMap::new(),
            parents: Vec::new(),
            data: DataFrame::default(),
            adjacencies: Vec::new(),
            edge_lengths: Vec::new(),
            graph: Arc::default(),
            geoms: None,
            hulls: None,
        }
    }

    /// Create a MapLayer from a DataFrame and optional Geometries.
    pub(super) fn from_dataframe(ty: GeoType, df: DataFrame) -> Result<Self> {
        let size = df.height();

        let geo_ids = df.column("geo_id")?.str()?
            .into_no_null_iter()
            .map(|val| GeoId::new(ty, val))
            .collect::<Vec<_>>();

        let index = geo_ids.iter().enumerate()
            .map(|(i, geo_id)| (geo_id.clone(), i as u32))
            .collect::<HashMap<_, _>>();

        Ok(Self {
            ty,
            geo_ids,
            index,
            parents: vec![ParentRefs::default(); size],
            data: df,
            adjacencies: vec![Vec::new(); size],
            edge_lengths: vec![Vec::new(); size],
            graph: Arc::default(),
            geoms: None,
            hulls: None,
        })
    }

    /// Construct a graph representation of the layer for partitioning.
    /// Requires data, adjacencies, and shared_perimeters to be computed first.
    pub(super) fn construct_graph(&mut self) {
        assert!(self.data.height() != 0, "DataFrame must be populated before constructing graph");

        let weights_i64 = self.data.get_columns().iter()
            .map(|column| (column.name().to_string(), column))
            .filter(|(name, _)| name != "idx")
            .filter_map(|(name, column)| match column.dtype() {
                DataType::Int64  => Some((name, column.i64().unwrap().into_no_null_iter().collect())),
                DataType::Int32  => Some((name, column.i32().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::Int16  => Some((name, column.i16().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::Int8   => Some((name, column.i8().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::UInt64 => Some((name, column.u64().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::UInt32 => Some((name, column.u32().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::UInt16 => Some((name, column.u16().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::UInt8  => Some((name, column.u8().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                _ => None,
            }).collect();

        let weights_f64 = self.data.get_columns().iter()
            .map(|column| (column.name().to_string(), column))
            .filter_map(|(name, column)| match column.dtype() {
                DataType::Float64 => Some((name, column.f64().unwrap().into_no_null_iter().collect())),
                DataType::Float32 => Some((name, column.f32().unwrap().into_no_null_iter().map(|v| v as f64).collect())),
                _ => None,
            }).collect();

        self.graph = Arc::new(Graph::new(
            self.len(),
            &self.adjacencies,
            &self.edge_lengths,
            WeightMatrix::new(self.len(), weights_i64, weights_f64),
            &self.hulls().unwrap(),
        ));
    }

    /// Get the number of entities in this layer.
    #[inline] pub fn len(&self) -> usize { self.geo_ids.len() }

    /// Check if the layer is empty (no entities).
    #[inline] pub fn is_empty(&self) -> bool { self.geo_ids.is_empty() }

    /// Get the geographic type of this layer.
    #[inline] pub fn ty(&self) -> GeoType { self.ty }

    /// Set the geographic type of this layer (only used for building without vtds).
    #[inline] pub(super) fn set_ty(&mut self, ty: GeoType) {
        self.ty = ty;
        self.geo_ids = self.geo_ids.iter()
            .map(|geo_id| GeoId::new(ty, geo_id.id()))
            .collect();
        self.index = self.geo_ids.iter().enumerate()
            .map(|(i, geo_id)| (geo_id.clone(), i as u32))
            .collect();
    }

    /// Get a reference to the list of GeoIds in this layer.
    #[inline] pub fn geo_ids(&self) -> &Vec<GeoId> { &self.geo_ids }

    /// Get a reference to the index mapping GeoIds to contiguous indices.
    #[inline] pub fn index(&self) -> &HashMap<GeoId, u32> { &self.index }

    /// Get a reference to the list of ParentRefs for each entity in this layer.
    #[inline] pub fn parents(&self) -> &Vec<ParentRefs> { &self.parents }

    /// Get a reference to the DataFrame containing entity data for this layer.
    #[inline] pub fn data(&self) -> &DataFrame { &self.data }

    /// Get a reference to the adjacency list for this layer.
    #[inline] pub fn adjacencies(&self) -> &Vec<Vec<u32>> { &self.adjacencies }

    /// Get a reference to the shared perimeter weights for this layer.
    #[inline] pub fn shared_perimeters(&self) -> &Vec<Vec<f64>> { &self.edge_lengths }

    /// Get the approximate convex hulls of all MultiPolygons in this layer, if geometries are present.
    #[inline] pub fn hulls(&self) -> Option<&Vec<Polygon<f64>>> { self.hulls.as_ref() }

    /// Get a reference to the shapes for this layer, if available.
    #[inline] pub fn shapes(&self) -> Option<&Vec<MultiPolygon<f64>>> { Some(self.geoms.as_ref()?.shapes()) }

    /// Get the bounding rectangle of all geometries in this layer, if available.
    #[inline] pub fn bounds(&self) -> Option<Rect<f64>> { self.geoms.as_ref()?.bounds() }

    /// Get the union of all MultiPolygons in this layer into a single MultiPolygon.
    /// Note that this can be computationally expensive for large layers.
    #[inline] pub fn union(&self) -> Option<MultiPolygon<f64>> { self.geoms.as_ref()?.union() }

    /// Get centroid lon/lat for each entity, preferring DataFrame columns if present, else computing from geometry.
    pub fn centroids(&self) -> Vec<Point<f64>> {
        if let (Some(lon_column), Some(lat_column)) = (
            self.data.column("centroid_lon").ok()
                .and_then(|column| column.f64().ok()),
            self.data.column("centroid_lat").ok()
                .and_then(|column| column.f64().ok())
        ) {
            assert_eq!(lon_column.len(), self.len(), "Expected centroid_lon length {} to match number of entities {}", lon_column.len(), self.len());
            assert_eq!(lat_column.len(), self.len(), "Expected centroid_lat length {} to match number of entities {}", lat_column.len(), self.len());

            return lon_column.into_iter().zip(lat_column.into_iter())
                .map(|(lon, lat)| Point::new(lon.unwrap_or(f64::NAN), lat.unwrap_or(f64::NAN)))
                .collect()
        }
        
        if let Some(geoms) = &self.geoms {
            assert_eq!(geoms.len(), self.len(), "Expected geoms length {} to match number of entities {}", geoms.len(), self.len());

            return geoms.centroids()
        }

        vec![Point::new(f64::NAN, f64::NAN); self.len()]
    }

    /// Get an Arc clone of the graph representation of this layer.
    #[inline] pub(crate) fn graph_handle(&self) -> Arc<Graph> { self.graph.clone() }
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
        for row in &self.edge_lengths {
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
            self.geo_ids.iter().take(5).map(|g| g.id()).collect()
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
