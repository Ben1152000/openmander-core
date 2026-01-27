use std::{collections::HashMap, fmt, sync::Arc};

use geo::{MultiPolygon, Point, Polygon, Rect};
use polars::{frame::DataFrame, prelude::DataType};

use crate::{geom::Geometries, graph::{WeightedGraph, WeightMatrix}, map::{GeoId, GeoType, ParentRefs}};

/// A single planar partition Layer of the map, containing entities and their relationships.
#[derive(Clone)]
pub struct MapLayer {
    ty: GeoType,
    pub(super) geo_ids: Vec<GeoId>,
    pub(super) index: HashMap<GeoId, u32>,        // Map between geo_ids and per-level contiguous indices
    pub(super) parents: Vec<ParentRefs>,          // References to parent entities (higher level types)
    pub(super) unit_data: DataFrame,              // Entity data (incl. name, centroid, geographic data, election data)
    pub(super) adjacencies: Vec<Vec<u32>>,        // Adjacency list of contiguous indices
    pub(super) edge_lengths: Vec<Vec<f64>>,       // Shared perimeter lengths for adjacencies
    pub(super) geoms: Option<Geometries>,         // Per-level geometry store, indexed by entities
    pub(super) hulls: Option<Vec<Polygon<f64>>>,  // Approximate hulls for each entity (todo: remove option)
    pub(super) graph: Arc<WeightedGraph>,         // Graph representation of layer used for partitioning
}

impl MapLayer {
    pub(super) fn new(ty: GeoType) -> Self {
        Self {
            ty,
            geo_ids: Vec::new(),
            index: HashMap::new(),
            parents: Vec::new(),
            unit_data: DataFrame::default(),
            adjacencies: Vec::new(),
            edge_lengths: Vec::new(),
            geoms: None,
            hulls: None,
            graph: Arc::default(),
        }
    }

    /// Get the number of entities in this layer.
    #[inline] pub fn len(&self) -> usize { self.geo_ids.len() }

    /// Check if the layer is empty (no entities).
    #[inline] pub fn is_empty(&self) -> bool { self.geo_ids.is_empty() }

    /// Get the geographic type of this layer.
    #[inline] pub fn ty(&self) -> GeoType { self.ty }

    /// Get a reference to the list of GeoIds in this layer.
    #[inline] pub fn geo_ids(&self) -> &Vec<GeoId> { &self.geo_ids }

    /// Get a reference to the index mapping GeoIds to contiguous indices.
    #[inline] pub fn index(&self) -> &HashMap<GeoId, u32> { &self.index }

    /// Get a reference to the list of ParentRefs for each entity in this layer.
    #[inline] pub fn parents(&self) -> &Vec<ParentRefs> { &self.parents }

    /// Get a reference to the DataFrame containing entity data for this layer.
    #[inline] pub fn data(&self) -> &DataFrame { &self.unit_data }

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
            self.unit_data.column("centroid_lon").ok()
                .and_then(|column| column.f64().ok()),
            self.unit_data.column("centroid_lat").ok()
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

    /// Construct a graph representation of the layer for partitioning.
    /// Requires data, adjacencies, and shared_perimeters to be computed first.
    pub(super) fn construct_graph(&mut self) {
        assert!(self.unit_data.height() != 0, "DataFrame must be populated before constructing graph");

        let weights_i64 = self.unit_data.get_columns().iter()
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

        let weights_f64 = self.unit_data.get_columns().iter()
            .map(|column| (column.name().to_string(), column))
            .filter_map(|(name, column)| match column.dtype() {
                DataType::Float64 => Some((name, column.f64().unwrap().into_no_null_iter().collect())),
                DataType::Float32 => Some((name, column.f32().unwrap().into_no_null_iter().map(|v| v as f64).collect())),
                _ => None,
            }).collect();

        let weights = WeightMatrix::new(self.len(), weights_i64, weights_f64);

        // Use empty hulls if not available (hulls are optional)
        // Create a local empty Vec that lives long enough for the function call
        let empty_hulls: Vec<geo::Polygon<f64>> = Vec::new();
        let hulls = self.hulls().unwrap_or(&empty_hulls);
        
        self.graph = Arc::new(WeightedGraph::new(
            self.len(),
            &self.adjacencies,
            &self.edge_lengths,
            weights,
            hulls,
        ));
    }

    /// Get an Arc clone of the graph representation of this layer.
    #[inline] pub(crate) fn get_graph_ref(&self) -> Arc<WeightedGraph> { self.graph.clone() }
}

impl fmt::Debug for MapLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapLayer")
            .field("ty", &self.ty)
            .field("n", &self.geo_ids.len())
            .field("data", &format_args!("{}x{}", self.unit_data.height(), self.unit_data.width()))
            .field("adj", &!self.adjacencies.is_empty())
            .field("geom", &self.geoms.is_some())
            .finish()
    }
}
