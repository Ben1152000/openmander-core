use std::{collections::HashMap, fmt, sync::Arc};

use geo::{MultiPolygon, Point};

use geograph::Region;

use crate::{graph::{UnitGraph, WeightMatrix}, map::{GeoId, GeoType, ParentRefs}};

/// A single planar partition Layer of the map, containing entities and their relationships.
#[derive(Clone)]
pub struct MapLayer {
    ty: GeoType,
    pub(super) geo_ids: Vec<GeoId>,
    pub(super) index: HashMap<GeoId, u32>,        // Map between geo_ids and per-level contiguous indices
    pub(super) parents: Vec<ParentRefs>,          // References to parent entities (higher level types)
    pub(super) unit_names: Vec<String>,           // Human-readable name for each unit (e.g. "Block 1234")
    pub(super) unit_weights: Arc<WeightMatrix>,   // Demographic/election weights
    pub(super) region: Arc<Region>,               // Planar map (geometry + adjacency + edge weights)
}

impl MapLayer {
    /// Create a fully-initialized layer from pre-computed parts.
    pub(crate) fn new(
        ty: GeoType,
        geo_ids: Vec<GeoId>,
        index: HashMap<GeoId, u32>,
        parents: Vec<ParentRefs>,
        unit_names: Vec<String>,
        unit_weights: Arc<WeightMatrix>,
        region: Arc<Region>,
    ) -> Self {
        Self { ty, geo_ids, index, parents, unit_names, unit_weights, region }
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

    /// Get a reference to the human-readable unit names for this layer.
    #[inline] pub fn unit_names(&self) -> &Vec<String> { &self.unit_names }

    /// Get a reference to the WeightMatrix for this layer.
    #[inline] pub(crate) fn weights(&self) -> &WeightMatrix { &self.unit_weights }

    /// Get the union of all MultiPolygons in this layer into a single MultiPolygon.
    /// Note that this can be computationally expensive for large layers.
    #[inline]
    pub fn union(&self) -> MultiPolygon<f64> {
        self.region.union_of(self.region.unit_ids())
    }

    /// Get centroid lon/lat for each entity from the WeightMatrix.
    pub fn centroids(&self) -> Vec<Point<f64>> {
        (0..self.len()).map(|i| Point::new(
            self.unit_weights.get_as_f64("centroid_lon", i).unwrap_or(f64::NAN),
            self.unit_weights.get_as_f64("centroid_lat", i).unwrap_or(f64::NAN),
        )).collect()
    }

    /// Get the unit graph for this layer.
    pub(crate) fn get_unit_graph(&self) -> UnitGraph {
        UnitGraph(self.region.clone())
    }

    /// Get an Arc clone of the unit weights for this layer.
    #[inline] pub(crate) fn get_unit_weights(&self) -> Arc<WeightMatrix> { self.unit_weights.clone() }

    /// Get a reference to the Region for this layer.
    #[inline] pub(crate) fn region(&self) -> &Region { &self.region }
}

impl fmt::Debug for MapLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapLayer")
            .field("ty", &self.ty)
            .field("n", &self.geo_ids.len())
            .field("region_units", &self.region.num_units())
            .finish()
    }
}
