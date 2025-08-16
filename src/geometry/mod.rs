pub mod adjacency;
pub mod crosswalk;
pub mod intersection;

use geo::{BoundingRect, MultiPolygon, Rect};
use rstar::{RTree, RTreeObject, AABB};

#[derive(Debug, Clone)]
pub struct BoundingBox {
    idx: usize, // Index of corresponding MultiPolygon in geoms
    bbox: Rect<f64>,
}

impl RTreeObject for BoundingBox {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners(self.bbox.min().into(), self.bbox.max().into())
    }
}

#[derive(Debug, Clone)]
pub struct PlanarPartition {
    pub geoms: Vec<MultiPolygon<f64>>,
    pub rtree: RTree<BoundingBox>,
    pub adj_list: Vec<Vec<u32>>,
}

impl PlanarPartition {
    /// Construct a PlanarPartition object from a vector of MultiPolygons
    pub fn new(polygons: Vec<MultiPolygon<f64>>) -> Self {
        Self {
            adj_list: polygons.iter().map(|_| Vec::new()).collect(),
            rtree: RTree::bulk_load(polygons.iter().enumerate()
                .map(|(i, poly)| BoundingBox { idx: i, bbox: poly.bounding_rect().unwrap() })
                .collect()),
            geoms: polygons,
        }
    }

    #[inline] pub fn len(&self) -> usize { self.geoms.len() }

    #[inline] pub fn is_empty(&self) -> bool { self.geoms.is_empty() }
}
