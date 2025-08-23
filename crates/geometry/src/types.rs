use geo::{BoundingRect, MultiPolygon, Rect};
use rstar::{RTree, RTreeObject, AABB};

#[derive(Debug, Clone)]
pub struct BoundingBox {
    pub idx: usize, // Index of corresponding MultiPolygon in geoms
    pub bbox: Rect<f64>,
}

impl RTreeObject for BoundingBox {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners(self.bbox.min().into(), self.bbox.max().into())
    }
}

/// PlanarPartition represents a collection of non-overlapping MultiPolygons with spatial relationships.
#[derive(Debug, Clone)]
pub struct PlanarPartition {
    pub shapes: Vec<MultiPolygon<f64>>,
    pub adjacencies: Vec<Vec<u32>>,
    pub shared_perimeters: Vec<Vec<f64>>,
    pub rtree: RTree<BoundingBox>,
}

impl PlanarPartition {
    /// Construct a PlanarPartition object from a vector of MultiPolygons
    pub fn new(polygons: Vec<MultiPolygon<f64>>) -> Self {
        Self {
            adjacencies: polygons.iter().map(|_| Vec::new()).collect(),
            shared_perimeters: polygons.iter().map(|_| Vec::new()).collect(),
            rtree: RTree::bulk_load(polygons.iter().enumerate()
                .map(|(i, poly)| BoundingBox { idx: i, bbox: poly.bounding_rect().unwrap() })
                    .collect()),
            shapes: polygons,
        }
    }

    #[inline] pub fn len(&self) -> usize { self.shapes.len() }

    #[inline] pub fn is_empty(&self) -> bool { self.shapes.is_empty() }
}
