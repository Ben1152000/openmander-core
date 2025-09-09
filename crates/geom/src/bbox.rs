use geo::{Rect};
use rstar::{RTreeObject, AABB};

/// A bounding box in an R-tree, associated with a MultiPolygon by index.
#[derive(Debug, Clone)]
pub struct BoundingBox {
    idx: usize, // Index of corresponding MultiPolygon in geoms
    bbox: Rect<f64>,
}

impl BoundingBox {
    pub fn new(idx: usize, bbox: Rect<f64>) -> Self {
        Self { idx, bbox }
    }

    /// Get the index of the corresponding MultiPolygon.
    pub fn idx(&self) -> usize { self.idx }

    /// Get a reference to the bounding rectangle.
    pub fn bbox(&self) -> &Rect<f64> { &self.bbox }
}

impl RTreeObject for BoundingBox {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners(self.bbox.min().into(), self.bbox.max().into())
    }
}
