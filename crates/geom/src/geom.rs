use geo::{BoundingRect, MultiPolygon};
use rstar::{RTree, AABB};

use crate::bbox::BoundingBox;

/// Geometries represents a collection of non-overlapping MultiPolygons with spatial relationships.
#[derive(Debug, Clone)]
pub struct Geometries {
    pub(crate) shapes: Vec<MultiPolygon<f64>>,
    rtree: RTree<BoundingBox>,
}

impl Geometries {
    /// Construct a Geometries object from a vector of MultiPolygons
    pub fn new(polygons: &[MultiPolygon<f64>]) -> Self {
        Self {
            rtree: RTree::bulk_load(
                polygons.iter().enumerate()
                    .map(|(i, polygon)| BoundingBox::new(i, polygon.bounding_rect().unwrap()))
                    .collect()
            ),
            shapes: polygons.to_vec(),
        }
    }

    /// Get the number of MultiPolygons.
    #[inline] pub fn len(&self) -> usize { self.shapes.len() }

    /// Check if there are no MultiPolygons.
    #[inline] pub fn is_empty(&self) -> bool { self.shapes.is_empty() }

    /// Get a reference to the list of MultiPolygons.
    #[inline] pub fn shapes(&self) -> &Vec<MultiPolygon<f64>> { &self.shapes }

    /// Query the R-tree for bounding boxes intersecting the given envelope.
    #[inline] pub fn query(&self, envelope: &AABB<[f64; 2]>) -> impl Iterator<Item=&BoundingBox> {
        self.rtree.locate_in_envelope_intersecting(envelope)
    }
}
