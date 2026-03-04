use geo::Rect;
use rstar::{RTree, RTreeObject, AABB};

use crate::unit::UnitId;

// ---------------------------------------------------------------------------
// BoundingBox (R-tree element)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct UnitBBox {
    unit: UnitId,
    bbox: Rect<f64>,
}

impl RTreeObject for UnitBBox {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners(self.bbox.min().into(), self.bbox.max().into())
    }
}

// ---------------------------------------------------------------------------
// SpatialIndex
// ---------------------------------------------------------------------------

/// R-tree spatial index over unit bounding boxes.
///
/// Supports fast point-location and envelope queries.  Built once during
/// `Region::new()` / `io::read()` and never mutated.
#[derive(Debug, Clone)]
pub(crate) struct SpatialIndex {
    tree: RTree<UnitBBox>,
}

impl SpatialIndex {
    /// Build a spatial index from per-unit bounding boxes.
    pub(crate) fn new(bounds: &[Rect<f64>]) -> Self {
        let entries: Vec<UnitBBox> = bounds
            .iter()
            .enumerate()
            .map(|(i, &bbox)| UnitBBox {
                unit: UnitId(i as u32),
                bbox,
            })
            .collect();
        Self {
            tree: RTree::bulk_load(entries),
        }
    }

    /// Return all `UnitId`s whose bounding box intersects `envelope`.
    #[inline]
    pub(crate) fn query(&self, envelope: AABB<[f64; 2]>) -> impl Iterator<Item = UnitId> + '_ {
        self.tree
            .locate_in_envelope_intersecting(&envelope)
            .map(|e| e.unit)
    }

    /// Return all `UnitId`s whose bounding box contains `point`.
    #[inline]
    pub(crate) fn query_point(&self, point: [f64; 2]) -> impl Iterator<Item = UnitId> + '_ {
        let envelope = AABB::from_point(point);
        self.query(envelope)
    }
}
