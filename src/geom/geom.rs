use geo::{BooleanOps, BoundingRect, Centroid, Coord, MultiPolygon, Point, Rect};
use rstar::{RTree, AABB};

use crate::geom::BoundingBox;

/// Geometries represents a collection of non-overlapping MultiPolygons with spatial relationships.
#[derive(Debug, Clone)]
pub(crate) struct Geometries {
    shapes: Vec<MultiPolygon<f64>>,
    rtree: RTree<BoundingBox>,
    epsg: Option<u32>, // EPSG code, if known
}

impl Geometries {
    /// Construct a Geometries object from a vector of MultiPolygons
    pub(crate) fn new(polygons: &[MultiPolygon<f64>], epsg: Option<u32>) -> Self {
        Self {
            rtree: RTree::bulk_load(
                polygons.iter().enumerate()
                    .map(|(i, polygon)| BoundingBox::new(i, polygon.bounding_rect().unwrap()))
                    .collect()
            ),
            shapes: polygons.to_vec(),
            epsg,
        }
    }

    /// Get the number of MultiPolygons.
    #[inline] pub(crate) fn len(&self) -> usize { self.shapes.len() }

    /// Check if there are no MultiPolygons.
    #[inline] pub(crate) fn is_empty(&self) -> bool { self.shapes.is_empty() }

    /// Get a reference to the list of MultiPolygons.
    #[inline] pub(crate) fn shapes(&self) -> &Vec<MultiPolygon<f64>> { &self.shapes }

    /// Get the EPSG code, or default to 4269 (NAD83 lon/lat) if unknown.
    #[inline] pub(crate) fn epsg(&self) -> u32 { self.epsg.unwrap_or(4269) }

    /// Query the R-tree for bounding boxes intersecting the given envelope.
    #[inline]
    pub(super) fn query(&self, envelope: &AABB<[f64; 2]>) -> impl Iterator<Item=&BoundingBox> {
        self.rtree.locate_in_envelope_intersecting(envelope)
    }

    /// Compute the bounding rectangle of all MultiPolygons.
    #[inline]
    pub(crate) fn bounds(&self) -> Option<Rect<f64>> {
        self.shapes.iter()
            .filter_map(|polygon| polygon.bounding_rect())
            .reduce(|a, b| Rect::new(
                Coord {
                    x: a.min().x.min(b.min().x),
                    y: a.min().y.min(b.min().y),
                },
                Coord {
                    x: a.max().x.max(b.max().x),
                    y: a.max().y.max(b.max().y),
                }
            ))
    }

    /// Compute the centroids of all MultiPolygons.
    #[inline]
    pub(crate) fn centroids(&self) -> Vec<Point<f64>> {
        self.shapes.iter()
            .map(|polygon| polygon.centroid()
                .unwrap_or(Point::new(f64::NAN, f64::NAN)))
            .collect()
    }

    /// Compute the union of all MultiPolygons into a single MultiPolygon.
    /// This method may be slow for large numbers of complex polygons.
    #[inline]
    pub(crate) fn union(&self) -> Option<MultiPolygon<f64>> {
        self.shapes.iter().cloned().reduce(|a, b| a.union(&b))
    }
}
