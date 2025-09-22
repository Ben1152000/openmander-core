use geo::{ConvexHull, Polygon};

use crate::Geometries;

/// Approximate the convex hull to a polygon with a limited number of vertices.
fn approximate_hull(hull: &Polygon<f64>, num_points: usize) -> Polygon<f64> {
    if hull.exterior().0.len() > num_points { return hull.clone() }

    // Simplify the convex hull to reduce vertex count, if necessary.
    // todo!()

    hull.clone()
}

impl Geometries {
    /// Compute the convex hulls of all MultiPolygons.
    #[inline]
    pub fn convex_hulls(&self) -> Vec<Polygon<f64>> {
        self.shapes().iter().map(|polygon| polygon.convex_hull()).collect()
    }

    /// Compute the approximate convex hulls of all MultiPolygons, simplified to `num_points` vertices.
    #[inline]
    pub fn approximate_hulls(&self, num_points: usize) -> Vec<Polygon<f64>> {
        self.shapes().iter()
            .map(|polygon| polygon.convex_hull())
            .map(|hull| approximate_hull(&hull, num_points))
            .collect()
    }
}
