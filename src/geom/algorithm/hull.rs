use std::{cmp::{Ordering}, collections::BinaryHeap};

use geo::{Area, ConvexHull, Coord, CoordsIter, Polygon};

use crate::geom::Geometries;

/// Approximate the convex hull to a polygon with a limited number of vertices.
/// Uses a greedy "smallest ear first" removal (Visvalingam-Whyatt style).
/// `max_points` determines the maximum number of vertices in the simplified hull.
/// `min_area` determines the minimum area of the simplified hull as a fraction of the original hull area.
fn approximate_hull(hull: &Polygon<f64>, max_points: usize, min_area: f64) -> Polygon<f64> {
    debug_assert!(max_points >= 3, "num_points must be at least 3");
    debug_assert!(min_area >= 0.0 && min_area < 1.0, "min_area must be in [0, 1)");

    // Check if hull is already below the threshold.
    if hull.exterior().coords_count().saturating_sub(1) <= max_points { return hull.clone() }

    let mut points = hull.exterior().coords_iter().collect::<Vec<_>>();
    points.pop(); // Remove the duplicate closing coord.

    #[inline]
    fn triangle_area(a: Coord<f64>, b: Coord<f64>, c: Coord<f64>) -> f64 {
        ((b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)).abs() * 0.5
    }

    // Circular "linked list" via index arrays.
    let mut prev = (0..points.len())
        .map(|i| if i == 0 { points.len() - 1 } else { i - 1 })
        .collect::<Vec<_>>();
    let mut next = (0..points.len())
        .map(|i| if i + 1 == points.len() { 0 } else { i + 1 })
        .collect::<Vec<_>>();
    let mut alive = vec![true; points.len()];
    let mut updated: Vec<u32> = vec![0; points.len()];

    // Use a binary min-heap to keep track of the next point to eliminate
    #[derive(Copy, Clone, Eq, PartialEq)]
    struct Entry {
        area_bits: u64, // f64::to_bits() is monotone for non-negative values
        i: usize,
        v: u32,
    }

    impl Ord for Entry {
        fn cmp(&self, other: &Self) -> Ordering {
            // Reverse so the smallest area pops first.
            other.area_bits.cmp(&self.area_bits)
                .then_with(|| other.v.cmp(&self.v))
                .then_with(|| other.i.cmp(&self.i))
        }
    }

    impl PartialOrd for Entry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
    }

    let mut heap = BinaryHeap::<Entry>::with_capacity(points.len());

    // Compute the initial area for each hull point.
    for i in 0..points.len() {
        let area = triangle_area(points[prev[i]], points[i], points[next[i]]);
        heap.push(Entry { area_bits: area.to_bits(), i, v: updated[i] });
    }

    // Keep track of how much of the original hull area has been removed.
    let area_budget = (1.0 - min_area) * hull.unsigned_area();
    let mut removed_area = 0.0;

    // Progressively remove the point from the hull that provides the smallest added area.
    let mut n = points.len();
    while n > max_points {
        let Some(Entry { area_bits, i, v, .. }) = heap.pop() else { break };
        if !alive[i] || updated[i] != v { continue }

        // Check if removing this vertex exceeds the area budget.
        removed_area += f64::from_bits(area_bits);
        if removed_area > area_budget { break }

        // Remove vertex i and link its neighbors together.
        alive[i] = false;
        n -= 1;

        next[prev[i]] = next[i];
        prev[next[i]] = prev[i];

        // Update neighbor keys.
        for &j in &[prev[i], next[i]] {
            if alive[j] {
                updated[j] = updated[j].wrapping_add(1);
                let area = triangle_area(points[prev[j]], points[j], points[next[j]]);
                heap.push(Entry { area_bits: area.to_bits(), i: j, v: updated[j] });
            }
        }
    }

    // Rebuild the simplified ring.
    let mut i = (0..points.len()).find(|&i| alive[i])
        .expect("no remaining vertices in hull");

    let mut ring = Vec::with_capacity(n + 1);
    for _ in 0 .. n + 1 {
        ring.push(points[i]);
        i = next[i];
    }

    Polygon::new(ring.into(), vec![])
}

impl Geometries {
    /// Compute the convex hulls of all MultiPolygons.
    #[inline]
    pub(crate) fn convex_hulls(&self) -> Vec<Polygon<f64>> {
        self.shapes().iter().map(|polygon| polygon.convex_hull()).collect()
    }

    /// Compute the approximate convex hulls of all MultiPolygons, simplified to `num_points` vertices.
    #[inline]
    pub(crate) fn approximate_hulls(&self, max_points: usize, min_area: f64) -> Vec<Polygon<f64>> {
        self.shapes().iter()
            .map(|polygon| polygon.convex_hull())
            .map(|hull| approximate_hull(&hull, max_points, min_area))
            .collect()
    }
}
