use std::collections::HashMap;

use geo::{BooleanOps, Coord, Distance, Geodesic, GeodesicArea, LineString, MultiPolygon, Point};

use crate::Geometries;

impl Geometries {
    /// For each polygon and its adjacency list, compute the shared perimeter with each neighbor.
    pub fn compute_shared_perimeters(&mut self, adjacencies: &Vec<Vec<u32>>) -> Vec<Vec<f64>>{
        /// Length of shared boundary between two (mutually adjacent) multipolygons.
        fn shared_perimeter(a: &MultiPolygon<f64>, b: &MultiPolygon<f64>) -> f64 {
            (a.geodesic_perimeter() + b.geodesic_perimeter() - a.union(b).geodesic_perimeter()) / 2.0
        }

        let mut shared_perimeters: Vec<Vec<f64>> = vec![Vec::new(); self.len()];
        for (i, neighbors) in adjacencies.iter().enumerate() {
            shared_perimeters[i] = neighbors.iter().map(|&j | {
                shared_perimeter(&self.shapes[i], &self.shapes[j as usize])
            }).collect();
        }

        shared_perimeters
    }

    /// Compute shared perimeters by matching identical edges (fast; no boolean ops).
    /// `scale` controls float -> integer key rounding (e.g., 1e9 for ~1e-9°).
    pub fn compute_shared_perimeters_fast(&self, adjacencies: &Vec<Vec<u32>>, scale: f64) -> Vec<Vec<f64>> {
        #[derive(Clone, Copy, PartialEq, Eq, Hash)]
        struct EdgeKey { ax: i64, ay: i64, bx: i64, by: i64 }

        #[inline]
        fn q(c: Coord<f64>, s: f64) -> (i64, i64) {
            ((c.x * s).round() as i64, (c.y * s).round() as i64)
        }
        #[inline]
        fn edge_key(a: Coord<f64>, b: Coord<f64>, s: f64) -> EdgeKey {
            let (ax, ay) = q(a, s);
            let (bx, by) = q(b, s);
            // normalize so (a,b) == (b,a)
            if (ax, ay) <= (bx, by) {
                EdgeKey { ax, ay, bx, by }
            } else {
                EdgeKey { ax: bx, ay: by, bx: ax, by: ay }
            }
        }
        #[inline]
        fn pair_key(a: u32, b: u32) -> u64 {
            let (lo, hi) = if a < b { (a, b) } else { (b, a) };
            ((hi as u64) << 32) | (lo as u64)
        }
        #[inline]
        fn ring_lines<'a>(ring: &'a LineString<f64>) -> impl Iterator<Item=(Coord<f64>, Coord<f64>)> + 'a {
            // Assumes closed rings (first == last). If not closed, add closing edge yourself.
            ring.0.windows(2).map(|w| (w[0], w[1]))
        }

        let n = self.shapes.len();
        // Map an edge to (owner polygon, length). When we see the same edge again, we know the neighbor.
        let mut edge_owner: HashMap<EdgeKey, (u32, f64)> = HashMap::new();
        // Sum of shared lengths per unordered polygon pair.
        let mut pair_len: HashMap<u64, f64> = HashMap::new();

        for (pi, mp) in self.shapes.iter().enumerate() {
            let pid = pi as u32;
            for poly in mp {
                // exterior + interiors
                for ring in std::iter::once(poly.exterior()).chain(poly.interiors().iter()) {
                    for (a, b) in ring_lines(ring) {
                        // geodesic segment length
                        let len = Geodesic.distance(Point::from(a), Point::from(b));
                        if len == 0.0 { continue; }

                        let key = edge_key(a, b, scale);
                        if let Some((other, _)) = edge_owner.remove(&key) {
                            // matched with neighbor polygon
                            let k = pair_key(pid, other);
                            *pair_len.entry(k).or_insert(0.0) += len;
                        } else {
                            edge_owner.insert(key, (pid, len));
                        }
                    }
                }
            }
        }

        // Build result aligned to existing adjacency lists
        let mut out: Vec<Vec<f64>> = Vec::with_capacity(n);
        for (i, nbrs) in adjacencies.iter().enumerate() {
            let pid = i as u32;
            let row: Vec<f64> = nbrs.iter().map(|&j| {
                let k = pair_key(pid, j);
                pair_len.get(&k).copied().unwrap_or(0.0)
            }).collect();
            out.push(row);
        }
        out
    }
}
