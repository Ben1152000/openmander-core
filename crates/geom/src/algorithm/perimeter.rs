use std::collections::HashMap;

use anyhow::Result;
use geo::{Coord, Euclidean, Length, Line, LineString};

use crate::Geometries;

impl Geometries {
    /// For each polygon and its adjacency list, compute the shared perimeter with each neighbor.
    /// Compute shared perimeters by matching identical edges (fast; no boolean ops).
    /// `scale` controls float -> integer key rounding (e.g., 1e9 for ~1e-9°).
    pub fn compute_shared_perimeters_fast(&self, adjacencies: &Vec<Vec<u32>>, scale: f64) -> Result<Vec<Vec<f64>>> {
        #[derive(Clone, Copy, PartialEq, Eq, Hash)]
        struct EdgeKey { ax: i64, ay: i64, bx: i64, by: i64 }

        #[inline]
        fn query(c: Coord<f64>, s: f64) -> (i64, i64) {
            ((c.x * s).round() as i64, (c.y * s).round() as i64)
        }

        #[inline]
        fn edge_key(a: Coord<f64>, b: Coord<f64>, s: f64) -> EdgeKey {
            let (ax, ay) = query(a, s);
            let (bx, by) = query(b, s);
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

        let projected_shapes = self.reproject_to_metric()?;

        // Map an edge to (owner polygon, length). When we see the same edge again, we know the neighbor.
        let mut edge_owner: HashMap<EdgeKey, (u32, f64)> = HashMap::new();
        // Sum of shared lengths per unordered polygon pair.
        let mut pair_len: HashMap<u64, f64> = HashMap::new();

        for (i, polygon) in projected_shapes.iter().enumerate() {
            for poly in polygon {
                // exterior + interiors
                for ring in std::iter::once(poly.exterior()).chain(poly.interiors().iter()) {
                    for (a, b) in ring_lines(ring) {
                        // let len = Geodesic.distance(Point::from(a), Point::from(b));
                        let len = Euclidean.length(&Line::new(a, b));
                        if len == 0.0 { continue }

                        let key = edge_key(a, b, scale);
                        if let Some((other, _)) = edge_owner.remove(&key) {
                            // matched with neighbor polygon
                            let key = pair_key(i as u32, other);
                            *pair_len.entry(key).or_insert(0.0) += len;
                        } else {
                            edge_owner.insert(key, (i as u32, len));
                        }
                    }
                }
            }
        }

        // Build result aligned to existing adjacency lists
        let mut out: Vec<Vec<f64>> = Vec::with_capacity(self.len());
        for (i, nbrs) in adjacencies.iter().enumerate() {
            let pid = i as u32;
            let row: Vec<f64> = nbrs.iter().map(|&j| {
                let k = pair_key(pid, j);
                pair_len.get(&k).copied().unwrap_or(0.0)
            }).collect();
            out.push(row);
        }

        Ok(out)
    }
}
