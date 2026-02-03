//! Geometry processing utilities for SVG generation.

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use geo::{Coord, CoordsIter, LineString, MultiPolygon};

/// Quantization scale (1e-7 deg ≈ 1 cm at equator). Adjust if your data is projected.
const Q_SCALE: f64 = 1e7;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct QuantizedPoint(pub i64, pub i64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct Segment(pub QuantizedPoint, pub QuantizedPoint); // undirected: stored with canonical (min,max) endpoint order

#[derive(Default, Clone)]
pub(crate) struct SegmentSet(pub HashSet<Segment>);

#[inline]
pub(crate) fn qpt(c: &Coord<f64>) -> QuantizedPoint {
    QuantizedPoint((c.x * Q_SCALE).round() as i64, (c.y * Q_SCALE).round() as i64)
}

#[inline]
pub(crate) fn seg_norm(a: QuantizedPoint, b: QuantizedPoint) -> Segment {
    if (a.0, a.1) <= (b.0, b.1) { Segment(a, b) } else { Segment(b, a) }
}

/// Collect all segments from a MultiPolygon (exteriors and holes).
pub(crate) fn collect_segments(mp: &MultiPolygon<f64>, ptmap: &mut HashMap<QuantizedPoint, Coord<f64>>) -> SegmentSet {
    let mut set = SegmentSet::default();
    for poly in &mp.0 {
        collect_ring_segments(poly.exterior(), &mut set, ptmap);
        for hole in poly.interiors() {
            collect_ring_segments(hole, &mut set, ptmap);
        }
    }
    set
}

/// Collect segments from a ring (LineString).
pub(crate) fn collect_ring_segments(ring: &LineString<f64>, set: &mut SegmentSet, ptmap: &mut HashMap<QuantizedPoint, Coord<f64>>) {
    let mut prev: Option<Coord<f64>> = None;
    for c in ring.coords_iter().map(|c| Coord { x: c.x, y: c.y }) {
        if let Some(p) = prev {
            let qa = qpt(&p);
            let qb = qpt(&c);
            set.0.insert(seg_norm(qa, qb));
            ptmap.entry(qa).or_insert(p);
            ptmap.entry(qb).or_insert(c);
        }
        prev = Some(c);
    }
    // close the ring
    if let (Some(first), Some(last)) = (ring.0.first(), ring.0.last()) {
        let qa = qpt(&Coord { x: last.x, y: last.y });
        let qb = qpt(&Coord { x: first.x, y: first.y });
        set.0.insert(seg_norm(qa, qb));
        ptmap.entry(qa).or_insert(Coord { x: last.x, y: last.y });
        ptmap.entry(qb).or_insert(Coord { x: first.x, y: first.y });
    }
}

/// Turn a set of undirected boundary segments into closed rings (list of coords).
pub(crate) fn polygonize_rings(boundary: &SegmentSet, ptmap: &HashMap<QuantizedPoint, Coord<f64>>) -> Result<Vec<Vec<Coord<f64>>>> {
    // adjacency (multi-graph): QPt -> multiset of neighbors
    let mut adj: HashMap<QuantizedPoint, Vec<QuantizedPoint>> = HashMap::new();
    for &Segment(a, b) in &boundary.0 {
        adj.entry(a).or_default().push(b);
        adj.entry(b).or_default().push(a);
    }

    // Helper to remove one undirected edge (a<->b)
    let remove_edge = |a: QuantizedPoint, b: QuantizedPoint, adj: &mut HashMap<QuantizedPoint, Vec<QuantizedPoint>>| {
        if let Some(v) = adj.get_mut(&a) {
            if let Some(pos) = v.iter().position(|&x| x == b) {
                v.swap_remove(pos);
            }
        }
        if let Some(v) = adj.get_mut(&b) {
            if let Some(pos) = v.iter().position(|&x| x == a) {
                v.swap_remove(pos);
            }
        }
    };

    // Walk cycles
    let mut rings: Vec<Vec<Coord<f64>>> = Vec::new();

    // Collect all nodes with degree > 0
    let nodes: Vec<QuantizedPoint> = adj.iter().filter(|(_, v)| !v.is_empty()).map(|(k, _)| *k).collect();

    while let Some(&start) = nodes.iter().find(|&&n| adj.get(&n).map_or(false, |v| !v.is_empty())) {
        let mut ring_q: Vec<QuantizedPoint> = Vec::new();
        ring_q.push(start);

        // Pick an arbitrary first neighbor
        let mut curr = start;
        let mut next = {
            let v = adj.get(&curr).ok_or_else(|| anyhow!("Broken adjacency"))?;
            *v.last().ok_or_else(|| anyhow!("Isolated vertex in boundary"))?
        };
        remove_edge(curr, next, &mut adj);
        ring_q.push(next);

        let mut prev = curr;
        curr = next;

        // Follow edges until we close the loop
        loop {
            let v = adj.get(&curr).ok_or_else(|| anyhow!("Broken adjacency"))?;
            if v.is_empty() {
                // Degenerate—shouldn't happen with valid polygon boundaries
                break;
            }
            // Prefer continuing direction (avoid going back)
            let &cand = v.iter().find(|&&u| u != prev).unwrap_or(&v[0]);
            next = cand;
            remove_edge(curr, next, &mut adj);
            ring_q.push(next);

            if next == start {
                break;
            }
            prev = curr;
            curr = next;
        }

        // Map back to f64 coords (drop duplicated last if present)
        if let Some(last) = ring_q.last() {
            if *last == start {
                ring_q.pop();
            }
        }
        let ring_coords = ring_q
            .into_iter()
            .map(|q| *ptmap.get(&q).expect("missing ptmap coord"))
            .collect();

        rings.push(ring_coords);

        // Update 'nodes' cache (optional: leave as-is since we query adj each time)
    }

    Ok(rings)
}

