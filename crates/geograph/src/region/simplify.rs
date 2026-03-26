//! Topology-preserving geometry simplification for [`Region`].
//!
//! Standard Douglas-Peucker applied per-polygon creates tiny gaps at shared
//! boundaries because adjacent polygons simplify their shared edge independently.
//! This module fixes that by operating on *arcs* — maximal chains of half-edges
//! whose interior vertices have out-degree exactly 2 (i.e., they lie strictly
//! between two junction vertices).  Each arc is simplified exactly once; both
//! faces that share it receive the identical simplified coordinate sequence
//! (one forward, one reversed), so no gaps can appear at shared boundaries.
//!
//! # Algorithm
//!
//! 1. Compute vertex out-degree from the DCEL half-edge list.
//! 2. Walk every arc (pass 1: junction-anchored; pass 2: isolated loops) and
//!    simplify its coordinate sequence with Douglas-Peucker.
//! 3. For each unit, walk its face cycles and reassemble rings from the
//!    pre-simplified arc segments.

use geo::{Coord, LineString, MultiPolygon, Polygon};

use crate::dcel::{Dcel, HalfEdgeId};
use super::Region;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

impl Region {
    /// Return simplified geometries for all units, preserving shared topology.
    ///
    /// Each arc (maximal chain of half-edges whose interior vertices have
    /// out-degree exactly 2) is simplified once with Douglas-Peucker; adjacent
    /// units share the identical simplified coordinates, eliminating gaps at
    /// shared boundaries.
    ///
    /// At `tolerance == 0.0` the original geometries are cloned unchanged.
    /// Units that collapse to fewer than 3 points after simplification are
    /// represented by an empty `MultiPolygon`.
    pub fn simplified_geometries(&self, tolerance: f64) -> Vec<MultiPolygon<f64>> {
        if tolerance == 0.0 {
            return self.geometries.clone();
        }

        let dcel = &self.dcel;
        let n_he = dcel.num_half_edges();

        // ── Step 1: vertex out-degree ─────────────────────────────────────────
        let mut out_degree: Vec<u32> = vec![0; dcel.num_vertices()];
        for i in 0..n_he {
            out_degree[dcel.half_edge(HalfEdgeId(i)).origin.0] += 1;
        }

        // ── Step 2: walk arcs and simplify ────────────────────────────────────
        //
        // arc_id[he]     = canonical start half-edge index of the arc he belongs to
        // arc_fwd[he]    = true if he is traversed forward in its canonical arc
        // arc_store      = simplified coords per arc (indexed by arc_store_idx)
        // arc_by_start   = arc_store_idx for a given canonical start he index
        let mut arc_id:      Vec<usize> = vec![usize::MAX; n_he];
        let mut arc_fwd:     Vec<bool>  = vec![true;        n_he];
        let mut arc_store:   Vec<Vec<Coord<f64>>> = Vec::new();
        let mut arc_by_start: Vec<usize> = vec![usize::MAX; n_he];
        let mut visited:     Vec<bool>  = vec![false; n_he];

        // Pass 1: arcs whose starting vertex is a junction (out-degree != 2).
        for start_id in 0..n_he {
            if visited[start_id] { continue; }
            let start  = HalfEdgeId(start_id);
            let origin = dcel.half_edge(start).origin;
            if out_degree[origin.0] == 2 { continue; } // not a junction start

            let mut raw: Vec<Coord<f64>> = vec![dcel.vertex(origin).coords];
            let mut cur = start;
            loop {
                let twin = dcel.half_edge(cur).twin;
                // Mark both the forward half-edge and its twin as visited.
                // Without marking the twin, a twin whose origin is a junction
                // vertex would later be mistaken for a new arc start, overwriting
                // the arc_id/arc_fwd assignments made here and corrupting ring
                // reconstruction for any face that traverses this arc in reverse.
                visited[cur.0]  = true;
                visited[twin.0] = true;
                arc_id[cur.0]   = start_id;
                arc_fwd[cur.0]  = true;
                arc_id[twin.0]  = start_id;
                arc_fwd[twin.0] = false;

                let dest_v = dcel.dest(cur);
                raw.push(dcel.vertex(dest_v).coords);

                if out_degree[dest_v.0] != 2 { break; } // reached next junction
                cur = dcel.half_edge(cur).next;
                if cur == start { break; } // safety guard
            }

            let simplified = dp_simplify_open(&raw, tolerance);
            let idx = arc_store.len();
            arc_store.push(simplified);
            arc_by_start[start_id] = idx;
        }

        // Pass 2: isolated loop arcs (all vertices have out-degree 2).
        for start_id in 0..n_he {
            if visited[start_id] { continue; }

            let mut raw: Vec<Coord<f64>> = Vec::new();
            let mut cur = HalfEdgeId(start_id);
            loop {
                let twin = dcel.half_edge(cur).twin;
                visited[cur.0]  = true;
                visited[twin.0] = true; // same reason as pass 1
                arc_id[cur.0]   = start_id;
                arc_fwd[cur.0]  = true;
                arc_id[twin.0]  = start_id;
                arc_fwd[twin.0] = false;
                raw.push(dcel.vertex(dcel.half_edge(cur).origin).coords);
                cur = dcel.half_edge(cur).next;
                if cur.0 == start_id { break; }
            }

            let simplified = dp_simplify_closed(&raw, tolerance);
            let idx = arc_store.len();
            arc_store.push(simplified);
            arc_by_start[start_id] = idx;
        }

        // ── Step 3: reconstruct unit geometries from simplified arcs ──────────
        let mut result: Vec<MultiPolygon<f64>> = Vec::with_capacity(self.num_units());

        for unit in self.unit_ids() {
            let mut polygons: Vec<Polygon<f64>> = Vec::new();

            for &face_id in &self.unit_to_faces[unit.0 as usize] {
                let face = dcel.face(face_id);
                let primary_start = match face.half_edge {
                    Some(he) => he,
                    None => continue,
                };

                // Primary outer cycle + inner hole cycles (for donut-shaped units).
                let mut cycle_starts: Vec<HalfEdgeId> = vec![primary_start];
                for &inner in &self.face_inner_cycles[face_id.0] {
                    cycle_starts.push(inner);
                }

                let mut rings: Vec<LineString<f64>> = Vec::new();
                for cycle_start in cycle_starts {
                    let ring = collect_ring(
                        dcel, cycle_start,
                        &arc_id, &arc_fwd, &arc_store, &arc_by_start,
                    );
                    // Closed ring needs at least 3 distinct points + closing duplicate = 4 coords.
                    if ring.len() >= 4 {
                        rings.push(LineString::from(ring));
                    }
                }

                if !rings.is_empty() {
                    let exterior = rings.remove(0);
                    polygons.push(Polygon::new(exterior, rings));
                }
            }

            result.push(MultiPolygon(polygons));
        }

        result
    }
}

// ---------------------------------------------------------------------------
// Ring assembly
// ---------------------------------------------------------------------------

/// Assemble one simplified ring by walking the face cycle from `start`.
///
/// A half-edge is the start of a new arc run if and only if its `prev`
/// half-edge belongs to a different (arc_id, arc_fwd) pair.  Using the
/// DCEL `prev` link — rather than mutable state accumulated during the
/// walk — correctly handles the case where `face.half_edge` points into
/// the middle of an arc.  With mutable state, a mid-arc start causes the
/// arc to appear as two separate runs in the traversal (one before and one
/// after the intervening arcs), each triggering a full emission and
/// producing a "jump to the other end and back" artifact.
fn collect_ring(
    dcel:          &Dcel<Coord<f64>>,
    start:         HalfEdgeId,
    arc_id:        &[usize],
    arc_fwd:       &[bool],
    arc_store:     &[Vec<Coord<f64>>],
    arc_by_start:  &[usize],
) -> Vec<Coord<f64>> {
    let mut coords = Vec::new();
    let mut cur    = start;

    loop {
        let this_arc = arc_id[cur.0];
        let this_fwd = arc_fwd[cur.0];

        // Only emit when this half-edge is the first of its arc run in this
        // face cycle, i.e. when the previous half-edge belongs to a different
        // arc or traversal direction.
        let prev_he  = dcel.half_edge(cur).prev;
        if arc_id[prev_he.0] != this_arc || arc_fwd[prev_he.0] != this_fwd {
            let idx = arc_by_start[this_arc];
            let all = &arc_store[idx];
            let n   = all.len();
            // Emit all coords except the last.  The last coord is the
            // junction that starts the next arc and will be emitted then.
            if this_fwd {
                for c in all.iter().take(n.saturating_sub(1)) {
                    coords.push(*c);
                }
            } else {
                for c in all.iter().rev().take(n.saturating_sub(1)) {
                    coords.push(*c);
                }
            }
        }

        cur = dcel.half_edge(cur).next;
        if cur == start { break; }
    }

    // Fallback for single-arc cycles (loop arcs).
    //
    // The prev-based detection above never triggers when the entire cycle is one
    // arc, because every half-edge's `prev` belongs to that same arc — so the
    // condition `arc_id[prev] != this_arc` is never true and nothing is emitted.
    // This is the normal case for inner-ring (hole) boundaries of donut-shaped
    // units where the embedded neighbour shares no junction vertices with the
    // surrounding unit.  Emit the arc once unconditionally from `start`.
    if coords.is_empty() {
        let this_arc = arc_id[start.0];
        let this_fwd = arc_fwd[start.0];
        let idx = arc_by_start[this_arc];
        let all = &arc_store[idx];
        // Loop arcs have no "next arc" to emit the last vertex, so emit all n
        // coords (unlike the multi-arc case above which takes n-1).
        if this_fwd {
            for c in all.iter() { coords.push(*c); }
        } else {
            for c in all.iter().rev() { coords.push(*c); }
        }
    }

    // Close the ring by repeating the first coordinate.
    if let Some(&first) = coords.first() {
        coords.push(first);
    }

    coords
}

// ---------------------------------------------------------------------------
// Douglas-Peucker simplification
// ---------------------------------------------------------------------------

/// Simplify an open polyline (endpoints are always retained).
fn dp_simplify_open(coords: &[Coord<f64>], tolerance: f64) -> Vec<Coord<f64>> {
    if coords.len() <= 2 || tolerance == 0.0 {
        return coords.to_vec();
    }
    let mut result = Vec::new();
    result.push(coords[0]);
    dp_recurse(coords, 0, coords.len() - 1, tolerance, &mut result);
    result.push(*coords.last().unwrap());
    result
}

/// Simplify a closed ring (no duplicate endpoint in input; none added to output).
fn dp_simplify_closed(coords: &[Coord<f64>], tolerance: f64) -> Vec<Coord<f64>> {
    if coords.len() <= 2 || tolerance == 0.0 {
        return coords.to_vec();
    }
    // Treat as open line from coords[0] to coords[0] via the whole ring.
    let mut extended = coords.to_vec();
    extended.push(coords[0]);
    let mut result = dp_simplify_open(&extended, tolerance);
    result.pop(); // remove the duplicate closing point
    result
}

fn dp_recurse(
    coords:    &[Coord<f64>],
    start:     usize,
    end:       usize,
    tolerance: f64,
    result:    &mut Vec<Coord<f64>>,
) {
    if end <= start + 1 { return; }
    let p1 = coords[start];
    let p2 = coords[end];
    let mut max_dist = 0.0f64;
    let mut max_idx  = start;
    for i in (start + 1)..end {
        let d = dist_to_segment(coords[i], p1, p2);
        if d > max_dist { max_dist = d; max_idx = i; }
    }
    if max_dist > tolerance {
        dp_recurse(coords, start,   max_idx, tolerance, result);
        result.push(coords[max_idx]);
        dp_recurse(coords, max_idx, end,     tolerance, result);
    }
}

/// Perpendicular distance from point `p` to the segment `a→b`.
#[inline]
fn dist_to_segment(p: Coord<f64>, a: Coord<f64>, b: Coord<f64>) -> f64 {
    let dx  = b.x - a.x;
    let dy  = b.y - a.y;
    let len2 = dx * dx + dy * dy;
    if len2 == 0.0 {
        let ex = p.x - a.x;
        let ey = p.y - a.y;
        return (ex * ex + ey * ey).sqrt();
    }
    let t  = ((p.x - a.x) * dx + (p.y - a.y) * dy) / len2;
    let t  = t.clamp(0.0, 1.0);
    let ex = p.x - (a.x + t * dx);
    let ey = p.y - (a.y + t * dy);
    (ex * ex + ey * ey).sqrt()
}
