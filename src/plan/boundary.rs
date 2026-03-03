//! District boundary extraction using frontier edges.
//!
//! Algorithm:
//! 1. Get all frontier blocks from frontier edges (blocks that touch other districts)
//! 2. For each frontier block, find boundary edges shared with external neighbors
//! 3. For ALL blocks in the district, find state boundary edges (not shared with any neighbor)
//! 4. Stitch all boundary edges together to form closed rings

use std::collections::{HashMap, HashSet};
use geo::{BooleanOps, Coord, LineString, MultiPolygon, Polygon};

// NOTE: Everything from here down to `extract_district_boundary_union` is only
// reachable through `Plan::debug_frontier_info()`.  If that method is removed,
// QUANT, QCoord, get_block_edges, StitchDebugInfo, stitch_edges_with_debug,
// BoundaryDebugInfo, and extract_district_boundary_with_debug can all be deleted.

/// Quantization for coordinate matching.
///
/// 1e-6 degrees ≈ 10 cm, which is coarse enough to match adjacent-block
/// edges in both GeoParquet (exact coordinates) and PMTiles (tile-quantized,
/// ~1–10 m rounding).  False-sharing of state-boundary edges was the original
/// motivation for using a fine scale here, but that issue is now handled by
/// explicitly skipping self-edges in Phase 2 rather than by tightening QUANT.
const QUANT: f64 = 1e-6;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct QCoord {
    x: i64,
    y: i64,
}

impl QCoord {
    fn new(c: Coord<f64>) -> Self {
        Self {
            x: (c.x / QUANT).round() as i64,
            y: (c.y / QUANT).round() as i64,
        }
    }

    fn to_coord(self) -> Coord<f64> {
        Coord {
            x: self.x as f64 * QUANT,
            y: self.y as f64 * QUANT,
        }
    }
}

/// Get all edges from every ring (exterior + holes) of a block as quantized
/// canonical pairs.  Uses `coords().windows(2)` for consecutive segments and
/// explicitly adds the closing edge when the ring is stored open.
fn get_block_edges(shapes: &[MultiPolygon<f64>], block: usize) -> HashSet<(QCoord, QCoord)> {
    let mut edges = HashSet::new();
    for poly in &shapes[block].0 {
        for ring in std::iter::once(poly.exterior()).chain(poly.interiors().iter()) {
            let coords: Vec<_> = ring.coords().collect();
            let n = coords.len();
            if n < 2 {
                continue;
            }
            for window in coords.windows(2) {
                let a = QCoord::new(*window[0]);
                let b = QCoord::new(*window[1]);
                if a != b {
                    let canonical = if (a.x, a.y) <= (b.x, b.y) { (a, b) } else { (b, a) };
                    edges.insert(canonical);
                }
            }
            // If the ring is stored without the closing coordinate, add it.
            let first = QCoord::new(*coords[0]);
            let last = QCoord::new(*coords[n - 1]);
            if first != last {
                let canonical = if (last.x, last.y) <= (first.x, first.y) {
                    (last, first)
                } else {
                    (first, last)
                };
                edges.insert(canonical);
            }
        }
    }
    edges
}

/// Debug info from stitching
#[derive(Debug, Default)]
pub struct StitchDebugInfo {
    pub num_vertices: usize,
    pub degree_1_count: usize,
    pub degree_2_count: usize,
    pub degree_3_plus_count: usize,
    pub walks_attempted: usize,
    pub walks_closed: usize,
    pub walks_stuck: usize,
    pub max_walk_len: usize,
    /// Coordinates of degree-1 vertices (gaps in the boundary)
    pub degree_1_coords: Vec<(f64, f64)>,
}

/// Stitch edges into closed polygon rings.
/// When a walk hits a dead end (state boundary), it walks backwards from the start
/// and closes the ring with a straight line between the two endpoints.
fn stitch_edges_with_debug(edges: &[(QCoord, QCoord)]) -> (Vec<Vec<Coord<f64>>>, StitchDebugInfo) {
    let mut debug = StitchDebugInfo::default();

    if edges.is_empty() {
        return (vec![], debug);
    }

    // Build adjacency: point -> list of connected points
    let mut adj: HashMap<QCoord, Vec<QCoord>> = HashMap::new();
    for &(a, b) in edges {
        adj.entry(a).or_default().push(b);
        adj.entry(b).or_default().push(a);
    }

    debug.num_vertices = adj.len();
    for (coord, neighbors) in &adj {
        match neighbors.len() {
            1 => {
                debug.degree_1_count += 1;
                debug.degree_1_coords.push((coord.to_coord().x, coord.to_coord().y));
            }
            2 => debug.degree_2_count += 1,
            _ => debug.degree_3_plus_count += 1,
        }
    }

    // Track used edges (canonical form)
    let mut used: HashSet<(QCoord, QCoord)> = HashSet::new();
    let canonical = |a: QCoord, b: QCoord| -> (QCoord, QCoord) {
        if (a.x, a.y) <= (b.x, b.y) { (a, b) } else { (b, a) }
    };

    let mut rings = Vec::new();

    for &(start_a, start_b) in edges {
        let start_edge = canonical(start_a, start_b);
        if used.contains(&start_edge) {
            continue;
        }

        debug.walks_attempted += 1;
        let mut ring = vec![start_a, start_b];
        used.insert(start_edge);

        let mut prev = start_a;
        let mut curr = start_b;
        let mut forward_stuck = false;

        // Walk forward until we close or get stuck
        for _ in 0..edges.len() {
            let neighbors = match adj.get(&curr) {
                Some(n) => n,
                None => break,
            };

            // Find unused edge from curr (not going back)
            let next = neighbors.iter().find(|&&n| {
                let edge = canonical(curr, n);
                n != prev && !used.contains(&edge)
            }).copied();

            match next {
                Some(n) if n == start_a => {
                    // Closed the ring
                    ring.push(start_a);
                    used.insert(canonical(curr, n));
                    debug.walks_closed += 1;
                    break;
                }
                Some(n) => {
                    used.insert(canonical(curr, n));
                    ring.push(n);
                    prev = curr;
                    curr = n;
                }
                None => {
                    // Hit a dead end (likely state boundary)
                    forward_stuck = true;
                    break;
                }
            }
        }

        // If we got stuck going forward, try walking backward from start_a
        // to find the other end of the state boundary
        if forward_stuck && ring.len() >= 2 {
            // Walk backward: start from start_a, going away from start_b
            let mut back_ring: Vec<QCoord> = Vec::new();
            let mut back_prev = start_b;
            let mut back_curr = start_a;

            for _ in 0..edges.len() {
                let neighbors = match adj.get(&back_curr) {
                    Some(n) => n,
                    None => break,
                };

                // Find unused edge from back_curr (not going back to back_prev)
                let next = neighbors.iter().find(|&&n| {
                    let edge = canonical(back_curr, n);
                    n != back_prev && !used.contains(&edge)
                }).copied();

                match next {
                    Some(n) => {
                        used.insert(canonical(back_curr, n));
                        back_ring.push(n);
                        back_prev = back_curr;
                        back_curr = n;
                    }
                    None => {
                        // Hit the other end of state boundary
                        break;
                    }
                }
            }

            // Combine: back_ring (reversed) + ring + close with straight line
            if !back_ring.is_empty() {
                back_ring.reverse();
                let mut combined = back_ring;
                combined.extend(ring);
                // Close with straight line back to first point
                combined.push(combined[0]);
                ring = combined;
            } else {
                // No backward path, just close the forward ring
                ring.push(ring[0]);
            }

            debug.walks_stuck += 1;
        }

        debug.max_walk_len = debug.max_walk_len.max(ring.len());

        // Valid ring: at least 4 points (3 + closing) and closed
        if ring.len() >= 4 && ring.first() == ring.last() {
            rings.push(ring.iter().map(|q| q.to_coord()).collect());
        }
    }

    (rings, debug)
}

/// Debug info from boundary extraction
#[derive(Debug, Default)]
pub struct BoundaryDebugInfo {
    pub frontier_blocks: usize,
    pub boundary_edges_found: usize,
    pub state_border_blocks: usize,
    pub state_border_edges_added: usize,
    pub rings_stitched: usize,
    pub stitch: StitchDebugInfo,
}

/// Extract district boundary using frontier edges and assignments.
/// Returns (MultiPolygon, BoundaryDebugInfo)
///
/// Phase 1: Inter-district boundary edges from frontier blocks (shared with external neighbors).
/// Phase 2: State boundary edges from exterior frontier nodes (edges not shared with any neighbor).
///          `frontier_nodes` must be the full frontier set for this district (from the partition
///          MultiSet), which includes both inter-district frontier blocks and exterior-only blocks.
///          `is_state_border[block]` must be true for blocks on the state border.
pub fn extract_district_boundary_with_debug(
    shapes: &[MultiPolygon<f64>],
    adjacencies: &[Vec<u32>],
    frontier_nodes: &[usize],
    frontier_edges: &[(usize, usize)],
    assignments: &[u32],
    district: u32,
    is_state_border: &[bool],
) -> (MultiPolygon<f64>, BoundaryDebugInfo) {
    let mut debug = BoundaryDebugInfo::default();

    // Get unique frontier blocks and their external neighbors
    let mut external_neighbors: HashMap<usize, HashSet<usize>> = HashMap::new();
    for &(src, tgt) in frontier_edges {
        external_neighbors.entry(src).or_default().insert(tgt);
    }

    debug.frontier_blocks = external_neighbors.len();

    // Cache of block edges to avoid recomputation
    let mut edge_cache: HashMap<usize, HashSet<(QCoord, QCoord)>> = HashMap::new();

    // Collect all boundary edges
    let mut boundary_edges: Vec<(QCoord, QCoord)> = Vec::new();
    let mut seen: HashSet<(QCoord, QCoord)> = HashSet::new();

    // --- Phase 1: Inter-district boundary edges (from frontier blocks) ---
    for (&block, ext_neighbors) in &external_neighbors {
        let block_edges = edge_cache
            .entry(block)
            .or_insert_with(|| get_block_edges(shapes, block))
            .clone();

        let all_neighbors = &adjacencies[block];

        for &neighbor in all_neighbors {
            let neighbor = neighbor as usize;
            let neighbor_edges = edge_cache
                .entry(neighbor)
                .or_insert_with(|| get_block_edges(shapes, neighbor));

            // If this neighbor is external, shared edges are inter-district boundary edges
            if ext_neighbors.contains(&neighbor) {
                for edge in block_edges.intersection(neighbor_edges) {
                    if !seen.contains(edge) {
                        seen.insert(*edge);
                        boundary_edges.push(*edge);
                    }
                }
            }
        }
    }

    // --- Phase 2: State boundary edges from exterior frontier nodes ---
    // The partition's frontier MultiSet includes both inter-district frontier blocks and
    // exterior-only blocks (state-border blocks surrounded by same-district neighbours).
    // For each exterior frontier node, find polygon edges not shared with any graph
    // neighbour — those are the state boundary edges.
    eprintln!("[boundary] Phase 2: checking {} frontier nodes for state boundary edges (district {})",
              frontier_nodes.iter().filter(|&&n| is_state_border[n]).count(), district);

    for &block in frontier_nodes {
        // Only process exterior (state-border) blocks
        if !is_state_border[block] {
            continue;
        }

        let block_edges = edge_cache
            .entry(block)
            .or_insert_with(|| get_block_edges(shapes, block))
            .clone();

        let all_neighbors = &adjacencies[block];

        // Find edges shared with any graph neighbour (regardless of district).
        // Skip self-edges (neighbor == block): packs built with older code may
        // still store a self-edge entry which would make block_edges ∩ neighbor_edges
        // equal to all of block_edges, hiding every state-boundary edge.
        let mut shared_with_any: HashSet<(QCoord, QCoord)> = HashSet::new();
        for &neighbor in all_neighbors {
            let neighbor = neighbor as usize;
            if neighbor == block {
                continue; // skip self-edge
            }
            let neighbor_edges = edge_cache
                .entry(neighbor)
                .or_insert_with(|| get_block_edges(shapes, neighbor));
            for edge in block_edges.intersection(neighbor_edges) {
                shared_with_any.insert(*edge);
            }
        }

        // Edges not shared with any neighbour are state boundary edges
        let mut found_any = false;
        for edge in &block_edges {
            if !shared_with_any.contains(edge) && !seen.contains(edge) {
                seen.insert(*edge);
                boundary_edges.push(*edge);
                debug.state_border_edges_added += 1;
                found_any = true;
            }
        }
        if found_any {
            debug.state_border_blocks += 1;
        }
    }

    debug.boundary_edges_found = boundary_edges.len();

    // Stitch edges into rings
    let (rings, stitch_debug) = stitch_edges_with_debug(&boundary_edges);
    debug.rings_stitched = rings.len();
    debug.stitch = stitch_debug;

    if rings.is_empty() {
        return (MultiPolygon::new(vec![]), debug);
    }

    // Convert to polygons
    let polygons: Vec<Polygon<f64>> = rings
        .into_iter()
        .map(|coords| Polygon::new(LineString::new(coords), vec![]))
        .collect();

    (MultiPolygon::new(polygons), debug)
}

// NOTE: _extract_district_boundary_centroids is dead code — no callers anywhere.

/// Extract district boundary by connecting frontier block centroids.
///
/// Uses a nearest-neighbor walk that prefers adjacent frontier blocks,
/// producing a spatially coherent polygon around the district boundary.
pub fn _extract_district_boundary_centroids(
    centroids: &[Coord<f64>],
    adjacencies: &[Vec<u32>],
    frontier_edges: &[(usize, usize)],
) -> MultiPolygon<f64> {
    // Unique frontier blocks (src nodes from frontier edges)
    let frontier_set: HashSet<usize> = frontier_edges.iter().map(|&(src, _)| src).collect();

    if frontier_set.is_empty() {
        return MultiPolygon::new(vec![]);
    }

    // Adjacency restricted to frontier blocks
    let mut frontier_adj: HashMap<usize, Vec<usize>> = HashMap::new();
    for &block in &frontier_set {
        let neighbors: Vec<usize> = adjacencies[block]
            .iter()
            .map(|&n| n as usize)
            .filter(|n| frontier_set.contains(n))
            .collect();
        frontier_adj.insert(block, neighbors);
    }

    // Start from the leftmost frontier block
    let start = *frontier_set
        .iter()
        .min_by(|&&a, &&b| {
            centroids[a].x.partial_cmp(&centroids[b].x).unwrap()
                .then(centroids[a].y.partial_cmp(&centroids[b].y).unwrap())
        })
        .unwrap();

    let dist_sq = |a: usize, b: usize| -> f64 {
        let dx = centroids[a].x - centroids[b].x;
        let dy = centroids[a].y - centroids[b].y;
        dx * dx + dy * dy
    };

    // Nearest-neighbor walk: prefer adjacent frontier blocks, fall back to global nearest.
    let mut visited: HashSet<usize> = HashSet::new();
    let mut ring = vec![start];
    visited.insert(start);

    while visited.len() < frontier_set.len() {
        let current = *ring.last().unwrap();

        // Try nearest unvisited adjacent frontier block first
        let next = frontier_adj[&current]
            .iter()
            .filter(|&&n| !visited.contains(&n))
            .min_by(|&&a, &&b| dist_sq(current, a).partial_cmp(&dist_sq(current, b)).unwrap())
            .copied()
            // Fall back to globally nearest unvisited frontier block
            .or_else(|| {
                frontier_set
                    .iter()
                    .filter(|&&n| !visited.contains(&n))
                    .min_by(|&&a, &&b| dist_sq(current, a).partial_cmp(&dist_sq(current, b)).unwrap())
                    .copied()
            });

        match next {
            Some(n) => {
                ring.push(n);
                visited.insert(n);
            }
            None => break,
        }
    }

    // Close the ring
    let mut coords: Vec<Coord<f64>> = ring.iter().map(|&b| centroids[b]).collect();
    coords.push(coords[0]);

    if coords.len() >= 4 {
        MultiPolygon::new(vec![Polygon::new(LineString::new(coords), vec![])])
    } else {
        MultiPolygon::new(vec![])
    }
}

/// Extract district boundary by unioning only the frontier block polygons.
///
/// Only frontier blocks (those on the district boundary — bordering another
/// district or the state edge) contribute edges to the final outline.
/// Interior blocks are entirely surrounded by same-district neighbors and add
/// no boundary edges, so including them is wasted work.
///
/// After unioning the frontier blocks we take the exterior ring of each result
/// polygon, which fills in the "holes" left by interior blocks and gives a
/// solid filled district polygon.
///
/// This approach is coordinate-format agnostic: it works for both GeoParquet
/// (exact coordinates) and PMTiles (tile-quantized, ~1–10 m rounding).
pub fn extract_district_boundary_union(
    shapes: &[MultiPolygon<f64>],
    frontier_nodes: &[usize],
) -> MultiPolygon<f64> {
    let mut polys: Vec<MultiPolygon<f64>> = frontier_nodes.iter()
        .filter_map(|&i| shapes.get(i))
        .cloned()
        .collect();

    if polys.is_empty() {
        return MultiPolygon::new(vec![]);
    }

    // Cascaded union: repeatedly merge adjacent pairs until one polygon remains.
    // This is O(M_final × log N) vs O(N × M_final) for sequential union.
    while polys.len() > 1 {
        let mut next = Vec::with_capacity((polys.len() + 1) / 2);
        let mut i = 0;
        while i + 1 < polys.len() {
            next.push(polys[i].union(&polys[i + 1]));
            i += 2;
        }
        if i < polys.len() {
            next.push(polys.remove(i));
        }
        polys = next;
    }

    let merged = polys.into_iter().next().unwrap();

    // Drop any interior holes: the frontier blocks form a ring around interior
    // blocks, so the union has holes where those interior blocks sit.
    // Taking only the exterior ring of each polygon fills in the interior and
    // gives a solid filled district polygon.
    let filled: Vec<Polygon<f64>> = merged.0.into_iter()
        .map(|p| Polygon::new(p.exterior().clone(), vec![]))
        .collect();

    MultiPolygon::new(filled)
}

// =============================================================================
// SEGMENT-BASED BOUNDARY EXTRACTION
// =============================================================================
//
// Alternative to `extract_district_boundary_union` that avoids full polygon
// union.  Instead it:
//   1. Walks each frontier block's polygon rings and retains only the edge
//      sequences that face a different district or the state exterior.
//      "Interior" edges (shared with a same-district neighbour) are detected
//      by checking whether the edge midpoint lies within INTERIOR_TOLERANCE of
//      the neighbour's polygon boundary.  This midpoint-distance test tolerates
//      the ~10 m coordinate quantisation present in PMTiles geometry, where the
//      "same" shared edge may round differently in adjacent tiles.
//   2. Stitches all the collected edge sequences into closed rings by
//      nearest-endpoint greedy matching, tolerating gaps up to STITCH_TOLERANCE.
//
// To revert: delete everything in this section and in plan.rs change the call
// back to `extract_district_boundary_union(shapes, frontier_nodes)`.
// =============================================================================

/// Distance (degrees) within which an edge midpoint is considered to lie on a
/// same-district neighbour's boundary → the edge is interior and discarded.
/// ~0.0002° ≈ 20 m, covering the ~10 m PMTile pixel rounding error with 2×
/// margin.  Increase if small urban blocks produce gaps in the boundary.
const INTERIOR_TOLERANCE: f64 = 2e-4;

/// Distance (degrees) within which two segment endpoints are considered the
/// same point for stitching purposes.  ~0.001° ≈ 100 m — 10× the PMTile pixel
/// size, so tile-boundary gaps are always bridged.
const STITCH_TOLERANCE: f64 = 1e-3;

/// Maximum gap (degrees) allowed when bridging between segments or chains.
/// Any connection larger than this is a stitching error, not a legitimate
/// PMTiles quantisation gap.  ~0.01° ≈ 1 km.
const MAX_GAP: f64 = STITCH_TOLERANCE * 10.0;

/// An ordered sequence of coordinates forming a contiguous piece of a block's
/// exterior boundary.
type Segment = Vec<Coord<f64>>;

/// Squared Euclidean distance between two coordinates.
#[inline]
fn dist2(a: Coord<f64>, b: Coord<f64>) -> f64 {
    let dx = a.x - b.x;
    let dy = a.y - b.y;
    dx * dx + dy * dy
}


/// Minimum squared distance from point `p` to any segment of `ring`.
fn point_to_ring_dist2(p: Coord<f64>, ring: &[Coord<f64>]) -> f64 {
    let n = ring.len();
    if n < 2 { return f64::MAX; }
    let num_segs = n - 1;
    (0..num_segs).fold(f64::MAX, |best, i| {
        let a = ring[i];
        let b = ring[i + 1];
        let ab2 = dist2(a, b);
        let d2 = if ab2 < 1e-24 {
            dist2(p, a)
        } else {
            let t = ((p.x - a.x) * (b.x - a.x) + (p.y - a.y) * (b.y - a.y)) / ab2;
            let t = t.clamp(0.0, 1.0);
            let proj = Coord { x: a.x + t * (b.x - a.x), y: a.y + t * (b.y - a.y) };
            dist2(p, proj)
        };
        best.min(d2)
    })
}

/// Extract the boundary-facing edge sequences from a single frontier block.
///
/// For each edge in the block's polygon rings, computes the edge midpoint and
/// checks whether it lies within `interior_tol` of any same-district
/// neighbour's polygon boundary.  Edges that do not pass that test face a
/// different district or the state exterior and are collected into contiguous
/// runs (Segments).
fn extract_block_segments(
    block: usize,
    shapes: &[MultiPolygon<f64>],
    adjacencies: &[Vec<u32>],
    assignments: &[u32],
    district: u32,
    interior_tol2: f64, // squared tolerance
) -> Vec<Segment> {
    // Collect all same-district neighbours' ring coordinate slices up-front.
    let nbr_rings: Vec<Vec<Coord<f64>>> = adjacencies[block]
        .iter()
        .map(|&n| n as usize)
        .filter(|&n| n != block && n < shapes.len() && assignments[n] == district)
        .flat_map(|n| {
            shapes[n].0.iter().map(|p| p.exterior().coords().cloned().collect::<Vec<_>>())
        })
        .collect();

    let is_interior_edge = |a: Coord<f64>, b: Coord<f64>| -> bool {
        let mid = Coord { x: (a.x + b.x) * 0.5, y: (a.y + b.y) * 0.5 };
        nbr_rings.iter().any(|ring| point_to_ring_dist2(mid, ring) < interior_tol2)
    };

    let mut result = Vec::new();

    for poly in &shapes[block].0 {
        for ring in std::iter::once(poly.exterior()).chain(poly.interiors().iter()) {
            let coords: Vec<Coord<f64>> = ring.coords().cloned().collect();
            let n = coords.len();
            if n < 2 { continue; }
            // A closed ring repeats its first vertex; num_edges excludes that.
            let num_edges = if coords.first() == coords.last() { n - 1 } else { n };

            // Classify each edge.
            let is_boundary: Vec<bool> = (0..num_edges).map(|i| {
                let a = coords[i];
                let b = coords[(i + 1) % n];
                // Skip degenerate edges.
                if dist2(a, b) < 1e-24 { return false; }
                !is_interior_edge(a, b)
            }).collect();

            // Find a break-point at a non-boundary edge so runs that span the
            // ring's wrap-around are kept together.
            let start = (0..num_edges).find(|&i| !is_boundary[i]).unwrap_or(0);

            let mut current: Segment = Vec::new();
            for k in 0..num_edges {
                let i = (start + k) % num_edges;
                let a = coords[i];
                let b = coords[(i + 1) % n];
                if is_boundary[i] {
                    if current.is_empty() { current.push(a); }
                    current.push(b);
                } else if current.len() >= 2 {
                    result.push(std::mem::take(&mut current));
                } else {
                    current.clear();
                }
            }
            if current.len() >= 2 {
                result.push(current);
            }
        }
    }

    result
}

/// Returns true if blocks `a` and `b` are within 2 hops in the adjacency graph.
/// 2-hop tolerance handles point-touching corners where the boundary trace may
/// skip a block that contributes no boundary segments.
fn is_near_adjacent(a: usize, b: usize, adjacencies: &[Vec<u32>]) -> bool {
    if a == b { return true; }
    let adj_a = &adjacencies[a];
    if adj_a.iter().any(|&n| n as usize == b) { return true; }
    adj_a.iter().any(|&n| adjacencies[n as usize].iter().any(|&m| m as usize == b))
}

/// Flatten a walk (ordered tagged segments) into a single coordinate vector.
/// Adjacent segments share an endpoint; we skip the duplicate when concatenating.
fn flatten_walk(walk: &[(Segment, usize)]) -> Vec<Coord<f64>> {
    let mut coords: Vec<Coord<f64>> = Vec::new();
    for (i, (seg, _)) in walk.iter().enumerate() {
        if i == 0 { coords.extend_from_slice(seg); }
        else       { coords.extend_from_slice(&seg[1..]); }
    }
    coords
}

/// Phase 1: greedy first-pass stitch.
///
/// Assembles tagged segments into rough rings using adjacency + direction
/// scoring.  Returns each ring as an ordered list of `(segment, block)` pairs
/// so that inter-segment transitions can be validated afterwards.
///
/// Some transitions may connect non-adjacent blocks; these are detected and
/// repaired by `split_bad_transitions` + `stitch_walks`.
fn rough_stitch(
    segments: Vec<(Segment, usize)>,
    adjacencies: &[Vec<u32>],
    stitch_tol2: f64,
) -> Vec<Vec<(Segment, usize)>> {
    if segments.is_empty() { return vec![]; }

    const SKIP_PENALTY: f64 = 8.0;    // 2-hop neighbour
    const FAR_PENALTY:  f64 = 1000.0; // unrelated block

    let n = segments.len();
    let mut used  = vec![false; n];
    let mut rings: Vec<Vec<(Segment, usize)>> = Vec::new();

    // Pre-emit already-closed segments.
    for i in 0..n {
        let (seg, blk) = &segments[i];
        if seg.len() >= 4 && dist2(*seg.first().unwrap(), *seg.last().unwrap()) < stitch_tol2 {
            used[i] = true;
            let mut s = seg.clone();
            *s.last_mut().unwrap() = s[0];
            rings.push(vec![(s, *blk)]);
        }
    }

    while let Some(seed) = used.iter().position(|&u| !u) {
        used[seed] = true;
        let (seed_seg, seed_blk) = segments[seed].clone();
        let origin    = seed_seg[0];
        let mut ring: Vec<(Segment, usize)> = vec![(seed_seg, seed_blk)];
        let mut cur_blk = seed_blk;

        for _ in 0..n {
            let frontier = *ring.last().unwrap().0.last().unwrap();

            if ring.len() > 1 && dist2(frontier, origin) < stitch_tol2 {
                *ring.last_mut().unwrap().0.last_mut().unwrap() = origin;
                break;
            }

            // Current traversal direction.
            let cur_dir: Option<(f64, f64)> = {
                let last_seg = &ring.last().unwrap().0;
                if last_seg.len() >= 2 {
                    let prev = last_seg[last_seg.len() - 2];
                    let dx = frontier.x - prev.x;
                    let dy = frontier.y - prev.y;
                    let mag = (dx * dx + dy * dy).sqrt();
                    if mag > 1e-12 { Some((dx / mag, dy / mag)) } else { None }
                } else { None }
            };

            let adj_factor = |blk: usize| -> f64 {
                if blk == cur_blk { return 1.0; }
                if adjacencies[cur_blk].iter().any(|&n| n as usize == blk) { return 1.0; }
                for &n in &adjacencies[cur_blk] {
                    if adjacencies[n as usize].iter().any(|&m| m as usize == blk) {
                        return SKIP_PENALTY;
                    }
                }
                FAR_PENALTY
            };

            // score = d² × direction_factor × adjacency_factor
            let seg_score = |(seg, blk): &(Segment, usize), rev: bool| -> f64 {
                let connect = if rev { *seg.last().unwrap() } else { seg[0] };
                let d2  = dist2(frontier, connect);
                let adj = adj_factor(*blk);
                let dir = if let Some((dx, dy)) = cur_dir {
                    let entry = if rev {
                        if seg.len() >= 2 { seg.get(seg.len() - 2).copied() } else { None }
                    } else {
                        seg.get(1).copied()
                    };
                    if let Some(ep) = entry {
                        let sdx = ep.x - connect.x;
                        let sdy = ep.y - connect.y;
                        let smag = (sdx * sdx + sdy * sdy).sqrt();
                        if smag > 1e-12 { 2.0 - (dx * sdx + dy * sdy) / smag } else { 2.0 }
                    } else { 2.0 }
                } else { 1.0 };
                d2 * dir * adj
            };

            let mut best_score = f64::MAX;
            let mut best_j: Option<usize> = None;
            let mut best_rev = false;
            for j in 0..n {
                if used[j] { continue; }
                let s_fwd = seg_score(&segments[j], false);
                let s_rev = seg_score(&segments[j], true);
                if s_fwd < best_score { best_score = s_fwd; best_j = Some(j); best_rev = false; }
                if s_rev < best_score { best_score = s_rev; best_j = Some(j); best_rev = true;  }
            }

            if ring.len() > 1 && dist2(frontier, origin) <= best_score.min(stitch_tol2 * 4.0) {
                *ring.last_mut().unwrap().0.last_mut().unwrap() = origin;
                break;
            }

            match best_j {
                Some(j) => {
                    used[j] = true;
                    let (seg, blk) = segments[j].clone();
                    let next_seg: Segment = if best_rev {
                        seg.iter().rev().cloned().collect()
                    } else { seg };
                    cur_blk = blk;
                    ring.push((next_seg, blk));
                }
                None => { ring.last_mut().unwrap().0.push(origin); break; }
            }
        }

        if !ring.is_empty() { rings.push(ring); }
    }

    rings
}

/// Phase 2: validate transitions and split at bad ones.
///
/// Walks each rough ring and checks whether consecutive segments come from
/// near-adjacent blocks (1-hop or 2-hop).  Valid rings are returned as flat
/// coordinate vectors immediately.  Rings with one or more bad transitions are
/// split into open chains at each bad transition point.
///
/// Returns `(valid_rings, chains)` where each chain is a validated run of
/// adjacent-block segments.
fn split_bad_transitions(
    rings: Vec<Vec<(Segment, usize)>>,
    adjacencies: &[Vec<u32>],
) -> (Vec<Vec<Coord<f64>>>, Vec<Vec<(Segment, usize)>>) {
    let mut valid_rings: Vec<Vec<Coord<f64>>>       = Vec::new();
    let mut chains:      Vec<Vec<(Segment, usize)>> = Vec::new();

    for ring in rings {
        let n = ring.len();
        if n == 0 { continue; }

        // Find indices where the transition to the next segment is bad
        // (source blocks are not near-adjacent).
        let bad: Vec<usize> = (0..n)
            .filter(|&i| !is_near_adjacent(ring[i].1, ring[(i + 1) % n].1, adjacencies))
            .collect();

        if bad.is_empty() {
            // All transitions validated — emit directly.
            let coords = flatten_walk(&ring);
            if coords.len() >= 4 { valid_rings.push(coords); }
        } else {
            // Split into one chain per bad transition.
            // Chain k spans ring[(bad[k]+1) % n] ..= ring[bad[(k+1) % len]].
            for k in 0..bad.len() {
                let start     = (bad[k] + 1) % n;
                let end_incl  = bad[(k + 1) % bad.len()];
                let mut chain = Vec::new();
                let mut i     = start;
                for _ in 0..=n {           // at most n steps before we hit end_incl
                    chain.push(ring[i].clone());
                    if i == end_incl { break; }
                    i = (i + 1) % n;
                }
                if !chain.is_empty() { chains.push(chain); }
            }
        }
    }

    (valid_rings, chains)
}

/// Phase 3: stitch validated chains (super-segments) into closed rings.
///
/// Each chain is a long run of correctly-ordered segments.  This pass only
/// needs to decide how to connect chain endpoints — a much simpler problem
/// than stitching individual segments because:
///   • there are far fewer items (one per bad transition in the rough ring),
///   • each endpoint's source block is known, so adjacency scoring is tight.
///
/// Uses adjacency-biased distance scoring; no direction bias needed at this
/// scale.
fn stitch_walks(
    chains: Vec<Vec<(Segment, usize)>>,
    adjacencies: &[Vec<u32>],
    stitch_tol2: f64,
) -> Vec<Vec<Coord<f64>>> {
    if chains.is_empty() { return vec![]; }

    const SKIP_PENALTY: f64 = 8.0;
    const FAR_PENALTY:  f64 = 1000.0;

    let n = chains.len();

    // Precompute flat coordinates and endpoint metadata for each chain.
    let flat: Vec<Vec<Coord<f64>>> = chains.iter().map(|c| flatten_walk(c)).collect();

    // (fwd_start_coord, fwd_start_block, fwd_end_coord, fwd_end_block)
    let info: Vec<(Coord<f64>, usize, Coord<f64>, usize)> = chains.iter()
        .zip(flat.iter())
        .map(|(chain, f)| {
            let sc = *f.first().unwrap();
            let ec = *f.last().unwrap();
            let sb = chain.first().unwrap().1;
            let eb = chain.last().unwrap().1;
            (sc, sb, ec, eb)
        })
        .collect();

    let adj_factor = |cur: usize, cand: usize| -> f64 {
        if cand == cur { return 1.0; }
        if adjacencies[cur].iter().any(|&n| n as usize == cand) { return 1.0; }
        for &n in &adjacencies[cur] {
            if adjacencies[n as usize].iter().any(|&m| m as usize == cand) {
                return SKIP_PENALTY;
            }
        }
        FAR_PENALTY
    };

    let mut used  = vec![false; n];
    let mut rings: Vec<Vec<Coord<f64>>> = Vec::new();

    while let Some(seed) = used.iter().position(|&u| !u) {
        used[seed] = true;
        let mut coords      = flat[seed].clone();
        let origin          = coords[0];
        let mut cur_end_blk = info[seed].3; // end block of the seed chain

        for _ in 0..n {
            let frontier = *coords.last().unwrap();

            if coords.len() > 2 && dist2(frontier, origin) < stitch_tol2 {
                *coords.last_mut().unwrap() = origin;
                break;
            }

            let mut best_score = f64::MAX;
            let mut best_j: Option<usize> = None;
            let mut best_rev = false;

            for j in 0..n {
                if used[j] { continue; }
                let (fsc, fsb, fec, feb) = info[j];
                let s_fwd = dist2(frontier, fsc) * adj_factor(cur_end_blk, fsb);
                let s_rev = dist2(frontier, fec) * adj_factor(cur_end_blk, feb);
                if s_fwd < best_score { best_score = s_fwd; best_j = Some(j); best_rev = false; }
                if s_rev < best_score { best_score = s_rev; best_j = Some(j); best_rev = true;  }
            }

            if coords.len() > 2 && dist2(frontier, origin) <= best_score.min(stitch_tol2 * 4.0) {
                *coords.last_mut().unwrap() = origin;
                break;
            }

            match best_j {
                Some(j) => {
                    used[j] = true;
                    if best_rev {
                        coords.extend(flat[j].iter().rev().skip(1).cloned());
                        cur_end_blk = info[j].1; // start block becomes the new end
                    } else {
                        coords.extend(flat[j].iter().skip(1).cloned());
                        cur_end_blk = info[j].3;
                    }
                }
                None => { coords.push(origin); break; }
            }
        }

        if coords.len() >= 4 {
            if coords.first() != coords.last() { coords.push(coords[0]); }
            rings.push(coords);
        }
    }

    rings
}

/// Extract district boundary using the segment-based approach.
///
/// Replaces `extract_district_boundary_union` in `plan.rs`.  Requires
/// `adjacencies` and `assignments` in addition to `shapes` and
/// `frontier_nodes`.
pub fn extract_district_boundary_segments(
    shapes: &[MultiPolygon<f64>],
    adjacencies: &[Vec<u32>],
    assignments: &[u32],
    frontier_nodes: &[usize],
    district: u32,
) -> MultiPolygon<f64> {
    let interior_tol2 = INTERIOR_TOLERANCE * INTERIOR_TOLERANCE;
    let stitch_tol2   = STITCH_TOLERANCE   * STITCH_TOLERANCE;

    // Phase 1: extract boundary-facing edge sequences from every frontier block,
    // tagging each segment with its source block index.
    let segments: Vec<(Segment, usize)> = frontier_nodes
        .iter()
        .flat_map(|&block| {
            extract_block_segments(
                block, shapes, adjacencies, assignments, district, interior_tol2,
            )
            .into_iter()
            .map(move |seg| (seg, block))
        })
        .collect();

    // Phase 2: greedy first-pass stitch into rough rings (preserves block tags).
    let rough_rings = rough_stitch(segments, adjacencies, stitch_tol2);

    // Phase 3: validate transitions; split at bad ones into verified chains.
    let (mut rings, chains) = split_bad_transitions(rough_rings, adjacencies);

    // Phase 4: stitch the chains (super-segments) into final closed rings.
    rings.extend(stitch_walks(chains, adjacencies, stitch_tol2));

    if rings.is_empty() {
        return MultiPolygon::new(vec![]);
    }

    MultiPolygon::new(
        rings.into_iter()
            .map(|coords| Polygon::new(LineString::new(coords), vec![]))
            .collect(),
    )
}
