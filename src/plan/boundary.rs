//! District boundary extraction using frontier edges.
//!
//! Algorithm:
//! 1. Get all frontier blocks from frontier edges (blocks that touch other districts)
//! 2. For each frontier block, find boundary edges shared with external neighbors
//! 3. For ALL blocks in the district, find state boundary edges (not shared with any neighbor)
//! 4. Stitch all boundary edges together to form closed rings

use std::collections::{HashMap, HashSet};
use geo::{Coord, LineString, MultiPolygon, Polygon};

/// Quantization for coordinate matching (1e-6 degrees ≈ 10cm precision)
/// Coarser to handle floating-point imprecision in geographic data.
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

/// Get all edges from a block's exterior ring as quantized pairs (canonical form).
fn get_block_edges(shapes: &[MultiPolygon<f64>], block: usize) -> HashSet<(QCoord, QCoord)> {
    let mut edges = HashSet::new();
    for poly in &shapes[block].0 {
        let coords: Vec<_> = poly.exterior().coords().collect();
        for window in coords.windows(2) {
            let a = QCoord::new(*window[0]);
            let b = QCoord::new(*window[1]);
            if a != b {
                let canonical = if (a.x, a.y) <= (b.x, b.y) { (a, b) } else { (b, a) };
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
/// Phase 2: State boundary edges from state-border blocks only (edges not shared with any neighbor).
///          `is_state_border[block]` must be true for blocks on the state border.
pub fn extract_district_boundary_with_debug(
    shapes: &[MultiPolygon<f64>],
    adjacencies: &[Vec<u32>],
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

    // --- Phase 2: State boundary edges from state-border blocks only ---
    // A state boundary edge is one that belongs to a block but isn't shared with any neighbor.
    // Only check blocks known to be on the state border (outer_perimeter_m > 0).
    for block in 0..shapes.len() {
        if assignments[block] != district { continue; }
        if !is_state_border[block] { continue; }

        let block_edges = edge_cache
            .entry(block)
            .or_insert_with(|| get_block_edges(shapes, block))
            .clone();

        let all_neighbors = &adjacencies[block];

        // Find edges shared with any neighbor
        let mut shared_with_any: HashSet<(QCoord, QCoord)> = HashSet::new();
        for &neighbor in all_neighbors {
            let neighbor_edges = edge_cache
                .entry(neighbor as usize)
                .or_insert_with(|| get_block_edges(shapes, neighbor as usize));
            for edge in block_edges.intersection(neighbor_edges) {
                shared_with_any.insert(*edge);
            }
        }

        // Edges not shared with any neighbor are state boundary edges
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

/// Extract district boundary by connecting frontier block centroids.
///
/// Sorts frontier blocks by angle from the center of the frontier block set,
/// producing a closed polygon that includes all frontier block centroids.
pub fn extract_district_boundary_centroids(
    centroids: &[Coord<f64>],
    _adjacencies: &[Vec<u32>],
    frontier_edges: &[(usize, usize)],
) -> MultiPolygon<f64> {
    // Unique frontier blocks (src nodes from frontier edges)
    let frontier_set: HashSet<usize> = frontier_edges.iter().map(|&(src, _)| src).collect();

    if frontier_set.is_empty() {
        return MultiPolygon::new(vec![]);
    }

    // Compute center of frontier block centroids
    let mut cx = 0.0_f64;
    let mut cy = 0.0_f64;
    for &b in &frontier_set {
        cx += centroids[b].x;
        cy += centroids[b].y;
    }
    cx /= frontier_set.len() as f64;
    cy /= frontier_set.len() as f64;

    // Sort frontier blocks by angle from center
    let mut blocks: Vec<usize> = frontier_set.into_iter().collect();
    blocks.sort_by(|&a, &b| {
        let angle_a = f64::atan2(centroids[a].y - cy, centroids[a].x - cx);
        let angle_b = f64::atan2(centroids[b].y - cy, centroids[b].x - cx);
        angle_a.partial_cmp(&angle_b).unwrap_or(std::cmp::Ordering::Equal)
    });

    // Build closed ring
    let mut coords: Vec<Coord<f64>> = blocks.iter().map(|&b| centroids[b]).collect();
    coords.push(coords[0]);

    MultiPolygon::new(vec![Polygon::new(LineString::new(coords), vec![])])
}
