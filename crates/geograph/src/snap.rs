use std::collections::HashMap;

use geo::Coord;

/// Snap near-coincident vertices along shared polygon edges to a canonical
/// position, repairing floating-point artefacts common in TIGER/Line and
/// quantised geodata.
///
/// Only vertices that are already connected by an edge in at least one input
/// polygon ring are candidates for snapping (conservative strategy — never
/// snaps across open space).
///
/// `rings` is a flat slice over all units; each entry is the list of rings
/// (outer + holes) for one unit, and each ring is a sequence of coordinates.
/// Coordinates are modified in place.
///
/// `tolerance` is the maximum distance (in degrees) at which two vertices are
/// considered coincident.  A value of `1e-7` (~1 cm) is appropriate for
/// full-precision GeoParquet data; coarser inputs may require up to `1e-4`.
pub(crate) fn snap_vertices(rings: &mut [Vec<Vec<Coord<f64>>>], tolerance: f64) {
    if rings.is_empty() { return; }

    // -----------------------------------------------------------------------
    // Step 1 — Build flat vertex table
    // -----------------------------------------------------------------------
    // flat_idxs[unit][ring][pos] = index into `coords`
    let mut coords: Vec<Coord<f64>> = Vec::new();
    let mut flat_idxs: Vec<Vec<Vec<usize>>> = Vec::with_capacity(rings.len());

    for unit_rings in rings.iter() {
        let mut unit_flat = Vec::with_capacity(unit_rings.len());
        for ring in unit_rings {
            let mut ring_flat = Vec::with_capacity(ring.len());
            for &c in ring {
                ring_flat.push(coords.len());
                coords.push(c);
            }
            unit_flat.push(ring_flat);
        }
        flat_idxs.push(unit_flat);
    }

    let n = coords.len();
    if n == 0 { return; }

    // -----------------------------------------------------------------------
    // Step 2 — Union-Find
    // -----------------------------------------------------------------------
    let mut parent: Vec<usize> = (0..n).collect();
    let mut rank:   Vec<u8>    = vec![0; n];

    // Iterative path-halving find.
    let find = |parent: &mut Vec<usize>, mut x: usize| -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]]; // path halving
            x = parent[x];
        }
        x
    };

    let union = |parent: &mut Vec<usize>, rank: &mut Vec<u8>, x: usize, y: usize| {
        let rx = {
            let mut v = x;
            while parent[v] != v { parent[v] = parent[parent[v]]; v = parent[v]; }
            v
        };
        let ry = {
            let mut v = y;
            while parent[v] != v { parent[v] = parent[parent[v]]; v = parent[v]; }
            v
        };
        if rx == ry { return; }
        match rank[rx].cmp(&rank[ry]) {
            std::cmp::Ordering::Less    => parent[rx] = ry,
            std::cmp::Ordering::Greater => parent[ry] = rx,
            std::cmp::Ordering::Equal   => { parent[ry] = rx; rank[rx] += 1; }
        }
    };

    // -----------------------------------------------------------------------
    // Step 3 — Collect all directed edges (unit, flat_i, flat_j)
    // -----------------------------------------------------------------------
    struct Edge { unit: usize, i: usize, j: usize }
    let mut edges: Vec<Edge> = Vec::new();

    for (u, unit_rings) in rings.iter().enumerate() {
        for (r, ring) in unit_rings.iter().enumerate() {
            let len = ring.len();
            for pos in 0..len {
                let next_pos = (pos + 1) % len;
                edges.push(Edge {
                    unit: u,
                    i: flat_idxs[u][r][pos],
                    j: flat_idxs[u][r][next_pos],
                });
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 4 — Find near-coincident edge pairs using spatial hash  O(E·k)
    //
    // For each directed edge e1 = (i→j) from unit A, we look up vertices k
    // near e1.i that belong to a different unit B.  For each such k we check
    // whether unit B has an outgoing edge (k→l) with l ≈ e1.j (same-direction
    // match) or an incoming edge (m→k) with m ≈ e1.j (reversed match).
    // -----------------------------------------------------------------------
    let tol_sq = tolerance * tolerance;

    let near = |a: Coord<f64>, b: Coord<f64>| -> bool {
        let dx = a.x - b.x;
        let dy = a.y - b.y;
        dx * dx + dy * dy <= tol_sq
    };

    // 4a. Map each flat vertex index to its owning unit.
    let mut vertex_unit: Vec<usize> = vec![0usize; n];
    for (u, unit_rings) in rings.iter().enumerate() {
        for (r, ring) in unit_rings.iter().enumerate() {
            for pos in 0..ring.len() {
                vertex_unit[flat_idxs[u][r][pos]] = u;
            }
        }
    }

    // 4b. Spatial hash: grid cell → list of flat vertex indices.
    //     Cell size = tolerance so each vertex only needs 3×3 neighbour check.
    let inv_tol = 1.0 / tolerance;
    let cell_of = |c: Coord<f64>| -> (i64, i64) {
        ((c.x * inv_tol).floor() as i64, (c.y * inv_tol).floor() as i64)
    };

    let mut vertex_grid: HashMap<(i64, i64), Vec<usize>> = HashMap::new();
    for vi in 0..n {
        vertex_grid.entry(cell_of(coords[vi])).or_default().push(vi);
    }

    // 4c. Edge adjacency lookups: vertex → outgoing / incoming edge indices.
    let mut edges_from: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut edges_to:   Vec<Vec<usize>> = vec![Vec::new(); n];
    for (ei, e) in edges.iter().enumerate() {
        edges_from[e.i].push(ei);
        edges_to[e.j].push(ei);
    }

    // 4d. For each edge, find cross-unit matches via the spatial hash.
    for ei in 0..edges.len() {
        let (e1_unit, e1_i, e1_j) = (edges[ei].unit, edges[ei].i, edges[ei].j);
        let p_i = coords[e1_i];
        let p_j = coords[e1_j];
        let (cx, cy) = cell_of(p_i);

        for dx in -1i64..=1 {
            for dy in -1i64..=1 {
                let Some(candidates) = vertex_grid.get(&(cx + dx, cy + dy)) else { continue };
                for &k in candidates {
                    if vertex_unit[k] == e1_unit { continue; }
                    if !near(p_i, coords[k]) { continue; }

                    // Same-direction match: e1.i ≈ k (start), e1.j ≈ l (end)
                    let from_k: Vec<usize> = edges_from[k].clone();
                    for ej in from_k {
                        let l = edges[ej].j;
                        if near(p_j, coords[l]) {
                            union(&mut parent, &mut rank, e1_i, k);
                            union(&mut parent, &mut rank, e1_j, l);
                        }
                    }

                    // Reversed match: e1.i ≈ k (end), e1.j ≈ m (start)
                    let to_k: Vec<usize> = edges_to[k].clone();
                    for ej in to_k {
                        let m = edges[ej].i;
                        if near(p_j, coords[m]) {
                            union(&mut parent, &mut rank, e1_i, k);
                            union(&mut parent, &mut rank, e1_j, m);
                        }
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 5 — Assign canonical coordinate for each component
    //          (use the root vertex's original coordinate)
    // -----------------------------------------------------------------------
    let canonical: Vec<Coord<f64>> = (0..n)
        .map(|i| coords[find(&mut parent, i)])
        .collect();

    // -----------------------------------------------------------------------
    // Step 6 — Write back
    // -----------------------------------------------------------------------
    for (u, unit_rings) in rings.iter_mut().enumerate() {
        for (r, ring) in unit_rings.iter_mut().enumerate() {
            for (pos, coord) in ring.iter_mut().enumerate() {
                *coord = canonical[flat_idxs[u][r][pos]];
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn c(x: f64, y: f64) -> Coord<f64> { Coord { x, y } }
    fn ring(pts: &[(f64, f64)]) -> Vec<Coord<f64>> {
        pts.iter().map(|&(x, y)| c(x, y)).collect()
    }

    // -----------------------------------------------------------------------
    // Edge cases / no-op
    // -----------------------------------------------------------------------

    #[test]
    fn empty_input_does_not_panic() {
        snap_vertices(&mut [], 1e-7);
    }

    #[test]
    fn single_unit_no_neighbours_unchanged() {
        let original = ring(&[(0.0, 0.0), (1.0, 0.0), (0.5, 1.0)]);
        let mut rings = vec![vec![original.clone()]];
        snap_vertices(&mut rings, 1e-7);
        assert_eq!(rings[0][0], original);
    }

    #[test]
    fn exact_coincident_vertices_are_stable() {
        // Two triangles sharing edge (0,0)-(1,0) exactly — already coincident.
        let mut rings = vec![
            vec![ring(&[(0.0, 0.0), (1.0, 0.0), (0.5,  1.0)])],
            vec![ring(&[(1.0, 0.0), (0.0, 0.0), (0.5, -1.0)])],
        ];
        let before_a = rings[0][0][0];
        let before_b = rings[1][0][1]; // (0,0) in unit 1
        snap_vertices(&mut rings, 1e-7);
        // Both must still equal their original value (they were already identical).
        assert_eq!(rings[0][0][0], before_a);
        assert_eq!(rings[1][0][1], before_b);
        assert_eq!(rings[0][0][0], rings[1][0][1]);
    }

    // -----------------------------------------------------------------------
    // Snapping within tolerance
    // -----------------------------------------------------------------------

    #[test]
    fn near_coincident_edge_is_snapped() {
        // Two triangles whose shared edge is offset by eps < tolerance.
        let eps = 5e-8_f64; // half of tolerance
        // Unit 0: (0,0)-(1,0)-(0.5,1)
        // Unit 1: (1+eps,eps)-(eps,eps)-(0.5,-1)  ← shared edge reversed
        let mut rings = vec![
            vec![ring(&[(0.0, 0.0), (1.0, 0.0), (0.5,  1.0)])],
            vec![ring(&[(1.0 + eps, eps), (eps, eps), (0.5, -1.0)])],
        ];
        snap_vertices(&mut rings, 1e-7);

        // Unit 0 vertex 0 == unit 1 vertex 1 (both were ≈ (0,0))
        assert_eq!(rings[0][0][0], rings[1][0][1]);
        // Unit 0 vertex 1 == unit 1 vertex 0 (both were ≈ (1,0))
        assert_eq!(rings[0][0][1], rings[1][0][0]);
    }

    #[test]
    fn snapped_coordinates_are_the_root_vertex() {
        // After snapping, each component uses the root vertex's original coord.
        // The root is whichever vertex was assigned a lower flat index (union by
        // rank with index tiebreak), so unit-0 vertices (lower flat indices)
        // end up as roots — unit-1 vertices are replaced by unit-0's coords.
        let eps = 5e-8_f64;
        let mut rings = vec![
            vec![ring(&[(0.0, 0.0), (1.0, 0.0), (0.5,  1.0)])],
            vec![ring(&[(1.0 + eps, eps), (eps, eps), (0.5, -1.0)])],
        ];
        snap_vertices(&mut rings, 1e-7);

        // Unit-0 coords are unchanged.
        assert_eq!(rings[0][0][0], c(0.0, 0.0));
        assert_eq!(rings[0][0][1], c(1.0, 0.0));
        // Unit-1 shared vertices now equal unit-0's original coords.
        assert_eq!(rings[1][0][1], c(0.0, 0.0));
        assert_eq!(rings[1][0][0], c(1.0, 0.0));
    }

    // -----------------------------------------------------------------------
    // Not snapped — beyond tolerance
    // -----------------------------------------------------------------------

    #[test]
    fn beyond_tolerance_not_snapped() {
        // Offset 2× the tolerance — must NOT snap.
        let eps = 2e-7_f64;
        let orig_b0 = c(1.0 + eps, 0.0);
        let orig_b1 = c(eps, 0.0);
        let mut rings = vec![
            vec![ring(&[(0.0, 0.0), (1.0, 0.0), (0.5,  1.0)])],
            vec![ring(&[(orig_b0.x, orig_b0.y), (orig_b1.x, orig_b1.y), (0.5, -1.0)])],
        ];
        snap_vertices(&mut rings, 1e-7);
        assert_eq!(rings[1][0][0], orig_b0);
        assert_eq!(rings[1][0][1], orig_b1);
    }

    // -----------------------------------------------------------------------
    // Conservative — not snapped without matching edge
    // -----------------------------------------------------------------------

    #[test]
    fn same_unit_close_vertices_not_snapped() {
        // Two vertices in the same unit are close but share no inter-unit edge.
        let eps = 5e-8_f64;
        let mut rings = vec![
            vec![ring(&[(0.0, 0.0), (eps, eps), (1.0, 0.0)])],
        ];
        let before = rings[0][0].clone();
        snap_vertices(&mut rings, 1e-7);
        assert_eq!(rings[0][0][0], before[0]);
        assert_eq!(rings[0][0][1], before[1]);
    }

    #[test]
    fn isolated_vertex_close_but_no_matching_edge() {
        // Unit B has a vertex near unit A's vertex (0,0), but B's neighbouring
        // vertex (5,0) is far from A's neighbour (1,0).  Conservative check
        // means only BOTH endpoints may be snapped together — so nothing snaps.
        let eps = 5e-8_f64;
        let orig_b0 = c(eps, eps);
        let mut rings = vec![
            vec![ring(&[(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)])],
            vec![ring(&[(orig_b0.x, orig_b0.y), (5.0, 0.0), (5.0, 5.0)])],
        ];
        snap_vertices(&mut rings, 1e-7);
        // B's vertex 0 must not be changed.
        assert_eq!(rings[1][0][0], orig_b0);
    }

    // -----------------------------------------------------------------------
    // Multiple shared vertices
    // -----------------------------------------------------------------------

    #[test]
    fn two_shared_vertices_both_snapped() {
        // Two rectangles sharing a full edge (two shared vertices).
        let eps = 5e-8_f64;
        // Unit A: (0,0)-(1,0)-(1,1)-(0,1)
        // Unit B: (2,0)-(2,1)-(1+eps,1+eps)-(1+eps,eps)   (CW so the shared edge is reversed)
        let mut rings = vec![
            vec![ring(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)])],
            vec![ring(&[(2.0, 0.0), (2.0, 1.0), (1.0 + eps, 1.0 + eps), (1.0 + eps, eps)])],
        ];
        snap_vertices(&mut rings, 1e-7);

        // A's (1,0) and B's (1+eps, eps) — shared corner
        assert_eq!(rings[0][0][1], rings[1][0][3]);
        // A's (1,1) and B's (1+eps, 1+eps) — shared corner
        assert_eq!(rings[0][0][2], rings[1][0][2]);
    }

    #[test]
    fn third_unit_also_snapped_transitively() {
        // Three units sharing the same vertex; snapping A-B and A-C should
        // result in B and C also sharing the same coordinate.
        let eps = 5e-8_f64;
        let mut rings = vec![
            // A: (0,0)-(1,0)-(0,1)
            vec![ring(&[(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)])],
            // B: (1+eps,eps)-(0+eps,eps)-(0,-1)  — shared edge with A reversed
            vec![ring(&[(1.0 + eps, eps), (eps, eps), (0.0, -1.0)])],
            // C: (eps,eps)-(0+eps,0+eps)-(−1,0) — also shares A's (0,0)-(1,0) edge
            // Actually, let's share the edge (0,0)→(1,0) again with tiny offsets
            vec![ring(&[(eps, eps), (1.0 + eps, eps), (1.0, -1.0)])],
        ];
        snap_vertices(&mut rings, 1e-7);

        // All three "origin" vertices should be equal.
        let a = rings[0][0][0];
        let b = rings[1][0][1]; // B's vertex near (0,0)
        let c_v = rings[2][0][0]; // C's vertex near (0,0)
        assert_eq!(a, b);
        assert_eq!(a, c_v);
    }
}
