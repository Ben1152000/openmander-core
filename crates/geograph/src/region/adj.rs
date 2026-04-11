use geo::Coord;

use crate::adj::AdjacencyMatrix;
use crate::dcel::{Dcel, HalfEdgeId, VertexId};
use crate::unit::UnitId;

use super::Region;

impl Region {
    /// Return `self` with extra undirected adjacency pairs added to both the
    /// Rook and Queen matrices.  The new edges carry weight `0.0` (no shared
    /// geometric boundary).  Pairs that already exist are silently ignored.
    ///
    /// Use this to bake in manually-patched island bridges before serialising
    /// the region, so the forced pairs survive round-trips through `.region.gz`.
    pub fn with_forced_adjacencies(mut self, pairs: &[(UnitId, UnitId)]) -> Self {
        if pairs.is_empty() { return self; }
        self.adjacent = self.adjacent.with_extra_edges(pairs);
        self.touching = self.touching.with_extra_edges(pairs);
        self
    }

    /// Returns `true` if `a` and `b` share a positive-length boundary segment.
    #[inline]
    pub fn are_adjacent(&self, a: UnitId, b: UnitId) -> bool {
        self.adjacent.contains(a, b)
    }

    /// Sorted slice of Rook-adjacent units for `unit`.
    #[inline]
    pub fn neighbors(&self, unit: UnitId) -> &[UnitId] {
        self.adjacent.neighbors(unit)
    }

    /// The Rook (shared-edge) adjacency matrix.
    #[inline] pub fn adjacency(&self) -> &AdjacencyMatrix { &self.adjacent }

    /// The Queen (shared-point) adjacency matrix.
    #[inline] pub fn touching(&self) -> &AdjacencyMatrix { &self.touching }
}

// ---------------------------------------------------------------------------
// Builders  (pub(crate) so Region constructors and io::read can call them)
// ---------------------------------------------------------------------------

/// Walk every half-edge; when the two faces on either side belong to different
/// non-EXTERIOR units, emit both directed pairs with edge lengths as weights.
///
/// Uses a two-pass CSR construction to avoid the ~1 GB intermediate `triples`
/// Vec that the naive approach allocates for large states (e.g. TX with 38M
/// half-edges).  Peak extra memory is two DCEL-scan passes plus the pre-dedup
/// flat buffer, which is at most `num_half_edges / 2 * 12` bytes (~228 MB for TX)
/// and shrinks further once rows are sorted and deduplicated in-place.
pub(crate) fn build_adjacent(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    edge_length: &[f64],
    num_units: usize,
) -> AdjacencyMatrix {
    // --- Pass 1: count raw boundary directed half-edges per source unit -------
    let mut degree = vec![0u32; num_units];
    for e in 0..dcel.num_half_edges() {
        let unit  = face_to_unit[dcel.half_edge(HalfEdgeId(e as u32)).face.0 as usize];
        let other = face_to_unit[dcel.half_edge(HalfEdgeId(e as u32 ^ 1)).face.0 as usize];
        if unit != other && unit != UnitId::EXTERIOR && other != UnitId::EXTERIOR {
            degree[unit.0 as usize] += 1;
        }
    }

    // Prefix-sum → row start offsets for the pre-dedup buffer.
    let mut offsets = vec![0u32; num_units + 1];
    for i in 0..num_units { offsets[i + 1] = offsets[i] + degree[i]; }
    let total_raw = offsets[num_units] as usize;
    drop(degree); // no longer needed; free before the large allocations below

    // Allocate flat (neighbor, weight) arrays — will be sorted+deduped in-place.
    let mut neighbors = vec![UnitId(0); total_raw];
    let mut weights   = vec![0.0f64;   total_raw];
    let mut cursors: Vec<u32> = offsets[..num_units].to_vec();

    // --- Pass 2: fill ---------------------------------------------------------
    for e in 0..dcel.num_half_edges() {
        let unit  = face_to_unit[dcel.half_edge(HalfEdgeId(e as u32)).face.0 as usize];
        let other = face_to_unit[dcel.half_edge(HalfEdgeId(e as u32 ^ 1)).face.0 as usize];
        if unit != other && unit != UnitId::EXTERIOR && other != UnitId::EXTERIOR {
            let pos = cursors[unit.0 as usize] as usize;
            neighbors[pos] = other;
            weights[pos]   = edge_length[e / 2];
            cursors[unit.0 as usize] += 1;
        }
    }
    drop(cursors);

    // --- Per-row: sort by neighbor, then merge duplicate (unit→nb) entries ----
    // We compact in-place: `compact_end` always ≤ current row_start, so reads
    // never overlap writes.
    let mut compact_end = 0usize;
    let mut new_offsets = vec![0u32; num_units + 1];

    for u in 0..num_units {
        let row_start = offsets[u]     as usize;
        let row_end   = offsets[u + 1] as usize;
        new_offsets[u] = compact_end as u32;
        if row_start == row_end { continue; }

        // Insertion-sort this row by neighbor ID (rows are small: typically 4–30
        // entries for census blocks), carrying weights alongside.
        for i in (row_start + 1)..row_end {
            let nb_i = neighbors[i];
            let w_i  = weights[i];
            let mut j = i;
            while j > row_start && neighbors[j - 1] > nb_i {
                neighbors[j] = neighbors[j - 1];
                weights[j]   = weights[j - 1];
                j -= 1;
            }
            neighbors[j] = nb_i;
            weights[j]   = w_i;
        }

        // Compact (dedup + sum) into the head of the output buffer.
        for i in row_start..row_end {
            let nb = neighbors[i];
            let w  = weights[i];
            if compact_end > new_offsets[u] as usize
                && neighbors[compact_end - 1] == nb
            {
                weights[compact_end - 1] += w;
            } else {
                neighbors[compact_end] = nb;
                weights[compact_end]   = w;
                compact_end += 1;
            }
        }
    }
    new_offsets[num_units] = compact_end as u32;
    neighbors.truncate(compact_end);
    weights.truncate(compact_end);
    neighbors.shrink_to_fit();
    weights.shrink_to_fit();

    AdjacencyMatrix::from_raw(new_offsets, neighbors, Some(weights))
}

/// Start from Rook pairs, then add all unit-pairs that share a vertex star.
pub(crate) fn build_touching(dcel: &Dcel<Coord<f64>>, face_to_unit: &[UnitId], num_units: usize) -> AdjacencyMatrix {
    let mut pairs = Vec::<(UnitId, UnitId)>::new();

    for v in 0..dcel.num_vertices() {
        let start = match dcel.vertex(VertexId(v as u32)).half_edge {
            Some(edge) => edge,
            None => continue,
        };
        let mut units: Vec<UnitId> = dcel.vertex_star(start)
            .map(|he| face_to_unit[dcel.half_edge(he).face.0 as usize])
            .collect();
        units.sort_unstable();
        units.dedup();

        for &a in &units {
            for &b in &units {
                if a != b { pairs.push((a, b)) }
            }
        }
    }

    AdjacencyMatrix::from_directed_pairs(num_units, pairs)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::unit::UnitId;
    use crate::region::test_helpers::make_two_unit_region;

    // -----------------------------------------------------------------------
    // are_adjacent
    // -----------------------------------------------------------------------

    #[test]
    fn adjacent_units_are_adjacent() {
        let r = make_two_unit_region();
        assert!(r.are_adjacent(UnitId(0), UnitId(1)));
        assert!(r.are_adjacent(UnitId(1), UnitId(0)));
    }

    #[test]
    fn unit_is_not_adjacent_to_itself() {
        let r = make_two_unit_region();
        assert!(!r.are_adjacent(UnitId(0), UnitId(0)));
        assert!(!r.are_adjacent(UnitId(1), UnitId(1)));
    }

    // -----------------------------------------------------------------------
    // neighbors
    // -----------------------------------------------------------------------

    #[test]
    fn each_unit_has_one_rook_neighbour() {
        let r = make_two_unit_region();
        assert_eq!(r.neighbors(UnitId(0)), &[UnitId(1)]);
        assert_eq!(r.neighbors(UnitId(1)), &[UnitId(0)]);
    }

    #[test]
    fn neighbours_are_sorted() {
        let r = make_two_unit_region();
        for uid in r.unit_ids() {
            let ns = r.neighbors(uid);
            for w in ns.windows(2) {
                assert!(w[0] < w[1]);
            }
        }
    }

    // -----------------------------------------------------------------------
    // adjacency (Rook)
    // -----------------------------------------------------------------------

    #[test]
    fn rook_matrix_covers_all_units() {
        let r = make_two_unit_region();
        assert_eq!(r.adjacency().num_units(), 2);
    }

    #[test]
    fn rook_adjacency_is_symmetric() {
        let r = make_two_unit_region();
        let adj = r.adjacency();
        for uid in r.unit_ids() {
            for &nb in adj.neighbors(uid) {
                assert!(adj.contains(nb, uid),
                    "asymmetry: {uid} -> {nb} but not reverse");
            }
        }
    }

    // -----------------------------------------------------------------------
    // touching (Queen)
    // -----------------------------------------------------------------------

    #[test]
    fn queen_matrix_covers_all_units() {
        let r = make_two_unit_region();
        assert_eq!(r.touching().num_units(), 2);
    }

    #[test]
    fn queen_is_superset_of_rook() {
        let r = make_two_unit_region();
        let rook  = r.adjacency();
        let queen = r.touching();
        for uid in r.unit_ids() {
            for &nb in rook.neighbors(uid) {
                assert!(queen.contains(uid, nb),
                    "Rook edge ({uid},{nb}) missing from Queen matrix");
            }
        }
    }

    // -----------------------------------------------------------------------
    // edge weights
    // -----------------------------------------------------------------------

    #[test]
    fn rook_adjacency_has_weights() {
        let r = make_two_unit_region();
        assert!(r.adjacency().has_weights());
    }

    #[test]
    fn shared_boundary_length_at_matches_shared_boundary_length() {
        let r = make_two_unit_region();
        // For each pair of Rook-adjacent units, the CSR weight should
        // equal the shared_boundary_length computed from the DCEL.
        for uid in r.unit_ids() {
            let offset = r.adjacency().offset(uid);
            for (i, &nb) in r.neighbors(uid).iter().enumerate() {
                let csr_weight = r.shared_boundary_length_at(offset + i);
                let dcel_weight = r.shared_boundary_length(uid, nb);
                assert!(
                    (csr_weight - dcel_weight).abs() < 1e-9,
                    "weight mismatch for ({uid},{nb}): csr={csr_weight} dcel={dcel_weight}"
                );
            }
        }
    }

    #[test]
    fn queen_adjacency_is_symmetric() {
        let r = make_two_unit_region();
        let q = r.touching();
        for uid in r.unit_ids() {
            for &nb in q.neighbors(uid) {
                assert!(q.contains(nb, uid));
            }
        }
    }
}
