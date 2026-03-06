use geo::Coord;

use crate::adj::AdjacencyMatrix;
use crate::dcel::{Dcel, HalfEdgeId, VertexId};
use crate::unit::UnitId;

use super::Region;

impl Region {
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
pub(crate) fn build_adjacent(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    edge_length: &[f64],
    num_units: usize,
) -> AdjacencyMatrix {
    let mut triples = Vec::<(UnitId, UnitId, f64)>::new();

    for h in 0..dcel.num_half_edges() {
        let he = dcel.half_edge(HalfEdgeId(h));
        let unit  = face_to_unit[he.face.0];
        let other = face_to_unit[dcel.half_edge(he.twin).face.0];
        if unit != other {
            triples.push((unit, other, edge_length[h / 2]));
        }
    }

    AdjacencyMatrix::from_directed_pairs_weighted(num_units, triples)
}

/// Start from Rook pairs, then add all unit-pairs that share a vertex star.
pub(crate) fn build_touching(dcel: &Dcel<Coord<f64>>, face_to_unit: &[UnitId], num_units: usize) -> AdjacencyMatrix {
    let mut pairs = Vec::<(UnitId, UnitId)>::new();

    for v in 0..dcel.num_vertices() {
        let start = match dcel.vertex(VertexId(v)).half_edge {
            Some(he) => he,
            None => continue,
        };
        let units: Vec<UnitId> = dcel
            .vertex_star(start)
            .map(|he| face_to_unit[dcel.half_edge(he).face.0])
            .collect();

        for &a in &units {
            for &b in &units {
                if a != b {
                    pairs.push((a, b));
                }
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
    fn edge_weight_at_matches_shared_boundary_length() {
        let r = make_two_unit_region();
        // For each pair of Rook-adjacent units, the CSR weight should
        // equal the shared_boundary_length computed from the DCEL.
        for uid in r.unit_ids() {
            let offset = r.adjacency().offset(uid);
            for (i, &nb) in r.neighbors(uid).iter().enumerate() {
                let csr_weight = r.edge_weight_at(offset + i);
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
