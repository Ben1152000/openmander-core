// Integration tests for Region adjacency queries:
//   Region::are_adjacent, neighbors, adjacency(), touching()

use geo::{Coord, LineString, MultiPolygon, Polygon};
use geograph::{Region, UnitId};

fn rect_poly(x0: f64, y0: f64, x1: f64, y1: f64) -> Polygon<f64> {
    Polygon::new(
        LineString(vec![
            Coord { x: x0, y: y0 },
            Coord { x: x1, y: y0 },
            Coord { x: x1, y: y1 },
            Coord { x: x0, y: y1 },
            Coord { x: x0, y: y0 },
        ]),
        vec![],
    )
}

/// Three squares in an L-shape:
///
/// ```text
///  u2
/// u0 u1
/// ```
///
/// u0 and u1 share an edge (Rook adjacent).
/// u0 and u2 share an edge (Rook adjacent).
/// u1 and u2 share only a corner at (1,1) — Queen only.
fn l_shape() -> Region {
    let geoms = vec![
        MultiPolygon(vec![rect_poly(0.0, 0.0, 1.0, 1.0)]),
        MultiPolygon(vec![rect_poly(1.0, 0.0, 2.0, 1.0)]),
        MultiPolygon(vec![rect_poly(0.0, 1.0, 1.0, 2.0)]),
    ];
    Region::new(geoms, 1e-7).expect("L-shape construction failed")
}

#[test]
fn rook_adjacent_units_share_an_edge() {
    let r = l_shape();
    assert!(r.are_adjacent(UnitId(0), UnitId(1)));
    assert!(r.are_adjacent(UnitId(0), UnitId(2)));
}

#[test]
fn rook_non_adjacent_units_do_not_share_an_edge() {
    let r = l_shape();
    // u1 and u2 share only a corner — not Rook adjacent.
    assert!(!r.are_adjacent(UnitId(1), UnitId(2)));
}

#[test]
fn queen_adjacency_includes_corner_touches() {
    let r = l_shape();
    // u1 and u2 share a corner at (1,1) — Queen adjacent.
    assert!(r.touching().contains(UnitId(1), UnitId(2)));
    assert!(r.touching().contains(UnitId(2), UnitId(1)));
}

#[test]
fn rook_is_subset_of_queen() {
    let r = l_shape();
    let rook = r.adjacency();
    let queen = r.touching();
    for uid in r.unit_ids() {
        for &nb in rook.neighbors(uid) {
            assert!(
                queen.contains(uid, nb),
                "Rook edge ({uid}, {nb}) missing from Queen"
            );
        }
    }
}

#[test]
fn neighbors_returns_sorted_slice() {
    let r = l_shape();
    for uid in r.unit_ids() {
        let ns = r.neighbors(uid);
        for w in ns.windows(2) {
            assert!(w[0] < w[1], "neighbors of {uid} are not sorted");
        }
    }
}

#[test]
fn adjacency_matrix_row_matches_neighbors() {
    let r = l_shape();
    let adj = r.adjacency();
    for uid in r.unit_ids() {
        assert_eq!(
            adj.neighbors(uid),
            r.neighbors(uid),
            "adjacency().neighbors({uid}) differs from neighbors({uid})"
        );
    }
}
