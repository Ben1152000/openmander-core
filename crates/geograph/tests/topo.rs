// Integration tests for Region topology queries:
//   is_contiguous, connected_components, has_holes, enclaves

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

/// Two squares side by side:
///
/// ```text
/// u0 u1
/// ```
fn two_squares() -> Region {
    let geoms = vec![
        MultiPolygon(vec![rect_poly(0.0, 0.0, 1.0, 1.0)]),
        MultiPolygon(vec![rect_poly(1.0, 0.0, 2.0, 1.0)]),
    ];
    Region::new(geoms, None).expect("two-square construction failed")
}

/// Three squares in an L-shape:
///
/// ```text
///  u2
/// u0 u1
/// ```
fn l_shape() -> Region {
    let geoms = vec![
        MultiPolygon(vec![rect_poly(0.0, 0.0, 1.0, 1.0)]),
        MultiPolygon(vec![rect_poly(1.0, 0.0, 2.0, 1.0)]),
        MultiPolygon(vec![rect_poly(0.0, 1.0, 1.0, 2.0)]),
    ];
    Region::new(geoms, None).expect("L-shape construction failed")
}

/// 3×3 grid of squares:
///
/// ```text
/// u6 u7 u8
/// u3 u4 u5
/// u0 u1 u2
/// ```
///
/// u4 is the centre cell (interior, not exterior).
fn three_by_three() -> Region {
    let mut geoms = Vec::new();
    for row in 0..3 {
        for col in 0..3 {
            let x0 = col as f64;
            let y0 = row as f64;
            geoms.push(MultiPolygon(vec![rect_poly(x0, y0, x0 + 1.0, y0 + 1.0)]));
        }
    }
    Region::new(geoms, None).expect("3x3 grid construction failed")
}

// ---------------------------------------------------------------------------
// is_contiguous
// ---------------------------------------------------------------------------

#[test]
fn single_unit_is_contiguous() {
    let r = two_squares();
    assert!(r.is_contiguous([UnitId(0)]));
    assert!(r.is_contiguous([UnitId(1)]));
}

#[test]
fn all_units_are_contiguous() {
    let r = l_shape();
    assert!(r.is_contiguous(r.unit_ids()));
}

#[test]
fn disconnected_subset_is_not_contiguous() {
    // In the L-shape, u1 and u2 only share a corner (Queen, not Rook).
    let r = l_shape();
    assert!(!r.is_contiguous([UnitId(1), UnitId(2)]));
}

// ---------------------------------------------------------------------------
// connected_components
// ---------------------------------------------------------------------------

#[test]
fn connected_components_of_contiguous_subset_has_one_element() {
    let r = l_shape();
    let comps = r.connected_components(r.unit_ids());
    assert_eq!(comps.len(), 1, "all 3 L-shape units should form one component");
}

#[test]
fn connected_components_count_matches_disconnected_pieces() {
    // u1 and u2 in the L-shape are not Rook-adjacent → 2 components.
    let r = l_shape();
    let comps = r.connected_components([UnitId(1), UnitId(2)]);
    assert_eq!(comps.len(), 2, "u1 and u2 should be in separate components");

    // Each component should have exactly one unit.
    for comp in &comps {
        assert_eq!(comp.len(), 1);
    }
}

// ---------------------------------------------------------------------------
// has_holes / enclaves
// ---------------------------------------------------------------------------

#[test]
fn subset_with_no_surrounded_complement_has_no_holes() {
    // In the L-shape, selecting u0 leaves u1 and u2 in the complement.
    // Both u1 and u2 are exterior units → no enclave.
    let r = l_shape();
    assert!(!r.has_holes([UnitId(0)]));
}

#[test]
fn subset_surrounding_interior_units_has_holes() {
    // In the 3×3 grid, select all 8 border units (everything except u4).
    // The complement is {u4}, which is interior (not exterior) → enclave.
    let r = three_by_three();
    let border: Vec<UnitId> = r.unit_ids().filter(|&u| u != UnitId(4)).collect();
    assert!(
        r.has_holes(border),
        "selecting all border units should create a hole at the centre"
    );
}

#[test]
fn enclaves_returns_surrounded_complement_components() {
    // Same setup: border units selected, centre unit is the sole enclave.
    let r = three_by_three();
    let border: Vec<UnitId> = r.unit_ids().filter(|&u| u != UnitId(4)).collect();
    let enc = r.enclaves(border);
    assert_eq!(enc.len(), 1, "should have exactly one enclave");
    assert_eq!(enc[0].len(), 1, "enclave should contain one unit");
    assert_eq!(enc[0][0], UnitId(4), "enclave should be the centre unit");
}

#[test]
fn exterior_adjacent_complement_component_is_not_an_enclave() {
    // In the 3×3 grid, select only u4 (centre). The complement is all 8
    // border units — all exterior. No enclave.
    let r = three_by_three();
    let enc = r.enclaves([UnitId(4)]);
    assert!(
        enc.is_empty(),
        "complement units are all exterior, so no enclaves"
    );
}
