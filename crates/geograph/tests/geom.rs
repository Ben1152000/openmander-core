// Integration tests for Region geometry queries:
//   area, perimeter, exterior_boundary_length, centroid, bounds, is_exterior,
//   boundary, and their subset (_of) variants, plus edge metrics.

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

fn two_squares() -> Region {
    let geoms = vec![
        MultiPolygon(vec![rect_poly(0.0, 0.0, 1.0, 1.0)]),
        MultiPolygon(vec![rect_poly(1.0, 0.0, 2.0, 1.0)]),
    ];
    Region::new(geoms, 1e-7).expect("two-square construction failed")
}

/// Four squares in a 2×2 grid:
///
/// ```text
/// u2 u3
/// u0 u1
/// ```
///
/// u0 is interior-adjacent to u1 (right), u2 (above), and corner-touches u3.
fn four_squares() -> Region {
    let geoms = vec![
        MultiPolygon(vec![rect_poly(0.0, 0.0, 1.0, 1.0)]),
        MultiPolygon(vec![rect_poly(1.0, 0.0, 2.0, 1.0)]),
        MultiPolygon(vec![rect_poly(0.0, 1.0, 1.0, 2.0)]),
        MultiPolygon(vec![rect_poly(1.0, 1.0, 2.0, 2.0)]),
    ];
    Region::new(geoms, 1e-7).expect("four-square construction failed")
}

#[test]
fn area_of_single_unit_matches_known_value() {
    let r = two_squares();
    // Both units are 1°×1° squares near the equator.
    // Area ≈ 1 deg² × cos(0.5° in rad) × 111320² ≈ 1.239e10 m².
    // Just check it's in a reasonable ballpark (> 1e10).
    let a = r.area(UnitId(0));
    assert!(a > 1e10, "area = {a}, expected > 1e10 m²");
    assert!(a < 2e10, "area = {a}, expected < 2e10 m²");
}

#[test]
fn area_of_subset_equals_sum_of_individual_areas() {
    let r = two_squares();
    let sum = r.area(UnitId(0)) + r.area(UnitId(1));
    let combined = r.area_of(r.unit_ids());
    assert!(
        (sum - combined).abs() < 1e-6,
        "sum={sum}, combined={combined}"
    );
}

#[test]
fn perimeter_of_subset_excludes_shared_internal_edges() {
    let r = two_squares();
    // Individual perimeters include the shared edge on both sides.
    let p0 = r.perimeter(UnitId(0));
    let p1 = r.perimeter(UnitId(1));
    // Combined perimeter should exclude the internal shared edge.
    let combined = r.perimeter_of(r.unit_ids());
    // The combined perimeter is the outer boundary only: 6 edges of length ~111 km each.
    // Each individual has 4 boundary edges, so p0+p1 = 8 edges worth.
    // Combined = 6 edges worth (the shared edge removed from both sides).
    assert!(combined < p0 + p1, "combined perimeter should be less than sum");
    // The shared edge is approximately 111 km (1 degree of latitude).
    let shared = r.shared_boundary_length(UnitId(0), UnitId(1));
    assert!(
        (combined - (p0 + p1 - 2.0 * shared)).abs() < 1.0,
        "combined={combined}, p0+p1-2*shared={}", p0 + p1 - 2.0 * shared
    );
}

#[test]
fn exterior_boundary_length_is_zero_for_interior_unit() {
    // In a 3×3 grid, the centre unit (1,1) has no exterior boundary.
    let mut geoms = Vec::new();
    for row in 0..3 {
        for col in 0..3 {
            let x0 = col as f64;
            let y0 = row as f64;
            geoms.push(MultiPolygon(vec![rect_poly(x0, y0, x0 + 1.0, y0 + 1.0)]));
        }
    }
    let r = Region::new(geoms, 1e-7).expect("3x3 grid construction failed");
    // Unit 4 is at (1,1) — the centre cell.
    let centre = UnitId(4);
    assert!(
        !r.is_exterior(centre),
        "centre unit should not be exterior"
    );
    assert!(
        r.exterior_boundary_length(centre) < 1e-6,
        "centre unit exterior_boundary_length should be ~0, got {}",
        r.exterior_boundary_length(centre)
    );
}

#[test]
fn exterior_boundary_length_of_subset_matches_sum() {
    let r = two_squares();
    let sum = r.exterior_boundary_length(UnitId(0)) + r.exterior_boundary_length(UnitId(1));
    let combined = r.exterior_boundary_length_of(r.unit_ids());
    assert!(
        (sum - combined).abs() < 1e-6,
        "sum={sum}, combined={combined}"
    );
}

#[test]
fn bounds_of_subset_contains_all_unit_bounds() {
    let r = four_squares();
    let combined = r.bounds_of(r.unit_ids());
    for uid in r.unit_ids() {
        let b = r.bounds(uid);
        assert!(
            combined.min().x <= b.min().x + 1e-12
                && combined.min().y <= b.min().y + 1e-12
                && combined.max().x >= b.max().x - 1e-12
                && combined.max().y >= b.max().y - 1e-12,
            "bounds_of does not contain bounds of {uid}"
        );
    }
}

#[test]
fn boundary_of_single_unit_is_closed() {
    let r = two_squares();
    let b = r.boundary_of([UnitId(0)]);
    assert!(!b.0.is_empty(), "boundary_of should return at least one ring");
    for ring in &b.0 {
        let pts = &ring.0;
        assert_eq!(pts.first(), pts.last(), "boundary ring is not closed");
    }
}

#[test]
fn boundary_of_single_unit_has_one_ring() {
    let r = two_squares();
    let b = r.boundary_of([UnitId(0)]);
    assert_eq!(b.0.len(), 1, "single convex unit should have exactly one boundary ring");
}

#[test]
fn boundary_of_all_units_has_one_ring() {
    let r = two_squares();
    let b = r.boundary_of(r.unit_ids());
    assert_eq!(b.0.len(), 1, "two adjacent units should have one outer boundary ring");
}

#[test]
fn boundary_of_all_units_excludes_internal_edge() {
    let r = two_squares();
    // Single unit: 4 edges → 5 coords (closing).
    let b_single = r.boundary_of([UnitId(0)]);
    assert_eq!(b_single.0[0].0.len(), 5, "single unit boundary has 5 coords");
    // Both units merged: outer boundary is 6 edges → 7 coords (closing).
    let b_both = r.boundary_of(r.unit_ids());
    assert_eq!(b_both.0[0].0.len(), 7, "merged boundary has 7 coords");
}

#[test]
fn compactness_of_circle_approximation_near_one() {
    // A regular polygon with many sides should have compactness approaching 1.
    // Build a 64-gon approximating a circle.
    let n = 64;
    let mut coords = Vec::with_capacity(n + 1);
    for i in 0..n {
        let angle = 2.0 * std::f64::consts::PI * i as f64 / n as f64;
        coords.push(Coord {
            x: 1.0 + 0.5 * angle.cos(), // centred near (1, 1)
            y: 1.0 + 0.5 * angle.sin(),
        });
    }
    coords.push(coords[0]); // close the ring
    let poly = Polygon::new(LineString(coords), vec![]);
    let geoms = vec![MultiPolygon(vec![poly])];
    let r = Region::new(geoms, 1e-7).expect("circle construction failed");
    let c = r.compactness_of([UnitId(0)]);
    assert!(
        c > 0.95,
        "64-gon compactness should be near 1.0, got {c}"
    );
}

#[test]
fn union_of_single_unit_has_one_polygon() {
    let r = two_squares();
    let mp = r.union_of([UnitId(0)]);
    assert_eq!(mp.0.len(), 1, "single unit union should be one polygon");
}

#[test]
fn union_of_single_unit_exterior_ring_has_five_coords() {
    let r = two_squares();
    let mp = r.union_of([UnitId(0)]);
    assert_eq!(mp.0[0].exterior().0.len(), 5, "square has 4 corners + closing = 5 coords");
}

#[test]
fn union_of_all_units_has_one_polygon() {
    let r = two_squares();
    let mp = r.union_of(r.unit_ids());
    assert_eq!(mp.0.len(), 1, "two adjacent units union should be one polygon");
}

#[test]
fn union_of_all_units_exterior_ring_has_seven_coords() {
    // Merged 2×1 rectangle: 6 edges → 7 coords (closing).
    let r = two_squares();
    let mp = r.union_of(r.unit_ids());
    assert_eq!(mp.0[0].exterior().0.len(), 7, "merged rectangle has 6 corners + closing");
}

#[test]
fn union_of_all_units_has_no_holes() {
    let r = two_squares();
    let mp = r.union_of(r.unit_ids());
    assert!(mp.0[0].interiors().is_empty(), "simple union should have no holes");
}

#[test]
fn shared_boundary_length_is_zero_for_non_adjacent_units() {
    // In the L-shape, u1 and u2 share only a corner.
    let geoms = vec![
        MultiPolygon(vec![rect_poly(0.0, 0.0, 1.0, 1.0)]),
        MultiPolygon(vec![rect_poly(1.0, 0.0, 2.0, 1.0)]),
        MultiPolygon(vec![rect_poly(0.0, 1.0, 1.0, 2.0)]),
    ];
    let r = Region::new(geoms, 1e-7).expect("L-shape construction failed");
    assert!(
        r.shared_boundary_length(UnitId(1), UnitId(2)) < 1e-6,
        "corner-touching units should have zero shared boundary"
    );
}

#[test]
fn shared_boundary_length_is_symmetric() {
    let r = two_squares();
    let ab = r.shared_boundary_length(UnitId(0), UnitId(1));
    let ba = r.shared_boundary_length(UnitId(1), UnitId(0));
    assert!(
        (ab - ba).abs() < 1e-6,
        "shared_boundary_length is not symmetric: {ab} vs {ba}"
    );
}
