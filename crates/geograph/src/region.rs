mod adj;
mod build;
mod geom;
mod topo;

pub use build::RegionError;

use geo::{Coord, MultiPolygon, Rect};

use crate::adj::AdjacencyMatrix;
use crate::dcel::Dcel;
use crate::rtree::SpatialIndex;
use crate::unit::UnitId;

// ---------------------------------------------------------------------------
// Region
// ---------------------------------------------------------------------------

/// A planar map: a complete subdivision of a geographic region into
/// non-overlapping `Unit`s.
///
/// See DESIGN.md for a full description of the internal representation and
/// the public API contract.
pub struct Region {
    /// The half-edge data structure encoding the full planar embedding.
    pub(crate) dcel: Dcel<Coord<f64>>,

    /// Maps every DCEL face (including `OUTER_FACE`) to its owning unit.
    /// `UnitId::EXTERIOR` for the unbounded face and any interior gaps.
    pub(crate) face_to_unit: Vec<UnitId>,

    /// Original input geometries, indexed by `UnitId.0`.
    /// `UnitId::EXTERIOR` has no entry here.
    pub(crate) geometries: Vec<MultiPolygon<f64>>,

    /// Pre-cached area in m² (per-edge cos(φ_mid) weighted shoelace).
    pub(crate) area: Vec<f64>,

    /// Pre-cached total perimeter in m (includes hole boundaries).
    pub(crate) perimeter: Vec<f64>,

    /// Pre-cached length of boundary touching `UnitId::EXTERIOR`, in m.
    /// Zero for interior units.
    pub(crate) exterior_boundary_length: Vec<f64>,

    /// Pre-cached centroid in lon/lat.
    pub(crate) centroid: Vec<Coord<f64>>,

    /// Pre-cached axis-aligned bounding box in lon/lat.
    pub(crate) bounds: Vec<Rect<f64>>,

    /// Pre-cached bounding box of the entire region (union of all unit bounds).
    pub(crate) bounds_all: Rect<f64>,

    /// Pre-cached flag: true if the unit has any half-edge whose twin belongs
    /// to `UnitId::EXTERIOR`.
    pub(crate) is_exterior: Vec<bool>,

    /// Edge lengths in m, indexed by `HalfEdgeId.0 / 2` (one entry per
    /// undirected edge).  Computed with the per-edge cos(φ_mid) correction.
    pub(crate) edge_length: Vec<f64>,

    /// Rook adjacency matrix (shared edge).
    pub(crate) adjacent: AdjacencyMatrix,

    /// Queen adjacency matrix (shared point, superset of Rook).
    pub(crate) touching: AdjacencyMatrix,

    /// R-tree spatial index over unit bounding boxes.
    pub(crate) rtree: SpatialIndex,
}

impl Region {
    // -----------------------------------------------------------------------
    // Unit access
    // -----------------------------------------------------------------------

    /// Number of units (excluding `UnitId::EXTERIOR`).
    #[inline]
    pub fn num_units(&self) -> usize { self.geometries.len() }

    /// Iterate over all valid `UnitId`s (excluding `UnitId::EXTERIOR`).
    #[inline]
    pub fn unit_ids(&self) -> impl Iterator<Item = UnitId> + '_ {
        (0..self.num_units()).map(|i| UnitId(i as u32))
    }

    /// The original `MultiPolygon` geometry for `unit`.
    #[inline]
    pub fn geometry(&self, unit: UnitId) -> &MultiPolygon<f64> {
        &self.geometries[unit.0 as usize]
    }

    // -----------------------------------------------------------------------
    // Validation
    // -----------------------------------------------------------------------

    /// Check structural invariants of the `Region`.
    ///
    /// This is called automatically under `debug_assertions` at the end of
    /// `Region::new()`.  It can also be called explicitly after
    /// deserialisation (`io::read`) to verify data integrity.
    ///
    /// Checks:
    /// - Every half-edge's twin-of-twin is itself.
    /// - Every half-edge's `next.prev` and `prev.next` are itself.
    /// - Every bounded face has at least one half-edge.
    /// - Every unit has non-negative area.
    pub fn validate(&self) -> Result<(), RegionError> {
        use crate::dcel::HalfEdgeId;

        let nhe = self.dcel.num_half_edges();

        // Twin consistency: twin(twin(h)) == h
        for h in 0..nhe {
            let twin = self.dcel.half_edge(HalfEdgeId(h)).twin;
            let twin_twin = self.dcel.half_edge(twin).twin;
            if twin_twin != HalfEdgeId(h) {
                return Err(RegionError::ValidationError(
                    format!("half-edge {h}: twin(twin) = {} != {h}", twin_twin.0),
                ));
            }
        }

        // Next/prev consistency: next(h).prev == h and prev(h).next == h
        for h in 0..nhe {
            let he = self.dcel.half_edge(HalfEdgeId(h));
            let next_prev = self.dcel.half_edge(he.next).prev;
            if next_prev != HalfEdgeId(h) {
                return Err(RegionError::ValidationError(
                    format!("half-edge {h}: next({}).prev = {} != {h}", he.next.0, next_prev.0),
                ));
            }
            let prev_next = self.dcel.half_edge(he.prev).next;
            if prev_next != HalfEdgeId(h) {
                return Err(RegionError::ValidationError(
                    format!("half-edge {h}: prev({}).next = {} != {h}", he.prev.0, prev_next.0),
                ));
            }
        }

        // Every bounded face (FaceId >= 1) has a half-edge.
        for f in 1..self.dcel.num_faces() {
            if self.dcel.face(crate::dcel::FaceId(f)).half_edge.is_none() {
                return Err(RegionError::ValidationError(
                    format!("face {f}: bounded face has no half-edge"),
                ));
            }
        }

        // Non-negative areas.
        for u in 0..self.num_units() {
            if self.area[u] < 0.0 {
                return Err(RegionError::ValidationError(
                    format!("unit {u}: negative area {}", self.area[u]),
                ));
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Shared fixture used by unit tests in `adj`, `geom`, and `topo` submodules.
///
/// Layout — two unit-square cells side by side:
///
/// ```text
/// D(0,1)---E(1,1)---F(2,1)
///   |  u0  |  u1   |
/// A(0,0)---B(1,0)---C(2,0)
/// ```
///
/// All edges have length 1.0 (degree units, no cos correction).
/// Pre-cached values are set directly rather than computed.
#[cfg(test)]
pub(crate) mod test_helpers {
    use geo::{Coord, LineString, MultiPolygon, Polygon, Rect};

    use crate::dcel::{Dcel, OUTER_FACE};
    use crate::rtree::SpatialIndex;
    use crate::unit::UnitId;

    use super::Region;
    use super::adj::{build_adjacent, build_touching};

    pub(crate) fn make_two_unit_region() -> Region {
        let mut dcel: Dcel<Coord<f64>> = Dcel::new();

        let a = dcel.add_vertex(Coord { x: 0.0, y: 0.0 });
        let b = dcel.add_vertex(Coord { x: 1.0, y: 0.0 });
        let c = dcel.add_vertex(Coord { x: 2.0, y: 0.0 });
        let d = dcel.add_vertex(Coord { x: 0.0, y: 1.0 });
        let e = dcel.add_vertex(Coord { x: 1.0, y: 1.0 });
        let f = dcel.add_vertex(Coord { x: 2.0, y: 1.0 });

        let left  = dcel.add_face(); // FaceId(1) → UnitId(0)
        let right = dcel.add_face(); // FaceId(2) → UnitId(1)

        // HE indices: ab=0, ba=1, be=2, eb=3, ed=4, de=5,
        //             da=6, ad=7, bc=8, cb=9, cf=10, fc=11, fe=12, ef=13
        let (ab, ba) = dcel.add_edge(a, b, left,      OUTER_FACE);
        let (be, eb) = dcel.add_edge(b, e, left,      right     );
        let (ed, de) = dcel.add_edge(e, d, left,      OUTER_FACE);
        let (da, ad) = dcel.add_edge(d, a, left,      OUTER_FACE);
        let (bc, cb) = dcel.add_edge(b, c, right,     OUTER_FACE);
        let (cf, fc) = dcel.add_edge(c, f, right,     OUTER_FACE);
        let (fe, ef) = dcel.add_edge(f, e, right,     OUTER_FACE);

        // Left face: A→B→E→D→A
        dcel.set_next(ab, be); dcel.set_next(be, ed);
        dcel.set_next(ed, da); dcel.set_next(da, ab);

        // Right face: B→C→F→E→B
        dcel.set_next(bc, cf); dcel.set_next(cf, fe);
        dcel.set_next(fe, eb); dcel.set_next(eb, bc);

        // Outer face: A→D→E→F→C→B→A
        dcel.set_next(ad, de); dcel.set_next(de, ef);
        dcel.set_next(ef, fc); dcel.set_next(fc, cb);
        dcel.set_next(cb, ba); dcel.set_next(ba, ad);

        dcel.face_mut(left      ).half_edge = Some(ab);
        dcel.face_mut(right     ).half_edge = Some(bc);
        dcel.face_mut(OUTER_FACE).half_edge = Some(ad);

        let face_to_unit = vec![
            UnitId::EXTERIOR, // FaceId(0) = outer
            UnitId(0),        // FaceId(1) = left
            UnitId(1),        // FaceId(2) = right
        ];

        let adj      = build_adjacent (&dcel, &face_to_unit, 2);
        let touching = build_touching(&dcel, &face_to_unit, 2);

        let bounds = vec![
            Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 1.0, y: 1.0 }),
            Rect::new(Coord { x: 1.0, y: 0.0 }, Coord { x: 2.0, y: 1.0 }),
        ];
        let rtree = SpatialIndex::new(&bounds);

        let make_poly = |pts: &[(f64, f64)]| -> MultiPolygon<f64> {
            MultiPolygon(vec![Polygon::new(
                LineString(pts.iter().map(|&(x, y)| Coord { x, y }).collect()),
                vec![],
            )])
        };

        Region {
            dcel,
            face_to_unit,
            geometries: vec![
                make_poly(&[(0.0,0.0),(1.0,0.0),(1.0,1.0),(0.0,1.0),(0.0,0.0)]),
                make_poly(&[(1.0,0.0),(2.0,0.0),(2.0,1.0),(1.0,1.0),(1.0,0.0)]),
            ],
            // All cached scalars set to known values for test assertions.
            area:                     vec![10.0, 20.0],
            perimeter:                vec![4.0,  4.0 ],
            exterior_boundary_length: vec![3.0,  3.0 ],
            centroid: vec![
                Coord { x: 0.5, y: 0.5 },
                Coord { x: 1.5, y: 0.5 },
            ],
            bounds,
            bounds_all: Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 2.0, y: 1.0 }),
            is_exterior: vec![true, true],
            // 7 undirected edges, each with length 1.0
            edge_length: vec![1.0; 7],
            adjacent: adj,
            touching,
            rtree,
        }
    }
}
