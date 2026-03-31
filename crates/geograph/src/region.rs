pub(crate) mod adj;
pub(crate) mod build;
mod geom;
mod simplify;
mod topo;
mod validate;

pub use build::RegionError;

use geo::{Coord, MultiPolygon, Rect};

use crate::adj::AdjacencyMatrix;
use crate::dcel::{Dcel, FaceId, HalfEdgeId};
use crate::rtree::SpatialIndex;
use crate::unit::UnitId;

// ---------------------------------------------------------------------------
// Region
// ---------------------------------------------------------------------------

/// A planar subdivision of a geographic region into non-overlapping [`UnitId`]-indexed units.
///
/// Encodes a complete planar map — typically census blocks, precincts, or similar units —
/// and provides efficient access to geometry, adjacency, and topology.
///
/// Build from a [`Vec`] of [`geo::MultiPolygon`] geometries (one per unit) using
/// [`Region::new`]. Each geometry becomes a unit assigned a [`UnitId`] in input order.
/// [`UnitId::EXTERIOR`] represents the area outside the region and interior gaps.
///
/// # Capabilities
///
/// - **Geometry** — area, perimeter, centroid, bounds, boundary, convex hull, union,
///   compactness. See [`Region::area`], [`Region::boundary_of`], [`Region::union_of`],
///   [`Region::compactness_of`].
/// - **Adjacency** — rook (shared edge) and queen (shared point) neighbors, shared boundary
///   lengths. See [`Region::neighbors`], [`Region::touching`],
///   [`Region::shared_boundary_length`].
/// - **Topology** — contiguity, connected components, holes, enclaves.
///   See [`Region::is_contiguous`], [`Region::connected_components`],
///   [`Region::has_holes`], [`Region::enclaves`].
/// - **Spatial queries** — point lookup and envelope queries via R-tree.
///   See [`Region::unit_at`], [`Region::units_in_envelope`].
/// - **Simplification** — topology-preserving Douglas–Peucker with shared boundaries
///   simplified identically to avoid gaps. See [`Region::simplified_geometries`].
///
/// Serialise and deserialise with [`crate::io::write`] and [`crate::io::read`].
#[derive(Clone)]
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

    /// Maps each unit to the DCEL faces it owns.
    /// Indexed by `UnitId.0`; most units have exactly one face.
    pub(crate) unit_to_faces: Vec<Vec<FaceId>>,

    /// Starting half-edges for inner boundary cycles of each face, indexed by `FaceId.0`.
    ///
    /// Populated only for faces containing enclaves — one entry per enclave pocket,
    /// pointing to the start of its inner ring cycle, which is otherwise unreachable
    /// via `face.half_edge`.
    pub(crate) face_inner_cycles: Vec<Vec<HalfEdgeId>>,
}

impl Region {
    // -----------------------------------------------------------------------
    // Unit access
    // -----------------------------------------------------------------------

    /// Returns the number of units in the region, excluding [`UnitId::EXTERIOR`].
    #[inline]
    pub fn num_units(&self) -> usize { self.geometries.len() }

    /// Returns an iterator over all [`UnitId`]s in the region, excluding [`UnitId::EXTERIOR`].
    ///
    /// IDs are yielded in input order: `UnitId(0)`, `UnitId(1)`, …, `UnitId(n-1)`.
    #[inline]
    pub fn unit_ids(&self) -> impl Iterator<Item = UnitId> + '_ {
        (0..self.num_units()).map(|i| UnitId(i as u32))
    }

    /// Returns the [`MultiPolygon`] geometry for `unit`.
    ///
    /// The geometry is reconstructed from the internal DCEL during construction, so
    /// hole rings for donut-shaped units (e.g. a block surrounding an enclave) are
    /// correctly included even if the original input polygon did not encode them.
    ///
    /// <div class="warning">Panics if <code>unit</code> is <a href="UnitId::EXTERIOR"><code>UnitId::EXTERIOR</code></a> or out of range.</div>
    #[inline]
    pub fn geometry(&self, unit: UnitId) -> &MultiPolygon<f64> {
        &self.geometries[unit.0 as usize]
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
        let (ab, ba) = dcel.add_edge(a, b, left, OUTER_FACE);
        let (be, eb) = dcel.add_edge(b, e, left, right);
        let (ed, de) = dcel.add_edge(e, d, left, OUTER_FACE);
        let (da, ad) = dcel.add_edge(d, a, left, OUTER_FACE);
        let (bc, cb) = dcel.add_edge(b, c, right, OUTER_FACE);
        let (cf, fc) = dcel.add_edge(c, f, right, OUTER_FACE);
        let (fe, ef) = dcel.add_edge(f, e, right, OUTER_FACE);

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

        // 7 undirected edges, each with length 1.0
        let edge_length = vec![1.0; 7];
        let adj      = build_adjacent (&dcel, &face_to_unit, &edge_length, 2);
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

        let unit_to_faces = crate::region::build::compute_unit_to_faces(&face_to_unit, 2);
        let face_inner_cycles = crate::region::build::compute_face_inner_cycles(&dcel);

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
            edge_length,
            adjacent: adj,
            touching,
            rtree,
            unit_to_faces,
            face_inner_cycles,
        }
    }
}
