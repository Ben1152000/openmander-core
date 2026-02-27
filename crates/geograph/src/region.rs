mod adj;
mod geom;
mod topo;

use std::sync::OnceLock;

use geo::{Coord, MultiPolygon, Rect};

use crate::adj::AdjacencyMatrix;
use crate::dcel::Dcel;
use crate::unit::UnitId;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur when constructing a `Region`.
#[derive(Debug)]
pub enum RegionError {
    /// One or more input geometries are invalid or empty.
    InvalidGeometry(String),
    /// The vertex snapping step failed.
    SnapError(String),
    /// The DCEL construction step failed.
    TopologyError(String),
}

// ---------------------------------------------------------------------------
// Region
// ---------------------------------------------------------------------------

/// A planar map: a complete subdivision of a geographic region into
/// non-overlapping `Unit`s.
///
/// See DESIGN.md for a full description of the internal representation and
/// the public API contract.
pub struct Region {
    // ----- DCEL -----

    /// The half-edge data structure encoding the full planar embedding.
    pub(crate) dcel: Dcel<Coord<f64>>,

    /// Maps every DCEL face (including `OUTER_FACE`) to its owning unit.
    /// `UnitId::EXTERIOR` for the unbounded face and any interior gaps.
    pub(crate) face_to_unit: Vec<UnitId>,

    // ----- Per-unit data -----

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

    /// Pre-cached flag: true if the unit has any half-edge whose twin belongs
    /// to `UnitId::EXTERIOR`.
    pub(crate) is_exterior: Vec<bool>,

    // ----- Per-edge data -----

    /// Edge lengths in m, indexed by `HalfEdgeId.0 / 2` (one entry per
    /// undirected edge).  Computed with the per-edge cos(φ_mid) correction.
    pub(crate) edge_length: Vec<f64>,

    // ----- Lazy adjacency -----

    /// Rook adjacency matrix (shared edge); built on first access.
    pub(crate) adj: OnceLock<AdjacencyMatrix>,

    /// Queen adjacency matrix (shared point, superset of Rook); built on
    /// first access.
    pub(crate) touching: OnceLock<AdjacencyMatrix>,
}

impl Region {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Build a `Region` from a vector of `MultiPolygon` geometries (one per
    /// unit, in the order that determines `UnitId` assignment).
    ///
    /// `snap_tol` is the vertex snapping tolerance in degrees (see DESIGN.md
    /// §9, question 1).  A value of `1e-7` is appropriate for most inputs.
    pub fn new(
        geometries: Vec<MultiPolygon<f64>>,
        snap_tol: f64,
    ) -> Result<Self, RegionError> {
        todo!()
    }

    /// Deserialise a `Region` from a GeoJSON string.
    ///
    /// Each feature in the collection becomes one unit; `UnitId` is assigned
    /// in feature order.
    pub fn from_geojson(data: &str, snap_tol: f64) -> Result<Self, RegionError> {
        todo!()
    }

    // -----------------------------------------------------------------------
    // Unit access
    // -----------------------------------------------------------------------

    /// Number of units (excluding `UnitId::EXTERIOR`).
    pub fn num_units(&self) -> usize {
        todo!()
    }

    /// Iterate over all valid `UnitId`s (excluding `UnitId::EXTERIOR`).
    pub fn unit_ids(&self) -> impl Iterator<Item = UnitId> + '_ {
        (0..self.num_units()).map(|i| UnitId(i as u32))
    }

    /// The original `MultiPolygon` geometry for `unit`.
    pub fn geometry(&self, unit: UnitId) -> &MultiPolygon<f64> {
        todo!()
    }
}
