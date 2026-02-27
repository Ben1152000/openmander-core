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
pub fn snap_vertices(rings: &mut [Vec<Vec<Coord<f64>>>], tolerance: f64) {
    todo!()
}
