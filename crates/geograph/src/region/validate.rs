use crate::dcel::HalfEdgeId;

use super::{Region, RegionError};

impl Region {
    /// Checks structural invariants of the `Region` and returns an error if any are violated.
    ///
    /// Called automatically under `debug_assertions` at the end of [`Region::new`].
    /// Can also be called explicitly after deserialisation ([`crate::io::read`]) to
    /// verify data integrity of loaded files.
    ///
    /// # Checks performed
    ///
    /// - Every half-edge's twin-of-twin is itself.
    /// - Every half-edge's `next.prev` and `prev.next` are itself.
    /// - Every bounded face has at least one half-edge.
    /// - Every unit has non-negative area.
    ///
    /// # Errors
    ///
    /// Returns [`RegionError::ValidationError`] with a description of the first
    /// invariant violation found.
    pub fn validate(&self) -> Result<(), RegionError> {
        let num_half_edges = self.dcel.num_half_edges();

        // Twin consistency: twin(twin(e)) == e  (by construction: (e^1)^1 == e)
        // We validate indirectly: for each e, e.twin() == e^1, which satisfies twin(twin(e))==e.
        // A direct structural check: verify that for every e, twin(e).twin() gives back e.
        for e in 0..num_half_edges {
            let id = HalfEdgeId(e as u32);
            let twin = id.twin();
            if twin.twin() != id {
                return Err(RegionError::ValidationError(
                    format!("half-edge {e}: twin(twin) = {} != {e}", twin.twin().0),
                ));
            }
        }

        // Next/prev consistency: next(e).prev == e and prev(e).next == e
        for e in 0..num_half_edges {
            let half_edge = self.dcel.half_edge(HalfEdgeId(e as u32));
            let next_prev = self.dcel.half_edge(half_edge.next).prev;
            if next_prev != HalfEdgeId(e as u32) {
                return Err(RegionError::ValidationError(
                    format!("half-edge {e}: next({}).prev = {} != {e}", half_edge.next.0, next_prev.0),
                ));
            }
            let prev_next = self.dcel.half_edge(half_edge.prev).next;
            if prev_next != HalfEdgeId(e as u32) {
                return Err(RegionError::ValidationError(
                    format!("half-edge {e}: prev({}).next = {} != {e}", half_edge.prev.0, prev_next.0),
                ));
            }
        }

        // Every bounded face (FaceId >= 1) has a half-edge.
        for f in 1..self.dcel.num_faces() {
            if self.dcel.face(crate::dcel::FaceId(f as u32)).half_edge.is_none() {
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
