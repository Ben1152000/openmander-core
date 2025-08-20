use anyhow::{bail, Result};
use geo::{BoundingRect, Relate};
use rstar::{AABB};

use super::PlanarPartition;

impl PlanarPartition {
    /// Returns true iff any two MultiPolygons overlap in area (or one contains the other).
    /// Pure boundary touches (edge or point) are NOT considered overlaps.
    pub fn assert_no_overlaps(&self, tol: f64) -> Result<()> {
        for i in 0..self.geoms.len() {
            let Some(rect) = self.geoms[i].bounding_rect() else { continue };
            let search = AABB::from_corners(
                [rect.min().x - tol, rect.min().y - tol],
                [rect.max().x + tol, rect.max().y + tol],
            );
    
            for cand in self.rtree.locate_in_envelope_intersecting(&search) {
                let j = cand.idx;
                if j <= i { continue; }
    
                // One relate() call gives you the full DE-9IM:
                let im = self.geoms[i].relate(&self.geoms[j]);
    
                // Overlap (including containment/equality) = intersects but not merely touching.
                if im.is_intersects() && !im.is_touches() {
                    bail!("Overlapping geometries found: {i} and {j}");
                }
            }
        }
        
        Ok(())
    }
}
