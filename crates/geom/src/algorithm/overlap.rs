use anyhow::{bail, Result};
use geo::{BoundingRect, Relate};
use rstar::{AABB};

use crate::Geometries;

impl Geometries {
    /// Returns true iff any two MultiPolygons overlap in area (or one contains the other).
    /// Pure boundary touches (edge or point) are NOT considered overlaps.
    pub fn assert_no_overlaps(&self, tol: f64) -> Result<()> {
        for i in 0..self.shapes.len() {
            let Some(rect) = self.shapes[i].bounding_rect() else { continue };
            let search = AABB::from_corners(
                [rect.min().x - tol, rect.min().y - tol],
                [rect.max().x + tol, rect.max().y + tol],
            );

            for candidate in self.query(&search) {
                let j = candidate.idx();
                if j <= i { continue; }

                // One relate() call gives you the full DE-9IM:
                let im = self.shapes[i].relate(&self.shapes[j]);

                // Overlap (including containment/equality) = intersects but not merely touching.
                if im.is_intersects() && !im.is_touches() {
                    bail!("Overlapping geometries found: {i} and {j}");
                }
            }
        }
        
        Ok(())
    }
}
