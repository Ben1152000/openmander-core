use anyhow::{anyhow, Ok, Result};
use geo::{Contains, InteriorPoint};
use rstar::{AABB};

use crate::Geometries;

impl Geometries {
    /// For each geometry in `self`, pick its interior point and find the (unique)
    /// geometry in `other` that contains it. Errors if none is found.
    pub fn compute_crosswalks(&self, other: &Geometries) -> Result<Vec<u32>> {
        let mut map = Vec::with_capacity(self.shapes.len());

        for (i, a) in self.shapes.iter().enumerate() {
            // Guaranteed interior point for areal geometries; returns None for degenerate/empty.
            let pt = a.interior_point()
                .ok_or_else(|| anyhow!("self.shapes[{i}] has no interior point (empty/degenerate)"))?;

            // Query OTHER’s R-tree with a degenerate AABB at `pt`
            let env = AABB::from_corners([pt.x(), pt.y()], [pt.x(), pt.y()]);

            // Among bbox candidates, pick the one whose geometry contains the point.
            let parent = other.rtree
                .locate_in_envelope_intersecting(&env)
                .map(|bb| bb.idx)
                .find(|&j| other.shapes[j].contains(&pt))
                .ok_or_else(|| {
                    anyhow!("No parent in `other` contains the interior point of self.shapes[{i}]")
                })?;

            map.push(parent as u32);
        }

        Ok(map)
    }
}
