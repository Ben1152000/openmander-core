use anyhow::{anyhow, Ok, Result};
use geo::{Contains, InteriorPoint};
use rstar::{AABB};

use crate::Geometries;

impl Geometries {
    /// For each geometry in `self`, pick its interior point and find the (unique)
    /// geometry in `other` that contains it. Errors if none is found.
    pub fn compute_crosswalks(&self, other: &Geometries) -> Result<Vec<u32>> {
        let mut parents = Vec::with_capacity(self.shapes().len());

        for (i, polygon) in self.shapes().iter().enumerate() {
            // Guaranteed interior point for areal geometries; returns None for degenerate/empty.
            let point = polygon.interior_point()
                .ok_or_else(|| anyhow!("self.shapes[{i}] has no interior point (empty/degenerate)"))?;

            // Query OTHER’s R-tree with a degenerate AABB at `pt`
            let envelope = AABB::from_corners([point.x(), point.y()], [point.x(), point.y()]);

            // Among bbox candidates, pick the one whose geometry contains the point.
            let parent = other.query(&envelope)
                .map(|bbox| bbox.idx())
                .find(|&j| other.shapes()[j].contains(&point))
                .ok_or_else(|| anyhow!("No parent in `other` contains the interior point of self.shapes[{i}]"))?;

            parents.push(parent as u32);
        }

        Ok(parents)
    }
}
