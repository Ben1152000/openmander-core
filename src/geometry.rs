use anyhow::{anyhow, Ok, Result};
use geo::{BoundingRect, Contains, InteriorPoint, MultiPolygon, Rect, Relate};
use rstar::{RTree, RTreeObject, AABB};

#[derive(Debug, Clone)]
pub struct BoundingBox {
    idx: usize, // Index of corresponding MultiPolygon in geoms
    bbox: Rect<f64>,
}

impl RTreeObject for BoundingBox {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners(self.bbox.min().into(), self.bbox.max().into())
    }
}

#[derive(Debug, Clone)]
pub struct PlanarPartition {
    pub geoms: Vec<MultiPolygon<f64>>,
    pub rtree: RTree<BoundingBox>,
    pub adj_list: Vec<Vec<u32>>,
}

impl PlanarPartition {

    /// Construct a PlanarPartition object from a vector of MultiPolygons
    pub fn new(polygons: Vec<MultiPolygon<f64>>) -> Self {
        Self {
            adj_list: polygons.iter().map(|_| Vec::new()).collect(),
            rtree: RTree::bulk_load(polygons.iter().enumerate()
                .map(|(i, poly)| BoundingBox { idx: i, bbox: poly.bounding_rect().unwrap() })
                .collect()),
            geoms: polygons,
        }
    }

    #[inline] pub fn len(&self) -> usize { self.geoms.len() }

    #[inline] pub fn is_empty(&self) -> bool { self.geoms.is_empty() }

    /// Populate `adj_list` with rook contiguity (shared edge with positive length).
    /// Uses DE‑9IM string: require `touches` AND boundary∩boundary has dimension 1.
    pub fn compute_adjacencies(&mut self) -> Result<()> {
        // clear any existing adjacencies
        for nbrs in &mut self.adj_list {
            nbrs.clear();
        }

        // bbox padding if you expect FP jitter; keep 0.0 if not needed
        let eps = 0.0_f64;

        for i in 0..self.geoms.len() {
            let Some(rect) = self.geoms[i].bounding_rect() else { continue };
            let search = AABB::from_corners(
                [rect.min().x - eps, rect.min().y - eps],
                [rect.max().x + eps, rect.max().y + eps],
            );

            for cand in self.rtree.locate_in_envelope_intersecting(&search) {
                let j = cand.idx;
                if j <= i { continue; } // check each unordered pair once

                let im = self.geoms[i].relate(&self.geoms[j]);

                // Rook predicate:
                // 1) touches = true (no interior overlap)
                // 2) boundary/boundary dimension == '1' (line segment)
                //    In the 9-char DE‑9IM string, index 4 is Boundary/Boundary.
                if im.is_touches() && im.matches("****1****")? {
                    self.adj_list[i].push(j as u32);
                    self.adj_list[j].push(i as u32);
                }
            }
        }

        Ok(())
    }


    /// For each geometry in `self`, pick its interior point and find the (unique)
    /// geometry in `other` that contains it. Errors if none is found.
    pub fn compute_crosswalks(&self, other: &PlanarPartition) -> Result<Vec<u32>> {
        let mut map = Vec::with_capacity(self.geoms.len());

        for (i, a) in self.geoms.iter().enumerate() {
            // Guaranteed interior point for areal geometries; returns None for degenerate/empty.
            let pt = a.interior_point()
                .ok_or_else(|| anyhow!("self.geoms[{i}] has no interior point (empty/degenerate)"))?;

            // Query OTHER’s R-tree with a degenerate AABB at `pt`
            let env = AABB::from_corners([pt.x(), pt.y()], [pt.x(), pt.y()]);

            // Among bbox candidates, pick the one whose geometry contains the point.
            let parent = other.rtree
                .locate_in_envelope_intersecting(&env)
                .map(|bb| bb.idx)
                .find(|&j| other.geoms[j].contains(&pt))
                .ok_or_else(|| {
                    anyhow!("No parent in `other` contains the interior point of self.geoms[{i}]")
                })?;

            map.push(parent as u32);
        }

        Ok(map)
    }

    /// Returns true iff any two MultiPolygons overlap in area (or one contains the other).
    /// Pure boundary touches (edge or point) are NOT considered overlaps.
    pub fn detect_overlaps(&self, tol: f64) -> bool {
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
                    return true;
                }
            }
        }
        false
    }

}
