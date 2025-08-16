use anyhow::{anyhow, Ok, Result};
use geo::*;
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


    /// Compute rook adjacencies by hashing shared edges. `scale` is the snapping factor
    /// used to quantize coordinates and defeat tiny FP mismatches (e.g., 1e7 for degrees).
    pub fn compute_adjacencies_fast(&mut self, scale: f64) -> Result<()> {
        use std::hash::{Hash, Hasher};
        use smallvec::SmallVec; // cargo add smallvec
        use ahash::AHashMap;    // cargo add ahash (fast hash)

        #[derive(Clone, Copy, Eq)]
        struct I2 { x: i64, y: i64 }
        impl PartialEq for I2 { fn eq(&self, o: &Self) -> bool { self.x == o.x && self.y == o.y } }
        impl Hash for I2 {
            fn hash<H: Hasher>(&self, state: &mut H) { self.x.hash(state); self.y.hash(state); }
        }

        // Undirected edge between two snapped coords; endpoints are stored sorted
        #[derive(Clone, Eq)]
        struct EdgeKey { a: I2, b: I2 }
        impl PartialEq for EdgeKey { fn eq(&self, o: &Self) -> bool { self.a == o.a && self.b == o.b } }
        impl Hash for EdgeKey {
            fn hash<H: Hasher>(&self, state: &mut H) { self.a.hash(state); self.b.hash(state); }
        }

        #[inline]
        fn snap(c: Coord, scale: f64) -> I2 {
            // Quantize (e.g., scale=1e7 for lat/lon; pick based on your data’s precision)
            let x = (c.x * scale).round() as i64;
            let y = (c.y * scale).round() as i64;
            I2 { x, y }
        }

        #[inline]
        fn edge_key(p: I2, q: I2) -> EdgeKey {
            if (p.x, p.y) <= (q.x, q.y) { EdgeKey { a: p, b: q } } else { EdgeKey { a: q, b: p } }
        }

        // Clear existing
        for v in &mut self.adj_list {
            v.clear();
        }

        // Edge -> polygons that contain this edge (usually 1 or 2)
        let mut edge_to_polys: AHashMap<EdgeKey, SmallVec<[u32; 2]>> = AHashMap::with_capacity(self.geoms.len() * 16);

        // 1) Ingest all edges
        for (pi, mp) in self.geoms.iter().enumerate() {
            // Iterate every polygon and ring
            for poly in &mp.0 {
                // exterior + holes
                for ring in std::iter::once(poly.exterior()).chain(poly.interiors().iter()) {
                    // Ensure closed; geo guarantees exterior/interiors are closed LineStrings
                    for seg in ring.lines() {
                        let p = snap(seg.start, scale);
                        let q = snap(seg.end, scale);
                        if p == q { continue; } // degenerate segment
                        let key = edge_key(p, q);
                        let entry = edge_to_polys.entry(key).or_insert_with(|| SmallVec::new());
                        // Avoid duplicates if ring repeats an edge
                        if entry.last().copied() != Some(pi as u32) {
                            entry.push(pi as u32);
                        }
                    }
                }
            }
        }

        // 2) For each shared edge, connect all polygon pairs (k usually 2)
        for polys in edge_to_polys.into_values() {
            match polys.len() {
                0 | 1 => {}
                2 => {
                    let a = polys[0] as usize;
                    let b = polys[1] as usize;
                    self.adj_list[a].push(b as u32);
                    self.adj_list[b].push(a as u32);
                }
                k => {
                    // Rare but possible with slivers or multi-coverage: fully connect the clique
                    for i in 0..k {
                        for j in (i + 1)..k {
                            let a = polys[i] as usize;
                            let b = polys[j] as usize;
                            self.adj_list[a].push(b as u32);
                            self.adj_list[b].push(a as u32);
                        }
                    }
                }
            }
        }

        // 3) Optional: dedup and sort neighbor lists for determinism
        for nbrs in &mut self.adj_list {
            nbrs.sort_unstable();
            nbrs.dedup();
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
