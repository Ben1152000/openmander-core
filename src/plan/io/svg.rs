use std::{collections::{HashMap, HashSet}, io::Write, path::Path};

use anyhow::{anyhow, Result};
use geo::Coord;

use crate::{io::svg::{Projection, SegmentSet}, plan::Plan};

impl Plan {
    /// Small wrapper with defaults.
    pub fn to_svg(&self, path: &Path, color_partisan: bool) -> Result<()> {
        self.to_svg_with_size(path, color_partisan, 1200, 10)
    }

    /// Draw dissolved districts using only frontier blocks + state boundary.
    fn to_svg_with_size(&self, path: &Path, color_partisan: bool, width: i32, margin: i32) -> Result<()> {
        let bounds = self.map().base()?.bounds()
            .ok_or_else(|| anyhow!("[to_svg] Could not determine bounds; nothing to draw."))?;

        let margin = margin as f64;
        let width = width as f64;
        let scale = (width - 2.0 * margin) / bounds.width();
        let height = bounds.height() * scale + 2.0 * margin;

        // lon/lat -> SVG coords (Y down)
        let project = move |coord: &Coord<f64>| -> (f64, f64) {
            let x = margin + (coord.x - bounds.min().x) * scale;
            let y = margin + (bounds.max().y - coord.y) * scale;
            (x, y)
        };

        // --- Precompute state outer boundary as a segment set ---
        // Build a set of undirected segments for the *outer* state boundary (all exteriors).
        let state_outline = {
            let outline = self.map().region()?.union()
                .ok_or_else(|| anyhow!("[to_svg] No state geoms available"))?;
            
            let mut ptmap: HashMap<crate::io::svg::QuantizedPoint, Coord<f64>> = HashMap::new();
            let mut set = crate::io::svg::SegmentSet::default();

            for polygon in outline {
                crate::io::svg::collect_ring_segments(polygon.exterior(), &mut set, &mut ptmap);
            }

            set
        };

        // --- Write SVG ---
        let mut writer = crate::io::svg::SvgWriter::new(path)?;
        writer.write_header(width, height, margin, scale, &bounds)?;
        writer.write_styles()?;

        // Draw each district as a single dissolved path (holes supported via even-odd fill).
        for part in 1..=self.num_districts() as usize {
            if let Some(path) = self.build_district_path_string(part as u32, &state_outline, &project)? {
                // Determine fill color based on boolean variable.
                let fill: String = if color_partisan {
                    crate::io::svg::partisan_color(self.partition.partisan_lean(part as u32, "E_20_PRES_Dem", "E_20_PRES_Rep")).to_string()
                } else {
                    let state_id = self.map().region()?.geo_ids()[0].id().parse::<usize>().expect("[Plan.to_svg] Couldn't determine state id.");
                    crate::io::svg::golden_angle_color((state_id + 1) * 100 + part).to_string()
                };

                writeln!(
                    writer,
                    r#"<path class="dist" fill-rule="evenodd" style="fill:{fill};stroke:#111827;stroke-width:0.6;fill-opacity:0.85" d="{path}"/>"#,
                )?;
            }
        }

        writer.write_footer()?;
        writer.flush()?;
        Ok(())
    }

    /// Build dissolved boundary for district `d` using frontier blocks, immediate same-district neighbors,
    /// and segments on the state outer boundary.
    fn build_district_path_string(&self, d: u32, state_outline: &SegmentSet, project: &Projection) -> Result<Option<String>> {
        let shapes = self.map().base()?.shapes()
            .ok_or_else(|| anyhow!("[to_svg] No block geoms available"))?;

        let adjacencies = self.map().base()?.adjacencies();

        // 1) indices to process: frontier(d) + same-district neighbors + state-edge blocks
        let frontier = self.partition.frontier(d as u32);

        let mut include: HashSet<usize> = HashSet::new();
        // frontier
        for &i in frontier.iter() {
            include.insert(i as usize);
        }
        // neighbors that are also in d (to cancel interior edges)
        for &i in frontier.iter() {
            for &j in &adjacencies[i as usize] {
                if self.partition.assignment(j as usize) == d {
                    include.insert(j as usize);
                }
            }
        }

        // Add blocks of district `d` that sit on the state outer boundary into `include`.
        // Uses a fast segment-set intersection instead of polygon-polygon tests.
        {
            // Iterate all blocks in district d; for each, intersect its segments with the state outline
            // (This is linear in vertices of blocks in d; if you want faster, filter via an R-tree.)
            let mut ptmap: HashMap<crate::io::svg::QuantizedPoint, Coord<f64>> = HashMap::new();
            for (i, _poly) in shapes.iter().enumerate() {
                if self.partition.assignment(i) != d {
                    continue;
                }
                // Skip ones we already have
                if include.contains(&i) {
                    continue;
                }
                let si = crate::io::svg::collect_segments(&shapes[i], &mut ptmap);
                if !si.0.is_disjoint(&state_outline.0) {
                    include.insert(i);
                }
            }
        }

        if include.is_empty() {
            return Ok(None);
        }

        // 2) Precompute segment sets for used blocks (quantized)
        let mut ptmap: HashMap<crate::io::svg::QuantizedPoint, Coord<f64>> = HashMap::new();
        let mut segs_cache: HashMap<usize, crate::io::svg::SegmentSet> = HashMap::new();
        for &i in &include {
            let set = crate::io::svg::collect_segments(&shapes[i], &mut ptmap);
            segs_cache.insert(i, set);
        }

        // 3) Collect boundary segments:
        //    - shared segments between frontier blocks in d and neighbors NOT in d
        //    - plus segments along state outer boundary
        let mut boundary: crate::io::svg::SegmentSet = crate::io::svg::SegmentSet::default();

        // helper to add only segments present in both polygons
        let mut add_shared = |ia: usize, ib: usize| {
            let sa = segs_cache
                .get(&ia)
                .cloned()
                .unwrap_or_else(|| crate::io::svg::collect_segments(&shapes[ia], &mut ptmap));
            let sb = segs_cache
                .get(&ib)
                .cloned()
                .unwrap_or_else(|| crate::io::svg::collect_segments(&shapes[ib], &mut ptmap));

            for seg in sa.0.intersection(&sb.0) {
                boundary.0.insert(*seg);
            }
        };

        // a) internal district borders vs other districts
        for &i in frontier.iter() {
            if self.partition.assignment(i as usize) != d {
                continue;
            }
            for &j in &adjacencies[i as usize] {
                if self.partition.assignment(j as usize) != d {
                    add_shared(i as usize, j as usize);
                }
            }
        }

        // b) portions lying on the state outer boundary
        for &i in &include {
            let si = segs_cache.get(&i).unwrap();
            for seg in si.0.intersection(&state_outline.0) {
                boundary.0.insert(*seg);
            }
        }

        if boundary.0.is_empty() {
            return Ok(None);
        }

        // 4) Stitch boundary segments into rings and emit a single path (multiple 'M…Z')
        let rings = crate::io::svg::polygonize_rings(&boundary, &ptmap)?;
        let mut path = String::new();
        for ring in rings {
            crate::io::svg::ring_to_path(&ring, project, &mut path);
        }
        Ok(Some(path))
    }
}
