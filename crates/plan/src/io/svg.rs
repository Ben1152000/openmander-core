use std::{collections::{HashMap, HashSet}, fs::File, io::{BufWriter, Write}, path::Path};

use anyhow::{anyhow, Context, Result};
use geo::{Coord, CoordsIter, LineString, MultiPolygon};
use openmander_map::GeoType;

use crate::Plan;

fn write_svg_header(writer: &mut impl Write, width: f64, height: f64) -> Result<()> {
    writeln!(writer, r##"<?xml version="1.0" encoding="UTF-8" standalone="no"?>"##)?;
    writeln!(writer, r##"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">"##)?;
    writeln!(writer, r##"<rect width="100%" height="100%" fill="#ffffff"/>"##)?;
    Ok(())
}

fn write_svg_footer(writer: &mut impl Write) -> Result<()> {
    writeln!(writer, "</svg>")?;
    Ok(())
}

fn write_svg_styles(writer: &mut impl Write) -> Result<()> {
    writeln!(writer, r##"<defs>
  <style>
    .blk {{ fill: #e5e7eb; stroke: #111827; stroke-width: 0.5; fill-opacity: 0.85; }}
    .edge {{ stroke: #2563eb; stroke-opacity: 0.35; stroke-width: 0.6; }}
    .dist {{ vector-effect: non-scaling-stroke; }}
  </style>
</defs>"##)?;
    Ok(())
}

/// Projection function: lon/lat -> SVG coords (x,y)
type Projection = dyn Fn(&Coord<f64>) -> (f64, f64);

/// Append a ring as an SVG subpath: "M x,y L x,y ... Z"
fn ring_to_path(ring: &[Coord<f64>], project: &Projection, out: &mut String) {
    if ring.is_empty() { return }
    let coords = ring.coords_iter().map(|coord| project(&coord)).collect::<Vec<_>>();
    out.push_str(&format!(" M{:.3},{:.3}", coords[0].0, coords[0].1));
    for &(x, y) in &coords[1..] {
        out.push_str(&format!(" L{x:.3},{y:.3}"));
    }
    out.push('Z');
}

impl Plan {
    /// Small wrapper with defaults.
    pub fn to_svg(&self, path: &Path) -> Result<()> {
        self.to_svg_with_size(path, 1200, 10)
    }

    /// Draw dissolved districts using only frontier blocks + state boundary.
    pub fn to_svg_with_size(&self, path: &Path, width: i32, margin: i32) -> Result<()> {
        let bounds = self.map().get_layer(GeoType::Block).bounds()
            .ok_or_else(|| anyhow!("[Plan::to_svg] Could not determine bounds; nothing to draw."))?;

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
            let outline = self.map().get_layer(GeoType::State).union()
                .ok_or_else(|| anyhow!("[to_svg] No state geoms available"))?;
            
            let mut ptmap: HashMap<QuantizedPoint, Coord<f64>> = HashMap::new();
            let mut set = SegmentSet::default();

            for polygon in outline {
                collect_ring_segments(polygon.exterior(), &mut set, &mut ptmap);
            }

            set
        };

        // --- Write SVG ---
        let file = File::create(path)
            .with_context(|| format!("[Plan::to_svg] Failed to create {}", path.display()))?;
        let mut writer = BufWriter::new(file);

        write_svg_header(&mut writer, width, height)?;
        write_svg_styles(&mut writer)?;

        // Draw each district as a single dissolved path (holes supported via even-odd fill).
        for d in 1..=self.num_districts() as usize {
            if let Some(path_d) = self.build_district_path_string(d as u32, &state_outline, &project)? {
                let hue = ((d as f64) * 137.50776405) % 360.0; // golden-angle palette
                writeln!(
                    writer,
                    r#"<path class="dist" fill-rule="evenodd" style="fill:hsl({h:.1},70%,55%);stroke:#111827;stroke-width:0.6;fill-opacity:0.85" d="{path}"/>"#,
                    h = hue,
                    path = path_d
                )?;
            }
        }

        write_svg_footer(&mut writer)?;
        writer.flush()?;
        Ok(())
    }

    /// Build dissolved boundary for district `d` using frontier blocks, immediate same-district neighbors,
    /// and segments on the state outer boundary.
    fn build_district_path_string(&self, d: u32, state_outline: &SegmentSet, project: &Projection) -> Result<Option<String>> {
        let shapes = self.map().get_layer(GeoType::Block).shapes()
            .ok_or_else(|| anyhow!("[to_svg] No block geoms available"))?;

        let adjacencies = self.map().get_layer(GeoType::Block).adjacencies();

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
            let mut ptmap: HashMap<QuantizedPoint, Coord<f64>> = HashMap::new();
            for (i, _poly) in shapes.iter().enumerate() {
                if self.partition.assignment(i) != d {
                    continue;
                }
                // Skip ones we already have
                if include.contains(&i) {
                    continue;
                }
                let si = collect_segments(&shapes[i], &mut ptmap);
                if !si.0.is_disjoint(&state_outline.0) {
                    include.insert(i);
                }
            }
        }

        if include.is_empty() {
            return Ok(None);
        }

        // 2) Precompute segment sets for used blocks (quantized)
        let mut ptmap: HashMap<QuantizedPoint, Coord<f64>> = HashMap::new();
        let mut segs_cache: HashMap<usize, SegmentSet> = HashMap::new();
        for &i in &include {
            let set = collect_segments(&shapes[i], &mut ptmap);
            segs_cache.insert(i, set);
        }

        // 3) Collect boundary segments:
        //    - shared segments between frontier blocks in d and neighbors NOT in d
        //    - plus segments along state outer boundary
        let mut boundary: SegmentSet = SegmentSet::default();

        // helper to add only segments present in both polygons
        let mut add_shared = |ia: usize, ib: usize| {
            let sa = segs_cache
                .get(&ia)
                .cloned()
                .unwrap_or_else(|| collect_segments(&shapes[ia], &mut ptmap));
            let sb = segs_cache
                .get(&ib)
                .cloned()
                .unwrap_or_else(|| collect_segments(&shapes[ib], &mut ptmap));

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
        let rings = polygonize_rings(&boundary, &ptmap)?;
        let mut path = String::new();
        for ring in rings {
            ring_to_path(&ring, project, &mut path);
        }
        Ok(Some(path))
    }
}

/* ---------- Geometry helpers (quantized segment accounting + polygonization) ---------- */

/// Quantization scale (1e-7 deg ≈ 1 cm at equator). Adjust if your data is projected.
const Q_SCALE: f64 = 1e7;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct QuantizedPoint(i64, i64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Segment(QuantizedPoint, QuantizedPoint); // undirected: stored with canonical (min,max) endpoint order

#[derive(Default, Clone)]
struct SegmentSet(HashSet<Segment>);

#[inline]
fn qpt(c: &Coord<f64>) -> QuantizedPoint {
    QuantizedPoint((c.x * Q_SCALE).round() as i64, (c.y * Q_SCALE).round() as i64)
}

#[inline]
fn seg_norm(a: QuantizedPoint, b: QuantizedPoint) -> Segment {
    if (a.0, a.1) <= (b.0, b.1) { Segment(a, b) } else { Segment(b, a) }
}

fn collect_segments(mp: &MultiPolygon<f64>, ptmap: &mut HashMap<QuantizedPoint, Coord<f64>>) -> SegmentSet {
    let mut set = SegmentSet::default();
    for poly in &mp.0 {
        collect_ring_segments(poly.exterior(), &mut set, ptmap);
        for hole in poly.interiors() {
            collect_ring_segments(hole, &mut set, ptmap);
        }
    }
    set
}

fn collect_ring_segments(ring: &LineString<f64>, set: &mut SegmentSet, ptmap: &mut HashMap<QuantizedPoint, Coord<f64>>) {
    let mut prev: Option<Coord<f64>> = None;
    for c in ring.coords_iter().map(|c| Coord { x: c.x, y: c.y }) {
        if let Some(p) = prev {
            let qa = qpt(&p);
            let qb = qpt(&c);
            set.0.insert(seg_norm(qa, qb));
            ptmap.entry(qa).or_insert(p);
            ptmap.entry(qb).or_insert(c);
        }
        prev = Some(c);
    }
    // close the ring
    if let (Some(first), Some(last)) = (ring.0.first(), ring.0.last()) {
        let qa = qpt(&Coord { x: last.x, y: last.y });
        let qb = qpt(&Coord { x: first.x, y: first.y });
        set.0.insert(seg_norm(qa, qb));
        ptmap.entry(qa).or_insert(Coord { x: last.x, y: last.y });
        ptmap.entry(qb).or_insert(Coord { x: first.x, y: first.y });
    }
}

/// Turn a set of undirected boundary segments into closed rings (list of coords).
fn polygonize_rings(boundary: &SegmentSet, ptmap: &HashMap<QuantizedPoint, Coord<f64>>) -> Result<Vec<Vec<Coord<f64>>>> {
    // adjacency (multi-graph): QPt -> multiset of neighbors
    let mut adj: HashMap<QuantizedPoint, Vec<QuantizedPoint>> = HashMap::new();
    for &Segment(a, b) in &boundary.0 {
        adj.entry(a).or_default().push(b);
        adj.entry(b).or_default().push(a);
    }

    // Helper to remove one undirected edge (a<->b)
    let remove_edge = |a: QuantizedPoint, b: QuantizedPoint, adj: &mut HashMap<QuantizedPoint, Vec<QuantizedPoint>>| {
        if let Some(v) = adj.get_mut(&a) {
            if let Some(pos) = v.iter().position(|&x| x == b) {
                v.swap_remove(pos);
            }
        }
        if let Some(v) = adj.get_mut(&b) {
            if let Some(pos) = v.iter().position(|&x| x == a) {
                v.swap_remove(pos);
            }
        }
    };

    // Walk cycles
    let mut rings: Vec<Vec<Coord<f64>>> = Vec::new();

    // Collect all nodes with degree > 0
    let nodes: Vec<QuantizedPoint> = adj.iter().filter(|(_, v)| !v.is_empty()).map(|(k, _)| *k).collect();

    while let Some(&start) = nodes.iter().find(|&&n| adj.get(&n).map_or(false, |v| !v.is_empty())) {
        let mut ring_q: Vec<QuantizedPoint> = Vec::new();
        ring_q.push(start);

        // Pick an arbitrary first neighbor
        let mut curr = start;
        let mut next = {
            let v = adj.get(&curr).ok_or_else(|| anyhow!("Broken adjacency"))?;
            *v.last().ok_or_else(|| anyhow!("Isolated vertex in boundary"))?
        };
        remove_edge(curr, next, &mut adj);
        ring_q.push(next);

        let mut prev = curr;
        curr = next;

        // Follow edges until we close the loop
        loop {
            let v = adj.get(&curr).ok_or_else(|| anyhow!("Broken adjacency"))?;
            if v.is_empty() {
                // Degenerate—shouldn't happen with valid polygon boundaries
                break;
            }
            // Prefer continuing direction (avoid going back)
            let &cand = v.iter().find(|&&u| u != prev).unwrap_or(&v[0]);
            next = cand;
            remove_edge(curr, next, &mut adj);
            ring_q.push(next);

            if next == start {
                break;
            }
            prev = curr;
            curr = next;
        }

        // Map back to f64 coords (drop duplicated last if present)
        if let Some(last) = ring_q.last() {
            if *last == start {
                ring_q.pop();
            }
        }
        let ring_coords = ring_q
            .into_iter()
            .map(|q| *ptmap.get(&q).expect("missing ptmap coord"))
            .collect();

        rings.push(ring_coords);

        // Update 'nodes' cache (optional: leave as-is since we query adj each time)
    }

    Ok(rings)
}
