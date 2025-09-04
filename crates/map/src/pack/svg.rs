use std::{fs::File, io::{BufWriter, Write}, path::Path};

use geo::{BoundingRect, Centroid, MultiPolygon, Polygon};

use crate::MapLayer;

impl MapLayer {
    pub fn to_svg(&self, path: &Path) {
        // --- Collect centroid lon/lat (from DataFrame if present, else compute from geom) ---
        let n = self.adjacencies.len();
        let mut lons = vec![f64::NAN; n];
        let mut lats = vec![f64::NAN; n];

        // Try from DataFrame first
        if let (Ok(lon_s), Ok(lat_s)) = (self.data.column("centroid_lon"), self.data.column("centroid_lat")) {
            if let (Ok(lon), Ok(lat)) = (lon_s.f64(), lat_s.f64()) {
                let len = lon.len().min(lat.len()).min(n);
                for i in 0..len {
                    if let (Some(x), Some(y)) = (lon.get(i), lat.get(i)) {
                        lons[i] = x;
                        lats[i] = y;
                    }
                }
            }
        }

        // Fill any missing centroids from geometry (interior/centroid)
        if let Some(g) = &self.geoms {
            let len = g.shapes.len().min(n);
            for i in 0..len {
                if !(lons[i].is_finite() && lats[i].is_finite()) {
                    if let Some(c) = g.shapes[i].centroid() {
                        lons[i] = c.x();
                        lats[i] = c.y();
                    }
                }
            }
        }

        // --- Compute bounds (from geoms if available, else from centroids) ---
        let mut minx = f64::INFINITY;
        let mut miny = f64::INFINITY;
        let mut maxx = f64::NEG_INFINITY;
        let mut maxy = f64::NEG_INFINITY;

        if let Some(g) = &self.geoms {
            for mp in &g.shapes {
                if let Some(rect) = mp.bounding_rect() {
                    let (x0, y0) = (rect.min().x, rect.min().y);
                    let (x1, y1) = (rect.max().x, rect.max().y);
                    minx = minx.min(x0);
                    miny = miny.min(y0);
                    maxx = maxx.max(x1);
                    maxy = maxy.max(y1);
                }
            }
        }

        // If bbox still invalid, try from centroids
        if !minx.is_finite() {
            for i in 0..n {
                if lons[i].is_finite() && lats[i].is_finite() {
                    minx = minx.min(lons[i]);
                    miny = miny.min(lats[i]);
                    maxx = maxx.max(lons[i]);
                    maxy = maxy.max(lats[i]);
                }
            }
        }

        if !minx.is_finite() || !maxx.is_finite() || minx >= maxx || miny >= maxy {
            eprintln!("[to_svg] Could not determine bounds; nothing to draw.");
            return;
        }

        // --- Map lon/lat -> SVG coords (preserve aspect, Y down) ---
        let width = 1200.0_f64;
        let margin = 8.0_f64;
        let sx = (width - 2.0 * margin) / (maxx - minx);
        let mut height = (maxy - miny) * sx + 2.0 * margin;
        if !height.is_finite() || height <= 0.0 {
            // fallback height
            height = width * 0.6;
        }
        let project = |lon: f64, lat: f64| -> (f64, f64) {
            let x = margin + (lon - minx) * sx;
            let y = margin + (maxy - lat) * sx; // invert vertically
            (x, y)
        };

        // --- Write SVG ---
        let file = match File::create(path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("[to_svg] Failed to create {}: {e}", path.display());
                return;
            }
        };
        let mut w = BufWriter::new(file);

        let _ = writeln!(
            w,
            r##"<?xml version="1.0" encoding="UTF-8" standalone="no"?>"##
        );
        let _ = writeln!(
            w,
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">"#
        );
        let _ = writeln!(
            w,
            r##"<rect width="100%" height="100%" fill="#ffffff"/>"##
        );

        // Styles
        let _ = writeln!(
            w,
            r#"<defs>
  <style>
    .blk {{ fill: #e5e7eb; stroke: #111827; stroke-width: 0.5; fill-opacity: 0.85; }}
    .edge {{ stroke: #2563eb; stroke-opacity: 0.35; stroke-width: 0.6; }}
  </style>
</defs>"#
        );

        // --- Draw polygons (if we have them)
        if let Some(g) = &self.geoms {
            for mp in &g.shapes {
                // Convert MultiPolygon -> a single path 'd'
                let d = multipolygon_to_path(mp, &project);
                let _ = writeln!(w, r#"<path class="blk" d="{}"/>"#, d);
            }
        }

        // --- Draw adjacency lines between centroids
        // Assumes self.adjacencies: Vec<Vec<u32>> (neighbors by index).
        for (i, nbrs) in self.adjacencies.iter().enumerate() {
            if !(lons[i].is_finite() && lats[i].is_finite()) {
                continue;
            }
            let (x1, y1) = project(lons[i], lats[i]);

            // If your adjacencies are not neighbor indices, adapt this loop accordingly.
            for &j in nbrs {
                let j = j as usize;
                if j <= i { continue; } // draw each edge once
                if j >= n { continue; }
                if !(lons[j].is_finite() && lats[j].is_finite()) { continue; }

                let (x2, y2) = project(lons[j], lats[j]);
                let _ = writeln!(
                    w,
                    r#"<line class="edge" x1="{x1:.3}" y1="{y1:.3}" x2="{x2:.3}" y2="{y2:.3}"/>"#
                );
            }
        }

        let _ = writeln!(w, "</svg>");
        let _ = w.flush();
    }
}

/// Build a compact SVG path string for a MultiPolygon (exteriors + holes).
fn multipolygon_to_path(
    mp: &MultiPolygon<f64>,
    project: &impl Fn(f64, f64) -> (f64, f64),
) -> String {
    let mut out = String::new();
    for poly in &mp.0 {
        polygon_to_path(poly, project, &mut out);
    }
    out
}

fn polygon_to_path(
    poly: &Polygon<f64>,
    project: &impl Fn(f64, f64) -> (f64, f64),
    out: &mut String,
) {
    use geo::CoordsIter;

    // exterior
    {
        let mut first = true;
        for c in poly.exterior().coords_iter() {
            let (x, y) = project(c.x, c.y);
            if first {
                first = false;
                out.push_str(&format!("M{:.3},{:.3}", x, y));
            } else {
                out.push_str(&format!(" L{:.3},{:.3}", x, y));
            }
        }
        out.push('Z');
    }

    // holes
    for hole in poly.interiors() {
        let mut first = true;
        for c in hole.coords_iter() {
            let (x, y) = project(c.x, c.y);
            if first {
                first = false;
                out.push_str(&format!(" M{:.3},{:.3}", x, y));
            } else {
                out.push_str(&format!(" L{:.3},{:.3}", x, y));
            }
        }
        out.push('Z');
    }
}

