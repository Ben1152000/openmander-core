use std::{fs::File, io::{BufWriter, Write}, path::Path};

use anyhow::{anyhow, Context, Ok, Result};
use geo::{Coord, CoordsIter, LineString, MultiPolygon, Point};

use crate::MapLayer;

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

fn draw_polygons(
    writer: &mut impl Write,
    polygons: &[MultiPolygon<f64>],
    project: &impl Fn(&Coord<f64>) -> (f64, f64)
) -> Result<()> {
    for polygon in polygons {
        writeln!(writer, r#"<path class="blk" d="{}"/>"#, multipolygon_to_path(polygon, &project))?;
    }
    Ok(())
}

/// Build a compact SVG path string for a MultiPolygon (exteriors + holes).
fn multipolygon_to_path(shape: &MultiPolygon<f64>, project: &impl Fn(&Coord<f64>) -> (f64, f64)) -> String {
    let mut out = String::new();

    let mut ring_to_path = |ring: &LineString<f64>| {
        let mut coords = ring.coords_iter()
            .map(|coord| project(&coord));
        if let Some((x, y)) = coords.next() {
            out.push_str(&format!(" M{x:.3},{y:.3}"));
            for (x, y) in coords {
                out.push_str(&format!(" L{x:.3},{y:.3}"));
            }
            out.push('Z');
        }
    };

    for polygon in &shape.0 {
        ring_to_path(polygon.exterior());
        for interior in polygon.interiors() {
            ring_to_path(interior);
        }
    }

    out
}

fn draw_edges(
    writer: &mut impl Write,
    edges: &[(&Point<f64>, &Point<f64>)],
    project: &impl Fn(&Coord<f64>) -> (f64, f64),
) -> Result<()> {
    for edge in edges {
        let (x1, y1) = project(&Coord { x: edge.0.x(), y: edge.0.y() });
        let (x2, y2) = project(&Coord { x: edge.1.x(), y: edge.1.y() });
        writeln!(writer, r##"<line class="edge" x1="{x1:.3}" y1="{y1:.3}" x2="{x2:.3}" y2="{y2:.3}"/>"##)?;
    }
    Ok(())
}

impl MapLayer {
    /// Small wrapper with defaults.
    pub fn to_svg(&self, path: &Path) -> Result<()> {
        self.to_svg_with_size(path, 1200, 10)
    }

    /// Display the layer as an SVG file, including polygons and adjacency lines between centroids.
    /// `width` is the desired width of the SVG in pixels.
    /// `margin` is the margin to leave around the edges in pixels.
    pub fn to_svg_with_size(&self, path: &Path, width: i32, margin: i32) -> Result<()>{
        let geoms = self.geoms.as_ref()
            .ok_or_else(|| anyhow!("[to_svg] No geometries available to draw."))?;

        let bounds = geoms.bounds()
            .ok_or_else(|| anyhow!("[to_svg] Could not determine bounds; nothing to draw."))?;

        let centroids = self.centroids();

        let margin = margin as f64;
        let width = width as f64;
        let scale = (width - 2.0 * margin) / bounds.width();
        let height = bounds.height() * scale + 2.0 * margin;

        // --- Map lon/lat -> SVG coords (preserve aspect, Y down) ---
        let project = |coord: &Coord<f64>| -> (f64, f64) {
            let x = margin + (coord.x - bounds.min().x) * scale;
            let y = margin + (bounds.max().y - coord.y) * scale; // invert vertically
            (x, y)
        };

        // --- Write SVG ---
        let file = File::create(path)
            .with_context(|| format!("[to_svg] Failed to create {}", path.display()))?;

        let mut writer = BufWriter::new(file);

        write_svg_header(&mut writer, width, height)?;
        write_svg_styles(&mut writer)?;
        draw_polygons(&mut writer, geoms.shapes(), &project)?;

        // Draw adjacency lines between centroids
        let edges = self.adjacencies.iter().enumerate()
            .flat_map(|(i, neighbors)| {
                neighbors.iter()
                    .filter_map(move |&j| ((j as usize) > i).then_some((i, j as usize))) // avoid duplicate edges
            })
            .map(|(i, j)| (&centroids[i], &centroids[j]))
            .collect::<Vec<_>>();
        draw_edges(&mut writer, &edges, &project)?;

        write_svg_footer(&mut writer)?;

        writer.flush()?;

        Ok(())
    }
}

