use std::{io::Write, path::Path};

use anyhow::{anyhow, Ok, Result};
use geo::{Coord, CoordsIter, LineString, MultiPolygon, Point};

use crate::{common, map::MapLayer};

/// Projection function: lon/lat -> SVG coords (x,y)
type Projection = dyn Fn(&Coord<f64>) -> (f64, f64);

fn draw_polygons(writer: &mut impl Write, polygons: &[MultiPolygon<f64>], project: &Projection) -> Result<()> {
    for polygon in polygons {
        writeln!(writer, r#"<path class="blk" d="{}"/>"#, multipolygon_to_path(polygon, project))?;
    }
    Ok(())
}

/// Build a compact SVG path string for a MultiPolygon (exteriors + holes).
fn multipolygon_to_path(shape: &MultiPolygon<f64>, project: &Projection) -> String {
    let mut out = String::new();

    for polygon in &shape.0 {
        out.push_str(&ring_to_path(polygon.exterior(), project));
        for interior in polygon.interiors() {
            out.push_str(&ring_to_path(interior, project));
        }
    }

    out
}

/// Build a compact SVG path string for a LineString (ring).
fn ring_to_path(ring: &LineString<f64>, project: &Projection) -> String {
    let mut out = String::new();

    let mut coords = ring.coords_iter()
        .map(|coord| project(&coord));
    if let Some((x, y)) = coords.next() {
        out.push_str(&format!(" M{x:.3},{y:.3}"));
        for (x, y) in coords {
            out.push_str(&format!(" L{x:.3},{y:.3}"));
        }
        out.push('Z');
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
        let project = move |coord: &Coord<f64>| -> (f64, f64) {
            let x = margin + (coord.x - bounds.min().x) * scale;
            let y = margin + (bounds.max().y - coord.y) * scale; // invert vertically
            (x, y)
        };

        // --- Write SVG ---
        let mut writer = common::SvgWriter::new(path)?;
        writer.write_header(width, height)?;
        writer.write_styles()?;
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

        writer.write_footer()?;
        writer.flush()?;

        Ok(())
    }
}
