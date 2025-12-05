use std::{io::Write, path::Path};

use anyhow::{Context, Ok, Result, anyhow};
use geo::{Coord, CoordsIter, LineString, MultiPolygon, Point};
use polars::prelude::{ChunkAgg, DataType};

use crate::{common, map::MapLayer};

/// Projection function: lon/lat -> SVG coords (x,y)
type Projection = dyn Fn(&Coord<f64>) -> (f64, f64);

fn draw_polygons(writer: &mut impl Write, polygons: &[MultiPolygon<f64>], project: &Projection) -> Result<()> {
    for polygon in polygons {
        writeln!(writer, r#"<path class="blk" d="{}"/>"#, multipolygon_to_path(polygon, project))?;
    }
    Ok(())
}

/// Draw polygons with specified fill colors.
fn draw_polygons_with_fill(
    writer: &mut impl Write,
    polygons: &[MultiPolygon<f64>],
    colors: &[String],
    project: &Projection,
) -> Result<()> {
    assert_eq!(colors.len(), polygons.len(),
        "[to_svg] length mismatch: {} colors for {} geometries",
        colors.len(),
        polygons.len(),
    );

    for (polygon, color) in polygons.iter().zip(colors.iter()) {
        for points in multipolygon_to_points(polygon, project) {
            writeln!(writer, r#"<polygon class="blk" points="{}" style="fill:{}"/>"#, points, color)?;
        }
    }

    Ok(())
}

/// Convert a MultiPolygon into a list of SVG `points` strings for each exterior ring.
/// (Holes are ignored for polygon output.)
fn multipolygon_to_points(shape: &MultiPolygon<f64>, project: &Projection) -> Vec<String> {
    shape.0.iter()
        .map(|polygon| ring_to_points(polygon.exterior(), project))
        .collect()
}

/// Build an SVG points string for a LineString (ring).
fn ring_to_points(ring: &LineString<f64>, project: &Projection) -> String {
    let mut out = String::new();

    for (i, coord) in ring.coords_iter().enumerate() {
        let (x, y) = project(&coord);
        if i > 0 { out.push(' ') }
        out.push_str(&format!("{x:.3},{y:.3}"));
    }

    out
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
    pub fn to_svg(&self, path: &Path, series: Option<&str>) -> Result<()> {
        self.to_svg_with_size(path, 1200, 10, series)
    }

    /// Display the layer as an SVG file, including polygons and adjacency lines between centroids.
    /// If `series` is Some(col), polygons are colored by that numeric column.
    pub fn to_svg_with_size(&self, path: &Path, width: i32, margin: i32, series: Option<&str>) -> Result<()> {
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

        // Compute colors if necessary.
        if let Some(series) = series {
            draw_polygons_with_fill(
                &mut writer,
                geoms.shapes(),
                &self.compute_fill_colors(series)?,
                &project,
            )?;
        } else {
            draw_polygons(&mut writer, geoms.shapes(), &project)?;
        }

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

    /// Compute a choropleth color for each row based on a numeric column.
    /// Returns one hex color string per geometry.
    fn compute_fill_colors(&self, series: &str) -> Result<Vec<String>> {
        let column = self.data.column(series)
            .with_context(|| format!("[to_svg] missing column {:?}", series))?;

        // Cast to f64 if necessary
        let column = if column.dtype() != &DataType::Float64 {
            column.cast(&DataType::Float64)?
        } else {
            column.clone()
        };

        let values = column.f64()
            .with_context(|| format!("[to_svg] column {:?} is not numeric", series))?;

        let min_val = values.min()
            .ok_or_else(|| anyhow!("[to_svg] no non-null values in series"))?;
        let max_val = values.max()
            .ok_or_else(|| anyhow!("[to_svg] no non-null values in series"))?;

        let range = if max_val > min_val { max_val - min_val } else { 1.0 };

        // Simple blue gradient: light (#deebf7) → dark (#08519c)
        let (r1, g1, b1) = (0xdeu8, 0xebu8, 0xf7u8);
        let (r2, g2, b2) = (0x08u8, 0x51u8, 0x9cu8);

        let lerp = |a: u8, b: u8, t: f64| -> u8 {
            (a as f64 + (b as f64 - a as f64) * t)
                .round()
                .clamp(0.0, 255.0) as u8
        };

        let mut colors = Vec::with_capacity(values.len());
        for v_opt in values.into_iter() {
            // Nulls → t = 0.0 (lowest color)
            let t = v_opt
                .map(|v| ((v - min_val) / range).clamp(0.0, 1.0))
                .unwrap_or(0.0);
            let r = lerp(r1, r2, t);
            let g = lerp(g1, g2, t);
            let b = lerp(b1, b2, t);
            colors.push(format!("#{:02x}{:02x}{:02x}", r, g, b));
        }

        Ok(colors)
    }
}
