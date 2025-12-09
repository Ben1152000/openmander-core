use std::{io::Write};

use anyhow::{Ok, Result};
use geo::{Coord, CoordsIter, LineString, MultiPolygon, Point};

/// Projection function: lon/lat -> SVG coords (x,y)
pub(crate) type Projection = dyn Fn(&Coord<f64>) -> (f64, f64);

/// Append a ring as an SVG subpath: "M x,y L x,y ... Z"
pub(crate) fn ring_to_path(ring: &[Coord<f64>], project: &Projection, out: &mut String) {
    if ring.is_empty() { return }
    let coords = ring.coords_iter().map(|coord| project(&coord)).collect::<Vec<_>>();
    out.push_str(&format!(" M{:.3},{:.3}", coords[0].0, coords[0].1));
    for &(x, y) in &coords[1..] {
        out.push_str(&format!(" L{x:.3},{y:.3}"));
    }
    out.push('Z');
}

pub(crate) fn draw_edges(
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

pub(crate) fn draw_polygons(writer: &mut impl Write, polygons: &[MultiPolygon<f64>], project: &Projection) -> Result<()> {
    for polygon in polygons {
        writeln!(writer, r#"<path class="blk" d="{}"/>"#, multipolygon_to_path(polygon, project))?;
    }
    Ok(())
}

/// Draw polygons with specified fill colors.
pub(crate) fn draw_polygons_with_fill(writer: &mut impl Write, polygons: &[MultiPolygon<f64>], colors: &[String], project: &Projection) -> Result<()> {
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
        out.push_str(&ring_to_path2(polygon.exterior(), project));
        for interior in polygon.interiors() {
            out.push_str(&ring_to_path2(interior, project));
        }
    }

    out
}

/// Build a compact SVG path string for a LineString (ring).
fn ring_to_path2(ring: &LineString<f64>, project: &Projection) -> String {
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
