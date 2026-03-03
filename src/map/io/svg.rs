use std::{io::Write, path::Path};

use anyhow::{Context, Ok, Result, anyhow};
use geo::Coord;
use polars::prelude::{ChunkAgg, DataType};

use crate::{io::svg::Hsl, map::MapLayer};

impl MapLayer {
    /// Small wrapper with defaults.
    pub fn to_svg(&self, path: &Path, series: Option<&str>) -> Result<()> {
        self.to_svg_with_size(path, 1200, 10, series)
    }

    /// Generate SVG as a string (for browser/WASM use).
    pub fn to_svg_string(&self, series: Option<&str>) -> Result<String> {
        self.to_svg_string_with_size(1200, 10, series)
    }

    fn to_svg_string_with_size(&self, width: i32, margin: i32, series: Option<&str>) -> Result<String> {
        let geoms = self.geoms.as_ref()
            .ok_or_else(|| anyhow!("[to_svg_string] No geometries available to draw."))?;

        let bounds = geoms.bounds()
            .ok_or_else(|| anyhow!("[to_svg_string] Could not determine bounds; nothing to draw."))?;

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

        // --- Write SVG to string ---
        let mut writer = crate::io::svg::SvgStringWriter::new();
        writer.write_header(width, height, margin, scale, &bounds)?;
        writer.write_styles()?;

        // Compute colors if necessary.
        if let Some(series) = series {
            crate::io::svg::draw_polygons_with_fill(
                &mut writer,
                geoms.shapes(),
                &self.compute_fill_colors(series)?,
                &project,
            )?;
        } else {
            crate::io::svg::draw_polygons(&mut writer, geoms.shapes(), &project)?;
        }

        // Draw adjacency lines between centroids
        let edges = self.adjacencies.iter().enumerate()
            .flat_map(|(i, neighbors)| {
                neighbors.iter()
                    .filter_map(move |&j| ((j as usize) > i).then_some((i, j as usize))) // avoid duplicate edges
            })
            .map(|(i, j)| (&centroids[i], &centroids[j]))
            .collect::<Vec<_>>();
        crate::io::svg::draw_edges(&mut writer, &edges, &project)?;

        writer.write_footer()?;
        writer.into_string()
    }

    /// Display the layer as an SVG file, including polygons and adjacency lines between centroids.
    /// If `series` is Some(col), polygons are colored by that numeric column.
    fn to_svg_with_size(&self, path: &Path, width: i32, margin: i32, series: Option<&str>) -> Result<()> {
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
        let mut writer = crate::io::svg::SvgWriter::new(path)?;
        writer.write_header(width, height, margin, scale, &bounds)?;
        writer.write_styles()?;

        // Compute colors if necessary.
        if let Some(series) = series {
            crate::io::svg::draw_polygons_with_fill(
                &mut writer,
                geoms.shapes(),
                &self.compute_fill_colors(series)?,
                &project,
            )?;
        } else {
            crate::io::svg::draw_polygons(&mut writer, geoms.shapes(), &project)?;
        }

        // Draw adjacency lines between centroids
        let edges = self.adjacencies.iter().enumerate()
            .flat_map(|(i, neighbors)| {
                neighbors.iter()
                    .filter_map(move |&j| ((j as usize) > i).then_some((i, j as usize))) // avoid duplicate edges
            })
            .map(|(i, j)| (&centroids[i], &centroids[j]))
            .collect::<Vec<_>>();
        crate::io::svg::draw_edges(&mut writer, &edges, &project)?;

        // // --- Draw a circle at each centroid (nodes), on top of edges ---
        // // Radius is in screen pixels; tweak as desired.
        // let node_radius = 50_f64;
        // for point in &centroids {
        //     let (x, y) = project(&point.coord().unwrap());
        //     writeln!(
        //         &mut writer,
        //         r##"<circle cx="{:.3}" cy="{:.3}" r="{:.3}"
        //             fill="#f4f4f4"
        //             stroke="#000000"
        //             stroke-width="2"
        //         />"##,
        //         x, y, node_radius
        //     )?;
        // }

        writer.write_footer()?;
        writer.flush()?;

        Ok(())
    }

    /// Write an SVG file visualizing the planar embedding (CCW adjacency order).
    ///
    /// Each directed half-edge is drawn as a colored ray from the source node
    /// toward the target, with color cycling through the rainbow based on its
    /// position in the source's CCW-sorted adjacency list.  Position 0 is red,
    /// progressing through orange → yellow → green → cyan → blue → violet.
    pub fn to_svg_embedding(&self, path: &Path) -> Result<()> {
        let mut writer = crate::io::svg::SvgWriter::new(path)?;
        self.write_embedding_svg(&mut writer)?;
        writer.flush()?;
        Ok(())
    }

    /// Generate an SVG string visualizing the planar embedding.
    pub fn to_svg_embedding_string(&self) -> Result<String> {
        let mut writer = crate::io::svg::SvgStringWriter::new();
        self.write_embedding_svg(&mut writer)?;
        writer.into_string()
    }

    /// Shared implementation for embedding SVG rendering.
    fn write_embedding_svg(&self, writer: &mut impl Write) -> Result<()> {
        let geoms = self.geoms.as_ref()
            .ok_or_else(|| anyhow!("[to_svg_embedding] No geometries available to draw."))?;

        let bounds = geoms.bounds()
            .ok_or_else(|| anyhow!("[to_svg_embedding] Could not determine bounds; nothing to draw."))?;

        let centroids = self.centroids();

        let margin = 10.0_f64;
        let width = 1200.0_f64;
        let scale = (width - 2.0 * margin) / bounds.width();
        let height = bounds.height() * scale + 2.0 * margin;

        let project = move |coord: &Coord<f64>| -> (f64, f64) {
            let x = margin + (coord.x - bounds.min().x) * scale;
            let y = margin + (bounds.max().y - coord.y) * scale;
            (x, y)
        };

        crate::io::svg::write_svg_header(writer, width, height, margin, scale, &bounds)?;
        crate::io::svg::write_svg_styles(writer)?;

        // Draw polygons with light fill
        crate::io::svg::draw_polygons(writer, geoms.shapes(), &project)?;

        // Draw directed half-edges colored by CCW angular position (rainbow).
        // Edges point toward neighbor centroids.
        let mut directed_edges: Vec<(Coord<f64>, Coord<f64>, Hsl)> = Vec::new();
        for (i, neighbors) in self.adjacencies.iter().enumerate() {
            let deg = neighbors.len();
            if deg == 0 { continue; }

            let ci_x = centroids[i].x();
            let ci_y = centroids[i].y();

            // Compute (neighbor, target_point, angle) for each neighbor
            let mut edges_with_angle: Vec<(u32, f64, f64, f64)> = neighbors.iter()
                .filter(|&&j| j as usize != i) // skip any leftover self-edges
                .map(|&j| {
                    let tx = centroids[j as usize].x();
                    let ty = centroids[j as usize].y();
                    let angle = (ty - ci_y).atan2(tx - ci_x);
                    (j, tx, ty, angle)
                })
                .collect();

            // Sort by angle (ascending = CCW from -π)
            edges_with_angle.sort_by(|a, b| {
                let qa = (a.3 * 1e6).round() as i64;
                let qb = (b.3 * 1e6).round() as i64;
                (qa, a.0).cmp(&(qb, b.0))
            });

            let total = edges_with_angle.len();
            for (k, &(_j, tx, ty, _)) in edges_with_angle.iter().enumerate() {
                let hue = (k as f64 / total as f64) * 360.0;
                let color = Hsl { h: hue, s: 0.85, l: 0.5 };

                let dx = tx - ci_x;
                let dy = ty - ci_y;
                let from = Coord { x: ci_x + 0.10 * dx, y: ci_y + 0.10 * dy };
                let to   = Coord { x: ci_x + 0.90 * dx, y: ci_y + 0.90 * dy };

                directed_edges.push((from, to, color));
            }
        }
        crate::io::svg::draw_directed_edges(writer, &directed_edges, &project)?;

        // Draw small dots at each centroid
        let centroid_coords: Vec<Coord<f64>> = centroids.iter()
            .map(|p| Coord { x: p.x(), y: p.y() })
            .collect();
        crate::io::svg::draw_nodes(writer, &centroid_coords, 1.5, &project)?;

        crate::io::svg::write_svg_footer(writer)?;
        Ok(())
    }

    /// Compute a choropleth color for each row based on a numeric column.
    /// Returns one hex color string per geometry.
    fn compute_fill_colors(&self, series: &str) -> Result<Vec<String>> {
        let column = self.unit_data.column(series)
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
