use std::{io::Write, path::Path};

use anyhow::{Context, Result, anyhow};
use geo::Coord;
use polars::prelude::{ChunkAgg, DataType};

use crate::{io::svg::Viewport, map::MapLayer};

impl MapLayer {
    /// Write the layer as an SVG file.
    /// If `series` is `Some(col)`, polygons are colored by that numeric column.
    pub fn to_svg(&self, path: &Path, series: Option<&str>) -> Result<()> {
        let mut writer = crate::io::svg::SvgWriter::new(path)?;
        self.render_svg(&mut writer, 1200, 10, series)?;
        writer.flush()?;
        Ok(())
    }

    /// Return the layer as an SVG string (for browser/WASM use).
    /// If `series` is `Some(col)`, polygons are colored by that numeric column.
    pub fn to_svg_string(&self, series: Option<&str>) -> Result<String> {
        let mut writer = crate::io::svg::SvgStringWriter::new();
        self.render_svg(&mut writer, 1200, 10, series)?;
        writer.into_string()
    }

    fn render_svg(&self, writer: &mut impl Write, width: i32, margin: i32, series: Option<&str>) -> Result<()> {
        let geoms = self.geoms.as_ref()
            .ok_or_else(|| anyhow!("[to_svg] No geometries available to draw."))?;
        let bounds = geoms.bounds()
            .ok_or_else(|| anyhow!("[to_svg] Could not determine bounds; nothing to draw."))?;

        let centroids = self.centroids();
        let vp = Viewport::new(bounds, width as f64, margin as f64);
        let project = move |coord: &Coord<f64>| vp.project(coord);

        crate::io::svg::write_svg_header(writer, &vp)?;
        crate::io::svg::write_svg_styles(writer)?;

        if let Some(series) = series {
            crate::io::svg::draw_polygons_with_fill(
                writer,
                geoms.shapes(),
                &self.compute_fill_colors(series)?,
                &project,
            )?;
        } else {
            crate::io::svg::draw_polygons(writer, geoms.shapes(), &project)?;
        }

        let edges = self.adjacencies.iter().enumerate()
            .flat_map(|(i, neighbors)| {
                neighbors.iter()
                    .filter_map(move |&j| ((j as usize) > i).then_some((i, j as usize)))
            })
            .map(|(i, j)| (&centroids[i], &centroids[j]))
            .collect::<Vec<_>>();
        crate::io::svg::draw_edges(writer, &edges, &project)?;

        crate::io::svg::write_svg_footer(writer)?;
        Ok(())
    }

    /// Compute a choropleth color for each row based on a numeric column.
    /// Returns one hex color string per geometry.
    fn compute_fill_colors(&self, series: &str) -> Result<Vec<String>> {
        let column = self.unit_data.column(series)
            .with_context(|| format!("[to_svg] missing column {:?}", series))?;

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
