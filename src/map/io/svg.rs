use std::{io::Write, path::Path};

use anyhow::{Result, anyhow};
use geo::Coord;

use crate::{io::svg::Viewport, map::MapLayer};

impl MapLayer {
    /// Write the layer as an SVG file.
    pub fn to_svg(&self, path: &Path, series: Option<&str>) -> Result<()> {
        let mut writer = crate::io::svg::SvgWriter::new(path)?;
        self.render_svg(&mut writer, 1200, 10, series)?;
        writer.flush()?;
        Ok(())
    }

    /// Return the layer as an SVG string (for browser/WASM use).
    pub fn to_svg_string(&self, series: Option<&str>) -> Result<String> {
        let mut writer = crate::io::svg::SvgStringWriter::new();
        self.render_svg(&mut writer, 1200, 10, series)?;
        writer.into_string()
    }

    fn render_svg(&self, writer: &mut impl Write, width: i32, margin: i32, series: Option<&str>) -> Result<()> {
        let region = &*self.region;
        let bounds = region.bounds_all();

        let shapes: Vec<geo::MultiPolygon<f64>> = region.unit_ids()
            .map(|u| region.geometry(u).clone())
            .collect();

        let centroids = self.centroids();
        let vp = Viewport::new(bounds, width as f64, margin as f64);
        let project = move |coord: &Coord<f64>| vp.project(coord);

        crate::io::svg::write_svg_header(writer, &vp)?;
        crate::io::svg::write_svg_styles(writer)?;

        if let Some(series) = series {
            crate::io::svg::draw_polygons_with_fill(
                writer,
                &shapes,
                &self.compute_fill_colors(series)?,
                &project,
            )?;
        } else {
            crate::io::svg::draw_polygons(writer, &shapes, &project)?;
        }

        let edges = region.unit_ids()
            .flat_map(|u| {
                let i = u.0 as usize;
                region.adjacency().neighbors(u).iter()
                    .filter_map(move |&v| (v.0 as usize > i).then_some((i, v.0 as usize)))
            })
            .map(|(i, j)| (&centroids[i], &centroids[j]))
            .collect::<Vec<_>>();
        crate::io::svg::draw_edges(writer, &edges, &project)?;

        crate::io::svg::write_svg_footer(writer)?;
        Ok(())
    }

    /// Compute a choropleth color for each unit based on a numeric weight series.
    fn compute_fill_colors(&self, series: &str) -> Result<Vec<String>> {
        let raw_values: Vec<f64> = (0..self.len())
            .map(|i| {
                self.unit_weights.get_as_f64(series, i)
                    .ok_or_else(|| anyhow!("[to_svg] missing series {:?} in weights", series))
            })
            .collect::<Result<Vec<_>>>()?;

        let min_val = raw_values.iter().copied().filter(|v| v.is_finite()).fold(f64::INFINITY, f64::min);
        let max_val = raw_values.iter().copied().filter(|v| v.is_finite()).fold(f64::NEG_INFINITY, f64::max);
        if !min_val.is_finite() {
            anyhow::bail!("[to_svg] no non-null values in series {:?}", series);
        }

        let range = if max_val > min_val { max_val - min_val } else { 1.0 };

        let (r1, g1, b1) = (0xdeu8, 0xebu8, 0xf7u8);
        let (r2, g2, b2) = (0x08u8, 0x51u8, 0x9cu8);

        let lerp = |a: u8, b: u8, t: f64| -> u8 {
            (a as f64 + (b as f64 - a as f64) * t).round().clamp(0.0, 255.0) as u8
        };

        let mut colors = Vec::with_capacity(raw_values.len());
        for v in raw_values {
            let t = if v.is_finite() { ((v - min_val) / range).clamp(0.0, 1.0) } else { 0.0 };
            colors.push(format!("#{:02x}{:02x}{:02x}", lerp(r1, r2, t), lerp(g1, g2, t), lerp(b1, b2, t)));
        }

        Ok(colors)
    }
}
