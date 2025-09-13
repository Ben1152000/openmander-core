use anyhow::{anyhow, Context, Result};
use geo::{Coord, MapCoords, MultiPolygon};
use proj4rs::{proj::Proj as Proj4, transform::transform};

use crate::Geometries;

impl Geometries {
    /// Build PROJ.4 string for the source geographic CRS (degrees → radians handled in code).
    #[inline]
    fn source_geog_proj4(&self) -> &'static str {
        match self.epsg() {
            4269 | 4937 => "+proj=longlat +datum=NAD83 +no_defs +type=crs",
            _            => "+proj=longlat +datum=WGS84 +no_defs +type=crs",
        }
    }

    /// Build PROJ.4 string for the target UTM CRS, chosen from a lon/lat center and source datum.
    /// - WGS84: 326zz (north) / 327zz (south)
    /// - NAD83: 269zz (north only; if south, fall back to WGS84 UTM-S)
    #[inline]
    fn utm_proj4(&self) -> String {
        let center = if let Some(b) = self.bounds() { b.center() }
        else { Coord { x: -104.0, y: 45.0 } }; // US geographic center (fallback)

        let zone = (((center.x + 180.0) / 6.0).floor() as i32 + 1).clamp(1, 60) as u32;
        let north = center.y >= 0.0;
        let is_nad83 = matches!(self.epsg(), 4269 | 4937);

        // NAD83 UTM only standard in north — fall back to WGS84 in south.
        let datum = if is_nad83 && north { "NAD83" } else { "WGS84" };
        let south = if north { "" } else { " +south" };

        format!("+proj=utm +zone={zone}{south} +datum={datum} +units=m +no_defs +type=crs")
    }

    /// Reproject shapes from lon/lat to a metric CRS for Euclidean distance calculations (UTM).
    pub(crate) fn reproject_to_metric(&self) -> Result<Vec<MultiPolygon<f64>>> {
        let from = {
            let proj_string = self.source_geog_proj4();
            Proj4::from_proj_string(proj_string)
                .with_context(|| anyhow!("failed to build source PROJ.4: {proj_string}"))?
        };

        let to = {
            let proj_string = self.utm_proj4();
            Proj4::from_proj_string(&proj_string)
                .with_context(|| anyhow!("failed to build target PROJ.4: {proj_string}"))?
        };

        // Map coords → radians in, meters out.
        let projected = self.shapes().iter()
            .map(|shape| shape.map_coords(|coord: Coord<f64>| {
                let mut point = (coord.x.to_radians(), coord.y.to_radians(), 0.0);
                transform(&from, &to, &mut point)
                    .expect("CRS transform failed");
                Coord { x: point.0, y: point.1 } // UTM meters
            }))
            .collect();

        Ok(projected)
    }
}
