use anyhow::{anyhow, Result};
use geo::{MultiPolygon, Rect};
use serde_json::{json, Map, Value};

use crate::map::MapLayer;

impl MapLayer {
    /// Export layer geometries as GeoJSON FeatureCollection.
    pub fn to_geojson(&self) -> Result<Value> {
        self.to_geojson_with_bounds(None)
    }

    /// Export layer geometries as GeoJSON FeatureCollection, optionally filtered by bounding box.
    pub fn to_geojson_with_bounds(&self, bounds: Option<[f64; 4]>) -> Result<Value> {
        let region = &*self.region;

        let indices: Vec<usize> = if let Some([min_lon, min_lat, max_lon, max_lat]) = bounds {
            let envelope = Rect::new(
                geo::Coord { x: min_lon, y: min_lat },
                geo::Coord { x: max_lon, y: max_lat },
            );
            region.units_in_envelope(envelope).into_iter().map(|u| u.0 as usize).collect()
        } else {
            (0..self.len()).collect()
        };

        let mut features = Vec::with_capacity(indices.len().min(10000));

        for idx in indices {
            let mp = region.geometry(geograph::UnitId(idx as u32));
            let mut properties = Map::new();

            if let Some(geo_id) = self.geo_ids.get(idx) {
                properties.insert("geo_id".to_string(), json!(geo_id.id()));
            }

            // "name" comes from unit_names; numeric columns from unit_weights.
            let essential_columns = ["name", "TOTPOP", "T_20_CENS_Total"];
            for col_name in essential_columns.iter() {
                let json_val = if *col_name == "name" {
                    self.unit_names.get(idx)
                        .map(|s| json!(s))
                        .unwrap_or(Value::Null)
                } else {
                    self.unit_weights.get_as_f64(col_name, idx)
                        .map(|v| json!(v))
                        .unwrap_or(Value::Null)
                };
                properties.insert((*col_name).to_string(), json_val);
            }

            let geo_id_str = self.geo_ids.get(idx)
                .map(|g| g.id().to_string())
                .unwrap_or_else(|| format!("{}", idx));

            properties.insert("_hash".to_string(), json!(geo_id_str.clone()));

            features.push(json!({
                "type": "Feature",
                "id": geo_id_str,
                "geometry": multipolygon_to_geojson(mp)?,
                "properties": properties,
            }));
        }

        Ok(json!({
            "type": "FeatureCollection",
            "features": features,
        }))
    }

    /// Export layer geometries as GeoJSON FeatureCollection with district assignments.
    pub fn to_geojson_with_districts(&self, assignments: &[u32]) -> Result<Value> {
        self.to_geojson_with_districts_and_bounds(assignments, None)
    }

    /// Export layer geometries as GeoJSON FeatureCollection with district assignments,
    /// optionally filtered by bounds.
    pub fn to_geojson_with_districts_and_bounds(
        &self,
        assignments: &[u32],
        bounds: Option<[f64; 4]>,
    ) -> Result<Value> {
        let region = &*self.region;
        let num_entities = self.geo_ids.len();

        if assignments.len() != num_entities {
            return Err(anyhow!(
                "[to_geojson_with_districts] Assignments length ({}) does not match entity count ({})",
                assignments.len(),
                num_entities
            ));
        }

        let indices: Vec<usize> = if let Some([min_lon, min_lat, max_lon, max_lat]) = bounds {
            let envelope = Rect::new(
                geo::Coord { x: min_lon, y: min_lat },
                geo::Coord { x: max_lon, y: max_lat },
            );
            region.units_in_envelope(envelope).into_iter().map(|u| u.0 as usize).collect()
        } else {
            (0..num_entities).collect()
        };

        let mut features = Vec::with_capacity(indices.len().min(10000));

        for idx in indices {
            let mp = region.geometry(geograph::UnitId(idx as u32));

            if mp.0.is_empty() {
                continue;
            }

            let district = assignments.get(idx).copied().unwrap_or(0);

            let mut properties = Map::new();

            if let Some(geo_id) = self.geo_ids.get(idx) {
                properties.insert("geo_id".to_string(), json!(geo_id.id()));
            }

            properties.insert("district".to_string(), json!(district));

            if district > 0 {
                let h = ((district as f64 * 57.0) % 360.0) as u32;
                properties.insert("district_color".to_string(), json!(format!("hsl({} 70% 50%)", h)));
            }

            let key_columns = ["name", "TOTPOP", "T_20_CENS_Total"];
            for col_name in key_columns.iter() {
                let json_val = if *col_name == "name" {
                    self.unit_names.get(idx)
                        .map(|s| json!(s))
                        .unwrap_or(Value::Null)
                } else {
                    self.unit_weights.get_as_f64(col_name, idx)
                        .map(|v| json!(v))
                        .unwrap_or(Value::Null)
                };
                properties.insert((*col_name).to_string(), json_val);
            }

            let geometry = match multipolygon_to_geojson(mp) {
                Ok(geom) => geom,
                Err(e) => {
                    eprintln!("Warning: Failed to convert geometry for feature {}: {}", idx, e);
                    continue;
                }
            };

            let geo_id_str = self.geo_ids.get(idx)
                .map(|g| g.id().to_string())
                .unwrap_or_else(|| format!("{}", idx));

            let feature_hash = format!("{}:{}", geo_id_str, district);
            properties.insert("_hash".to_string(), json!(feature_hash));

            features.push(json!({
                "type": "Feature",
                "id": geo_id_str,
                "geometry": geometry,
                "properties": properties,
            }));
        }

        Ok(json!({
            "type": "FeatureCollection",
            "features": features,
        }))
    }
}

fn multipolygon_to_geojson(mp: &MultiPolygon<f64>) -> Result<Value> {
    let mut polygons_json = Vec::new();
    for polygon in mp.0.iter() {
        let exterior: Vec<Vec<f64>> = polygon.exterior().coords()
            .map(|c| vec![c.x, c.y])
            .collect();
        let interiors: Vec<Vec<Vec<f64>>> = polygon.interiors().iter()
            .map(|ls| ls.coords().map(|c| vec![c.x, c.y]).collect())
            .collect();
        polygons_json.push(json!([exterior, interiors]));
    }
    Ok(json!({
        "type": "MultiPolygon",
        "coordinates": polygons_json
    }))
}
