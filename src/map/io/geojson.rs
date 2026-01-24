use anyhow::{anyhow, Result};
use geo::MultiPolygon;
use rstar::AABB;
use serde_json::{json, Map, Value};

use crate::map::MapLayer;

impl MapLayer {
    /// Export layer geometries as GeoJSON FeatureCollection.
    /// Each feature includes the geometry and essential properties from unit_data.
    /// Only includes essential properties (geo_id, name, TOTPOP) to keep file size manageable.
    pub fn to_geojson(&self) -> Result<Value> {
        self.to_geojson_with_bounds(None)
    }

    /// Export layer geometries as GeoJSON FeatureCollection, optionally filtered by bounding box.
    /// bounds: Optional bounding box [min_lon, min_lat, max_lon, max_lat] to filter features.
    /// Only features that intersect the bounds will be included. If None, all features are included.
    pub fn to_geojson_with_bounds(&self, bounds: Option<[f64; 4]>) -> Result<Value> {
        let geoms = self.geoms.as_ref()
            .ok_or_else(|| anyhow!("[to_geojson_with_bounds] No geometries available"))?;

        // Determine which indices to include based on bounds
        let indices: Vec<usize> = if let Some([min_lon, min_lat, max_lon, max_lat]) = bounds {
            // Use R-tree to efficiently find features intersecting the bounds
            let envelope = AABB::from_corners([min_lon, min_lat], [max_lon, max_lat]);
            geoms.query_indices(&envelope)
        } else {
            // Include all features
            (0..geoms.len()).collect()
        };

        let mut features = Vec::new();
        features.reserve(indices.len().min(10000)); // Cap initial capacity

        for idx in indices {
            let mp = geoms.shapes().get(idx)
                .ok_or_else(|| anyhow!("[to_geojson_with_bounds] Index {} out of bounds", idx))?;
            let mut properties = Map::new();

            // Add geo_id
            if let Some(geo_id) = self.geo_ids.get(idx) {
                properties.insert("geo_id".to_string(), json!(geo_id.id()));
            }

            // Only include essential properties to reduce file size and improve performance
            // This is especially important for higher-level layers (county, vtd) that are used for zoomed-out views
            let essential_columns = ["name", "TOTPOP", "T_20_CENS_Total"];
            for col_name in essential_columns.iter() {
                if let Ok(col) = self.unit_data.column(*col_name) {
                    let json_val = match col.dtype() {
                        polars::prelude::DataType::String => {
                            if let Ok(s) = col.str() {
                                s.get(idx).map(|s| json!(s)).unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            }
                        }
                        polars::prelude::DataType::Int64 => {
                            if let Ok(v) = col.i64() {
                                v.get(idx).map(|v| json!(v)).unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            }
                        }
                        polars::prelude::DataType::Float64 => {
                            if let Ok(v) = col.f64() {
                                v.get(idx).map(|v| json!(v)).unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            }
                        }
                        polars::prelude::DataType::UInt32 => {
                            if let Ok(v) = col.u32() {
                                v.get(idx).map(|v| json!(v)).unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            }
                        }
                        _ => Value::Null,
                    };
                    properties.insert((*col_name).to_string(), json_val);
                }
            }

            // Add feature ID and hash for efficient updates
            let geo_id_str = self.geo_ids.get(idx)
                .map(|g| g.id().to_string())
                .unwrap_or_else(|| format!("{}", idx));
            
            // Create hash based on geo_id (for now, just use geo_id as hash)
            // This will be extended to include district/color in the districts version
            let feature_hash = format!("{}", geo_id_str);
            
            // Add hash to properties
            properties.insert("_hash".to_string(), json!(feature_hash));
            
            features.push(json!({
                "type": "Feature",
                "id": geo_id_str, // Feature ID for MapLibre efficient updates
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
    /// assignments: index -> district_id mapping
    pub fn to_geojson_with_districts(&self, assignments: &[u32]) -> Result<Value> {
        self.to_geojson_with_districts_and_bounds(assignments, None)
    }

    /// Export layer geometries as GeoJSON FeatureCollection with district assignments, optionally filtered by bounds.
    /// assignments: index -> district_id mapping
    /// bounds: Optional bounding box [min_lon, min_lat, max_lon, max_lat] to filter features.
    pub fn to_geojson_with_districts_and_bounds(&self, assignments: &[u32], bounds: Option<[f64; 4]>) -> Result<Value> {
        let geoms = self.geoms.as_ref()
            .ok_or_else(|| anyhow!("[to_geojson_with_districts] No geometries available"))?;

        let shapes = geoms.shapes();
        let num_shapes = shapes.len();
        let num_entities = self.geo_ids.len();
        
        // Verify that geometries match the number of entities
        if num_shapes != num_entities {
            return Err(anyhow!(
                "[to_geojson_with_districts] Geometry count ({}) does not match entity count ({})",
                num_shapes,
                num_entities
            ));
        }
        
        // Verify that assignments length matches
        if assignments.len() != num_entities {
            return Err(anyhow!(
                "[to_geojson_with_districts] Assignments length ({}) does not match entity count ({})",
                assignments.len(),
                num_entities
            ));
        }

        // Determine which indices to include based on bounds
        let indices: Vec<usize> = if let Some([min_lon, min_lat, max_lon, max_lat]) = bounds {
            // Use R-tree to efficiently find features intersecting the bounds
            let envelope = AABB::from_corners([min_lon, min_lat], [max_lon, max_lat]);
            geoms.query_indices(&envelope)
        } else {
            // Include all features
            (0..num_entities).collect()
        };

        let mut features = Vec::new();
        features.reserve(indices.len().min(10000)); // Cap initial capacity

        for idx in indices {
            let mp = shapes.get(idx)
                .ok_or_else(|| anyhow!("[to_geojson_with_districts_and_bounds] Index {} out of bounds", idx))?;
            
            let district = assignments.get(idx).copied().unwrap_or(0);
            
            let mut properties = Map::new();
            
            // Add geo_id
            if let Some(geo_id) = self.geo_ids.get(idx) {
                properties.insert("geo_id".to_string(), json!(geo_id.id()));
            }
            
            // Add district
            properties.insert("district".to_string(), json!(district));
            
            // Add district color (for visualization)
            if district > 0 {
                let h = ((district as f64 * 57.0) % 360.0) as u32;
                properties.insert("district_color".to_string(), json!(format!("hsl({} 70% 50%)", h)));
            }

            // Limit properties to essential ones for large datasets to reduce memory usage
            // Only add a few key columns instead of all columns
            let key_columns = ["name", "TOTPOP", "T_20_CENS_Total"];
            for col_name in key_columns.iter() {
                if let Ok(col) = self.unit_data.column(*col_name) {
                    let json_val = match col.dtype() {
                        polars::prelude::DataType::String => {
                            if let Ok(s) = col.str() {
                                s.get(idx).map(|s| json!(s)).unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            }
                        }
                        polars::prelude::DataType::Int64 => {
                            if let Ok(v) = col.i64() {
                                v.get(idx).map(|v| json!(v)).unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            }
                        }
                        polars::prelude::DataType::Float64 => {
                            if let Ok(v) = col.f64() {
                                v.get(idx).map(|v| json!(v)).unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            }
                        }
                        polars::prelude::DataType::UInt32 => {
                            if let Ok(v) = col.u32() {
                                v.get(idx).map(|v| json!(v)).unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            }
                        }
                        _ => Value::Null,
                    };
                    properties.insert((*col_name).to_string(), json_val);
                }
            }

            // Convert multipolygon to GeoJSON geometry - handle errors gracefully
            let geometry = match multipolygon_to_geojson(mp) {
                Ok(geom) => geom,
                Err(e) => {
                    // If geometry conversion fails, skip this feature
                    eprintln!("Warning: Failed to convert geometry for feature {}: {}", idx, e);
                    continue; // Skip this feature
                }
            };
            
            // Add feature ID and hash for efficient updates
            let geo_id_str = self.geo_ids.get(idx)
                .map(|g| g.id().to_string())
                .unwrap_or_else(|| format!("{}", idx));
            
            // Create hash based on geo_id + district (for change detection)
            let feature_hash = format!("{}:{}", geo_id_str, district);
            
            // Add hash to properties
            properties.insert("_hash".to_string(), json!(feature_hash));
            
            features.push(json!({
                "type": "Feature",
                "id": geo_id_str, // Feature ID for MapLibre efficient updates
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

/// Helper to convert a MultiPolygon to a serde_json::Value representing GeoJSON Geometry.
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

