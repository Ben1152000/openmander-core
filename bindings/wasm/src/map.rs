use std::sync::Arc;

use anyhow::Result;
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};

use crate::common::*;

#[wasm_bindgen]
pub struct WasmMap {
    inner: Arc<openmander_core::Map>,
}

#[wasm_bindgen]
impl WasmMap {
    /// Construct a Map from an in-memory pack:
    /// files: { "data/block.parquet": Uint8Array, "adj/block.csr.bin": Uint8Array, ... }
    #[wasm_bindgen(constructor)]
    pub fn new(files: JsValue) -> Result<WasmMap, JsValue> {
        let mem = js_files_to_mempack(files).map_err(js_err)?;
        // Auto-detect format from available files
        let format = openmander_core::Map::detect_pack_format(&mem)
            .map_err(js_err)
            .unwrap_or_else(|_| openmander_core::PackFormat::Pmtiles); // Default to PMTiles for WASM
        let map = openmander_core::Map::read_from_pack_source(&mem, format).map_err(js_err)?;
        Ok(WasmMap { inner: Arc::new(map) })
    }

    /// Return present layers as an array of strings.
    pub fn layers_present(&self) -> Result<JsValue, JsValue> {
        let mut out: Vec<String> = Vec::new();
        for ty in openmander_core::GeoType::ALL {
            if self.inner.layer(ty).is_some() {
                out.push(ty.to_str().to_string());
            }
        }
        serde_wasm_bindgen::to_value(&out).map_err(|e| e.into())
    }

    /// Generate SVG text for a given layer, optionally colored by series.
    /// Returns SVG XML string (UI can set innerHTML or create Blob).
    #[wasm_bindgen(js_name = "to_svg")]
    pub fn to_svg(&self, layer: Option<String>, series: Option<String>) -> Result<String, JsValue> {
        let ty = parse_layer(layer).map_err(js_err)?;
        let lyr = self.inner.layer(ty)
            .ok_or_else(|| js_err(format!("Layer {:?} is not present in this map/pack.", ty.to_str())))?;

        lyr.to_svg_string(series.as_deref()).map_err(js_err)
    }

    /// Export layer geometries as GeoJSON FeatureCollection.
    /// Returns GeoJSON as a JavaScript object.
    /// bounds: Optional bounding box [min_lon, min_lat, max_lon, max_lat] to filter features.
    #[wasm_bindgen(js_name = "to_geojson")]
    pub fn to_geojson(&self, layer: Option<String>, bounds: Option<Vec<f64>>) -> Result<JsValue, JsValue> {
        let ty = parse_layer(layer).map_err(js_err)?;
        let lyr = self.inner.layer(ty)
            .ok_or_else(|| js_err(format!("Layer {:?} is not present in this map/pack.", ty.to_str())))?;

        // Convert bounds from Vec<f64> to Option<[f64; 4]>
        let bounds_opt = bounds.and_then(|b| {
            if b.len() == 4 {
                Some([b[0], b[1], b[2], b[3]])
            } else {
                None
            }
        });

        let geojson = lyr.to_geojson_with_bounds(bounds_opt).map_err(js_err)?;
        
        // Serialize to JSON string first, then parse in JS to avoid large in-memory structures
        // This is more memory-efficient and avoids serialization issues
        let json_string = serde_json::to_string(&geojson)
            .map_err(|e| js_err(format!("Failed to serialize GeoJSON to string: {}", e)))?;
        
        // Parse the JSON string in JavaScript
        let parsed: JsValue = js_sys::JSON::parse(&json_string)
            .map_err(|e| js_err(format!("Failed to parse GeoJSON string: {:?}", e)))?;
        
        Ok(parsed)
    }

    /// Return geo_ids in index order for every layer, as a JSON string.
    /// Shape: `{"block": ["geo1", "geo2", ...], "county": [...], ...}`
    /// Used by the plan worker to populate geoIdByIndex immediately on ready,
    /// without waiting for the metrics worker to parse the full CSV.
    pub fn geo_id_index_json(&self) -> Result<String, JsValue> {
        let mut map = serde_json::Map::new();
        for ty in openmander_core::GeoType::ALL {
            if let Some(lyr) = self.inner.layer(ty) {
                let ids: Vec<&str> = lyr.geo_ids().iter().map(|g| g.id()).collect();
                map.insert(
                    ty.to_str().to_string(),
                    serde_json::Value::Array(ids.into_iter().map(|s| serde_json::Value::String(s.to_string())).collect()),
                );
            }
        }
        serde_json::to_string(&map).map_err(|e| js_err(format!("geo_id_index_json: {e}")))
    }

    /// Expose the internal Arc<Map> to create plans.
    /// (Not exported to JS; used by WasmPlan::new)
    pub(crate) fn inner_arc(&self) -> Arc<openmander_core::Map> { self.inner.clone() }
}
