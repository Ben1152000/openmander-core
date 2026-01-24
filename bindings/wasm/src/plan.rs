use std::sync::Arc;

use anyhow::Result;
use js_sys::{Object, Reflect, Uint32Array};
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};

use crate::{WasmMap, common::*};

#[wasm_bindgen]
pub struct WasmPlan {
    inner: openmander_core::Plan,
    map: Arc<openmander_core::Map>, // keep map alive like your Python wrapper does
}

#[wasm_bindgen]
impl WasmPlan {
    #[wasm_bindgen(constructor)]
    pub fn new(map: &WasmMap, num_districts: u32) -> Result<WasmPlan, JsValue> {
        let arc = map.inner_arc();
        let plan = openmander_core::Plan::new(arc.clone(), num_districts).map_err(js_err)?;
        Ok(WasmPlan { inner: plan, map: arc })
    }

    pub fn num_districts(&self) -> u32 {
        self.inner.num_districts()
    }

    /// Series available in the map's weights.
    pub fn series(&self) -> Result<JsValue, JsValue> {
        let mut s: Vec<String> = self.inner.series().into_iter().collect();
        s.sort();
        serde_wasm_bindgen::to_value(&s).map_err(|e| e.into())
    }

    /// District totals for a series. Returns a JS array of numbers.
    pub fn district_totals(&self, series: String) -> Result<JsValue, JsValue> {
        let v = self.inner.district_totals(&series).map_err(js_err)?;
        serde_wasm_bindgen::to_value(&v).map_err(|e| e.into())
    }

    pub fn randomize(&mut self) -> Result<(), JsValue> {
        self.inner.randomize().map_err(js_err)
    }

    pub fn equalize(&mut self, series: String, tolerance: f64, max_iter: usize) -> Result<(), JsValue> {
        self.inner.equalize(&series, tolerance, max_iter).map_err(js_err)
    }

    pub fn anneal_balance(
        &mut self,
        series: String,
        max_iter: usize,
        initial_temp: f64,
        final_temp: f64,
        boundary_factor: f64,
    ) -> Result<(), JsValue> {
        self.inner
            .anneal_balance(&series, max_iter, initial_temp, final_temp, boundary_factor)
            .map_err(js_err)
    }

    pub fn tabu_balance(
        &mut self,
        series: String,
        max_iter: usize,
        tabu_tenure: usize,
        boundary_factor: f64,
        candidates_per_iter: usize,
    ) -> Result<(), JsValue> {
        self.inner
            .tabu_balance(&series, max_iter, tabu_tenure, boundary_factor, candidates_per_iter)
            .map_err(js_err)
    }

    pub fn recombine(&mut self, a: u32, b: u32) -> Result<(), JsValue> {
        self.inner.recombine(a, b).map_err(js_err)
    }

    /// FAST assignments export: return a Uint32Array of length = #units in active layer.
    pub fn assignments_u32(&self) -> Result<Uint32Array, JsValue> {
        let a: Vec<u32> = self.inner.get_assignments_vec().map_err(js_err)?;
        Ok(Uint32Array::from(a.as_slice()))
    }

    /// Compatibility assignments export: returns { "geoid": district } (slow for blocks).
    pub fn assignments_dict(&self) -> Result<JsValue, JsValue> {
        let assignments = self.inner.get_assignments().map_err(js_err)?;
        let obj = Object::new();
        for (geo_id, district) in assignments {
            Reflect::set(&obj, &JsValue::from_str(geo_id.id()), &JsValue::from_f64(district as f64))
                .map_err(|_| js_err("failed to set dict item"))?;
        }
        Ok(obj.into())
    }

    /// Set assignments from a Uint32Array (index-based).
    pub fn set_assignments_u32(&mut self, arr: Uint32Array) -> Result<(), JsValue> {
        let mut v = vec![0u32; arr.length() as usize];
        arr.copy_to(&mut v[..]);
        self.inner.set_assignments_vec(v).map_err(js_err)
    }

    /// Load assignments from CSV *text* (browser has no file paths).
    pub fn load_csv_text(&mut self, csv: String) -> Result<(), JsValue> {
        self.inner.load_csv(&csv).map_err(js_err)
    }

    /// Export CSV as *text*.
    pub fn to_csv_text(&self) -> Result<String, JsValue> {
        self.inner.to_csv().map_err(js_err)
    }

    /// Export layer geometries as GeoJSON FeatureCollection with district assignments.
    /// Returns GeoJSON as a JavaScript object.
    /// Note: assignments are for the base layer (blocks), so this only works for the base layer.
    /// bounds: Optional bounding box [min_lon, min_lat, max_lon, max_lat] to filter features.
    #[wasm_bindgen(js_name = "to_geojson")]
    pub fn to_geojson(&self, layer: Option<String>, bounds: Option<Vec<f64>>) -> Result<JsValue, JsValue> {
        // Wrap everything in a catch_unwind to convert panics to errors
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let ty = parse_layer(layer).map_err(js_err)?;
            
            // Get the base layer to verify assignments match
            let base_layer = self.map.base().map_err(js_err)?;
            
            // Only allow base layer for now (assignments are block-level)
            if ty != base_layer.ty() {
                return Err(js_err(format!(
                    "to_geojson currently only supports the base layer ({:?}), not {:?}",
                    base_layer.ty().to_str(),
                    ty.to_str()
                )));
            }
            
            let lyr = self.map.layer(ty)
                .ok_or_else(|| js_err(format!("Layer {:?} is not present in this map/pack.", ty.to_str())))?;
            
            let assignments = self.inner.get_assignments_vec().map_err(js_err)?;
            
            // Verify assignments length matches layer length
            if assignments.len() != lyr.len() {
                return Err(js_err(format!(
                    "Assignments length ({}) does not match layer length ({})",
                    assignments.len(),
                    lyr.len()
                )));
            }
            
            // Check if layer has geometries
            if lyr.shapes().is_none() {
                return Err(js_err("Layer does not have geometries loaded"));
            }
            
            // Convert bounds from Vec<f64> to Option<[f64; 4]>
            let bounds_opt = bounds.and_then(|b| {
                if b.len() == 4 {
                    Some([b[0], b[1], b[2], b[3]])
                } else {
                    None
                }
            });
            
            let geojson = lyr.to_geojson_with_districts_and_bounds(&assignments, bounds_opt).map_err(js_err)?;
            
            // Serialize to JSON string first, then parse in JS to avoid large in-memory structures
            // This is more memory-efficient for large GeoJSON structures
            let json_string = serde_json::to_string(&geojson)
                .map_err(|e| js_err(format!("Failed to serialize GeoJSON to string: {}", e)))?;
            
            // Parse the JSON string in JavaScript
            // This avoids potential issues with serde_wasm_bindgen and large structures
            let parsed: JsValue = js_sys::JSON::parse(&json_string)
                .map_err(|e| js_err(format!("Failed to parse GeoJSON string: {:?}", e)))?;
            
            Ok(parsed)
        }));
        
        match result {
            Ok(Ok(val)) => Ok(val),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(js_err("Panic occurred while generating GeoJSON. This may be due to memory limits or data size.")),
        }
    }
}
