use std::sync::Arc;

use anyhow::Result;
use js_sys::{Array, Object, Reflect, Uint32Array, Uint8Array};
use serde::Deserialize;
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};

use crate::{WasmMap, common::*};

/// Deserializable metric config passed from JS.
#[derive(Deserialize)]
#[serde(tag = "type")]
enum MetricConfig {
    PopulationDeviation         { pop_series: String },
    PopulationDeviationAbsolute { pop_series: String },
    PopulationDeviationSmooth   { pop_series: String },
    PopulationDeviationSharp    { pop_series: String },
    CompactnessPolsbyPopper,
    CompactnessSchwartzberg,
    CompetitivenessBinary    { dem_series: String, rep_series: String, threshold: f64 },
    CompetitivenessQuadratic { dem_series: String, rep_series: String, threshold: f64 },
    CompetitivenessGaussian  { dem_series: String, rep_series: String, sigma: f64 },
    Proportionality          { dem_series: String, rep_series: String },
}

impl MetricConfig {
    fn into_metric(self) -> openmander_core::Metric {
        use openmander_core::Metric;
        match self {
            Self::PopulationDeviation         { pop_series } => Metric::population_deviation(pop_series),
            Self::PopulationDeviationAbsolute { pop_series } => Metric::population_deviation_absolute(pop_series),
            Self::PopulationDeviationSmooth   { pop_series } => Metric::population_deviation_smooth(pop_series),
            Self::PopulationDeviationSharp    { pop_series } => Metric::population_deviation_sharp(pop_series),
            Self::CompactnessPolsbyPopper                    => Metric::compactness_polsby_popper(),
            Self::CompactnessSchwartzberg                    => Metric::compactness_schwartzberg(),
            Self::CompetitivenessBinary    { dem_series, rep_series, threshold } => Metric::competitiveness_binary(dem_series, rep_series, threshold),
            Self::CompetitivenessQuadratic { dem_series, rep_series, threshold } => Metric::competitiveness_quadratic(dem_series, rep_series, threshold),
            Self::CompetitivenessGaussian  { dem_series, rep_series, sigma }     => Metric::competitiveness_gaussian(dem_series, rep_series, sigma),
            Self::Proportionality          { dem_series, rep_series }            => Metric::proportionality(dem_series, rep_series),
        }
    }
}

#[derive(Deserialize)]
struct ObjectiveConfig {
    metrics: Vec<MetricConfig>,
    weights: Option<Vec<f64>>,
}

#[derive(Deserialize)]
struct AnnealConfig {
    objectives:              Vec<ObjectiveConfig>,
    max_iter:                usize,
    #[serde(default = "default_init_temp")]
    init_temp:               f64,
    phase_start_probs:       Vec<f64>,
    phase_end_probs:         Vec<Option<f64>>,
    phase_cooling_rates:     Vec<f64>,
    #[serde(default = "default_early_stop_iters")]
    early_stop_iters:        usize,
    #[serde(default = "default_batch_size")]
    temp_search_batch_size:  usize,
    #[serde(default = "default_batch_size")]
    batch_size:              usize,
}

fn default_init_temp()        -> f64   { 1.0 }
fn default_early_stop_iters() -> usize { 100_000 }
fn default_batch_size()       -> usize { 1_000 }

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
        let mut series: Vec<String> = self.inner.series().into_iter().collect();
        series.sort();
        serde_wasm_bindgen::to_value(&series).map_err(|e| e.into())
    }

    /// District totals for a series. Returns a JS array of numbers.
    pub fn district_totals(&self, series: String) -> Result<JsValue, JsValue> {
        let v = self.inner.district_totals(&series).map_err(js_err)?;
        serde_wasm_bindgen::to_value(&v).map_err(|e| e.into())
    }

    /// Totals for all parts including unassigned (index 0). Returns a JS array of numbers.
    pub fn all_part_totals(&self, series: String) -> Result<JsValue, JsValue> {
        let v = self.inner.all_part_totals(&series).map_err(js_err)?;
        serde_wasm_bindgen::to_value(&v).map_err(|e| e.into())
    }

    pub fn randomize(&mut self) -> Result<(), JsValue> {
        self.inner.randomize().map_err(js_err)
    }

    pub fn randomize_minimize_county_splits(&mut self, series: String) -> Result<(), JsValue> {
        self.inner.randomize_minimize_county_splits(&series).map_err(js_err)
    }

    /// Perform exact population equalization using ILP block swaps.
    /// Returns the number of blocks moved (fallback edge count is not
    /// exposed in the WASM binding).
    pub fn equalize_exact(&mut self, series: String) -> Result<usize, JsValue> {
        self.inner.equalize_exact(&series)
            .map(|(blocks_moved, _)| blocks_moved)
            .map_err(js_err)
    }

    /// Build the equalization graph and spanning tree for the current partition.
    /// Prints all edges and feasible net_flows to the browser console, and
    /// returns a one-line summary string.  Intended for development/debugging.
    pub fn equalize_exact_debug(&mut self, series: String) -> Result<String, JsValue> {
        self.inner.equalize_exact_debug(&series).map_err(js_err)
    }

    /// Run one outer iteration of equalization. Returns `true` if converged.
    pub fn equalize_step(&mut self, series: String, tolerance: f64) -> Result<bool, JsValue> {
        self.inner.equalize_step(&series, tolerance).map_err(js_err)
    }

    pub fn equalize(&mut self, series: String, tolerance: f64, max_iter: usize) -> Result<(), JsValue> {
        self.inner.equalize(&series, tolerance, max_iter).map_err(js_err)
    }

    /// Tune temperature via binary search until average acceptance probability reaches `target_prob`.
    /// Returns the tuned temperature. Runs blocking (fast: ~10-20 batches).
    ///
    /// Config format (subset of anneal_from_json):
    /// ```json
    /// { "objectives": [...], "init_temp": 1.0, "phase_start_probs": [0.8], "temp_search_batch_size": 1000 }
    /// ```
    pub fn anneal_tune_temp_from_json(&mut self, config_json: String) -> Result<f64, JsValue> {
        let config: AnnealConfig = serde_json::from_str(&config_json)
            .map_err(|e| js_err(format!("Invalid anneal config: {e}")))?;

        let objectives: Vec<openmander_core::Objective> = config.objectives.into_iter()
            .map(|o| {
                let metrics = o.metrics.into_iter().map(MetricConfig::into_metric).collect();
                openmander_core::Objective::new(metrics, o.weights)
            })
            .collect();

        if objectives.is_empty() {
            return Err(js_err("anneal_tune_temp_from_json: objectives must not be empty"));
        }
        if config.phase_start_probs.is_empty() {
            return Err(js_err("anneal_tune_temp_from_json: phase_start_probs must not be empty"));
        }

        let tuned = self.inner.tune_temperature(
            &objectives[0],
            config.phase_start_probs[0],
            config.init_temp,
            config.temp_search_batch_size,
        );

        Ok(tuned)
    }

    /// Run a chunk of annealing iterations at a given temperature with geometric cooling.
    /// Returns `{ new_temp, avg_prob, any_accepted }` so the caller can manage the schedule.
    ///
    /// Config format (subset of anneal_from_json):
    /// ```json
    /// { "objectives": [...], "phase_cooling_rates": [0.001], "batch_size": 1000 }
    /// ```
    pub fn anneal_chunk_from_json(&mut self, config_json: String, temperature: f64) -> Result<JsValue, JsValue> {
        let config: AnnealConfig = serde_json::from_str(&config_json)
            .map_err(|e| js_err(format!("Invalid anneal config: {e}")))?;

        let objectives: Vec<openmander_core::Objective> = config.objectives.into_iter()
            .map(|o| {
                let metrics = o.metrics.into_iter().map(MetricConfig::into_metric).collect();
                openmander_core::Objective::new(metrics, o.weights)
            })
            .collect();

        if objectives.is_empty() {
            return Err(js_err("anneal_chunk_from_json: objectives must not be empty"));
        }
        if config.phase_cooling_rates.is_empty() {
            return Err(js_err("anneal_chunk_from_json: phase_cooling_rates must not be empty"));
        }

        let (new_temp, avg_prob, any_accepted) = self.inner.anneal_raw_chunk(
            &objectives[0],
            temperature,
            config.phase_cooling_rates[0],
            config.batch_size,
        );

        let obj = Object::new();
        Reflect::set(&obj, &"new_temp".into(),     &new_temp.into())    .unwrap();
        Reflect::set(&obj, &"avg_prob".into(),     &avg_prob.into())    .unwrap();
        Reflect::set(&obj, &"any_accepted".into(), &any_accepted.into()).unwrap();
        Ok(obj.into())
    }

    /// Run simulated annealing optimization with a JSON config.
    ///
    /// Config format:
    /// ```json
    /// {
    ///   "objectives": [{ "metrics": [{ "type": "PopulationDeviationSmooth", "pop_series": "T_20_CENS_Total" }], "weights": [1.0] }],
    ///   "max_iter": 500000,
    ///   "init_temp": 1.0,
    ///   "phase_start_probs": [0.8],
    ///   "phase_end_probs": [null],
    ///   "phase_cooling_rates": [0.001],
    ///   "early_stop_iters": 100000,
    ///   "temp_search_batch_size": 1000,
    ///   "batch_size": 1000
    /// }
    /// ```
    pub fn anneal_from_json(&mut self, config_json: String) -> Result<(), JsValue> {
        let config: AnnealConfig = serde_json::from_str(&config_json)
            .map_err(|e| js_err(format!("Invalid anneal config: {e}")))?;

        let objectives: Vec<openmander_core::Objective> = config.objectives.into_iter()
            .map(|o| {
                let metrics = o.metrics.into_iter().map(MetricConfig::into_metric).collect();
                openmander_core::Objective::new(metrics, o.weights)
            })
            .collect();

        self.inner.anneal(
            &objectives,
            config.max_iter,
            config.init_temp,
            &config.phase_start_probs,
            &config.phase_end_probs,
            &config.phase_cooling_rates,
            config.early_stop_iters,
            config.temp_search_batch_size,
            config.batch_size,
        ).map_err(js_err)
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

    /// Assign all blocks belonging to a geographic unit to a given district.
    /// `layer`: geographic level ("block", "vtd", "tract", "county", etc.)
    /// `geo_id`: FIPS identifier for the unit at that level.
    /// `district`: target district (1-indexed; 0 = unassigned). Contiguity is not enforced.
    pub fn assign_unit(&mut self, layer: String, geo_id: String, district: u32) -> Result<(), JsValue> {
        self.inner.assign_unit(&layer, &geo_id, district).map_err(js_err)
    }

    /// Assign all blocks belonging to multiple geographic units to a district in one pass.
    /// `geo_ids`: JS array of FIPS strings. Processes all units before returning, so only
    /// one geometry/stats recomputation is needed instead of one per unit.
    pub fn assign_units_batch(&mut self, layer: String, geo_ids: Array, district: u32) -> Result<(), JsValue> {
        let ids: Vec<String> = (0..geo_ids.length())
            .filter_map(|i| geo_ids.get(i).as_string())
            .collect();
        let ids_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
        self.inner.assign_units_batch(&layer, &ids_refs, district).map_err(js_err)
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
            
            let assignments = self.inner.get_assignments().map_err(js_err)?
                .into_iter().map(|(_, part)| part).collect::<Vec<_>>();
            
            // Verify assignments length matches layer length
            if assignments.len() != lyr.len() {
                return Err(js_err(format!(
                    "Assignments length ({}) does not match layer length ({})",
                    assignments.len(),
                    lyr.len()
                )));
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

    /// Get district geometries as WKB bytes.
    ///
    /// Returns a JavaScript array of objects: [{ district: number, wkb: Uint8Array }, ...]
    /// Districts 1 through num_districts are included. District 0 (unassigned) is excluded.
    #[wasm_bindgen(js_name = "district_geometries_wkb")]
    pub fn district_geometries_wkb(&self) -> Result<Array, JsValue> {
        let geometries = self.inner.district_geometries_wkb().map_err(js_err)?;

        let arr = Array::new();
        for (district, wkb) in &geometries {
            let obj = Object::new();
            Reflect::set(&obj, &JsValue::from_str("district"), &JsValue::from_f64(*district as f64))
                .map_err(|_| js_err("failed to set district"))?;
            Reflect::set(&obj, &JsValue::from_str("wkb"), &Uint8Array::from(wkb.as_slice()))
                .map_err(|_| js_err("failed to set wkb"))?;
            arr.push(&obj);
        }

        Ok(arr)
    }

}
