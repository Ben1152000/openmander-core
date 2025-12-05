#![allow(unsafe_op_in_unsafe_fn)]
use std::{collections::HashMap, path::PathBuf};

use pyo3::{pyclass, pymethods, Bound, Py, PyResult, Python};
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList};

use crate::Map;

/// Python-facing Plan wrapper that holds a strong ref to the PyMap owner.
/// This ensures the underlying Map outlives the Plan reference stored in `inner`.
#[pyclass]
pub struct Plan {
    inner: openmander_core::Plan,
}

#[pymethods]
impl Plan {
    /// Construct a Plan from a Python Map.
    /// Clones the Arc<Map> and passes it into `Plan::new(map: impl Into<Arc<Map>>)` safely.
    #[new]
    pub fn new(py: Python<'_>, map: Py<Map>, num_districts: u32) -> PyResult<Self> {
        let arc = map.borrow(py).inner_arc();
        Ok(Self { inner: openmander_core::Plan::new(arc, num_districts) })
    }

    /// Get the number of districts in this plan (excluding unassigned 0).
    pub fn num_districts(&self) -> PyResult<u32> {
        Ok(self.inner.num_districts())
    }

    /// Get block assignments as a Python dict { "block_geoid": district:int }.
    /// Includes zeros for unassigned blocks.
    pub fn assignments<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new_bound(py);
        let assignments = self.inner.get_assignments()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        for (geo_id, district) in assignments {
            dict.set_item(geo_id.id(), district)?;
        }

        Ok(dict)
    }

    /// Set block assignments from a Python dict { "block_geoid": district:int }.
    /// Any block not present in the dict is set to 0 (unassigned).
    pub fn set_assignments(&mut self, assignments: Bound<'_, PyDict>) -> PyResult<()> {
        let map = assignments.iter()
            .map(|(key, value)| Ok((
                openmander_core::GeoId::new(
                    openmander_core::GeoType::Block,
                    &key.extract::<String>()
                        .map_err(|_| PyValueError::new_err("[Plan.set_assignments] keys must be strings (geo_id)"))?,
                ),
                value.extract::<u32>()
                    .map_err(|_| PyValueError::new_err("[Plan.set_assignments] values must be integers (district)"))?
            )))
            .collect::<PyResult<HashMap<_, _>>>()?;
        
        self.inner.set_assignments(map)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Get the list of weight series available in the map's node weights.
    pub fn series<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        Ok(PyList::new_bound(py, self.inner.series()))
    }

    /// Sum of a weight series for each district (excluding unassigned 0).
    pub fn district_totals<'py>(&self, py: Python<'py>, series: &str) -> PyResult<Vec<f64>> {
        py.allow_threads(|| {
            self.inner.district_totals(series)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Compute metric values for the current partition (per-district scores).
    pub fn compute_metric<'py>(&self, py: Python<'py>, metric: &crate::Metric) -> PyResult<Vec<f64>> {
        py.allow_threads(||
            Ok(self.inner.compute_metric(&metric.inner))
        )
    }

    /// Compute the aggregated score for a metric for the current partition.
    pub fn compute_metric_score<'py>(&self, py: Python<'py>, metric: &crate::Metric) -> PyResult<f64> {
        py.allow_threads(||
            Ok(self.inner.compute_metric_score(&metric.inner))
        )
    }

    /// Compute objective value for the current partition.
    pub fn compute_objective<'py>(&self, py: Python<'py>, objective: &crate::Objective) -> PyResult<f64> {
        py.allow_threads(||
            Ok(self.inner.compute_objective(&objective.inner))
        )
    }

    /// Randomize partition into contiguous districts
    pub fn randomize(&mut self) -> PyResult<()> {
        self.inner.randomize()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Equalize a weight series across districts using greedy swaps
    pub fn equalize<'py>(&mut self, py: Python<'py>, series: &str, tolerance: f64, max_iter: usize) -> PyResult<()> {
        py.allow_threads(||
            self.inner.equalize(series, tolerance, max_iter)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        )
    }

    pub fn anneal_balance<'py>(&mut self,
        py: Python<'py>,
        series: &str,
        max_iter: usize,
        initial_temp: f64,
        final_temp: f64,
        boundary_factor: f64
    ) -> PyResult<()> {
        py.allow_threads(||
            self.inner.anneal_balance(series, max_iter, initial_temp, final_temp, boundary_factor)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        )
    }

    /// Run simulated annealing to optimize a generic objective function.
    ///
    /// The algorithm maximizes the objective value (higher is better).
    /// If you want to include compactness or boundary considerations, add them
    /// as metrics in your objective.
    ///
    /// Three-phase adaptive annealing:
    /// 1. Find initial temperature where acceptance rate ≈ 0.9
    /// 2. Cool geometrically at specified rate until acceptance rate ≈ 0.1
    /// 3. Run at final temperature with early stopping (stops after N iters without improvement)
    ///
    /// Parameters
    /// ----------
    /// objective : Objective
    ///     The objective to maximize.
    /// max_iter : int
    ///     Safety maximum iterations (prevents infinite loops).
    /// init_temp : float, optional
    ///     Initial temperature guess for phase 1 (default: 1.0).
    /// cooling_rate : float, optional
    ///     Geometric cooling rate (temp *= rate each iteration, default: 0.99999).
    /// early_stop_iters : int, optional
    ///     Stop phase 3 after this many iterations without improvement (default: 100000).
    /// window_size : int, optional
    ///     Rolling window size for measuring acceptance rates (default: 1000).
    #[pyo3(signature = (objective, max_iter, init_temp=1.0, cooling_rate=0.99999, early_stop_iters=100000, window_size=1000, log_every=1000))]
    pub fn anneal<'py>(&mut self,
        py: Python<'py>,
        objective: &crate::Objective,
        max_iter: usize,
        init_temp: f64,
        cooling_rate: f64,
        early_stop_iters: usize,
        window_size: usize,
        log_every: usize,
    ) -> PyResult<()> {
        py.allow_threads(||
            self.inner.anneal(&objective.inner, max_iter, init_temp, cooling_rate, early_stop_iters, window_size, log_every)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        )
    }

    /// Improve balance using a Tabu search heuristic.
    ///
    /// Parameters
    /// ----------
    /// series : str
    ///     Name of the node-weight series (e.g., total population).
    /// max_iter : int
    ///     Maximum number of Tabu iterations.
    /// tabu_tenure : int
    ///     Iterations for which the reverse move is tabu.
    /// boundary_factor : float
    ///     0.0 = population balance only, 1.0 = boundary length only.
    /// candidates_per_iter : int
    ///     How many random neighbor moves to sample per iteration.
    pub fn tabu_balance<'py>(&mut self,
        py: Python<'py>,
        series: &str,
        max_iter: usize,
        tabu_tenure: usize,
        boundary_factor: f64,
        candidates_per_iter: usize,
    ) -> PyResult<()> {
        py.allow_threads(|| {
            self.inner.tabu_balance(series, max_iter, tabu_tenure, boundary_factor, candidates_per_iter)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    pub fn recombine<'py>(&mut self, py: Python<'py>, a: u32, b: u32) -> PyResult<()> {
        py.allow_threads(||
            self.inner.recombine(a, b)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        )
    }

    /// Load assignments from a CSV path (same validation as Rust `load_csv`)
    pub fn load_csv(&mut self, path: &str) -> PyResult<()> {
        self.inner.load_csv(&PathBuf::from(path))
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    /// Save plan to CSV at the given path (non-zero assignments only)
    pub fn to_csv(&self, path: &str) -> PyResult<()> {
        self.inner.to_csv(&PathBuf::from(path))
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    /// Save plan to SVG at the given path (shows district outlines and fills)
    pub fn to_svg(&self, path: &str) -> PyResult<()> {
        self.inner.to_svg(&PathBuf::from(path))
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }
}
