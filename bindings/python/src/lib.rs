use std::{collections::HashMap, path::PathBuf, sync::Arc};

use pyo3::{pyclass, pymethods, pymodule, Bound, Py, PyResult, Python};
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyModule};

/// Python-facing Map wrapper.
#[pyclass]
pub struct Map {
    inner: Arc<openmander_map::Map>,
}

#[pymethods]
impl Map {
    #[new]
    pub fn new(pack_dir: &str) -> PyResult<Self> {
        let map = openmander_map::Map::read_from_pack(&std::path::PathBuf::from(pack_dir))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: Arc::new(map) })
    }
}

/// Python-facing Plan wrapper that holds a strong ref to the PyMap owner.
/// This ensures the underlying Map outlives the Plan reference stored in `inner`.
#[pyclass]
pub struct Plan {
    inner: openmander_plan::Plan,
}

#[pymethods]
impl Plan {
    /// Construct a Plan from a Python Map.
    /// Clones the Arc<Map> and passes it into `Plan::new(map: impl Into<Arc<Map>>)` safely.
    #[new]
    pub fn new(py: Python<'_>, map: Py<Map>, num_districts: u32) -> PyResult<Self> {
        Ok(Self { inner: openmander_plan::Plan::new(map.borrow(py).inner.clone(), num_districts) })
    }

    /// Get the number of districts in this plan (excluding unassigned 0).
    pub fn num_districts(&self) -> PyResult<u32> {
        Ok(self.inner.num_districts())
    }

    /// Get the list of weight series available in the map's node weights.
    pub fn get_series<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        Ok(PyList::new_bound(py, self.inner.get_series()))
    }

    /// Set block assignments from a Python dict { "block_geoid": district:int }.
    /// Any block not present in the dict is set to 0 (unassigned).
    pub fn set_assignments(&mut self, assignments: Bound<'_, PyDict>) -> PyResult<()> {
        let map = assignments.iter()
            .map(|(key, value)| Ok((
                openmander_map::GeoId {
                    ty: openmander_map::GeoType::Block,
                    id: key.extract::<String>()
                            .map_err(|_| PyValueError::new_err("[Plan.set_assignments] keys must be strings (geo_id)"))?
                            .into()
                },
                value.extract::<u32>()
                    .map_err(|_| PyValueError::new_err("[Plan.set_assignments] values must be integers (district)"))?
            )))
            .collect::<PyResult<HashMap<_, _>>>()?;
        
        self.inner.set_assignments(map)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Get block assignments as a Python dict { "block_geoid": district:int }.
    /// Includes zeros for unassigned blocks.
    pub fn get_assignments<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new_bound(py);
        let assignments = self.inner.get_assignments()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        for (geo_id, district) in assignments {
            dict.set_item(geo_id.id.as_ref(), district)?;
        }

        Ok(dict)
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
}

#[pymodule]
fn openmander(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Map>()?;
    m.add_class::<Plan>()?;
    Ok(())
}
