use pyo3::{pyclass, pymethods, pymodule, types::PyModule, Bound, Py, PyResult, Python};
use numpy::{PyArray1, IntoPyArray};

/// Python-facing Map wrapper.
#[pyclass]
pub struct Map {
    inner: openmander_map::Map, // your type
}

#[pymethods]
impl Map {
    #[new]
    pub fn new(pack_dir: &str) -> PyResult<Self> {
        let map = openmander_map::Map::read_from_pack(&std::path::PathBuf::from(pack_dir))
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: map })
    }
}

/// Python-facing Plan wrapper that holds a strong ref to the PyMap owner.
/// This ensures the underlying Map outlives the Plan reference stored in `inner`.
#[pyclass]
pub struct Plan {
    /// Strong Python ref that keeps the PyMap (and its inner Map) alive.
    _owner: Py<Map>,

    /// Borrowing plan; lifetime erased because `owner` guarantees validity.
    inner: openmander_redistrict::Plan<'static>,
}

#[pymethods]
impl Plan {
    /// Build while holding the GIL (no threads). This avoids capturing Py types in a worker thread.
    #[new]
    pub fn new(py: Python<'_>, map: Py<Map>, num_districts: usize) -> PyResult<Self> {
        // Take a raw pointer to the inner Map while holding the GIL
        let map_ptr: *const openmander_map::Map = {
            let map_ref = map.borrow(py);
            &map_ref.inner as *const _
        }; // `map_ref` dropped here

        // Construct the borrowing Plan **without** holding a PyRef
        let plan_local = unsafe {
            // SAFETY: `map` (Py<PyMap>) is stored in `owner` below, which keeps the
            // underlying Map alive for as long as `PyPlan` exists. We only create
            // a temporary shared reference to that Map here.
            openmander_redistrict::Plan::new(&*map_ptr, num_districts as u32)
        };

        // SAFETY: `plan_local` borrows from `map_ref.inner`. We store `map` in `owner`,
        // which keeps the underlying PyMap alive for the lifetime of PyPlan.
        let inner: openmander_redistrict::Plan<'static> = unsafe {
            std::mem::transmute::<_, openmander_redistrict::Plan<'static>>(plan_local)
        };
        Ok(Self { _owner: map, inner })
    }

    /// Get assignments as a NumPy int32 array
    pub fn assignments<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<i32>> {
        let v: Vec<i32> = self.inner.partition.assignments.iter().map(|&p| p as i32).collect();
        v.into_pyarray_bound(py)  // returns Bound<'py, PyArray1<i32>>
    }

    /// Randomize partition (adjust path to your API as needed)
    pub fn randomize(&mut self) {
        self.inner.partition.randomize()
    }

    /// Equalize population of partition
    pub fn equalize(&mut self, py: Python<'_>, series: &str, tolerance: f64, max_iter: usize) -> PyResult<()> {
        py.allow_threads(|| self.inner.partition.equalize(series, tolerance, max_iter));
        Ok(())
    }

    /// Save plan to CSV/Parquet path
    pub fn to_csv(&self, path: &str) -> PyResult<()> {
        self.inner.to_csv(&std::path::PathBuf::from(path))
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }
}

#[pymodule]
fn openmander(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Map>()?;
    m.add_class::<Plan>()?;
    Ok(())
}
