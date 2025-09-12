#![allow(unsafe_op_in_unsafe_fn)]
use std::sync::Arc;

use pyo3::{pyclass, pymethods, PyResult};
use pyo3::exceptions::PyValueError;

/// Python-facing Map wrapper.
#[pyclass]
pub struct Map {
    inner: Arc<openmander_core::Map>,
}

impl Map {
    #[inline] pub(crate) fn inner_arc(&self) -> Arc<openmander_core::Map> { self.inner.clone() }
}

#[pymethods]
impl Map {
    #[new]
    pub fn new(pack_dir: &str) -> PyResult<Self> {
        let map = openmander_core::Map::read_from_pack(&std::path::PathBuf::from(pack_dir))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: Arc::new(map) })
    }
}
