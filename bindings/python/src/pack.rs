#![allow(unsafe_op_in_unsafe_fn)]
use std::path::PathBuf;

use pyo3::{pyfunction, PyResult, Python};
use pyo3::exceptions::PyRuntimeError;

#[pyfunction]
#[pyo3(text_signature = "(state_code, path='.', has_vtd=True, verbose=0)")]
#[pyo3(signature = (state_code, path=".", has_vtd=true, verbose=0))]
pub fn build_pack(py: Python<'_>, state_code: &str, path: &str, has_vtd: bool, verbose: u8) -> PyResult<String> {
    let pathbuf = PathBuf::from(path);
    let p = py.allow_threads(|| openmander_core::build_pack(state_code, &pathbuf, has_vtd, verbose))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(p.to_string_lossy().into_owned())
}

#[pyfunction]
#[pyo3(text_signature = "(state_code, path='.', verbose=0)")]
#[pyo3(signature = (state_code, path=".", verbose=0))]
pub fn download_pack(py: Python<'_>, state_code: &str, path: &str, verbose: u8) -> PyResult<String> {
    let pathbuf = PathBuf::from(path);
    let p = py.allow_threads(|| openmander_core::download_pack(state_code, &pathbuf, verbose))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(p.to_string_lossy().into_owned())
}

#[pyfunction]
#[pyo3(text_signature = "(pack_path, verbose=0)")]
#[pyo3(signature = (pack_path, verbose=0))]
pub fn validate_pack(py: Python<'_>, pack_path: &str, verbose: u8) -> PyResult<()> {
    let pathbuf = PathBuf::from(pack_path);
    py.allow_threads(|| openmander_core::validate_pack(&pathbuf, verbose))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}
