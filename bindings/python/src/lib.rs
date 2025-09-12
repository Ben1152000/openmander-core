// #![allow(unsafe_op_in_unsafe_fn)]
mod map;
mod plan;
mod pack;

pub use map::Map;
pub use plan::Plan;
pub use pack::*;

use pyo3::{pymodule, Bound, PyResult, Python, types::PyModule};

#[pymodule]
fn openmander(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Map>()?;
    m.add_class::<Plan>()?;

    m.add_function(pyo3::wrap_pyfunction!(build_pack, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(download_pack, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(validate_pack, m)?)?;

    Ok(())
}
