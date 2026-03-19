#![allow(unsafe_op_in_unsafe_fn)]
use std::sync::Arc;

use pyo3::{pyclass, pymethods, Bound, PyResult, Python};
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

    /// Read a map from a pack directory, optionally specifying format.
    ///
    /// Parameters
    /// ----------
    /// pack_dir : str
    ///     Path to the pack directory.
    /// format : str, optional
    ///     Pack format: "parquet" or "json". If None, auto-detects from files.
    #[pyo3(signature = (pack_dir, format=None))]
    #[classmethod]
    pub fn from_pack(_cls: &Bound<'_, pyo3::types::PyType>, pack_dir: &str, format: Option<&str>) -> PyResult<Self> {
        use std::str::FromStr;
        let path = std::path::PathBuf::from(pack_dir);
        let map = if let Some(fmt_str) = format {
            let fmt = openmander_core::PackFormat::from_str(fmt_str)
                .map_err(|e| PyValueError::new_err(format!("Invalid format: {}. Expected 'parquet' or 'json'", e)))?;
            let src = openmander_core::DiskPack::new(&path);
            openmander_core::Map::read_from_pack_source(&src, fmt)
                .map_err(|e| PyValueError::new_err(e.to_string()))?
        } else {
            openmander_core::Map::read_from_pack(&path)
                .map_err(|e| PyValueError::new_err(e.to_string()))?
        };
        Ok(Self { inner: Arc::new(map) })
    }

    /// Write the map to a pack directory.
    ///
    /// Parameters
    /// ----------
    /// pack_dir : str
    ///     Path to the output pack directory.
    /// format : str, optional
    ///     Pack format: "parquet" or "json". Defaults to parquet if available, otherwise json.
    #[pyo3(signature = (pack_dir, format=None))]
    pub fn to_pack(&self, pack_dir: &str, format: Option<&str>) -> PyResult<()> {
        use std::str::FromStr;
        let path = std::path::PathBuf::from(pack_dir);
        if let Some(fmt_str) = format {
            let fmt = openmander_core::PackFormat::from_str(fmt_str)
                .map_err(|e| PyValueError::new_err(format!("Invalid format: {}. Expected 'parquet' or 'json'", e)))?;
            self.inner.write_to_pack_with_format(&path, fmt)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
        } else {
            self.inner.write_to_pack(&path)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
        }
        Ok(())
    }

    /// Return per-unit geometry statistics for a given layer.
    ///
    /// Returns a list of dicts with keys:
    ///   - ``geo_id``: unit identifier string
    ///   - ``idx``: 0-based unit index within the layer
    ///   - ``num_polygons``: number of polygons in the MultiPolygon
    ///   - ``holes_per_polygon``: list[int] — interior-ring count for each polygon
    ///   - ``is_exterior``: bool — touches the region boundary
    ///
    /// Parameters
    /// ----------
    /// layer : str, default="block"
    ///     One of: "state", "county", "tract", "group", "vtd", "block".
    #[pyo3(text_signature = "(self, layer='block')")]
    pub fn geometry_stats<'py>(&self, py: Python<'py>, layer: Option<&str>) -> PyResult<Bound<'py, pyo3::types::PyList>> {
        use pyo3::types::{PyDict, PyList};

        let layer_name = layer.unwrap_or("block");
        let ty = openmander_core::GeoType::from_str(layer_name).ok_or_else(|| {
            PyValueError::new_err(format!(
                "Unknown layer {:?}. Expected one of: state, county, tract, group, vtd, block",
                layer_name
            ))
        })?;

        let stats = self.inner.geometry_stats(ty)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        use pyo3::types::{PyDictMethods, PyListMethods};
        let out = PyList::empty_bound(py);
        for (geo_id, idx, holes, is_ext) in stats {
            let num_polygons = holes.len();
            let d = PyDict::new_bound(py);
            d.set_item("geo_id", &geo_id)?;
            d.set_item("idx", idx)?;
            d.set_item("num_polygons", num_polygons)?;
            d.set_item("holes_per_polygon", holes)?;
            d.set_item("is_exterior", is_ext)?;
            out.append(d)?;
        }
        Ok(out)
    }

    /// Write an SVG for a given layer.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     Output SVG path.
    /// layer : str, default="block"
    ///     One of: "state", "county", "tract", "group", "vtd", "block".
    /// series : Optional[str]
    ///     Optional column name in the layer's dataframe to use for coloring.
    #[pyo3(text_signature = "(self, path, layer='block', series=None)")]
    pub fn to_svg(&self, path: &str, layer: Option<&str>, series: Option<&str>) -> PyResult<()> {
        // Determine which layer to use (default = "block")
        let layer = layer.unwrap_or("block");
        let ty = openmander_core::GeoType::from_str(layer).ok_or_else(|| {
            PyValueError::new_err(format!(
                "Unknown layer {:?}. Expected one of: state, county, tract, group, vtd, block",
                layer
            ))
        })?;

        self.inner.as_ref().layer(ty)
            .ok_or_else(|| PyValueError::new_err(format!("Layer {:?} is not present in this map/pack.", layer)))?
            .to_svg(&std::path::PathBuf::from(path), series)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
}
