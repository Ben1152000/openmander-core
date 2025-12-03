#![allow(unsafe_op_in_unsafe_fn)]
use pyo3::{pyclass, pymethods};

/// A single metric used in a multi-objective optimization.
/// Examples: population equality, compactness, competitiveness, proportionality.
#[pyclass]
#[derive(Clone)]
pub struct Metric {
    pub(crate) inner: openmander_core::Metric,
}

#[pymethods]
impl Metric {
    /// Population equality metric for a given weight series (e.g., total population).
    ///
    /// Parameters
    /// ----------
    /// series : str
    ///     Name of the node-weight column.
    #[staticmethod]
    pub fn population_deviation(series: &str) -> Self {
        let inner = openmander_core::Metric::population_deviation(series.to_string());
        Self { inner }
    }

    /// Polsby–Popper compactness metric.
    #[staticmethod]
    pub fn compactness_polsby_popper() -> Self {
        let inner = openmander_core::Metric::compactness_polsby_popper();
        Self { inner }
    }

    /// Competitiveness metric based on district-level vote shares.
    ///
    /// Parameters
    /// ----------
    /// dem_series : str
    ///     Node-weight series for Democratic votes.
    /// rep_series : str
    ///     Node-weight series for Republican votes.
    /// threshold : float
    ///     Margin threshold (e.g., 0.10 for districts within 10 percentage points).
    #[staticmethod]
    pub fn competitiveness(dem_series: &str, rep_series: &str, threshold: f64) -> Self {
        let inner = openmander_core::Metric::competitiveness(
            dem_series.to_string(),
            rep_series.to_string(),
            threshold,
        );
        Self { inner }
    }

    /// Seats–votes proportionality / partisan fairness metric.
    ///
    /// Parameters
    /// ----------
    /// dem_series : str
    ///     Node-weight series for Democratic votes.
    /// rep_series : str
    ///     Node-weight series for Republican votes.
    #[staticmethod]
    pub fn proportionality(dem_series: &str, rep_series: &str) -> Self {
        let inner = openmander_core::Metric::proportionality(
            dem_series.to_string(),
            rep_series.to_string(),
        );
        Self { inner }
    }

    fn __repr__(&self) -> String { format!("{}", self.inner) }
}
