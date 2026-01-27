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
    #[staticmethod]
    pub fn population_deviation(pop_series: &str) -> Self {
        let inner = openmander_core::Metric::population_deviation(pop_series.to_string());
        Self { inner }
    }

    /// Population equality absolute metric for a given weight series (e.g., total population).
    #[staticmethod]
    pub fn population_deviation_absolute(pop_series: &str) -> Self {
        let inner = openmander_core::Metric::population_deviation_absolute(pop_series.to_string());
        Self { inner }
    }

    /// Population equality smooth metric for a given weight series (e.g., total population).
    #[staticmethod]
    pub fn population_deviation_smooth(pop_series: &str) -> Self {
        let inner = openmander_core::Metric::population_deviation_smooth(pop_series.to_string());
        Self { inner }
    }

    /// Population equality sharp (linear) metric for a given weight series (e.g., total population).
    /// Scores on a linear scale: 0 for empty district, 1 for target population, 0 for double target or above.
    #[staticmethod]
    pub fn population_deviation_sharp(pop_series: &str) -> Self {
        let inner = openmander_core::Metric::population_deviation_sharp(pop_series.to_string());
        Self { inner }
    }

    /// Polsby–Popper compactness metric.
    #[staticmethod]
    pub fn compactness_polsby_popper() -> Self {
        let inner = openmander_core::Metric::compactness_polsby_popper();
        Self { inner }
    }

    /// Schwartzberg compactness metric.
    #[staticmethod]
    pub fn compactness_schwartzberg() -> Self {
        let inner = openmander_core::Metric::compactness_schwartzberg();
        Self { inner }
    }

    /// Competitiveness metric based on district-level vote shares (binary).
    #[staticmethod]
    pub fn competitiveness_binary(dem_series: &str, rep_series: &str, threshold: f64) -> Self {
        let inner = openmander_core::Metric::competitiveness_binary(
            dem_series.to_string(),
            rep_series.to_string(),
            threshold,
        );
        Self { inner }
    }

    /// Competitiveness metric based on district-level vote shares (piecewise quadratic).
    #[staticmethod]
    pub fn competitiveness_quadratic(dem_series: &str, rep_series: &str, threshold: f64) -> Self {
        let inner = openmander_core::Metric::competitiveness_quadratic(
            dem_series.to_string(),
            rep_series.to_string(),
            threshold,
        );
        Self { inner }
    }

    /// Competitiveness metric based on district-level vote shares (Gaussian).
    #[staticmethod]
    pub fn competitiveness_gaussian(dem_series: &str, rep_series: &str, sigma: f64) -> Self {
        let inner = openmander_core::Metric::competitiveness_gaussian(
            dem_series.to_string(),
            rep_series.to_string(),
            sigma,
        );
        Self { inner }
    }

    /// Seats–votes proportionality / partisan fairness metric.
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
