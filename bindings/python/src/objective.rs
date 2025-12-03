#![allow(unsafe_op_in_unsafe_fn)]
use pyo3::{pyclass, pymethods};

use crate::Metric;

/// A multi-objective scalarization: a set of metrics + a set of weights.
/// Weights are separate so you can change them over time (e.g., schedules).
#[pyclass]
pub struct Objective {
    pub(crate) inner: openmander_core::Objective,
}

#[pymethods]
impl Objective {
    /// Create a new Objective from a list of metrics and an optional list of weights.
    ///
    /// Parameters
    /// ----------
    /// metrics : list[Metric]
    ///     List of metric objects.
    /// weights : list[float] | None, default None
    ///     Optional list of weights; if None, the core may default to all 1.0
    ///     or a normalized scheme.
    ///
    /// Examples
    /// --------
    /// >>> obj = Objective(
    /// ...     [Metric.population_equality("T_20_CENS_Total"),
    /// ...      Metric.compactness_polsby_popper()],
    /// ...     weights=[0.7, 0.3],
    /// ... )
    #[new]
    pub fn new(metrics: Vec<Metric>, weights: Option<Vec<f64>>) -> Self {
        let inner_metrics = metrics.into_iter().map(|m| m.inner).collect();
        let inner = openmander_core::Objective::new(inner_metrics, weights);
        Self { inner }
    }

    /// Number of metric terms in this objective.
    #[getter]
    pub fn num_metrics(&self) -> usize {
        self.inner.num_metrics()
    }

    /// Get a copy of the current weight vector.
    #[getter]
    pub fn weights(&self) -> Vec<f64> { self.inner.weights().to_vec() }

    /// Replace the current weights with a new vector.
    ///
    /// The length must match the number of metrics.
    pub fn set_weights(&mut self, weights: Vec<f64>) {
        // You can add validation / error propagation later if core returns Result.
        self.inner.set_weights(weights);
    }

    fn __repr__(&self) -> String { format!("{}", self.inner) }
}
