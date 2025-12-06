//! Multi-objective scalarization: a set of metrics plus a set of weights.
//!
//! This is the “objective” as seen by search algorithms such as Tabu
//! search or simulated annealing. It is deliberately simple: a weighted
//! sum of metric values. More advanced schemes (lexicographic ordering,
//! epsilon-constraints, etc.) can be layered on top later.

use crate::objective::Metric;
use crate::partition::Partition;

/// A multi-objective scalarization: metrics + corresponding weights.
///
/// In the simplest form, this represents:
///
///     total_cost = sum_i weights[i] * metric_i(plan)
///
/// The actual metric implementations are handled elsewhere; this type
/// just stores configuration and provides evaluation entry-points.
#[derive(Clone)]
pub struct Objective {
    metrics: Vec<Metric>,
    weights: Vec<f64>,
}

impl Objective {
    /// Construct a new Objective from a list of metrics and an optional
    /// list of weights.
    ///
    /// If `weights` is `None`, all metrics default to weight 1.0.
    /// If `Some`, the length must match `metrics.len()`.
    pub fn new(metrics: Vec<Metric>, weights: Option<Vec<f64>>) -> Self {
        let weights = match weights {
            Some(weights) => weights,
            None => vec![1.0; metrics.len()],
        };

        assert_eq!(weights.len(), metrics.len(),
            "Objective::new: weights length ({}) must match metrics length ({})",
            weights.len(),
            metrics.len(),
        );

        Self { metrics, weights }
    }

    /// Number of metric terms in this objective.
    #[inline] pub fn num_metrics(&self) -> usize { self.metrics.len() }

    /// Accessor for weights vector.
    #[inline] pub fn weights(&self) -> &[f64] { &self.weights }

    /// Replace the current weights with a new vector (length must match num_metrics).
    pub fn set_weights(&mut self, weights: Vec<f64>) {
        assert_eq!(weights.len(), self.metrics.len(),
            "Objective::set_weights: weights length ({}) must match metrics length ({})",
            weights.len(),
            self.metrics.len(),
        );

        self.weights = weights;
    }

    /// Internal accessor for metrics.
    pub(crate) fn metrics(&self) -> &[Metric] { &self.metrics }

    /// Evaluate this objective for a given partition.
    /// Returns the weighted average of metric scores.
    pub(crate) fn compute(&self, partition: &Partition) -> f64 {
        let mut weighted_sum = 0.0;
        let total_weight: f64 = self.weights.iter().sum();

        for (metric, &weight) in self.metrics.iter().zip(&self.weights) {
            let score = metric.compute_score(partition);
            weighted_sum += weight * score;
        }

        if total_weight > 0.0 {
            weighted_sum / total_weight
        } else {
            0.0
        }
    }
}

use std::fmt;

impl fmt::Display for Objective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Objective {{")?;
        for (i, (metric, weight)) in self.metrics.iter().zip(&self.weights).enumerate() {
            writeln!(f, "  {:2}: {} * weight={}", i, metric, weight)?;
        }
        write!(f, "}}")
    }
}

impl fmt::Debug for Objective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
