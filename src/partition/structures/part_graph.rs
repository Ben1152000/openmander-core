use ndarray::Array2;

use crate::graph::WeightMatrix;

/// A dense graph over the parts (districts) of a partition.
///
/// Stores per-district aggregated node weights and a symmetric `num_parts × num_parts`
/// matrix of shared perimeter lengths between every pair of districts.
#[derive(Clone, Debug)]
pub(crate) struct PartGraph {
    weights: WeightMatrix,
    perimeters: Array2<f64>,
}

impl PartGraph {
    pub(crate) fn new(num_parts: usize, weights: WeightMatrix) -> Self {
        Self {
            weights,
            perimeters: Array2::zeros((num_parts, num_parts)),
        }
    }

    pub(crate) fn node_weights(&self) -> &WeightMatrix { &self.weights }
    pub(crate) fn node_weights_mut(&mut self) -> &mut WeightMatrix { &mut self.weights }

    /// Add `delta` to the shared perimeter between parts `a` and `b`.
    pub(crate) fn add_perimeter(&mut self, a: usize, b: usize, delta: f64) {
        self.perimeters[[a, b]] += delta;
    }

    /// Total perimeter of a part: sum of shared perimeters with all other parts.
    pub(crate) fn total_perimeter(&self, part: usize) -> f64 {
        self.perimeters.row(part).iter().enumerate()
            .filter(|&(j, _)| j != part)
            .map(|(_, &w)| w)
            .sum()
    }

    /// Zero out all perimeter values.
    pub(crate) fn clear_perimeters(&mut self) {
        self.perimeters.fill(0.0);
    }

    /// Merge `source` into `target`: accumulate weights and perimeters, then zero out `source`.
    pub(crate) fn merge_into(&mut self, target: usize, source: usize) {
        let n = self.perimeters.nrows();
        for part in 0..n {
            if part != target && part != source {
                self.perimeters[[target, part]] += self.perimeters[[source, part]];
                self.perimeters[[part, target]] += self.perimeters[[part, source]];
            }
            self.perimeters[[source, part]] = 0.0;
            self.perimeters[[part, source]] = 0.0;
        }
        self.weights.add_row(target, source);
        self.weights.clear_row(source);
    }
}
