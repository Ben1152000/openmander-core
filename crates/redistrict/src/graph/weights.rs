use std::collections::HashMap;

use ndarray::Array2;

#[derive(Clone, Debug)]
pub enum WeightType { I64, F64 }

/// Node weights stored as type-separated matrices.
#[derive(Clone, Debug)]
pub struct WeightMatrix {
    pub series: HashMap<String, (WeightType, usize)>, // len = k_i + k_f
    pub i64: Array2<i64>, // (n, k_i)
    pub f64: Array2<f64>, // (n, k_f)
}

impl WeightMatrix {
    /// Get a weight value as f64, regardless of original type.
    pub fn get_as_f64(&self, series: &str, u: usize) -> Option<f64> {
        self.series.get(series).map(|(kind, c)| match kind {
            WeightType::I64 => self.i64[(u, *c)] as f64,
            WeightType::F64 => self.f64[(u, *c)],
        })
    }
}
