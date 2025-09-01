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
