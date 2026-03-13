use std::collections::{HashMap, HashSet};

use ndarray::{s, Array1, Array2, Axis};
use polars::{frame::DataFrame, prelude::DataType};

#[derive(Clone, Debug)]
pub(crate) enum WeightType { I64, F64 }

/// Node weights stored as type-separated matrices.
#[derive(Clone, Debug, Default)]
pub(crate) struct WeightMatrix {
    series: HashMap<String, (WeightType, usize)>, // len = k_i + k_f
    i64: Array2<i64>, // (n, k_i)
    f64: Array2<f64>, // (n, k_f)
}

impl WeightMatrix {
    /// Create a new WeightMatrix from type-separated weight vectors.
    pub(crate) fn new(size: usize, weights_i64: HashMap<String, Vec<i64>>, weights_f64: HashMap<String, Vec<f64>>) -> Self {
        let mut weights = Self {
            series: HashMap::new(),
            i64: Array2::<i64>::zeros((size, weights_i64.len())),
            f64: Array2::<f64>::zeros((size, weights_f64.len())),
        };

        weights_i64.into_iter().enumerate().for_each(|(i, (name, values))| {
            assert!(values.len() == size, "weights_i64[{}].len() must equal num_nodes", name);
            weights.i64.slice_mut(s![.., i]).assign(&Array1::from(values));
            weights.series.insert(name, (WeightType::I64, i));
        });

        weights_f64.into_iter().enumerate().for_each(|(i, (name, values))| {
            assert!(values.len() == size, "weights_f64[{}].len() must equal num_nodes", name);
            weights.f64.slice_mut(s![.., i]).assign(&Array1::from(values));
            weights.series.insert(name, (WeightType::F64, i));
        });

        weights
    }

    /// Build a `WeightMatrix` from the numeric columns of a DataFrame, skipping `idx`.
    pub(crate) fn from_dataframe(df: &DataFrame) -> Self {
        let weights_i64 = df.get_columns().iter()
            .map(|c| (c.name().to_string(), c))
            .filter(|(name, _)| name != "idx")
            .filter_map(|(name, c)| match c.dtype() {
                DataType::Int64  => Some((name, c.i64().unwrap().into_no_null_iter().collect())),
                DataType::Int32  => Some((name, c.i32().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::Int16  => Some((name, c.i16().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::Int8   => Some((name, c.i8().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::UInt64 => Some((name, c.u64().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::UInt32 => Some((name, c.u32().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::UInt16 => Some((name, c.u16().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                DataType::UInt8  => Some((name, c.u8().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                _ => None,
            }).collect();

        let weights_f64 = df.get_columns().iter()
            .map(|c| (c.name().to_string(), c))
            .filter_map(|(name, c)| match c.dtype() {
                DataType::Float64 => Some((name, c.f64().unwrap().into_no_null_iter().collect())),
                DataType::Float32 => Some((name, c.f32().unwrap().into_no_null_iter().map(|v| v as f64).collect())),
                _ => None,
            }).collect();

        Self::new(df.height(), weights_i64, weights_f64)
    }

    /// Create an empty WeightMatrix with zero series.
    pub(crate) fn empty(size: usize) -> Self {
        Self {
            series: HashMap::new(),
            i64: Array2::<i64>::zeros((size, 0)),
            f64: Array2::<f64>::zeros((size, 0)),
        }
    }

    /// Get a list of available weight series names.
    pub(crate) fn series(&self) -> HashSet<String> { self.series.keys().cloned().collect() }

    /// Check if a weight series exists.
    pub(crate) fn contains(&self, series: &str) -> bool { self.series.contains_key(series) }

    /// Get a weight value as f64, regardless of original type.
    pub(crate) fn get_as_f64(&self, series: &str, u: usize) -> Option<f64> {
        self.series.get(series).map(|(kind, c)| match kind {
            WeightType::I64 => self.i64[(u, *c)] as f64,
            WeightType::F64 => self.f64[(u, *c)],
        })
    }

    /// Get the total weight of a subgraph as f64, regardless of original type.
    pub(crate) fn get_subgraph_weight_as_f64(&self, series: &str, subgraph: &[usize]) -> Option<f64> {
        self.series.get(series).map(|(kind, c)| match kind {
            WeightType::I64 => subgraph.iter().map(|&u| self.i64[(u, *c)] as f64).sum(),
            WeightType::F64 => subgraph.iter().map(|&u| self.f64[(u, *c)]).sum(),
        })
    }

    /// Create a new empty WeightMatrix with a given size, copying the existing series.
    pub(crate) fn copy_of_size(&self, size: usize) -> Self {
        Self {
            series: self.series.clone(),
            i64: Array2::<i64>::zeros((size, self.i64.ncols())),
            f64: Array2::<f64>::zeros((size, self.f64.ncols())),
        }
    }

    /// Clear all weights to zero.
    pub(crate) fn clear_all_rows(&mut self) {
        self.i64.fill(0);
        self.f64.fill(0.0);
    }

    /// Clear a specific row to zero.
    pub(crate) fn clear_row(&mut self, row: usize) {
        self.i64.row_mut(row).fill(0);
        self.f64.row_mut(row).fill(0.0);
    }

    /// Add a row to another row in place.
    pub(crate) fn add_row(&mut self, to_row: usize, from_row: usize) {
        let row_i = self.i64.row(from_row).to_owned();
        self.i64.row_mut(to_row).scaled_add(1, &row_i);

        let row_f = self.f64.row(from_row).to_owned();
        self.f64.row_mut(to_row).scaled_add(1.0, &row_f);
    }

    /// Set a row to be the sum of all weights from another WeightMatrix.
    pub(crate) fn set_row_to_sum_of(&mut self, to_row: usize, other: &Self) {
        self.i64.row_mut(to_row).assign(&other.i64.sum_axis(Axis(0)));
        self.f64.row_mut(to_row).assign(&other.f64.sum_axis(Axis(0)));
    }

    /// Add a row of another WeightMatrix to a row in this one.
    pub(crate) fn add_row_from(&mut self, to_row: usize, other: &Self, from_row: usize) {
        self.i64.row_mut(to_row).scaled_add(1, &other.i64.row(from_row));
        self.f64.row_mut(to_row).scaled_add(1.0, &other.f64.row(from_row));
    }

    /// Subtract a row of another WeightMatrix from the a row in this one.
    pub(crate) fn subtract_row_from(&mut self, to_row: usize, other: &Self, from_row: usize) {
        self.i64.row_mut(to_row).scaled_add(-1, &other.i64.row(from_row));
        self.f64.row_mut(to_row).scaled_add(-1.0, &other.f64.row(from_row));
    }

    /// Add multiple rows of another WeightMatrix to a row in this one.
    pub(crate) fn add_rows_from(&mut self, to_row: usize, other: &Self, from_rows: &[usize]) {
        let mut sum_i = Array1::<i64>::zeros(other.i64.ncols());
        let mut sum_f = Array1::<f64>::zeros(other.f64.ncols());

        for &row in from_rows {
            sum_i += &other.i64.row(row);
            sum_f += &other.f64.row(row);
        }

        self.i64.row_mut(to_row).scaled_add(1, &sum_i);
        self.f64.row_mut(to_row).scaled_add(1.0, &sum_f);
    }

    /// Subtract multiple rows of another WeightMatrix from a row in this one.
    pub(crate) fn subtract_rows_from(&mut self, to_row: usize, other: &Self, from_rows: &[usize]) {
        let mut sum_i = Array1::<i64>::zeros(other.i64.ncols());
        let mut sum_f = Array1::<f64>::zeros(other.f64.ncols());
        for &row in from_rows {
            sum_i += &other.i64.row(row);
            sum_f += &other.f64.row(row);
        }
        self.i64.row_mut(to_row).scaled_add(-1, &sum_i);
        self.f64.row_mut(to_row).scaled_add(-1.0, &sum_f);
    }
}
