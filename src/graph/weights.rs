use std::collections::{HashMap, HashSet};

use ndarray::{s, Array1, Array2, ArrayView1, Axis};

#[derive(Clone, Debug)]
pub(crate) enum WeightType { Int, F64 }

/// Node weights stored as type-separated matrices.
#[derive(Clone, Debug, Default)]
pub(crate) struct WeightMatrix {
    series: HashMap<String, (WeightType, usize)>, // len = k_i + k_f
    int: Array2<i32>, // (n, k_i)  — census/election integers always fit in i32
    f64: Array2<f64>, // (n, k_f)
}

impl WeightMatrix {
    /// Create a new WeightMatrix from type-separated weight vectors.
    #[allow(unused)]
    pub(crate) fn new(size: usize, weights_i64: HashMap<String, Vec<i64>>, weights_f64: HashMap<String, Vec<f64>>) -> Self {
        let mut weights = Self {
            series: HashMap::new(),
            int: Array2::<i32>::zeros((size, weights_i64.len())),
            f64: Array2::<f64>::zeros((size, weights_f64.len())),
        };

        weights_i64.into_iter().enumerate().for_each(|(i, (name, values))| {
            assert!(values.len() == size, "weights_i64[{}].len() must equal num_nodes", name);
            weights.int.slice_mut(s![.., i]).assign(&Array1::from_iter(values.iter().map(|&v| v as i32)));
            weights.series.insert(name, (WeightType::Int, i));
        });

        weights_f64.into_iter().enumerate().for_each(|(i, (name, values))| {
            assert!(values.len() == size, "weights_f64[{}].len() must equal num_nodes", name);
            weights.f64.slice_mut(s![.., i]).assign(&Array1::from(values));
            weights.series.insert(name, (WeightType::F64, i));
        });

        weights
    }

    /// Build a `WeightMatrix` directly from pre-typed column arrays.
    ///
    /// `int_series` / `f64_series` are the column names in the same order as the columns
    /// of `int_data` / `f64_data`.  Called only from `pack_csv.rs` and `build.rs`;
    /// `Array2` internals are not exposed to redistricting code.
    pub(crate) fn from_arrays(
        int_series: Vec<String>,
        int_data:   Array2<i32>,
        f64_series: Vec<String>,
        f64_data:   Array2<f64>,
    ) -> Self {
        let mut series = HashMap::with_capacity(int_series.len() + f64_series.len());
        for (col_idx, name) in int_series.into_iter().enumerate() {
            series.insert(name, (WeightType::Int, col_idx));
        }
        for (col_idx, name) in f64_series.into_iter().enumerate() {
            series.insert(name, (WeightType::F64, col_idx));
        }
        Self { series, int: int_data, f64: f64_data }
    }

    /// Ordered list of integer series names (column order matches `int_row` columns).
    pub(crate) fn int_series_names(&self) -> Vec<&str> {
        let mut names: Vec<(&str, usize)> = self.series.iter()
            .filter_map(|(name, (kind, idx))| {
                if matches!(kind, WeightType::Int) { Some((name.as_str(), *idx)) } else { None }
            })
            .collect();
        names.sort_by_key(|(_, idx)| *idx);
        names.into_iter().map(|(n, _)| n).collect()
    }

    /// Ordered list of f64 series names (column order matches `f64_row` columns).
    pub(crate) fn f64_series_names(&self) -> Vec<&str> {
        let mut names: Vec<(&str, usize)> = self.series.iter()
            .filter_map(|(name, (kind, idx))| {
                if matches!(kind, WeightType::F64) { Some((name.as_str(), *idx)) } else { None }
            })
            .collect();
        names.sort_by_key(|(_, idx)| *idx);
        names.into_iter().map(|(n, _)| n).collect()
    }

    /// View of the integer data row for a given unit index.
    pub(crate) fn int_row(&self, row: usize) -> ArrayView1<'_, i32> { self.int.row(row) }

    /// View of the f64 data row for a given unit index.
    pub(crate) fn f64_row(&self, row: usize) -> ArrayView1<'_, f64> { self.f64.row(row) }

    /// Build a `WeightMatrix` from the numeric columns of a DataFrame, skipping `idx`.
    ///
    /// Writes directly into the ndarray without building intermediate per-column `Vec`s,
    /// avoiding ~800 MB of peak allocation for large states.
    #[cfg(any(feature = "download", feature = "parquet"))]
    pub(crate) fn from_dataframe(df: &polars::frame::DataFrame) -> Self {
        use polars::prelude::DataType;

        let n = df.height();
        let all_cols = df.get_columns();

        // First pass: categorize columns (names only, no data copied).
        let mut int_names: Vec<&str> = Vec::new();
        let mut f64_names: Vec<&str> = Vec::new();
        for c in all_cols.iter() {
            let name = c.name().as_str();
            if name == "idx" { continue; }
            match c.dtype() {
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 |
                DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => {
                    int_names.push(name);
                }
                DataType::Float64 | DataType::Float32 => {
                    f64_names.push(name);
                }
                _ => {}
            }
        }

        // Pre-allocate final ndarrays and fill column-by-column, avoiding any intermediate Vec.
        let mut series = HashMap::with_capacity(int_names.len() + f64_names.len());
        let mut int_mat = Array2::<i32>::zeros((n, int_names.len()));
        let mut f64_mat = Array2::<f64>::zeros((n, f64_names.len()));

        for (col_idx, &name) in int_names.iter().enumerate() {
            let c = df.column(name).unwrap();
            let mut col = int_mat.column_mut(col_idx);
            match c.dtype() {
                DataType::Int64  => col.iter_mut().zip(c.i64().unwrap().into_no_null_iter()).for_each(|(d, v)| *d = v as i32),
                DataType::Int32  => col.iter_mut().zip(c.i32().unwrap().into_no_null_iter()).for_each(|(d, v)| *d = v),
                DataType::Int16  => col.iter_mut().zip(c.i16().unwrap().into_no_null_iter()).for_each(|(d, v)| *d = v as i32),
                DataType::Int8   => col.iter_mut().zip(c.i8().unwrap().into_no_null_iter()).for_each(|(d, v)| *d = v as i32),
                DataType::UInt64 => col.iter_mut().zip(c.u64().unwrap().into_no_null_iter()).for_each(|(d, v)| *d = v as i32),
                DataType::UInt32 => col.iter_mut().zip(c.u32().unwrap().into_no_null_iter()).for_each(|(d, v)| *d = v as i32),
                DataType::UInt16 => col.iter_mut().zip(c.u16().unwrap().into_no_null_iter()).for_each(|(d, v)| *d = v as i32),
                DataType::UInt8  => col.iter_mut().zip(c.u8().unwrap().into_no_null_iter()).for_each(|(d, v)| *d = v as i32),
                _ => unreachable!(),
            }
            series.insert(name.to_string(), (WeightType::Int, col_idx));
        }

        for (col_idx, &name) in f64_names.iter().enumerate() {
            let c = df.column(name).unwrap();
            let mut col = f64_mat.column_mut(col_idx);
            match c.dtype() {
                DataType::Float64 => col.iter_mut().zip(c.f64().unwrap().into_no_null_iter()).for_each(|(d, v)| *d = v),
                DataType::Float32 => col.iter_mut().zip(c.f32().unwrap().into_no_null_iter()).for_each(|(d, v)| *d = v as f64),
                _ => unreachable!(),
            }
            series.insert(name.to_string(), (WeightType::F64, col_idx));
        }

        Self { series, int: int_mat, f64: f64_mat }
    }

    /// Create an empty WeightMatrix with zero series.
    #[allow(unused)]
    pub(crate) fn empty(size: usize) -> Self {
        Self {
            series: HashMap::new(),
            int: Array2::<i32>::zeros((size, 0)),
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
            WeightType::Int => self.int[(u, *c)] as f64,
            WeightType::F64 => self.f64[(u, *c)],
        })
    }

    /// Create a new empty WeightMatrix with a given size, copying the existing series.
    pub(crate) fn copy_of_size(&self, size: usize) -> Self {
        Self {
            series: self.series.clone(),
            int: Array2::<i32>::zeros((size, self.int.ncols())),
            f64: Array2::<f64>::zeros((size, self.f64.ncols())),
        }
    }

    /// Clear all weights to zero.
    pub(crate) fn clear_all_rows(&mut self) {
        self.int.fill(0);
        self.f64.fill(0.0);
    }

    /// Clear a specific row to zero.
    pub(crate) fn clear_row(&mut self, row: usize) {
        self.int.row_mut(row).fill(0);
        self.f64.row_mut(row).fill(0.0);
    }

    /// Add a row to another row in place.
    pub(crate) fn add_row(&mut self, to_row: usize, from_row: usize) {
        let row_i = self.int.row(from_row).to_owned();
        self.int.row_mut(to_row).scaled_add(1, &row_i);

        let row_f = self.f64.row(from_row).to_owned();
        self.f64.row_mut(to_row).scaled_add(1.0, &row_f);
    }

    /// Set a row to be the sum of all weights from another WeightMatrix.
    pub(crate) fn set_row_to_sum_of(&mut self, to_row: usize, other: &Self) {
        self.int.row_mut(to_row).assign(&other.int.sum_axis(Axis(0)));
        self.f64.row_mut(to_row).assign(&other.f64.sum_axis(Axis(0)));
    }

    /// Add a row of another WeightMatrix to a row in this one.
    pub(crate) fn add_row_from(&mut self, to_row: usize, other: &Self, from_row: usize) {
        self.int.row_mut(to_row).scaled_add(1, &other.int.row(from_row));
        self.f64.row_mut(to_row).scaled_add(1.0, &other.f64.row(from_row));
    }

    /// Subtract a row of another WeightMatrix from the a row in this one.
    pub(crate) fn subtract_row_from(&mut self, to_row: usize, other: &Self, from_row: usize) {
        self.int.row_mut(to_row).scaled_add(-1, &other.int.row(from_row));
        self.f64.row_mut(to_row).scaled_add(-1.0, &other.f64.row(from_row));
    }

    /// Add multiple rows of another WeightMatrix to a row in this one.
    pub(crate) fn add_rows_from(&mut self, to_row: usize, other: &Self, from_rows: &[usize]) {
        let mut sum_i = Array1::<i32>::zeros(other.int.ncols());
        let mut sum_f = Array1::<f64>::zeros(other.f64.ncols());

        for &row in from_rows {
            sum_i += &other.int.row(row);
            sum_f += &other.f64.row(row);
        }

        self.int.row_mut(to_row).scaled_add(1, &sum_i);
        self.f64.row_mut(to_row).scaled_add(1.0, &sum_f);
    }

    /// Subtract multiple rows of another WeightMatrix from a row in this one.
    pub(crate) fn subtract_rows_from(&mut self, to_row: usize, other: &Self, from_rows: &[usize]) {
        let mut sum_i = Array1::<i32>::zeros(other.int.ncols());
        let mut sum_f = Array1::<f64>::zeros(other.f64.ncols());
        for &row in from_rows {
            sum_i += &other.int.row(row);
            sum_f += &other.f64.row(row);
        }
        self.int.row_mut(to_row).scaled_add(-1, &sum_i);
        self.f64.row_mut(to_row).scaled_add(-1.0, &sum_f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_matrix() -> WeightMatrix {
        WeightMatrix::new(
            3,
            HashMap::from([
                ("pop".into(), vec![100i64, 200, 300]),
                ("votes".into(), vec![10i64, 20, 30]),
            ]),
            HashMap::from([
                ("area".into(), vec![1.5f64, 2.5, 3.5]),
            ]),
        )
    }

    #[test]
    fn test_series_and_contains() {
        let m = make_matrix();
        assert!(m.contains("pop"));
        assert!(m.contains("votes"));
        assert!(m.contains("area"));
        assert!(!m.contains("missing"));
        assert_eq!(m.series().len(), 3);
    }

    #[test]
    fn test_get_as_f64() {
        let m = make_matrix();
        assert_eq!(m.get_as_f64("pop", 0), Some(100.0));
        assert_eq!(m.get_as_f64("pop", 2), Some(300.0));
        assert_eq!(m.get_as_f64("area", 1), Some(2.5));
        assert_eq!(m.get_as_f64("missing", 0), None);
    }

    #[test]
    fn test_copy_of_size() {
        let m = make_matrix();
        let copy = m.copy_of_size(5);
        assert_eq!(copy.series().len(), 3);
        assert!(copy.contains("pop"));
        assert_eq!(copy.get_as_f64("pop", 0), Some(0.0));
        assert_eq!(copy.get_as_f64("area", 4), Some(0.0));
    }

    #[test]
    fn test_clear_all_rows() {
        let mut m = make_matrix();
        m.clear_all_rows();
        for i in 0..3 {
            assert_eq!(m.get_as_f64("pop", i), Some(0.0));
            assert_eq!(m.get_as_f64("area", i), Some(0.0));
        }
    }

    #[test]
    fn test_clear_row() {
        let mut m = make_matrix();
        m.clear_row(1);
        assert_eq!(m.get_as_f64("pop", 0), Some(100.0));
        assert_eq!(m.get_as_f64("pop", 1), Some(0.0));
        assert_eq!(m.get_as_f64("area", 1), Some(0.0));
        assert_eq!(m.get_as_f64("pop", 2), Some(300.0));
    }

    #[test]
    fn test_add_row() {
        let mut m = make_matrix();
        m.add_row(0, 1); // row 0 += row 1
        assert_eq!(m.get_as_f64("pop", 0), Some(300.0));  // 100+200
        assert_eq!(m.get_as_f64("area", 0), Some(4.0));   // 1.5+2.5
        assert_eq!(m.get_as_f64("pop", 1), Some(200.0));  // unchanged
    }

    #[test]
    fn test_set_row_to_sum_of() {
        let src = make_matrix();
        let mut dst = src.copy_of_size(1); // same column layout required for raw row ops
        dst.set_row_to_sum_of(0, &src);
        assert_eq!(dst.get_as_f64("pop", 0), Some(600.0));   // 100+200+300
        assert_eq!(dst.get_as_f64("votes", 0), Some(60.0));  // 10+20+30
        assert_eq!(dst.get_as_f64("area", 0), Some(7.5));    // 1.5+2.5+3.5
    }

    #[test]
    fn test_add_row_from() {
        let src = make_matrix();
        let mut dst = src.clone(); // same column layout required for raw row ops
        dst.add_row_from(0, &src, 2); // dst[0] += src[2]
        assert_eq!(dst.get_as_f64("pop", 0), Some(400.0));  // 100+300
        assert_eq!(dst.get_as_f64("area", 0), Some(5.0));   // 1.5+3.5
    }

    #[test]
    fn test_subtract_row_from() {
        let src = make_matrix();
        let mut dst = src.clone();
        dst.subtract_row_from(2, &src, 1); // dst[2] -= src[1]
        assert_eq!(dst.get_as_f64("pop", 2), Some(100.0));  // 300-200
        assert_eq!(dst.get_as_f64("area", 2), Some(1.0));   // 3.5-2.5
    }

    #[test]
    fn test_add_rows_from() {
        let src = make_matrix();
        let mut dst = src.clone();
        dst.add_rows_from(0, &src, &[1, 2]); // dst[0] += src[1] + src[2]
        assert_eq!(dst.get_as_f64("pop", 0), Some(600.0));  // 100+200+300
        assert_eq!(dst.get_as_f64("area", 0), Some(7.5));   // 1.5+2.5+3.5
    }

    #[test]
    fn test_subtract_rows_from() {
        let src = make_matrix();
        let mut dst = src.clone();
        dst.subtract_rows_from(2, &src, &[0, 1]); // dst[2] -= src[0] + src[1]
        assert_eq!(dst.get_as_f64("pop", 2), Some(0.0));    // 300-100-200
        assert_eq!(dst.get_as_f64("area", 2), Some(-0.5));  // 3.5-1.5-2.5
    }

    #[test]
    fn test_empty_weights() {
        let m = WeightMatrix::new(3, HashMap::new(), HashMap::new());
        assert_eq!(m.series().len(), 0);
        assert_eq!(m.get_as_f64("anything", 0), None);
    }
}
