use crate::partition::Partition;

impl Partition {
    /// Partisan lean metric based on district-level vote shares.
    /// Ranges from -1.0 (strongly Republican) to +1.0 (strongly Democratic).
    pub(crate) fn partisan_lean(&self, part: u32, dem_series: &str, rep_series: &str) -> f64 {
        let dem_votes = self.part_total(dem_series, part);
        let rep_votes = self.part_total(rep_series, part);
        let total_votes = dem_votes + rep_votes;
        if total_votes == 0.0 { 0.0 } else { (dem_votes - rep_votes) / total_votes }
    }

    /// Competitiveness metric based on district-level vote shares (binary).
    pub(crate) fn binary_competitiveness(&self, part: u32, dem_series: &str, rep_series: &str, threshold: f64) -> f64 {
        let lean = self.partisan_lean(part, dem_series, rep_series).abs() / 2.0;
        if lean <= threshold { 1.0 } else { 0.0 }
    }

    /// Competitiveness metric based on district-level vote shares (piecewise quadratic).
    pub(crate) fn quadratic_competitiveness(&self, part: u32, dem_series: &str, rep_series: &str, threshold: f64) -> f64 {
        let lean = self.partisan_lean(part, dem_series, rep_series).abs() / 2.0;
        if lean <= threshold { 1.0 - 2.0 / threshold * lean * lean } else { 2.0 / (0.5 - threshold) * (0.5 - lean) * (0.5 - lean)}
    }

    /// Competitiveness metric based on district-level vote shares (Gaussian).
    pub(crate) fn gaussian_competitiveness(&self, part: u32, dem_series: &str, rep_series: &str, sigma: f64) -> f64 {
        let lean = self.partisan_lean(part, dem_series, rep_series).abs() / 2.0;
        (- (lean * lean) / (2.0 * sigma * sigma)).exp()
    }

    /// Seats–votes proportionality / partisan fairness metric.
    #[allow(unused_variables)]
    pub(crate) fn proportionality(&self, dem_series: &str, rep_series: &str) -> Vec<f64> { todo!() }

    /// Partisan bias metric.
    #[allow(unused_variables)]
    pub(crate) fn partisan_bias(&self, dem_series: &str, rep_series: &str) -> Vec<f64> { todo!(); }
}
