use crate::partition::Partition;

impl Partition {
    /// Competitiveness metric based on district-level vote shares.
    pub(crate) fn competitiveness(&self, part: u32, dem_series: &str, rep_series: &str, threshold: f64) -> f64 {
        let dem_votes = self.part_total(dem_series, part);
        let rep_votes = self.part_total(rep_series, part);
        let total_votes = dem_votes + rep_votes;
        if total_votes == 0.0 { return 0.0 }

        // Uses a piecewise quadratic function.
        let lean = (dem_votes - rep_votes).abs() / (2.0 * total_votes);
        if lean <= threshold {
            1.0 - 2.0 / threshold * lean * lean
        } else {
            2.0 / (0.5 - threshold) * (0.5 - lean) * (0.5 - lean)
        }
    }

    /// Seats–votes proportionality / partisan fairness metric.
    #[allow(unused_variables)]
    pub(crate) fn proportionality(&self, dem_series: &str, rep_series: &str) -> Vec<f64> { todo!() }

    /// Partisan bias metric.
    #[allow(unused_variables)]
    pub(crate) fn partisan_bias(&self, dem_series: &str, rep_series: &str) -> Vec<f64> { todo!(); }
}
