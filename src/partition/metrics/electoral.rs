use std::f64::{consts::PI, INFINITY};

use crate::partition::Partition;

impl Partition {

    // competitiveness:
    pub(crate) fn competitiveness(&self, dem_series: &str, rep_series: &str, threshold: f64) -> f64 {
        let mut competitive_seats = 0;
        for part in 1..self.num_parts() {
            let dem_votes = self.part_weights.get_as_f64(dem_series, part as usize).unwrap_or(0.0);
            let rep_votes = self.part_weights.get_as_f64(rep_series, part as usize).unwrap_or(0.0);
            let total_votes = dem_votes + rep_votes;
            if total_votes == 0.0 { continue; }

            let dem_share = dem_votes / total_votes;
            let rep_share = rep_votes / total_votes;

            if (dem_share - rep_share).abs() <= threshold {
                competitive_seats += 1;
            }
        }
        competitive_seats as f64
    }
}
