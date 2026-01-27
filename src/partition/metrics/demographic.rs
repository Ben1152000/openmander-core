use crate::partition::Partition;

impl Partition {
    /// Compute the population deviation for a given partition.
    pub(crate) fn population_deviation(&self, part: u32, pop_series: &str) -> f64 {
        let average = self.region_total(pop_series) / (self.num_parts() - 1) as f64;
        self.part_total(pop_series, part) / average - 1.0
    }

    /// Compute the absolute population deviation for a given partition.
    pub(crate) fn absolute_population_deviation(&self, part: u32, pop_series: &str) -> f64 {
        let average = self.region_total(pop_series) / (self.num_parts() - 1) as f64;
        (self.part_total(pop_series, part) / average - 1.0).abs()
    }

    /// Compute a smooth population deviation metric for a given partition.
    pub(crate) fn smooth_population_deviation(&self, part: u32, pop_series: &str) -> f64 {
        let deviation = self.population_deviation(part, pop_series);
        let limit = if deviation <= 0.0 { 1 } else { self.num_parts() - 2 };
        (1.0 - deviation.powi(2) / limit.pow(2) as f64) / (1.0 + deviation.powi(2))
    }

    /// Compute a sharp (linear) population deviation metric for a given partition.
    /// Scores on a linear scale:
    /// - Score 0 for an empty district (pop = 0)
    /// - Score 1 for a district at target population (pop = target)
    /// - Score 0 for anything above double the target (pop >= 2 * target)
    /// - Linear interpolation between these points
    pub(crate) fn sharp_population_deviation(&self, part: u32, pop_series: &str) -> f64 {
        (1.0 - self.absolute_population_deviation(part, pop_series)).min(1.0)
    }

    /// Compute the minority opportunity metric for a given partition.
    pub(crate) fn minority_opportunity(&self, part: u32, pop_series: &str, min_series: &str) -> f64 {
        let total = self.part_total(pop_series, part);
        if total == 0.0 { 0.0 } else { self.part_total(min_series, part) / total }
    }
}
