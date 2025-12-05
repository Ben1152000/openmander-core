use crate::partition::Partition;

#[derive(Clone, Debug)]
pub(crate) enum MetricKind {
    /// Population equality for a given node-weight series
    /// (e.g., total population).
    PopulationDeviation { series: String },

    /// Compactness based on Polsby–Popper, computed per district and
    /// aggregated in some way (e.g., average, minimum, etc.).
    Compactness,

    /// District-level competitiveness (e.g., share of districts within
    /// some vote margin).
    Competitiveness { dem_series: String, rep_series: String, threshold: f64 },

    /// Seats–votes proportionality / partisan fairness metric.
    Proportionality { dem_series: String, rep_series: String },
}

/// A single metric specification used in a multi-objective optimization.
/// This does *not* carry a weight; weights live in `Objective`.
#[derive(Clone)]
pub struct Metric {
    kind: MetricKind,
}

impl Metric {
    /// Population equality metric for the given weight series.
    pub fn population_deviation(series: String) -> Self {
        Self { kind: MetricKind::PopulationDeviation { series } }
    }

    /// Polsby–Popper compactness metric.
    pub fn compactness_polsby_popper() -> Self {
        Self { kind: MetricKind::Compactness }
    }

    /// Competitiveness metric based on district-level vote shares.
    pub fn competitiveness(dem_series: String, rep_series: String, threshold: f64) -> Self {
        Self { kind: MetricKind::Competitiveness { dem_series, rep_series, threshold } }
    }

    /// Seats–votes proportionality / partisan fairness metric.
    pub fn proportionality(dem_series: String, rep_series: String) -> Self {
        Self { kind: MetricKind::Proportionality { dem_series, rep_series } }
    }

    /// Get a short name for this metric (for display purposes).
    pub(crate) fn short_name(&self) -> &str {
        match &self.kind {
            MetricKind::PopulationDeviation { .. } => "PopulationEquality",
            MetricKind::Compactness => "CompactnessPolsbyPopper",
            MetricKind::Competitiveness { .. } => "Competitiveness",
            MetricKind::Proportionality { .. } => "Proportionality",
        }
    }

    /// Evaluate this metric for a given partition, returning per-district scores.
    pub(crate) fn compute(&self, partition: &Partition) -> Vec<f64> {
        match &self.kind {
            MetricKind::PopulationDeviation { series } => {
                let total = partition.region_total(series);
                let average = total / (partition.num_parts() - 1) as f64;

                (1..partition.num_parts())
                    .map(|part| {
                        let deviation = partition.part_total(series, part) / average - 1.0;
                        let limit = if deviation <= 0.0 { 1 } else { partition.num_parts() - 2 };
                        (1.0 - deviation.powi(2) / limit.pow(2) as f64) / (1.0 + deviation.powi(2))
                    })
                    .collect()
            }
            MetricKind::Compactness => {
                (1..partition.num_parts())
                    .map(|part| partition.polsby_pobber(part))
                    .collect()
            }
            MetricKind::Competitiveness { dem_series, rep_series, threshold } => {
                (1..partition.num_parts())
                    .map(|part| partition.competitiveness(part, dem_series, rep_series, *threshold))
                    .collect()
            }
            MetricKind::Proportionality { dem_series, rep_series } =>
                todo!("{dem_series}, {rep_series}"),
        }
    }

    /// Compute the overall score for this metric by aggregating per-district scores.
    /// Currently uses average for all metrics, but can be customized per metric type in the future.
    pub(crate) fn compute_score(&self, partition: &Partition) -> f64 {
        let values = self.compute(partition);
        if values.is_empty() { 0.0 } else { values.iter().sum::<f64>() / values.len() as f64 }
    }
}

use std::fmt;

impl fmt::Display for MetricKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricKind::PopulationDeviation { series } =>
                write!(f, "PopulationEquality(series=\"{}\")", series),

            MetricKind::Compactness =>
                write!(f, "CompactnessPolsbyPopper"),

            MetricKind::Competitiveness { dem_series, rep_series, threshold } =>
                write!(f, "Competitiveness(dem=\"{}\", rep=\"{}\", threshold={})", dem_series, rep_series, threshold),

            MetricKind::Proportionality { dem_series, rep_series } =>
                write!(f, "Proportionality(dem=\"{}\", rep=\"{}\")", dem_series, rep_series),
        }
    }
}

/// Allow Rust-side pretty printing
impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Metric({})", self.kind)
    }
}

impl fmt::Debug for Metric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
