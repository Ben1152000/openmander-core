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

    /// Evaluate this metric for a given partition.
    pub(crate) fn compute(&self, partition: &Partition) -> Vec<f64> {
        match &self.kind {
            MetricKind::PopulationDeviation { series } => {
                let total = partition.region_total(series);
                let average = total / (partition.num_parts() as f64 - 1.0);

                (1..partition.num_parts())
                    .map(|part| (partition.part_total(series, part) - average) / average)
                    .collect()
            }
            MetricKind::Compactness => {
                (1..partition.num_parts())
                    .map(|part| partition.polsby_pobber(part))
                    .collect()
            }
            MetricKind::Competitiveness { dem_series, rep_series, threshold } => {
                (1..partition.num_parts())
                    .map(|part| {
                        let dem = partition.part_total(dem_series, part);
                        let rep = partition.part_total(rep_series, part);
                        let total = dem + rep;
                        
                        if total == 0.0 {
                            return 0.0;
                        }
                        
                        let x = dem / total;
                        let t = *threshold;
                        
                        // Piecewise quadratic formula for competitiveness
                        if x <= 0.5 - t {
                            // Left tail: a * x^2
                            let a = 4.0 / (1.0 - 2.0 * t);
                            a * x * x
                        } else if x >= 0.5 + t {
                            // Right tail: a * (1 - x)^2
                            let a = 4.0 / (1.0 - 2.0 * t);
                            let diff = 1.0 - x;
                            a * diff * diff
                        } else {
                            // Middle competitive range: 1 - b * (0.5 - x)^2
                            let b = 2.0 / t;
                            let diff = 0.5 - x;
                            1.0 - b * diff * diff
                        }
                    })
                    .collect()
            }
            MetricKind::Proportionality { dem_series, rep_series } =>
                todo!(),
        }
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
