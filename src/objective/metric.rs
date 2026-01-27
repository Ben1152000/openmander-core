use crate::partition::Partition;

#[derive(Clone, Debug)]
pub(crate) enum MetricKind {
    // Demographic metrics:
    PopulationDeviation { pop_series: String },
    PopulationDeviationAbsolute { pop_series: String },
    PopulationDeviationSmooth { pop_series: String },
    PopulationDeviationSharp { pop_series: String },

    // Geometric metrics:
    CompactnessPolsbyPopper,
    CompactnessSchwartzberg,

    // Electoral metrics:
    CompetitivenessBinary { dem_series: String, rep_series: String, threshold: f64 },
    CompetitivenessQuadratic { dem_series: String, rep_series: String, threshold: f64 },
    CompetitivenessGaussian { dem_series: String, rep_series: String, sigma: f64 },
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
    pub fn population_deviation(pop_series: String) -> Self {
        Self { kind: MetricKind::PopulationDeviation { pop_series } }
    }

    /// Absolute population equality metric for the given weight series.
    pub fn population_deviation_absolute(pop_series: String) -> Self {
        Self { kind: MetricKind::PopulationDeviationAbsolute { pop_series } }
    }

    /// Smooth population equality metric for the given weight series.
    pub fn population_deviation_smooth(pop_series: String) -> Self {
        Self { kind: MetricKind::PopulationDeviationSmooth { pop_series } }
    }

    /// Sharp (linear) population equality metric for the given weight series.
    /// Scores on a linear scale: 0 for empty district, 1 for target population,
    /// 0 for double target or above.
    pub fn population_deviation_sharp(pop_series: String) -> Self {
        Self { kind: MetricKind::PopulationDeviationSharp { pop_series } }
    }

    /// Polsby–Popper compactness metric.
    pub fn compactness_polsby_popper() -> Self {
        Self { kind: MetricKind::CompactnessPolsbyPopper }
    }

    /// Schwartzberg compactness metric.
    pub fn compactness_schwartzberg() -> Self {
        Self { kind: MetricKind::CompactnessSchwartzberg }
    }

    /// Competitiveness metric based on district-level vote shares (binary).
    pub fn competitiveness_binary(dem_series: String, rep_series: String, threshold: f64) -> Self {
        Self { kind: MetricKind::CompetitivenessBinary { dem_series, rep_series, threshold } }
    }

    /// Competitiveness metric based on district-level vote shares (piecewise quadratic).
    pub fn competitiveness_quadratic(dem_series: String, rep_series: String, threshold: f64) -> Self {
        Self { kind: MetricKind::CompetitivenessQuadratic { dem_series, rep_series, threshold } }
    }

    /// Competitiveness metric based on district-level vote shares (Gaussian).
    pub fn competitiveness_gaussian(dem_series: String, rep_series: String, sigma: f64) -> Self {
        Self { kind: MetricKind::CompetitivenessGaussian { dem_series, rep_series, sigma } }
    }

    /// Seats–votes proportionality / partisan fairness metric.
    pub fn proportionality(dem_series: String, rep_series: String) -> Self {
        Self { kind: MetricKind::Proportionality { dem_series, rep_series } }
    }

    /// Get a short name for this metric (for display purposes).
    pub(crate) fn short_name(&self) -> &str {
        match &self.kind {
            MetricKind::PopulationDeviation { .. } => "PopulationDeviation",
            MetricKind::PopulationDeviationAbsolute { .. } => "PopulationDeviationAbsolute",
            MetricKind::PopulationDeviationSmooth { .. } => "PopulationDeviationSmooth",
            MetricKind::PopulationDeviationSharp { .. } => "PopulationDeviationSharp",
            MetricKind::CompactnessPolsbyPopper => "CompactnessPolsbyPopper",
            MetricKind::CompactnessSchwartzberg => "CompactnessSchwartzberg",
            MetricKind::CompetitivenessBinary { .. } => "CompetitivenessBinary",
            MetricKind::CompetitivenessQuadratic { .. } => "CompetitivenessQuadratic",
            MetricKind::CompetitivenessGaussian { .. } => "CompetitivenessGaussian",
            MetricKind::Proportionality { .. } => "Proportionality",
        }
    }

    /// Evaluate this metric for a given partition, returning per-district scores.
    pub(crate) fn compute(&self, partition: &Partition) -> Vec<f64> {
        let districts = 1..partition.num_parts();
        match &self.kind {
            MetricKind::PopulationDeviation { pop_series } => {
                districts.map(|part| partition.population_deviation(part, pop_series)).collect()
            }
            MetricKind::PopulationDeviationAbsolute { pop_series } => {
                districts.map(|part| partition.absolute_population_deviation(part, pop_series)).collect()
            }
            MetricKind::PopulationDeviationSmooth { pop_series } => {
                districts.map(|part| partition.smooth_population_deviation(part, pop_series)).collect()
            }
            MetricKind::PopulationDeviationSharp { pop_series } => {
                districts.map(|part| partition.sharp_population_deviation(part, pop_series)).collect()
            }
            MetricKind::CompactnessPolsbyPopper => {
                districts.map(|part| partition.polsby_pobber(part)).collect()
            }
            MetricKind::CompactnessSchwartzberg => {
                districts.map(|part| partition.schwartzberg(part)).collect()
            }
            MetricKind::CompetitivenessBinary { dem_series, rep_series, threshold } => {
                districts.map(|part| partition.binary_competitiveness(part, dem_series, rep_series, *threshold)).collect()
            }
            MetricKind::CompetitivenessQuadratic { dem_series, rep_series, threshold } => {
                districts.map(|part| partition.quadratic_competitiveness(part, dem_series, rep_series, *threshold)).collect()
            }
            MetricKind::CompetitivenessGaussian { dem_series, rep_series, sigma } => {
                districts.map(|part| partition.gaussian_competitiveness(part, dem_series, rep_series, *sigma)).collect()
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
        match &self {
            MetricKind::PopulationDeviation { pop_series } =>
                write!(f, "PopulationDeviation(series='{}')", pop_series),
            MetricKind::PopulationDeviationAbsolute { pop_series } =>
                write!(f, "PopulationDeviationAbsolute(series='{}')", pop_series),
            MetricKind::PopulationDeviationSmooth { pop_series } =>
                write!(f, "PopulationDeviationSmooth(series='{}')", pop_series),
            MetricKind::PopulationDeviationSharp { pop_series } =>
                write!(f, "PopulationDeviationSharp(series='{}')", pop_series),
            MetricKind::CompactnessPolsbyPopper =>
                write!(f, "CompactnessPolsbyPopper"),
            MetricKind::CompactnessSchwartzberg =>
                write!(f, "CompactnessSchwartzberg"),
            MetricKind::CompetitivenessBinary { dem_series, rep_series, threshold } =>
                write!(f, "CompetitivenessBinary(dem_series='{}', rep_series='{}', threshold={})",
                    dem_series, rep_series, threshold),
            MetricKind::CompetitivenessQuadratic { dem_series, rep_series, threshold } =>
                write!(f, "CompetitivenessQuadratic(dem_series='{}', rep_series='{}', threshold={})",
                    dem_series, rep_series, threshold),
            MetricKind::CompetitivenessGaussian { dem_series, rep_series, sigma } =>
                write!(f, "CompetitivenessGaussian(dem_series='{}', rep_series='{}', sigma={})",
                    dem_series, rep_series, sigma),
            MetricKind::Proportionality { dem_series, rep_series } =>
                write!(f, "Proportionality(dem_series='{}', rep_series='{}')",
                    dem_series, rep_series),
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
