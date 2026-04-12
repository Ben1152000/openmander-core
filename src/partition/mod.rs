mod algorithm;
mod contiguity;
mod metrics;
mod ops;
#[allow(clippy::module_inception)]
mod partition;
mod structures;

pub(crate) use partition::Partition;
use structures::*;
