#![allow(dead_code)]
mod algorithm;
mod contiguity;
mod multi_set;
mod partition_set;
mod partition;

pub(self) use multi_set::MultiSet;
pub(self) use partition_set::PartitionSet;
pub(crate) use partition::Partition;
