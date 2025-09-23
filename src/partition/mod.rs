#![allow(dead_code)]
mod algorithm;
mod contiguity;
mod frontier;
mod partition;

pub(crate) use partition::Partition;
use frontier::FrontierSet;
