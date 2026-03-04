pub(crate) mod adj;
pub(crate) mod dcel;
pub mod io;
pub mod region;
pub(crate) mod rtree;
pub(crate) mod snap;
pub(crate) mod unit;

pub use adj::AdjacencyMatrix;
pub use region::{Region, RegionError};
pub use unit::UnitId;
