pub mod adj;
pub mod dcel;
pub mod io;
pub mod region;
pub mod snap;
pub mod unit;

pub use adj::AdjacencyMatrix;
pub use dcel::{Dcel, FaceId, HalfEdgeId, VertexId, OUTER_FACE};
pub use region::{Region, RegionError};
pub use unit::UnitId;
