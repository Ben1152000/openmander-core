#[cfg(all(target_arch = "wasm32", feature = "wasm-console"))]
#[macro_use]
mod wasm_console {
    macro_rules! println {
        () => { ::web_sys::console::log_1(&"".into()) };
        ($($arg:tt)*) => { ::web_sys::console::log_1(&format!($($arg)*).into()) };
    }
}

pub(crate) mod adj;
pub(crate) mod dcel;
pub mod io;
pub mod region;
pub(crate) mod rtree;
pub(crate) mod unit;

pub use adj::AdjacencyMatrix;
pub use region::{Region, RegionError};
pub use unit::UnitId;
