#![doc = "OpenMander public API"]

// On WASM with the `wasm-console` feature, shadow std's println!/eprintln!
// so all existing print statements route to the browser console automatically.
#[cfg(all(target_arch = "wasm32", feature = "wasm-console"))]
#[macro_use]
mod wasm_console {
    macro_rules! println {
        () => { ::web_sys::console::log_1(&"".into()) };
        ($($arg:tt)*) => { ::web_sys::console::log_1(&format!($($arg)*).into()) };
    }
    macro_rules! eprintln {
        () => { ::web_sys::console::error_1(&"".into()) };
        ($($arg:tt)*) => { ::web_sys::console::error_1(&format!($($arg)*).into()) };
    }
}

mod common;
mod geom;
mod graph;
mod io;
mod map;
mod objective;
mod pack;
mod partition;
mod plan;

#[doc(inline)]
pub use map::{GeoId, GeoType, Map, MapLayer, ParentRefs};

#[doc(inline)]
pub use plan::{Plan};

#[doc(inline)]
pub use objective::{Metric, Objective};

#[doc(inline)]
#[cfg(feature = "download")]
pub use pack::{build_pack, download_pack};

#[doc(inline)]
pub use pack::{PackSource, PackSink, DiskPack, MemPack, PackFormat, validate_pack};
