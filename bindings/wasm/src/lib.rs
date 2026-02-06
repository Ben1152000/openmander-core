use wasm_bindgen::prelude::*;

mod common;
mod map;
mod plan;

pub use map::WasmMap;
pub use plan::WasmPlan;

/// Called automatically when the WASM module is instantiated.
/// Sets up panic hook so Rust panics appear as console.error in the browser.
#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}
