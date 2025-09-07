// bindings/python/build.rs
fn main() {
    // Ensure `-undefined dynamic_lookup` etc. are passed on macOS, and the right cfgs are set.
    pyo3_build_config::add_extension_module_link_args();
}
