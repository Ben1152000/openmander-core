# pmtiles2 Fork

This is a local fork of the `pmtiles2` crate, based on the upstream repository at:
https://github.com/arma-place/pmtiles-rs

## Purpose

This fork is used to enable PMTiles support for WebAssembly (WASM) builds. The upstream `pmtiles2` crate has an unconditional dependency on `zstd`, which in turn depends on `zstd-sys` (C code) that cannot be compiled for the `wasm32-unknown-unknown` target.

## Changes Made

1. **Conditional zstd dependency**: The `zstd` dependency is now only included for non-WASM targets using target-specific dependencies in `Cargo.toml`:
   ```toml
   [target.'cfg(not(target_arch = "wasm32"))'.dependencies]
   zstd = { version = "0.13.3", default-features = false }
   ```

2. **Removed zstd from async-compression**: The `async-compression` dependency no longer includes the `zstd` feature, preventing it from pulling in `zstd-sys` through the async-compression → compression-codecs → zstd-safe → zstd-sys chain.

3. **Conditional zstd usage in code**: All zstd-related code in `src/util/compress.rs` is gated with `#[cfg(not(target_arch = "wasm32"))]` to prevent compilation errors on WASM targets. Attempts to use zstd compression on WASM will return an error.

## Usage

This fork is used automatically by the `openmander-core` crate. For WASM builds, the `default-features = false` flag is set to disable the async feature (which we don't need since we use the sync API with `Cursor<Vec<u8>>`).

## Upstream Version

Based on `pmtiles2` version 0.3.1 from https://github.com/arma-place/pmtiles-rs

