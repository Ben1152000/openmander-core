use std::path::Path;

use anyhow::Result;

use crate::{common, map::Map, pack::{DiskPack, PackSource, PackFormat, PackFormats, Manifest}};

impl Map {
    /// Old API: read a map from a pack directory at `path`.
    pub fn read_from_pack(path: &Path) -> Result<Self> {
        common::require_dir_exists(path)?;
        let src = DiskPack::new(path);
        
        // Try to read format from manifest first
        if src.has("manifest.json") {
            match Manifest::from_pack_source(&src) {
                Ok(manifest) => {
                    let manifest_formats = manifest.formats();
                    // Check if formats are using defaults (old manifest without formats field)
                    // If formats match defaults, detect from file extensions instead
                    let default_formats = PackFormats::default();
                    let is_using_defaults = manifest_formats.data == default_formats.data
                        && manifest_formats.geometry == default_formats.geometry
                        && manifest_formats.hull == default_formats.hull
                        && manifest_formats.adjacency == default_formats.adjacency;
                    
                    let formats = if is_using_defaults {
                        // Manifest has default formats (likely old manifest without formats field)
                        // Detect formats from actual file extensions
                        crate::io::pack::detect_formats_from_files(&src)
                    } else {
                        // Manifest has explicit formats, use them
                        let mut formats = manifest_formats.clone();
                        // Still detect hull format for backward compatibility (in case it's missing)
                        if let Some(hull_format) = crate::io::pack::detect_hull_format(&src) {
                            formats.hull = hull_format;
                        }
                        formats
                    };
                    
                    return crate::io::pack::read_map_from_pack_source_with_formats(&src, &formats);
                }
                Err(_) => {
                    // If manifest parsing fails, fall back to detection
                }
            }
        }
        
        // Fall back to format detection for backward compatibility (no manifest or manifest parse failed)
        let formats = crate::io::pack::detect_formats_from_files(&src);
        crate::io::pack::read_map_from_pack_source_with_formats(&src, &formats)
    }

    /// Detect pack format by checking for parquet or pmtiles files
    pub fn detect_pack_format(src: &dyn PackSource) -> Result<PackFormat> {
        crate::io::pack::detect_pack_format(src)
    }

    /// Legacy API: read map from any PackSource using a single PackFormat (for backward compatibility)
    pub fn read_from_pack_source(src: &dyn PackSource, format: PackFormat) -> Result<Self> {
        let formats = PackFormats::from_pack_format(format);
        crate::io::pack::read_map_from_pack_source_with_formats(src, &formats)
    }
}
