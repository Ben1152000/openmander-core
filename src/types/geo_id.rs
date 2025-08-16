use std::sync::Arc;

use super::{geo_type::GeoType};

/// Stable key for any entity across levels.
/// Keep the original GEOID text (with leading zeros) but avoid repeated owned Strings.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeoId {
    pub ty: GeoType,
    pub id: Arc<str>, // e.g., "31001" for county, "310010001001001" for block
}

impl GeoId {
    /// Returns a new `GeoId` corresponding to the higher-level `GeoType`
    /// by truncating this GeoId's string to the correct prefix length.
    pub fn to_parent(&self, parent_ty: GeoType) -> GeoId {
        let len = match parent_ty {
            GeoType::State  => 2,
            GeoType::County => 5,
            GeoType::Tract  => 11,
            GeoType::Group  => 12,
            GeoType::VTD    => 11,
            GeoType::Block  => 15,
        };

        // If the id is shorter than expected, just take the full id.
        let prefix: Arc<str> = Arc::from(&self.id[..self.id.len().min(len)]);

        GeoId {
            ty: parent_ty,
            id: prefix,
        }
    }
}
