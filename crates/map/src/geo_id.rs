use std::sync::Arc;

use crate::GeoType;

/// Stable key for any entity across levels.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeoId {
    pub ty: GeoType,  // ex: county, block
    pub id: Arc<str>, // ex: "17019" for county, "170190111002007" for block
}

impl GeoId {
    pub fn new(ty: GeoType, id: &str) -> Self {
        assert_eq!(id.len(), ty.id_len(), "GEOID length does not match GeoType");
        GeoId { ty, id: Arc::from(id) }
    }

    /// Syntactic sugar for creating a new GeoId of type Block.
    #[inline] pub fn new_block(id: &str) -> Self { Self::new(GeoType::Block, id) }

    /// Returns a new `GeoId` corresponding to the higher-level `GeoType`
    /// by truncating this GeoId's string to the correct prefix length.
    #[inline]
    pub fn to_parent(&self, parent_ty: GeoType) -> GeoId {
        // If the id is shorter than expected, just take the full id.
        GeoId { ty: parent_ty, id: Arc::from(&self.id[..self.id.len().min(parent_ty.id_len())]) }
    }
}
