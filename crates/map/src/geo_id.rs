use crate::GeoType;

/// Stable key for any entity across levels.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeoId {
    ty: GeoType,  // ex: county, block
    id: Box<str>, // ex: "17019" for county, "170190111002007" for block
}

impl GeoId {
    pub fn new(ty: GeoType, id: &str) -> Self {
        assert_eq!(id.len(), ty.id_len(), "GEOID length does not match GeoType");
        GeoId { ty, id: id.into() }
    }

    /// Get the geographic type of this GeoId.
    #[inline] pub fn ty(&self) -> GeoType { self.ty }

    /// Get the string identifier of this GeoId.
    #[inline] pub fn id(&self) -> &str { &self.id }

    /// Syntactic sugar for creating a new GeoId of type Block.
    #[inline] pub(crate) fn new_block(id: &str) -> Self { Self::new(GeoType::Block, id) }

    /// Returns a new `GeoId` corresponding to the higher-level `GeoType`
    /// by truncating this GeoId's string to the correct prefix length.
    #[inline] pub(crate) fn to_parent(&self, parent_ty: GeoType) -> GeoId {
        let n = self.id.len().min(parent_ty.id_len());
        GeoId { ty: parent_ty, id: self.id[..n].into() }
    }
}
