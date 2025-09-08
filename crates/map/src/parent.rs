use crate::{GeoId, GeoType};

/// Quick way to access parent entities across levels.
#[derive(Debug, Clone, Default)]
pub struct ParentRefs {
    pub refs: [Option<GeoId>; GeoType::COUNT - 1],
}

impl ParentRefs {
    /// Create ParentRefs from an array of optional GeoIds.
    pub fn new(refs: [Option<GeoId>; GeoType::COUNT - 1]) -> Self {
        Self { refs }
    }

    /// Create an empty ParentRefs with all None.
    pub fn empty() -> Self {
        Self { refs: [(); GeoType::COUNT - 1].map(|_| None) }
    }

    /// Get a reference to a specific parent GeoId by geographic type.
    /// Returns None if `ty` is the bottom (smallest) GeoType.
    pub fn get(&self, ty: GeoType) -> Option<&GeoId> {
        match ty {
            GeoType::BOTTOM => None,
            ty => self.refs[ty as usize].as_ref(),
        }
    }

    /// Set a reference to a specific parent GeoId by geographic type.
    /// No-op if `ty` is the bottom (smallest) GeoType.
    pub fn set(&mut self, ty: GeoType, value: Option<GeoId>) {
        match ty {
            GeoType::BOTTOM => (),
            ty => self.refs[ty as usize] = value,
        }
    }
}
