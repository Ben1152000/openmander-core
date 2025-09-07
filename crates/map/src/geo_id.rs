use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GeoType {
    State,      // Highest-level entity
    County,     // County -> State
    Tract,      // Tract -> County
    Group,      // Group -> Tract
    VTD,        // VTD -> County
    Block,      // Lowest-level entity
}

impl GeoType {
    pub fn to_str(&self) -> &'static str {
        match self {
            GeoType::State => "state",
            GeoType::County => "county",
            GeoType::Tract => "tract",
            GeoType::Group => "group",
            GeoType::VTD => "vtd",
            GeoType::Block => "block",
        }
    }

    pub fn order() -> [GeoType; 6] {
        [
            GeoType::State,
            GeoType::County,
            GeoType::Tract,
            GeoType::Group,
            GeoType::VTD,
            GeoType::Block,
        ]
    }
}

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
        let id_len = match parent_ty {
            GeoType::State  => 2,
            GeoType::County => 5,
            GeoType::Tract  => 11,
            GeoType::Group  => 12,
            GeoType::VTD    => 11,
            GeoType::Block  => 15,
        };

        // If the id is shorter than expected, just take the full id.
        let prefix: Arc<str> = Arc::from(&self.id[..self.id.len().min(id_len)]);

        GeoId {
            ty: parent_ty,
            id: prefix,
        }
    }
}
