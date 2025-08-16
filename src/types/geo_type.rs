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
