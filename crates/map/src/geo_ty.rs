
/// Geographic entity types, ordered from largest to smallest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GeoType {
    State,  // ex: Illinois
    County, // ex: Champaign County
    Tract,  // ex: Census Tract 111
    Group,  // ex: Block Group 2
    VTD,    // ex: Cunningham 2
    Block,  // ex: Block 2007
}

impl GeoType {
    pub const COUNT: usize = 6;

    pub const ALL: [GeoType; Self::COUNT] = [
        GeoType::State,
        GeoType::County,
        GeoType::Tract,
        GeoType::Group,
        GeoType::VTD,
        GeoType::Block,
    ];

    /// The top (largest) and bottom (smallest) GeoTypes.
    pub const TOP: GeoType = Self::ALL[0];
    pub const BOTTOM: GeoType = Self::ALL[Self::COUNT - 1];

    /// Get the string representation of the GeoType.
    #[inline] pub fn to_str(&self) -> &'static str {
        match self {
            GeoType::State  => "state",
            GeoType::County => "county",
            GeoType::Tract  => "tract",
            GeoType::Group  => "group",
            GeoType::VTD    => "vtd",
            GeoType::Block  => "block",
        }
    }

    /// Get the expected length of the GEOID string for this GeoType.
    #[inline] pub fn id_len(&self) -> usize {
        match self {
            GeoType::State  => 2,
            GeoType::County => 5,
            GeoType::Tract  => 11,
            GeoType::Group  => 12,
            GeoType::VTD    => 11,
            GeoType::Block  => 15,
        }
    }
}
