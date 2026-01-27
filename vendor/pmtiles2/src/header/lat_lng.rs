use deku::{
    bitvec::{BitSlice, BitVec, Msb0},
    prelude::*,
};

#[derive(DekuRead, DekuWrite, Debug, PartialEq)]
#[deku(endian = "endian", ctx = "_endian: deku::ctx::Endian")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LatLng {
    #[deku(
        reader = "Self::read_lat_lon(deku::rest)",
        writer = "Self::write_lat_lon(deku::output, self.longitude)"
    )]
    pub longitude: f64,

    #[deku(
        reader = "Self::read_lat_lon(deku::rest)",
        writer = "Self::write_lat_lon(deku::output, self.latitude)"
    )]
    pub latitude: f64,
}

const LAT_LONG_FACTOR: f64 = 10_000_000.0;

impl LatLng {
    fn read_lat_lon(rest: &BitSlice<u8, Msb0>) -> Result<(&BitSlice<u8, Msb0>, f64), DekuError> {
        let (rest, value) = i32::read(rest, ())?;
        Ok((rest, f64::from(value) / LAT_LONG_FACTOR))
    }

    #[allow(clippy::cast_possible_truncation)]
    fn write_lat_lon(output: &mut BitVec<u8, Msb0>, field: f64) -> Result<(), DekuError> {
        let value = (field * LAT_LONG_FACTOR) as i32;
        value.write(output, ())
    }
}

// Test module removed - test files are not included in this fork
