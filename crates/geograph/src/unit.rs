use std::fmt;

/// Identifies a single unit (census block, precinct, etc.) within a `Region`.
///
/// Units are assigned contiguous indices starting from `0`.  The reserved
/// sentinel `UnitId::EXTERIOR` represents the exterior of the region — it owns
/// the unbounded DCEL face and any interior gaps — and is never a valid
/// district assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UnitId(pub u32);

impl UnitId {
    /// Sentinel for the exterior of the region (unbounded face + interior gaps).
    /// Never a valid district assignment.
    pub const EXTERIOR: Self = Self(u32::MAX);
}

impl fmt::Display for UnitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::EXTERIOR {
            write!(f, "UnitId(EXTERIOR)")
        } else {
            write!(f, "UnitId({})", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exterior_sentinel_is_u32_max() {
        assert_eq!(UnitId::EXTERIOR.0, u32::MAX);
    }

    #[test]
    fn exterior_is_not_a_normal_unit() {
        assert_ne!(UnitId(0), UnitId::EXTERIOR);
        assert_ne!(UnitId(1000), UnitId::EXTERIOR);
    }

    #[test]
    fn display_normal() {
        assert_eq!(UnitId(42).to_string(), "UnitId(42)");
    }

    #[test]
    fn display_exterior() {
        assert_eq!(UnitId::EXTERIOR.to_string(), "UnitId(EXTERIOR)");
    }

    #[test]
    fn ordering() {
        assert!(UnitId(0) < UnitId(1));
        assert!(UnitId(1) < UnitId::EXTERIOR);
    }
}
