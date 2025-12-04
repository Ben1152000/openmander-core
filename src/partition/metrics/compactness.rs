use std::f64::{consts::PI, INFINITY};

use crate::partition::Partition;

impl Partition {
    /// Get the area of a part in square meters.
    #[inline]
    fn area(&self, part: u32) -> f64 {
        self.part_weights().get_as_f64("area_m2", part as usize).unwrap()
    }

    /// Get the perimeter of a part in meters.
    #[inline]
    fn perimeter(&self, part: u32) -> f64 {
        self.frontiers.get(part as usize).iter()
            .flat_map(|&u| self.graph().edges_with_weights(u))
            .filter_map(|(v, w)| (self.assignment(v) != part).then_some(w))
            .sum()
    }

    /// Compute the Polsby-Popper compactness score for a part.
    /// Formula: 4 * pi * area / (perimeter^2)
    /// If the perimeter is zero, returns infinity.
    #[inline]
    pub(crate) fn polsby_pobber(&self, part: u32) -> f64 {
        let area = self.area(part);
        let perimeter = self.perimeter(part);
        if perimeter == 0.0 { return INFINITY }
        4.0 * PI * area / (perimeter * perimeter)
    }

    /// Compute the Schwartzberg compactness score for a part.
    /// Formula: 2 * pi * sqrt(area / pi) / perimeter
    /// If the perimeter is zero, returns infinity.
    #[inline]
    pub(crate) fn schwartzberg(&self, part: u32) -> f64 {
        let area = self.area(part);
        let perimeter = self.perimeter(part);
        if perimeter == 0.0 { return INFINITY }
        2.0 * PI * (area / PI).sqrt() / perimeter
    }

    /// Compute the Reock compactness score for a part (0 to 1).
    /// Formula: area(part) / area(minimum_bounding_circle(part))
    /// If the minimum bounding circle area is zero, returns infinity.
    #[allow(dead_code, unused_variables)]
    pub(crate) fn reock(&self, part: u32) -> f64 { todo!() }

    /// Get the moment of a part (defined as the sum of the square weighted
    /// distance from the population center).
    #[allow(dead_code, unused_variables)]
    fn moment(&self, part: u32) -> f64 { todo!() }

    /// Compute the convex hull compactness score for a part (0 to 1).
    /// Formula: area(part) / area(convex_hull(part))
    /// If the convex hull area is zero, returns infinity.
    #[allow(dead_code, unused_variables)]
    pub(crate) fn convex_hull(&self, part: u32) -> f64 { todo!() }
}
