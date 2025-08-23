use geo::{BooleanOps, GeodesicArea, MultiPolygon};

use crate::PlanarPartition;

impl PlanarPartition {
    /// For each polygon and its adjacency list, compute the shared perimeter with each neighbor.
    pub fn compute_shared_perimeters(&mut self) {
        /// Length of shared boundary between two (mutually adjacent) multipolygons.
        fn shared_perimeter(a: &MultiPolygon<f64>, b: &MultiPolygon<f64>) -> f64 {
            let perimeter = (a.geodesic_perimeter() + b.geodesic_perimeter() - a.union(b).geodesic_perimeter()) / 2.0;
            if perimeter > 1e-9 { perimeter } else { 0.0 }
        }

        self.shared_perimeters = vec![Vec::new(); self.len()];
        for (i, neighbors) in self.adjacencies.iter().enumerate() {
            self.shared_perimeters[i] = neighbors.iter().map(|&j | {
                shared_perimeter(&self.shapes[i], &self.shapes[j as usize])
            }).collect();
        }
    }
}
