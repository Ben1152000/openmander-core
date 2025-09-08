use crate::{GeoId, GeoType, Map};

impl Map {
    /// Patch adjacency list with manual bridges for island/remote blocks.
    pub fn patch_adjacencies(&mut self) {
        let patches = [
            // Washington County, Rhode Island
            (GeoId::new_block("440099902000001"), GeoId::new_block("440099901000017")),
            // Monroe County, Florida
            (GeoId::new_block("120879801001000"), GeoId::new_block("120879900000030")),
            // San Francisco County, California
            (GeoId::new_block("060759804011000"), GeoId::new_block("060759901000001")),
            // Ventura County, California
            (GeoId::new_block("061119901000013"), GeoId::new_block("061119901000001")),
            (GeoId::new_block("061119901000013"), GeoId::new_block("061119901000008")),
            (GeoId::new_block("061119901000013"), GeoId::new_block("061119901000011")),
            (GeoId::new_block("061119901000013"), GeoId::new_block("060839900000034")),
            // Los Angeles County, California
            (GeoId::new_block("060375991002000"), GeoId::new_block("060379903000006")),
            (GeoId::new_block("060375991002000"), GeoId::new_block("060379903000007")),
            (GeoId::new_block("060375991002000"), GeoId::new_block("060379903000010")),
            (GeoId::new_block("060375991002000"), GeoId::new_block("060375991001000")),
        ];

        patches.into_iter().for_each(|(left, right)| {
            let blocks = self.get_layer_mut(GeoType::Block);
            if let (Some(&left), Some(&right)) = (blocks.index.get(&left), blocks.index.get(&right)) {
                if !blocks.adjacencies[left as usize].contains(&right) {
                    blocks.adjacencies[left as usize].push(right);
                    println!("adding adjacency: {:?} <-> {:?}", blocks.geo_ids[left as usize], blocks.geo_ids[right as usize]);
                }
                if !blocks.adjacencies[right as usize].contains(&left) {
                    blocks.adjacencies[right as usize].push(left);
                }
            }
        });
    }
}
