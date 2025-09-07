use std::sync::Arc;

use crate::{GeoId, GeoType, Map};

impl Map {
    /// Patch adjacency list with manual bridges for island/remote blocks.
    pub fn patch_adjacencies(&mut self) {
        macro_rules! block_id {($s:expr) => {GeoId { ty: GeoType::Block, id: Arc::from($s) }}}
        let patches = [
            // Washington County, Rhode Island
            (block_id!("440099902000001"), block_id!("440099901000017")),
            // Monroe County, Florida
            (block_id!("120879801001000"), block_id!("120879900000030")),
            // San Francisco County, California
            (block_id!("060759804011000"), block_id!("060759901000001")),
            // Ventura County, California
            (block_id!("061119901000013"), block_id!("061119901000001")),
            (block_id!("061119901000013"), block_id!("061119901000008")),
            (block_id!("061119901000013"), block_id!("061119901000011")),
            (block_id!("061119901000013"), block_id!("060839900000034")),
            // Los Angeles County, California
            (block_id!("060375991002000"), block_id!("060379903000006")),
            (block_id!("060375991002000"), block_id!("060379903000007")),
            (block_id!("060375991002000"), block_id!("060379903000010")),
            (block_id!("060375991002000"), block_id!("060375991001000")),
        ];

        patches.into_iter().for_each(|(left, right)| {
            if let (Some(&left), Some(&right)) = (self.blocks.index.get(&left), self.blocks.index.get(&right)) {
                if !self.blocks.adjacencies[left as usize].contains(&right) {
                    self.blocks.adjacencies[left as usize].push(right);
                    println!("adding adjacency: {:?} <-> {:?}", self.blocks.geo_ids[left as usize], self.blocks.geo_ids[right as usize]);
                }
                if !self.blocks.adjacencies[right as usize].contains(&left) {
                    self.blocks.adjacencies[right as usize].push(left);
                }
            }
        });
    }
}
