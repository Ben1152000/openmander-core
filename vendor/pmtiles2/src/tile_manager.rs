use duplicate::duplicate_item;
#[cfg(feature = "async")]
use futures::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    io::{Cursor, Error, ErrorKind, Read, Result, Seek},
};

use ahash::{AHasher, RandomState};

use crate::{Directory, Entry};

#[derive(Debug)]
enum TileManagerTile {
    Hash(u64),
    OffsetLength(u64, u32),
}

pub struct FinishResult {
    pub data: Vec<u8>,
    pub num_addressed_tiles: u64,
    pub num_tile_entries: u64,
    pub num_tile_content: u64,
    pub directory: Directory,
}

#[derive(Debug)]
pub struct TileManager<R> {
    /// hash of tile -> bytes of tile
    data_by_hash: HashMap<u64, Vec<u8>>,

    /// `tile_id` -> hash of tile
    tile_by_id: HashMap<u64, TileManagerTile>,

    /// hash of tile -> ids with this hash
    ids_by_hash: HashMap<u64, HashSet<u64>, RandomState>,

    reader: Option<R>,
}

impl<R> TileManager<R> {
    pub fn new(reader: Option<R>) -> Self {
        Self {
            data_by_hash: HashMap::default(),
            tile_by_id: HashMap::default(),
            ids_by_hash: HashMap::default(),
            reader,
        }
    }

    fn calculate_hash(value: &impl Hash) -> u64 {
        let mut hasher = AHasher::default();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Add tile to writer
    pub fn add_tile(&mut self, tile_id: u64, data: impl Into<Vec<u8>>) -> Result<()> {
        let vec: Vec<u8> = data.into();

        if vec.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "A tile must have at least 1 byte of data.",
            ));
        }

        // remove tile just to make sure that there
        // are no unreachable tiles
        self.remove_tile(tile_id);

        let hash = Self::calculate_hash(&vec);

        self.tile_by_id.insert(tile_id, TileManagerTile::Hash(hash));

        self.data_by_hash.insert(hash, vec);

        self.ids_by_hash.entry(hash).or_default().insert(tile_id);

        Ok(())
    }

    pub(crate) fn add_offset_tile(&mut self, tile_id: u64, offset: u64, length: u32) -> Result<()> {
        if length == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Length of a directory entry must be greater than 0.",
            ));
        }

        self.tile_by_id
            .insert(tile_id, TileManagerTile::OffsetLength(offset, length));

        Ok(())
    }

    /// Remove tile from writer
    pub fn remove_tile(&mut self, tile_id: u64) -> bool {
        match self.tile_by_id.remove(&tile_id) {
            None => false, // tile was not found
            Some(tile) => {
                let TileManagerTile::Hash(hash) = tile else {
                    return true;
                };

                // find set which includes all ids which have this hash
                let ids_with_hash = self.ids_by_hash.entry(hash).or_default();

                // remove current id from set
                ids_with_hash.remove(&tile_id);

                // delete data for this hash, if there are
                // no other ids that reference this hash
                if ids_with_hash.is_empty() {
                    self.data_by_hash.remove(&hash);
                    self.ids_by_hash.remove(&hash);
                }

                true
            }
        }
    }

    pub fn get_tile_ids(&self) -> Vec<&u64> {
        self.tile_by_id.keys().collect()
    }

    pub fn num_addressed_tiles(&self) -> usize {
        self.tile_by_id.len()
    }

    fn push_entry(entries: &mut Vec<Entry>, tile_id: u64, offset: u64, length: u32) {
        if let Some(last) = entries.last_mut() {
            if tile_id == last.tile_id + u64::from(last.run_length)
                && last.offset == offset
                && last.length == length
            {
                last.run_length += 1;
                return;
            }
        }

        entries.push(Entry {
            tile_id,
            offset,
            length,
            run_length: 1,
        });
    }
}

#[duplicate_item(
    async    add_await(code) cfg_async_filter       RTraits                                                  SeekFrom                get_tile_content         get_tile         finish;
    []       [code]          [cfg(all())]           [Read + Seek]                                            [std::io::SeekFrom]     [get_tile_content]       [get_tile]       [finish];
    [async]  [code.await]    [cfg(feature="async")] [AsyncRead + AsyncReadExt + Send + Unpin + AsyncSeekExt] [futures::io::SeekFrom] [get_tile_content_async] [get_tile_async] [finish_async];
)]
#[cfg_async_filter]
impl<R: RTraits> TileManager<R> {
    async fn get_tile_content(
        reader: &mut Option<R>,
        data_by_hash: &HashMap<u64, Vec<u8>>,
        tile: &TileManagerTile,
    ) -> Result<Option<Vec<u8>>> {
        match tile {
            TileManagerTile::Hash(hash) => Ok(data_by_hash.get(hash).cloned()),
            TileManagerTile::OffsetLength(offset, length) => match reader {
                Some(r) => {
                    add_await([r.seek(SeekFrom::Start(*offset))])?;
                    let mut buf = vec![0; *length as usize];
                    add_await([r.read_exact(&mut buf)])?;
                    Ok(Some(buf))
                }
                None => Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "Tried to read from non-existent reader",
                )),
            },
        }
    }

    pub async fn get_tile(&mut self, tile_id: u64) -> Result<Option<Vec<u8>>> {
        match self.tile_by_id.get(&tile_id) {
            None => Ok(None),
            Some(tile) => add_await([Self::get_tile_content(
                &mut self.reader,
                &self.data_by_hash,
                tile,
            )]),
        }
    }

    pub async fn finish(mut self) -> Result<FinishResult> {
        type OffsetLen = (u64, u32);

        let mut id_tile = self
            .tile_by_id
            .into_iter()
            .collect::<Vec<(u64, TileManagerTile)>>();
        id_tile.sort_by(|a, b| a.0.cmp(&b.0));

        let mut entries = Vec::<Entry>::new();
        let mut data = Vec::<u8>::new();

        let mut num_addressed_tiles: u64 = 0;
        let mut num_tile_content: u64 = 0;

        // hash => offset+length
        let mut offset_length_map = HashMap::<u64, OffsetLen, RandomState>::default();

        for (tile_id, tile) in id_tile {
            let Some(mut tile_data) = add_await([Self::get_tile_content(
                &mut self.reader,
                &self.data_by_hash,
                &tile,
            )])?
            else {
                continue;
            };

            let hash = if let TileManagerTile::Hash(h) = tile {
                h
            } else {
                Self::calculate_hash(&tile_data)
            };

            num_addressed_tiles += 1;

            if let Some((offset, length)) = offset_length_map.get(&hash) {
                Self::push_entry(&mut entries, tile_id, *offset, *length);
            } else {
                let offset = data.len() as u64;

                #[allow(clippy::cast_possible_truncation)]
                let length = tile_data.len() as u32;

                data.append(&mut tile_data);
                num_tile_content += 1;

                Self::push_entry(&mut entries, tile_id, offset, length);
                offset_length_map.insert(hash, (offset, length));
            }
        }

        let num_tile_entries = entries.len() as u64;

        Ok(FinishResult {
            data,
            directory: entries.into(),
            num_addressed_tiles,
            num_tile_content,
            num_tile_entries,
        })
    }
}

impl Default for TileManager<Cursor<&[u8]>> {
    fn default() -> Self {
        Self::new(None)
    }
}

// Test module removed - test files are not included in this fork
