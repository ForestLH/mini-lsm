use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
    blk_num: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block(0)?;
        let blk_num = table.num_of_blocks();
        Ok(Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(block),
            blk_idx: 0,
            blk_num,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        self.blk_iter = BlockIterator::create_and_seek_to_first(self.table.read_block(0)?);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let blk_idx = table.find_block_idx(key);
        let block = table.read_block_cached(blk_idx)?;
        let blk_num = table.num_of_blocks();
        Ok(Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_key(block, key),
            blk_idx,
            blk_num,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let display_key = String::from_utf8_lossy(key.raw_ref()); // just for debug
        let blk_idx = self.table.find_block_idx(key);
        if let Ok(block) = self.table.read_block_cached(blk_idx) {
            self.blk_idx = blk_idx;
            self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() && self.blk_idx + 1 < self.blk_num {
            let next_blk = self.table.read_block(self.blk_idx + 1)?;
            self.blk_idx += 1;
            self.blk_iter = BlockIterator::create_and_seek_to_first(next_blk);
        }
        Ok(())
    }
}
