#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::block::SIZEOF_U16;
use std::cmp::Ordering::{Greater, Less};
use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let offset_len = block.offsets.len();
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, offset_len),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }
    fn invalid_iterator(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Default::default(),
            value_range: (0, 0),
            idx: 0,
            first_key: Default::default(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut res = Self::decode_base_idx(block, 0);
        res.idx = 0;
        res.first_key = res.key.clone();
        res
    }
    fn decode_key_base_idx(block: &Arc<Block>, idx: usize) -> KeySlice {
        let begin_offset = block.offsets[idx] as usize;
        let entry = if idx + 1 >= block.offsets.len() {
            &block.data[begin_offset..]
        } else {
            &block.data[begin_offset..block.offsets[idx + 1] as usize]
        };
        let key_len = u16::from_be_bytes([entry[0], entry[1]]) as usize;
        let key = &entry[2..2 + key_len];
        KeySlice::from_slice(key)
    }
    /// decode from block base idx, return BlockIterator without first_key
    fn decode_base_idx(block: Arc<Block>, idx: usize) -> Self {
        let key: KeySlice = Self::decode_key_base_idx(&block, idx);
        let key_vec = key.to_key_vec();
        let offset_len = block.offsets.len();
        Self {
            block,
            key: key_vec,
            value_range: (0, offset_len),
            idx,
            first_key: Default::default(),
        }
    }
    fn binary_search_seek_key(block: &Arc<Block>, key: &KeySlice) -> i32 {
        let mut left = 0i32;
        let mut right = (block.offsets.len() - 1) as i32;
        while left <= right {
            let mid = (left + right) / 2;
            if KeySlice::cmp(&Self::decode_key_base_idx(&block, mid as usize), &key) == Less {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        left
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    /// use binary search, cuz block is sorted
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let target_idx = Self::binary_search_seek_key(&block, &key) as usize;
        if target_idx >= block.offsets.len() {
            Self::invalid_iterator(block)
        } else {
            let target_key_slice = Self::decode_key_base_idx(&block, target_idx);
            let target_key = target_key_slice.to_key_vec();
            let offset_len = block.offsets.len();
            Self {
                block,
                key: target_key,
                value_range: (0, offset_len),
                idx: 0,
                first_key: Default::default(),
            }
        }
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        KeySlice::from_slice(self.key.raw_ref())
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let begin_offset = self.block.offsets[self.idx] as usize;
        let entry = if self.idx + 1 >= self.block.offsets.len() {
            &self.block.data[begin_offset..]
        } else {
            &self.block.data[begin_offset..self.block.offsets[self.idx + 1] as usize]
        };
        let key_len = u16::from_be_bytes([entry[0], entry[1]]) as usize;
        let value_len =
            u16::from_be_bytes([entry[SIZEOF_U16 + key_len], entry[1 + SIZEOF_U16 + key_len]])
                as usize;
        let value_begin: usize = 2 * SIZEOF_U16 + key_len;
        &entry[value_begin..value_begin + value_len]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        self.idx < self.value_range.1
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let key = Self::decode_key_base_idx(&self.block, 0);
        self.first_key = key.to_key_vec();
        self.key = key.to_key_vec();
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if !self.is_valid() {
            return;
        }
        let next_key = Self::decode_key_base_idx(&self.block, self.idx);
        self.key = next_key.to_key_vec();
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let target_idx = Self::binary_search_seek_key(&self.block, &key) as usize;
        self.idx = target_idx;
        if !self.is_valid() {
            return;
        }
        let target_key = Self::decode_key_base_idx(&self.block, self.idx);
        self.key = target_key.to_key_vec();
    }
}
