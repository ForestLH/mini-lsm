#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::block::SIZEOF_U16;
use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: Default::default(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.rate_current_size() + key.len() + value.len() + SIZEOF_U16 * 3 > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        self.offsets.push(self.data.len() as u16);

        if self.is_empty() {
            self.first_key = KeyVec::from_vec(Vec::from(key.raw_ref()));
        }

        self.data.put_u16(key.len() as u16);
        self.data.extend_from_slice(key.raw_ref());
        self.data.put_u16(value.len() as u16);
        self.data.extend_from_slice(value);
        true
    }
    fn push_u16_to_vec(number: u16, array: &mut Vec<u8>) {
        let number_low = (number & 0xFF) as u8;
        let number_high = ((number >> 8) & 0xFF) as u8;
        array.push(number_low);
        array.push(number_high);
    }
    fn rate_current_size(&self) -> usize {
        SIZEOF_U16 + self.offsets.len() * SIZEOF_U16 + self.data.len()
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
