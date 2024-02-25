#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use crate::key::KeySlice;
pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let copy_data = self.data.clone();
        let copy_offset = self.offsets.clone();
        let mut combined_vec: Vec<u8> = copy_data
            .into_iter()
            .flat_map(|byte| byte.to_be_bytes())
            .chain(
                copy_offset
                    .into_iter()
                    .flat_map(|offset| offset.to_be_bytes()),
            )
            .collect();
        combined_vec.put_u16(self.offsets.len() as u16);
        Bytes::from(combined_vec)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let last_two = &data[data.len() - 2..];
        let offset_len: usize = u16::from_be_bytes([last_two[0], last_two[1]]) as usize;
        let u8_offset_arr = &data[data.len() - 2 - offset_len * SIZEOF_U16..data.len() - 2];
        let u16_offset_arr: Vec<u16> = u8_offset_arr
            .iter()
            .cloned()
            .zip(u8_offset_arr.iter().skip(1))
            .step_by(2)
            .map(|(first, second)| u16::from_be_bytes([first, *second]))
            .collect();
        let u8_data = &data[..data.len() - 2 - offset_len * SIZEOF_U16];
        Self {
            data: Vec::from(u8_data),
            offsets: u16_offset_arr,
        }
    }
    /// decode a key from an entry
    pub(crate) fn decode_key_from_entry(data: &Vec<u8>) -> KeySlice {
        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        let key = &data[2..key_len + 2];
        KeySlice::from_slice(key)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_connect() {
        let mut u8_arr = vec![];
        for _ in 0..2 {
            u8_arr.push(2u8);
        }
        let mut u16_arr = vec![];
        for _ in 0..2 {
            u16_arr.push(257u16);
        }
        let aaa: Vec<u8> = u16_arr.iter().flat_map(|byte| byte.to_le_bytes()).collect();
        println!("{:?}", u8_arr);
        println!("{:?}", aaa);
        let combined_vec: Vec<u8> = u8_arr
            .into_iter()
            .flat_map(|byte| byte.to_le_bytes())
            .chain(u16_arr.into_iter().flat_map(|byte| byte.to_le_bytes()))
            .collect();

        println!("{:?}", combined_vec);
    }
}
