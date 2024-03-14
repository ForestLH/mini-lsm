#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, SsTable};
use crate::key::KeyBytes;
use crate::table::FileObject;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use crate::iterators::StorageIterator;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.extend_from_slice(key.raw_ref());
            self.meta.push(BlockMeta {
                offset: 0,
                first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(key.raw_ref())),
                last_key: Default::default(),
            });
        }

        // judge current block is full
        if !self.builder.add(key, value) {
            let old_block_builder =
                std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let old_block = old_block_builder.build();
            let block_bytes = old_block.encode();
            self.data.extend(block_bytes);

            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(key.raw_ref())),
                last_key: Default::default(),
            });

            // add kv to new blockbuilder
            let _ = self.builder.add(key, value);
        }
        self.meta.last_mut().map(|last_meta| {
            last_meta.last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(key.raw_ref()));
        });
        self.last_key = Vec::from(key.raw_ref());
    }
    pub fn add_iter<I>(&mut self, mut iter: I) -> Result<()>
    where
        I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>
    {
        while iter.is_valid() {
            self.add(iter.key(), iter.value());
            iter.next()?;
        }
        Ok(())
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.meta.iter().map(|block_meta| block_meta.offset).sum()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let current_block_bytes = self.builder.build().encode();

        let mut serialized_data = self.data.clone();
        serialized_data.extend(current_block_bytes);
        let block_meta_offset = serialized_data.len();

        BlockMeta::encode_block_meta(&self.meta, &mut serialized_data);
        serialized_data.put_u32(block_meta_offset as u32);
        Ok(SsTable {
            file: FileObject::create(path.as_ref(), serialized_data)?,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(Bytes::from(self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::from(self.last_key)),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
#[cfg(test)]
mod tests {
    use crate::table::{BlockMeta, SsTableBuilder};
    use bytes::Bytes;
    #[test]
    fn test_estimated_size() {
        let mut sst_builder = SsTableBuilder::new(100);
        for _ in 0..5 {
            sst_builder.meta.push(BlockMeta {
                offset: 20,
                first_key: Default::default(),
                last_key: Default::default(),
            });
        }
        assert_eq!(sst_builder.estimated_size(), 20 * 5);
    }
    #[test]
    fn test_vec_append_bytes() {
        // 假设有一个 Vec<u8>
        let mut vec_bytes: Vec<u8> = vec![1, 2, 3, 4, 5];
        // 假设有一个 Bytes 对象
        let bytes_to_append = Bytes::from(vec![6, 7, 8, 9, 10]);
        // 使用 extend_from_slice 将 Bytes 追加到 Vec 中
        vec_bytes.extend_from_slice(bytes_to_append.as_ref());
        // vec_bytes.extend(bytes_to_append);

        // 打印结果
        println!("Combined Vec: {:?}", vec_bytes);
    }
}
