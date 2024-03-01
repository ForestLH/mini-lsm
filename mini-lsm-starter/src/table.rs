pub(crate) mod bloom;
mod builder;
mod iterator;

use std::cmp::Ordering::Less;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::{Block, BlockBuilder};
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        /// | number of  BlockMetas |                             BlockMeta 0                                            |  BlockMeta 1 |
        /// |       number(2B)      | BlockMeta.offset(4B) | first_key_len(4B) | first_key | last_key_len(4B) | last_key |   ...        |
        buf.put_u16(block_meta.len() as u16);
        for each_meta in block_meta {
            let first_key_len = each_meta.first_key.len() as u32;
            let last_key_len = each_meta.last_key.len() as u32;
            buf.put_u32(each_meta.offset as u32);

            buf.put_u32(first_key_len);
            buf.extend_from_slice(each_meta.first_key.raw_ref());

            buf.put_u32(last_key_len);
            buf.extend_from_slice(each_meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut metas: Vec<BlockMeta> = vec![];
        let number = buf.get_u16();
        for _ in 0..number {
            let offset = buf.get_u32();
            let first_key_len = buf.get_u32();
            let first_key = buf.copy_to_bytes(first_key_len as usize);

            let last_key_len = buf.get_u32();
            let last_key = buf.copy_to_bytes(last_key_len as usize);

            metas.push(BlockMeta {
                offset: offset as usize,
                first_key: KeyBytes::from_bytes(first_key),
                last_key: KeyBytes::from_bytes(last_key),
            });
        }
        metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let mut metas = vec![];
        let mut block_meta_offset: usize = 0;
        file.0.as_ref().map(|mut file| {
            let mut buf_vec = vec![];
            file.read_to_end(&mut buf_vec).unwrap();
            let mut all_buf = Bytes::from(buf_vec);
            let mut buf = all_buf.copy_to_bytes(all_buf.len() - 4);
            block_meta_offset = all_buf.get_u32() as usize;
            buf.copy_to_bytes(block_meta_offset);
            metas = BlockMeta::decode_block_meta(buf);
        });

        let first_key = metas.first().unwrap().first_key.clone();
        let last_key = metas.last().unwrap().last_key.clone();
        Ok(Self {
            file,
            block_meta: metas,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let meta_len = self.file.size() - (self.block_meta_offset + 4) as u64; // meta_len : 660
        let data_blocks = self.file.read(self.block_meta_offset as u64, meta_len)?; // block_meta_offset:2950
        let metas = BlockMeta::decode_block_meta(Bytes::from(data_blocks));

        if block_idx >= metas.len() {
            return Err(anyhow::anyhow!("the block_idx out index of meta blocks"));
        }

        let target_block = if block_idx == metas.len() - 1 {
            let blk_offset = &metas[block_idx].offset;
            let blk_len = self.block_meta_offset - blk_offset;
            let vec_buf = self.file.read(*blk_offset as u64, blk_len as u64)?;
            Block::decode(vec_buf.as_ref())
        } else {
            let blk_offset = &metas[block_idx].offset;
            let blk_len = &metas[block_idx + 1].offset - blk_offset;
            let vec_buf = self.file.read(*blk_offset as u64, blk_len as u64)?;
            Block::decode(vec_buf.as_ref())
        };
        Ok(Arc::new(target_block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        self.block_cache
            .as_ref()
            .map(|block_cache| {
                let key = &(self.id, block_idx);
                let block = block_cache
                    .try_get_with(*key, || self.read_block(block_idx))
                    .map_err(|e| anyhow!("{}", e))?;
                Ok(block)
            })
            .unwrap_or(Ok(self.read_block(block_idx)?))
    }
    pub fn debug_display_meta(&self) {
        for meta in &self.block_meta {
            println!(
                "{:?} ~ {:?}",
                String::from_utf8_lossy(meta.first_key.raw_ref()),
                String::from_utf8_lossy(meta.last_key.raw_ref())
            );
        }
    }
    fn binary_search_block_idx(&self, key: KeySlice) -> i32 {
        let mut left = 0i32;
        let mut right = (self.block_meta.len() - 1) as i32;
        while left <= right {
            let mid = (left + right) / 2;
            if KeySlice::cmp(
                &self.block_meta[mid as usize].first_key.as_key_slice(),
                &key,
            ) == Less
                && KeySlice::cmp(&self.block_meta[mid as usize].last_key.as_key_slice(), &key)
                    == Less
            {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        left
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.binary_search_block_idx(key) as usize
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use clap::builder::Str;

    #[test]
    fn test_buf1() {
        let mut whole_bytes = (&b"hello world"[..]);
        let bytes = whole_bytes.copy_to_bytes(5);
        assert_eq!(&bytes[..], &b"hello"[..]);
        assert_eq!(&whole_bytes[..], &b" world"[..]);
    }
    #[test]
    fn test_buf2() {
        let mut whole_bytes = (&b"hello world"[..]);
        // whole_bytes.len();
        let bytes = whole_bytes.copy_to_bytes(whole_bytes.len() - 6);
        // assert_eq!(&whole_bytes[..], &b"hello"[..]);
        assert_eq!(&bytes[..], &b" world"[..]);
    }
    #[derive(Debug)]
    struct Person(String);
    #[test]
    fn test_iter() {
        let mut arr = vec![Person(String::from("a")), Person(String::from("b"))];
        foo(arr.as_ref());
    }
    fn foo(arr: &[Person]) {
        let _ = arr.iter().map(|p| {
            println!("{:?}", p);
        });
    }
}
