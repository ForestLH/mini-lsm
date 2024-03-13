#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use nom::AsBytes;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: Option::None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(1)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let mut without_wal = Self::create(id);
        without_wal.wal = Some(Wal::create(path)?);
        Ok(without_wal)
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let mut skiplist = SkipMap::new();
        Wal::recover(path, &mut skiplist)?;
        Ok(Self {
            map: Arc::new(skiplist),
            wal: None,
            id: id,
            approximate_size: Arc::new(AtomicUsize::new(1)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let entry = self.map.get(key)?;
        Some(entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.map.contains_key(key) {
            self.map.remove(key);
        }
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        let add_size = key.len() + value.len();
        self.approximate_size
            .fetch_add(add_size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    // pub fn force_freeze_memtable(&self) {
    //     self.
    // }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let (l, u) = (map_bound(lower), map_bound(upper));
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((l, u)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        // let entry = self.map.iter().next();  // 这样是不行的，这样相当于是重新拿到了一个iter，
        // 并没有实际上移动需要返回的iter这个MemTableIterator类型的迭代器
        // iter.with_item_mut(|item| {*item = MemTableIterator::entry_to_item(entry)});

        let item = iter.with_iter_mut(|it| MemTableIterator::entry_to_item(it.next()));
        iter.with_item_mut(|field| *field = item);
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        unimplemented!()
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}
impl MemTableIterator {
    pub fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|en| (en.key().clone(), en.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        let (_, val) = self.borrow_item();
        val.as_bytes()
    }

    fn key(&self) -> KeySlice {
        let (key, _) = self.borrow_item();
        KeySlice::from_slice(key)
    }

    fn is_valid(&self) -> bool {
        let (key, _) = self.borrow_item();
        !key.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let next_item = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| {
            *x.item = next_item;
        });
        Ok(())
    }
}
