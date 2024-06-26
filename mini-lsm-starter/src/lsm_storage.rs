#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::cmp::Ordering::{Equal, Less};
use std::collections::HashMap;
use std::mem::replace;
use std::ops::{Bound, Deref};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Error, Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{Key, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::lsm_storage;
use crate::manifest::Manifest;
use crate::mem_table::{self, map_bound, MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

/// Key = (sst id, key) Value = (Arc Block)
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
/// 里面有一个memtable还有一组imm_memtables
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
    fn testa(&mut self) {}
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        unimplemented!()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let lsm_storage = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let res = lsm_storage.memtable.get(key);
        if let Some(val) = &res {
            if val.is_empty() {
                return Ok(None);
            }
            return Ok(res);
        }
        // if this key not found in current memtable, then found in older memtable
        for imm_table in &lsm_storage.imm_memtables {
            let res = imm_table.get(key);
            if let Some(val) = &res {
                if val.is_empty() {
                    return Ok(None);
                }
                return Ok(res);
            }
        }
        // if this key not found in whole mem table(include mem table and immmem table)
        // So find in sst, firstly find in l0, then find in l1~lx
        let mut iters = vec![];
        for l0_sst_id in &lsm_storage.l0_sstables {
            let sst = lsm_storage.sstables.get(l0_sst_id).unwrap();
            let _ = SsTableIterator::create_and_seek_to_key(sst.clone(), KeySlice::from_slice(key))
                .map(|iter| {
                    iters.push(Box::new(iter));
                });
        }
        let mut l0_sst_merge_iter = MergeIterator::create(iters);
        while l0_sst_merge_iter.is_valid()
            && l0_sst_merge_iter.key().cmp(&Key::from_slice(key)) == Less
        {
            l0_sst_merge_iter.next()?;
        }
        if l0_sst_merge_iter.is_valid()
            && l0_sst_merge_iter.key().cmp(&Key::from_slice(key)) == Equal
        {
            return if l0_sst_merge_iter.value().is_empty() {
                Ok(None)
            } else {
                Ok(Some(Bytes::copy_from_slice(l0_sst_merge_iter.value())))
            };
        }
        // todo(leehao): just look at level 0, need to look at other layers
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let res;
        let size;
        {
            let lsm_storage_state = self.state.write();
            let mem_table = &lsm_storage_state.memtable;
            res = mem_table.put(key, value);
            size = mem_table.approximate_size();
        }
        self.try_freeze(size)?;
        res
    }
    fn try_freeze(&self, approximate_size: usize) -> Result<()> {
        if approximate_size > self.options.target_sst_size {
            let lock = self.state_lock.lock();
            let guard = self.state.read();
            if guard.memtable.approximate_size() > self.options.target_sst_size {
                drop(guard);
                return self.force_freeze_memtable(&lock);
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _: &MutexGuard<'_, ()>) -> Result<()> {
        println!("begin freeze");
        let next_id = self.next_sst_id();
        let new_mem_table = if self.options.enable_wal {
            MemTable::create_with_wal(next_id, self.path.clone())?
        } else {
            MemTable::create(next_id)
        };
        let mut guard = self.state.write();
        let mut lsm_storage_state = guard.as_ref().clone();
        let old_mem_table = replace(&mut lsm_storage_state.memtable, Arc::new(new_mem_table));
        lsm_storage_state.imm_memtables.insert(0, old_mem_table);
        *guard = Arc::new(lsm_storage_state);
        drop(guard);
        println!("end freeze");
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let mut sst_builder =
            SsTableBuilder::new(self.options.target_sst_size);
        let next_imm_memtable =
        {
            let mut state = self.state.write();
            let mut sta = state.as_ref().clone();
            let res = sta.imm_memtables.pop().expect("No imm mem table in lsm storage").clone();
            *state = Arc::new(sta);
            res
        };
        let id = next_imm_memtable.id();
        let imm_iter = next_imm_memtable.scan(Bound::Unbounded, Bound::Unbounded);
        sst_builder.add_iter(imm_iter)?;
        let new_sst_name = format!("{}.sst", id);
        let new_sst = sst_builder.build(id, Some(Arc::clone(&self.block_cache)), &self.path.join(new_sst_name))?;
        {
            let mut state = self.state.write();
            let mut sta = state.as_ref().clone();
            sta.l0_sstables.insert(0, id);
            sta.sstables.insert(id, Arc::new(new_sst));
            *state = Arc::new(sta);
        }
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mem_table_iter = snapshot.memtable.scan(lower, upper);
        let mut mem_table_iters = vec![];
        mem_table_iters.push(Box::new(mem_table_iter));
        for it in &snapshot.imm_memtables {
            mem_table_iters.push(it.scan(lower, upper).into());
        }
        let mut sst_table_iters = vec![];
        //todo(leehao): 这里只做了l0层的，还有其他层的sst没做
        for l0_sst_id in &snapshot.l0_sstables {
            let sst = snapshot.sstables.get(l0_sst_id).unwrap();
            let iter = match lower {
                Bound::Included(lower_key) => SsTableIterator::create_and_seek_to_key(
                    sst.clone(),
                    KeySlice::from_slice(lower_key),
                )?,
                Bound::Excluded(lower_key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        sst.clone(),
                        KeySlice::from_slice(lower_key),
                    )?;
                    iter.next()?;
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst.clone())?,
            };
            sst_table_iters.push(Box::new(iter));
        }
        let two_merge_iter = TwoMergeIterator::create(
            MergeIterator::create(mem_table_iters),
            MergeIterator::create(sst_table_iters),
        )?;
        let ret_iter = FusedIterator::new(LsmIterator::new(two_merge_iter, map_bound(upper))?);
        Ok(ret_iter)
    }
}
mod tests {
    #[test]
    fn test_vec_order() {
        let arr = vec![1, 2, 3];
        let mut expected = 1;
        for it in arr {
            assert_eq!(it, expected);
            expected += 1;
        }
    }
}
