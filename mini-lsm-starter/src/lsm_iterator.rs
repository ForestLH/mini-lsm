#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::Ordering::{Greater, Less};
use std::collections::Bound;
use anyhow::{anyhow, bail, Error, Result};
use bytes::Bytes;
use nom::combinator::value;

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::table::SsTableIterator;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut lsm_iter = Self {
            inner: iter,
            end_bound,
        };
        lsm_iter.move_to_non_delete_non_overbound()?;
        Ok(lsm_iter)
    }
    fn move_to_non_delete_non_overbound(&mut self) -> Result<()> {
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }
        match &self.end_bound {
            Bound::Included(upper) => {
                if self.inner.is_valid() && !self.inner.value().is_empty() && self.key().cmp(upper) != Less {
                    Err(anyhow!("lsm iter is over bounded!"))
                } else {
                    Ok(())
                }
            }
            Bound::Excluded(upper) => {
                if self.inner.is_valid() && !self.inner.value().is_empty() && self.key().cmp(upper) == Greater {
                    Err(anyhow!("lsm iter is over bounded!"))
                } else {
                    Ok(())
                }
            }
            Bound::Unbounded => {
                Ok(())
            }
        }
        // let upper = *self.end_bound;
        // if self.inner.is_valid() && !self.inner.value().is_empty()
        //     && self.key().cmp(upper) == Greater {
        //     return Err(Error::from("LsmIterator over bounded!"));
        // }
        // Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().for_testing_key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.move_to_non_delete_non_overbound()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        if self.has_errored {
            false
        } else {
            self.iter.is_valid()
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("the iterator is tainted");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }
}
