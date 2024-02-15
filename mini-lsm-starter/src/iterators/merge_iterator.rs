#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::{BTreeMap, BinaryHeap, HashMap};

use anyhow::Result;
use std::borrow::BorrowMut;
use std::collections::binary_heap::PeekMut;
use std::mem::swap;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

// 大的在前面，小的在后面，如果iter内容相同，那么就比较前面的顺序
impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        if iters.iter().all(|x| !x.is_valid()) {
            // copy
            let mut iter = iters;
            return Self {
                iters: BinaryHeap::new(),
                current: Some(HeapWrapper(0, iter.pop().unwrap())),
            };
        }

        let mut res = Self {
            iters: BinaryHeap::new(),
            current: None,
        };
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                res.iters.push(HeapWrapper(idx, iter));
            }
        }
        res.current = Some(res.iters.pop().unwrap());
        res
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        if let Some(heap_wrapper) = &self.current {
            let heap = &heap_wrapper.1;
            heap.key()
        } else {
            KeySlice::default()
        }
    }

    fn value(&self) -> &[u8] {
        match &self.current {
            None => &[],
            Some(heap_wrapper) => {
                let heap = &heap_wrapper.1;
                heap.value()
            }
        }
    }

    fn is_valid(&self) -> bool {
        match &self.current {
            None => false,
            Some(heap_wrapper) => heap_wrapper.1.is_valid(),
        }
    }

    // iter2: b->2, c->3

    // iter1: b->del, c->4, d->5
    // iter3: e->4
    // a->1, b->del, c->4, d->5, e->4
    fn next(&mut self) -> Result<()> {
        let cur_iter = self.current.as_mut().unwrap();
        let outdated_key = &cur_iter.1.key();

        while let Some(mut inner_iter) = self.iters.peek_mut() {
            if inner_iter.1.key() != *outdated_key {
                break;
            } else {
                if let e @ Err(_) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            }
        }
        cur_iter.1.next()?;
        if !cur_iter.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *cur_iter = iter;
            }
            return Ok(());
        }

        if let Some(mut heap_top) = self.iters.peek_mut() {
            if  *heap_top > *cur_iter  && heap_top.1.is_valid() {
                std::mem::swap(cur_iter, &mut heap_top);
            }
        }

        Ok(())
    }
}
