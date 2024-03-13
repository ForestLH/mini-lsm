#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;
use std::cmp::Ordering::{Equal, Greater, Less};

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    // Indicates whether it is currently a or B
    current: i32,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut current = 0;
        if a.is_valid() && b.is_valid() {
            current = if a.key().cmp(&b.key()) == Greater {
                1
            } else {
                0
            };
        } else if a.is_valid() {
            current = 0;
        } else {
            current = 1;
        }

        Ok(TwoMergeIterator { a, b, current })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.current == 0 {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.current == 0 {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.current == 0 {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() {
            let order = self.a.key().cmp(&self.b.key());
            match order {
                Less => {
                    self.a.next()?;
                }
                Equal => {
                    self.a.next()?;
                    self.b.next()?;
                }
                Greater => {
                    self.b.next()?;
                }
            }
            if self.a.is_valid() && self.b.is_valid() {
                self.current = if self.a.key().cmp(&self.b.key()) == Greater {
                    1
                } else {
                    0
                }
            } else if self.a.is_valid() {
                self.current = 0;
            } else {
                self.current = 1;
            }
        } else if self.a.is_valid() {
            self.a.next()?;
            self.current = 0;
        } else if self.b.is_valid() {
            self.b.next()?;
            self.current = 1;
        }
        Ok(())
    }
}
