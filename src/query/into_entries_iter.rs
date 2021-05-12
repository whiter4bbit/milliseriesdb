use crate::storage::{error::Error, Entry, SeriesReader, SeriesIterator};
use std::sync::Arc;

pub trait IntoEntriesIter {
    type Iter: Iterator<Item = Result<Entry, Error>>;
    fn into_iter(&self, from: i64) -> Result<Self::Iter, Error>;
}

impl IntoEntriesIter for Arc<SeriesReader> {
    type Iter = SeriesIterator;
    fn into_iter(&self, from: i64) -> Result<Self::Iter, Error> {
        self.iterator(from)
    }
}

#[cfg(test)]
use std::collections::VecDeque;

#[cfg(test)]
pub struct VecIterator {
    deque: VecDeque<Entry>,
}

#[cfg(test)]
impl Iterator for VecIterator {
    type Item = Result<Entry, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.deque.pop_front().map(Ok)
    }
}

#[cfg(test)]
impl IntoEntriesIter for Vec<Entry> {
    type Iter = VecIterator;
    fn into_iter(&self, from: i64) -> Result<Self::Iter, Error> {
        let mut iter = VecIterator {
            deque: VecDeque::new(),
        };
        for entry in self.iter() {
            if entry.ts >= from {
                iter.deque.push_back(entry.to_owned());
            }
        }
        Ok(iter)
    }
}