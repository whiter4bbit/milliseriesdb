use super::Entry;
use super::error::Error;
use std::collections::VecDeque;

pub trait IntoEntriesIterator {
    type Iter: Iterator<Item = Result<Entry, Error>>;
    fn into_iter(&self, from: u64) -> Result<Self::Iter, Error>;
}

pub struct VecIterator {
    deque: VecDeque<Entry>,
}

impl Iterator for VecIterator {
    type Item = Result<Entry, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.deque.pop_front().map(Ok)
    }
}

impl IntoEntriesIterator for Vec<Entry> {
    type Iter = VecIterator;
    fn into_iter(&self, from: u64) -> Result<Self::Iter, Error> {
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