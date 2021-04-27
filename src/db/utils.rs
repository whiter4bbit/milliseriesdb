use super::Entry;
use std::collections::VecDeque;
use std::io;

pub trait IntoEntriesIterator {
    type Iter: Iterator<Item = io::Result<Entry>>;
    fn into_iter(&self, from: u64) -> io::Result<Self::Iter>;
}

pub struct VecIterator {
    deque: VecDeque<Entry>,
}

impl Iterator for VecIterator {
    type Item = io::Result<Entry>;
    fn next(&mut self) -> Option<Self::Item> {
        self.deque.pop_front().map(|entry| Ok(entry))
    }
}

impl IntoEntriesIterator for Vec<Entry> {
    type Iter = VecIterator;
    fn into_iter(&self, from: u64) -> io::Result<Self::Iter> {
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