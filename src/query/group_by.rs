use crate::storage::Entry;
use std::io;
use strength_reduce::StrengthReducedU64;

pub trait Folder {
    type Result;
    fn fold(&mut self, value: f64);
    fn complete(&mut self) -> Self::Result;
}

pub struct GroupBy<I, F>
where
    I: Iterator<Item = io::Result<Entry>>,
    F: Folder,
{
    pub iterator: I,
    pub folder: F,
    pub granularity: StrengthReducedU64,
    pub current: Option<Entry>,
    pub iterations: usize,
}

impl<I, F> GroupBy<I, F>
where
    I: Iterator<Item = io::Result<Entry>>,
    F: Folder,
{
    fn key(&self, entry: &Entry) -> u64 {
        entry.ts - (entry.ts % self.granularity)
    }
}

impl<I, F> Iterator for GroupBy<I, F>
where
    I: Iterator<Item = io::Result<Entry>>,
    F: Folder,
{
    type Item = io::Result<(u64, F::Result)>;

    fn next(&mut self) -> Option<io::Result<(u64, F::Result)>> {
        let head = self.current.take().map(Ok).or_else(|| self.iterator.next());

        if let Some(head) = head {
            let head = match head {
                Ok(head) => head,
                Err(err) => return Some(Err(err)),
            };

            let group_key = self.key(&head);

            self.folder.fold(head.value);

            while let Some(next) = self.iterator.next() {
                let next = match next {
                    Ok(next) => next,
                    Err(err) => return Some(Err(err)),
                };

                self.iterations += 1;

                if self.key(&next) != group_key {
                    self.current = Some(next);

                    return Some(Ok((group_key, self.folder.complete())));
                }

                self.folder.fold(next.value);
            }
            return Some(Ok((group_key, self.folder.complete())));
        }

        None
    }
}