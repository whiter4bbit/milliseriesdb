use crate::storage::{error::Error, Entry};

pub trait Folder {
    type Result;
    fn fold(&mut self, value: f64);
    fn complete(&mut self) -> Self::Result;
}

pub struct GroupBy<I, F, K>
where
    I: Iterator<Item = Result<Entry, Error>>,
    F: Folder,
    K: Fn(&Entry) -> i64,
{
    pub iterator: I,
    pub folder: F,
    pub current: Option<Entry>,
    pub iterations: usize,
    pub key: K,
}

impl<I, F, K> Iterator for GroupBy<I, F, K>
where
    I: Iterator<Item = Result<Entry, Error>>,
    F: Folder,
    K: Fn(&Entry) -> i64,
{
    type Item = Result<(i64, F::Result), Error>;

    fn next(&mut self) -> Option<Result<(i64, F::Result), Error>> {
        let head = self.current.take().map(Ok).or_else(|| self.iterator.next());

        if let Some(head) = head {
            let head = match head {
                Ok(head) => head,
                Err(err) => return Some(Err(err)),
            };

            let group_key = (self.key)(&head);

            self.folder.fold(head.value);

            while let Some(next) = self.iterator.next() {
                let next = match next {
                    Ok(next) => next,
                    Err(err) => return Some(Err(err)),
                };

                self.iterations += 1;

                if (self.key)(&next) != group_key {
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
