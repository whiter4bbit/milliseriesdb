mod aggregation;
mod statement;
mod statement_expr;

use crate::storage::{Entry, IntoEntriesIterator};
pub use aggregation::Aggregation;
use aggregation::AggregatorState;
use serde_derive::{Deserialize, Serialize};
pub use statement::Statement;
pub use statement_expr::StatementExpr;
use std::io;
use std::time::SystemTime;
use strength_reduce::StrengthReducedU64;

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub ts: u64,
    pub values: Vec<Aggregation>,
}

pub trait QueryBuilder {
    fn query(self, statement: Statement) -> Query<Self>
    where
        Self: IntoEntriesIterator + Sized,
    {
        Query {
            into_iterator: self,
            statement,
        }
    }
}

impl<I> QueryBuilder for I where I: IntoEntriesIterator + Sized {}

pub struct Query<I>
where
    I: IntoEntriesIterator,
{
    into_iterator: I,
    statement: Statement,
}

fn as_group_ts(ts: u64, group_by: u64) -> u64 {
    (ts / group_by) * group_by
}

impl<I> Query<I>
where
    I: IntoEntriesIterator,
{
    pub fn rows(self) -> io::Result<Vec<Row>> {
        let group_by = &mut GroupBy {
            iterator: self.into_iterator.into_iter(self.statement.from)?,
            folder: MeanFolder { count: 0, sum: 0.0 },
            current: None,
            group_by: self.statement.group_by,
            iterations: 0,
            group_by_reduced: StrengthReducedU64::new(self.statement.group_by),
        };

        let start_ts = SystemTime::now();

        let rows = group_by
            .map(|e| {
                e.map(|(key, value)| Row {
                    ts: key,
                    values: vec![Aggregation::Mean(value)],
                })
            })
            .take(self.statement.limit)
            .collect::<io::Result<Vec<Row>>>()?;

        log::debug!(
            "Scanned {} entries in {}ms",
            group_by.iterations,
            start_ts.elapsed().unwrap().as_millis()
        );

        Ok(rows)
    }
}

impl<I> Query<I>
where
    I: IntoEntriesIterator + Send + 'static,
{
    pub async fn rows_async(self) -> io::Result<Vec<Row>> {
        tokio::task::spawn_blocking(move || self.rows())
            .await
            .unwrap()
    }
}

trait Folder {
    type Result;
    fn fold(&mut self, value: f64);
    fn result(&self) -> Self::Result;
    fn clear(&mut self);
}

struct MeanFolder {
    count: usize,
    sum: f64,
}

impl Folder for MeanFolder {
    type Result = f64;
    fn fold(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
    }
    fn result(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
    fn clear(&mut self) {
        self.count = 0;
        self.sum = 0.0;
    }
}

struct GroupBy<I, F>
where
    I: Iterator<Item = io::Result<Entry>>,
    F: Folder,
{
    iterator: I,
    folder: F,
    current: Option<Entry>,
    group_by: u64,
    iterations: usize,
    group_by_reduced: StrengthReducedU64,
}

impl<I, F> GroupBy<I, F>
where
    I: Iterator<Item = io::Result<Entry>>,
    F: Folder,
{
    fn next_row(&mut self) -> io::Result<Option<(u64, F::Result)>> {
        let head = match self.current.take() {
            Some(current) => Some(current),
            _ => match self.iterator.next() {
                Some(next) => {
                    self.iterations += 1;

                    Some(next?)
                }
                _ => None,
            },
        };

        if let Some(head) = head {
            let group_key = head.ts - (head.ts % self.group_by_reduced);

            self.folder.fold(head.value);

            while let Some(next) = self.iterator.next() {
                self.iterations += 1;

                let next = next?;

                let next_key = next.ts - (next.ts % self.group_by_reduced);

                if next_key != group_key {
                    self.current = Some(next);

                    let result = self.folder.result();

                    self.folder.clear();

                    return Ok(Some((group_key, result)));
                }

                self.folder.fold(next.value);
            }
            return Ok(Some((group_key, self.folder.result())));
        }

        Ok(None)
    }
}

impl<I, F> Iterator for GroupBy<I, F>
where
    I: Iterator<Item = io::Result<Entry>>,
    F: Folder,
{
    type Item = io::Result<(u64, F::Result)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.next_row() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::storage::Entry;
    use super::aggregation::Aggregator;
    use super::*;

    #[test]
    fn test_query() {
        let series = vec![
            Entry { ts: 0, value: 1.0 },
            Entry { ts: 1, value: 4.0 },
            Entry { ts: 3, value: 6.0 },
            Entry { ts: 6, value: 1.0 },
            Entry { ts: 10, value: 9.0 },
            Entry { ts: 15, value: 4.0 },
            Entry { ts: 16, value: 2.0 },
        ];

        let result = series
            .query(Statement {
                from: 0,
                group_by: 10,
                aggregators: vec![Aggregator::Mean],
                limit: 100,
            })
            .rows()
            .unwrap();

        assert_eq!(2, result.len());
        assert_eq!(0, result[0].ts);
        assert_eq!(10, result[1].ts);

        assert_eq!(
            true,
            match result[0].values[0] {
                Aggregation::Mean(value) => (value - 3.0).abs() <= 10e-6,
            }
        );

        assert_eq!(
            true,
            match result[1].values[0] {
                Aggregation::Mean(value) => (value - 5.0).abs() <= 10e-6,
            }
        )
    }
}
