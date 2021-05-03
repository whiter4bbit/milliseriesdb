mod aggregation;
mod statement;
mod statement_expr;

use crate::storage::{IntoEntriesIterator, LowLevelEntriesIterator, IntoLowLevelEntriesIterator};
pub use aggregation::Aggregation;
use aggregation::AggregatorState;
use serde_derive::{Deserialize, Serialize};
pub use statement::Statement;
pub use statement_expr::StatementExpr;
use std::io;
use std::time::SystemTime;

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
        let start_ts = SystemTime::now();
        let mut rows = Vec::new();
        let mut scanned = 0usize;
        let mut group_ts = 0u64;
        let mut state: Vec<AggregatorState> = self
            .statement
            .aggregators
            .iter()
            .map(|aggregator| aggregator.default_state())
            .collect();
        let mut is_empty = true;
        for entry in self.into_iterator.into_iter(self.statement.from)? {
            scanned += 1;
            let entry = entry?;
            let entry_group_ts = as_group_ts(entry.ts, self.statement.group_by);
            if entry_group_ts == group_ts || is_empty {
                state.iter_mut().for_each(|state| state.update(&entry));
                group_ts = entry_group_ts;
                is_empty = false;
            } else {
                let row = Row {
                    ts: group_ts,
                    values: state.iter_mut().map(|state| state.pop()).collect(),
                };
                state.iter_mut().for_each(|state| state.update(&entry));
                group_ts = entry_group_ts;
                rows.push(row);
            }

            if rows.len() >= self.statement.limit {
                break;
            }
        }

        if !is_empty && rows.len() < self.statement.limit {
            rows.push(Row {
                ts: group_ts,
                values: state.iter_mut().map(|state| state.pop()).collect(),
            })
        }
        log::debug!(
            "Scanned {} entries in {}ms",
            scanned,
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

/////
///
pub trait LowLevelQueryBuilder {
    fn low_level_query(self, statement: Statement) -> LowLevelQuery<Self>
    where
        Self: IntoLowLevelEntriesIterator + Sized,
    {
        LowLevelQuery {
            into_iterator: self,
            statement,
        }
    }
}

impl<I> LowLevelQueryBuilder for I where I: IntoLowLevelEntriesIterator + Sized {}

pub struct LowLevelQuery<I>
where
    I: IntoLowLevelEntriesIterator,
{
    into_iterator: I,
    statement: Statement,
}

fn round_ts(ts: &mut [u64], group_by: u64, size: usize) {
    for i in 0..size {
        ts[i] = ts[i] - ts[i] % group_by;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_round_ts() {
        let mut ts: Vec<u64> = vec![1, 2, 10,  15, 20, 24, 30, 40, 50];
        round_ts(&mut ts[..], 10, ts.len());
    }
}

impl<I> LowLevelQuery<I>
where
    I: IntoLowLevelEntriesIterator,
{
    pub fn rows(self) -> io::Result<Vec<Row>> {
        let start_ts = SystemTime::now();
        let mut scanned_entries = 0u64;

        let mut rows = Vec::new();

        let mut row_key = 0u64;
        let mut row_count = 0u64;
        let mut row_sum = 0f64;

        let mut iter = self.into_iterator.into_low_level_iter(self.statement.from)?;

        let mut ts = [0u64; 1000];
        let mut values = [0f64; 1000];

        let group_by = self.statement.group_by;
        let limit = self.statement.limit;

        let mut min_batch_size = 1000usize;
        let mut max_batch_size = 0usize;

        while rows.len() < limit {
            let mut batch_size = 0usize;

            while batch_size < 500 {
                let small_batch_size = iter.next(&mut ts[batch_size..], &mut values[batch_size..])?;

                if small_batch_size == 0 {
                    break
                }

                batch_size += small_batch_size;
            }

            if batch_size == 0 {
                break
            }

            min_batch_size = min_batch_size.min(batch_size);
            max_batch_size = max_batch_size.max(batch_size);

            round_ts(&mut ts, group_by, batch_size);

            scanned_entries += batch_size as u64;

            let mut batch_pos = 0usize;

            while batch_pos < batch_size {
                let mut group_end_pos = batch_pos;

                let mut group_row_sum = 0f64;

                while group_end_pos < batch_size && ts[group_end_pos] == ts[batch_pos] {
                    group_row_sum += values[group_end_pos];
                    group_end_pos += 1
                }

                if row_key != ts[batch_pos] {
                    if row_count > 0 && rows.len() < limit {
                        rows.push(Row {
                            ts: row_key,
                            values: vec![Aggregation::Mean(row_sum / row_count as f64)],
                        });
                    }

                    row_key = ts[batch_pos];
                    row_sum = 0.0;
                    row_count = 0;
                }

                row_count += (group_end_pos - batch_pos) as u64;
                row_sum += group_row_sum;

                batch_pos = group_end_pos;
            }
        }

        if row_count > 0 && rows.len() < limit {
            rows.push(Row {
                ts: row_key,
                values: vec![Aggregation::Mean(row_sum / row_count as f64)],
            });
        }

        log::debug!(
            "Scanned {} entries in {}ms. Min batch size: {}, max batch size: {}",
            scanned_entries,
            start_ts.elapsed().unwrap().as_millis(),
            min_batch_size,
            max_batch_size,
        );

        return Ok(rows);
    }
}

#[cfg(test)]
mod test {
    use super::super::storage::Entry;
    use super::aggregation::Aggregator;
    use super::*;

    pub struct VecIter {
        pos: usize,
        content: Vec<Entry>,
    }

    impl LowLevelEntriesIterator for VecIter {
        fn next(&mut self, ts: &mut [u64], values: &mut [f64]) -> io::Result<usize> {
            let mut size = 0usize;
            while self.pos < self.content.len() && size < 2 {
                ts[size] = self.content[self.pos].ts;
                values[size] = self.content[self.pos].value;
                size += 1;
                self.pos += 1;
            }
            Ok(size)
        }
    }

    impl IntoLowLevelEntriesIterator for Vec<Entry> {
        type Iter = VecIter;
        fn into_low_level_iter(&self, from: u64) -> io::Result<Self::Iter> {
            Ok(VecIter {
                pos: 0,
                content: self.iter().filter(|entry| entry.ts >= from).map(|e| e.clone()).collect(),
            })
        }
    }

    #[test]
    fn test_low_level_query() {
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
            .low_level_query(Statement {
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
