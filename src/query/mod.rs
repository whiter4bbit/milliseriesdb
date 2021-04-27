mod aggregation;
mod statement;
mod statement_expr;

use crate::storage::IntoEntriesIterator;
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
            statement: statement,
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
