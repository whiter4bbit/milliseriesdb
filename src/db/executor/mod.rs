mod agg;
mod query_expr;

use super::utils::IntoEntriesIterator;
use super::Entry;
use agg::{Aggregator, AggregatorState};
pub use query_expr::QueryExpr;
pub use agg::Aggregation;
use serde_derive::{Deserialize, Serialize};
use std::io;
use std::time::SystemTime;

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub ts: u64,
    pub values: Vec<Aggregation>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub struct Query {
    from: u64,
    group_by: u64,
    aggregators: Vec<Aggregator>,
    limit: usize,
}

pub struct Executor {
    state: Vec<AggregatorState>,
    group_ts: u64,
    is_empty: bool,
    group_by: u64,
    limit: usize,
    from: u64,
}

impl Executor {
    #[allow(dead_code)]
    pub fn new(query: &Query) -> Executor {
        Executor {
            state: query.aggregators.iter().map(|aggregator| aggregator.default_state()).collect(),
            group_ts: 0,
            is_empty: true,
            limit: query.limit,
            group_by: query.group_by,
            from: query.from,
        }
    }

    fn as_group_ts(&self, ts: u64) -> u64 {
        (ts / self.group_by) * self.group_by
    }

    #[allow(dead_code)]
    pub fn execute<I>(&mut self, series: I) -> io::Result<Vec<Row>>
    where
        I: IntoEntriesIterator,
    {
        let start_ts = SystemTime::now();
        
        let mut rows = Vec::new();
        let mut scanned = 0usize;
        for entry in series.into_iter(self.from)? {
            scanned += 1;
            let entry = entry?;
            let entry_group_ts = self.as_group_ts(entry.ts);
            if entry_group_ts == self.group_ts || self.is_empty {
                self.state.iter_mut().for_each(|state| state.update(&entry));
                self.group_ts = entry_group_ts;
                self.is_empty = false;
            } else {
                let row = Row {
                    ts: self.group_ts,
                    values: self.state.iter_mut().map(|state| state.pop()).collect(),
                };
                self.state.iter_mut().for_each(|state| state.update(&entry));
                self.group_ts = entry_group_ts;
                rows.push(row);
            }

            if rows.len() >= self.limit {
                break;
            }
        }

        if !self.is_empty && rows.len() < self.limit {
            rows.push(Row {
                ts: self.group_ts,
                values: self.state.iter_mut().map(|state| state.pop()).collect(),
            })
        }
        log::debug!("Scanned {} entries in {}ms", scanned, start_ts.elapsed().unwrap().as_millis());
        Ok(rows)
    }
}

#[cfg(test)]
mod test {
    use super::agg::Aggregator;
    use super::*;

    #[test]
    fn test_execute() {
        let mut executor = Executor::new(&Query {
            from: 0,
            group_by: 10,
            aggregators: vec![Aggregator::Mean],
            limit: 100,
        });

        let result = executor
            .execute(vec![
                Entry { ts: 0, value: 1.0 },
                Entry { ts: 1, value: 4.0 },
                Entry { ts: 3, value: 6.0 },
                Entry { ts: 6, value: 1.0 },
                Entry { ts: 10, value: 9.0 },
                Entry { ts: 15, value: 4.0 },
                Entry { ts: 16, value: 2.0 },
            ])
            .unwrap();

        assert_eq!(2, result.len());
        assert_eq!(0, result[0].ts);
        assert_eq!(10, result[1].ts);

        assert_eq!(
            true,
            match result[0].values[0] {
                Aggregation::Mean(value) => (value - 3.0).abs() <= 10e-6
            }
        );

        assert_eq!(
            true,
            match result[1].values[0] {
                Aggregation::Mean(value) => (value - 5.0).abs() <= 10e-6
            }
        )
    }
}
