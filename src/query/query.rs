use super::aggregation::{Aggregation, AggregatorsFolder};
use super::into_entries_iter::IntoEntriesIter;
use crate::storage::error::Error;
use serde_derive::{Deserialize, Serialize};
use super::statement::Statement;
use std::time::SystemTime;
use strength_reduce::StrengthReducedU64;
use super::group_by::GroupBy;
use std::convert::From;

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub ts: i64,
    pub values: Vec<Aggregation>,
}

impl From<(i64, Vec<Aggregation>)> for Row {
    fn from(row: (i64, Vec<Aggregation>)) -> Row {
        Row {
            ts: row.0,
            values: row.1,
        }
    }
}

pub trait QueryBuilder {
    fn query(self, statement: Statement) -> Query<Self>
    where
        Self: IntoEntriesIter + Sized,
    {
        Query {
            into_iterator: self,
            statement,
        }
    }
}

impl<I> QueryBuilder for I where I: IntoEntriesIter + Sized {}

pub struct Query<I>
where
    I: IntoEntriesIter,
{
    into_iterator: I,
    statement: Statement,
}

impl<I> Query<I>
where
    I: IntoEntriesIter,
{
    pub fn rows(self) -> Result<Vec<Row>, Error> {
        let folder = AggregatorsFolder::new(&self.statement.aggregators);

        let group_by = &mut GroupBy {
            iterator: self.into_iterator.into_iter(self.statement.from)?,
            folder: folder,
            current: None,
            iterations: 0,
            granularity: StrengthReducedU64::new(self.statement.group_by),
        };

        let start_ts = SystemTime::now();

        let rows = group_by
            .map(|e| e.map(|e| e.into()))
            .take(self.statement.limit)
            .collect::<Result<Vec<Row>, Error>>()?;

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
    I: IntoEntriesIter + Send + 'static,
{
    pub async fn rows_async(self) -> Result<Vec<Row>, Error> {
        tokio::task::spawn_blocking(move || self.rows())
            .await
            .unwrap()
    }
}

#[cfg(test)]
mod test {
    use crate::storage::Entry;
    use super::super::aggregation::Aggregator;
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
                _ => false,
            }
        );

        assert_eq!(
            true,
            match result[1].values[0] {
                Aggregation::Mean(value) => (value - 5.0).abs() <= 10e-6,
                _ => false,
            }
        )
    }
}
