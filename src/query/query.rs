use super::aggregation::{Aggregation, AggregatorsFolder};
use super::group_by::GroupBy;
use super::into_entries_iter::IntoEntriesIter;
use super::statement::Statement;
use super::round::round_to;
use crate::storage::{error::Error, Entry};
use serde_derive::{Deserialize, Serialize};
use std::convert::From;
use std::time::SystemTime;

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub ts: i64,
    pub values: Vec<Aggregation>,
}

#[cfg(test)]
impl PartialEq<Row> for Row {
    fn eq(&self, other: &Row) -> bool {
        self.ts == other.ts && self.values == other.values
    }
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

        let granularity = self.statement.group_by as i64;

        let group_by = &mut GroupBy {
            iterator: self.into_iterator.into_iter(self.statement.from)?,
            folder: folder,
            current: None,
            iterations: 0,
            key: { |e: &Entry| round_to(e.ts, granularity) },
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
