use chrono::{TimeZone, Utc};
use milliseriesdb::query::{Aggregation, QueryBuilder, Row, Statement, StatementExpr};
use milliseriesdb::storage::{Entry, SeriesTable};
use serde_derive::{Deserialize, Serialize};
use std::convert::TryInto;
use std::sync::Arc;
use warp::reject::{Reject, Rejection};
use warp::Filter;

#[derive(Deserialize)]
pub struct JsonEntries {
    pub entries: Vec<Entry>,
}

#[derive(Serialize)]
pub struct JsonRows {
    pub rows: Vec<JsonRow>,
}

impl JsonRows {
    fn from_rows(rows: Vec<Row>) -> JsonRows {
        JsonRows {
            rows: rows
                .into_iter()
                .map(|row| JsonRow {
                    timestamp: Utc.timestamp_millis(row.ts as i64).to_rfc3339(),
                    values: row.values,
                })
                .collect(),
        }
    }
}

#[derive(Serialize)]
pub struct JsonRow {
    pub timestamp: String,
    pub values: Vec<Aggregation>,
}

#[derive(Debug)]
struct StatementParseError {}

impl Reject for StatementParseError {}

#[derive(Debug)]
struct QueryError {}

impl Reject for QueryError {}

async fn query(
    name: String,
    statement_expr: StatementExpr,
    series_table: Arc<SeriesTable>,
) -> Result<warp::reply::Json, Rejection> {
    let reader = series_table
        .reader(name)
        .ok_or_else(|| warp::reject::not_found())?;
    let statement: Statement = statement_expr
        .try_into()
        .map_err(|_| warp::reject::custom(StatementParseError {}))?;
    reader
        .query(statement)
        .rows_async()
        .await
        .map(|rows| warp::reply::json(&JsonRows::from_rows(rows)))
        .map_err(|_| warp::reject::custom(QueryError {}))
}

pub fn filter(series_table: Arc<SeriesTable>) -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
    warp::path!("series" / String)
        .and(warp::get())
        .and(warp::query::<StatementExpr>())
        .and(super::with_series_table(series_table.clone()))
        .and_then(self::query)
        .boxed()
}
