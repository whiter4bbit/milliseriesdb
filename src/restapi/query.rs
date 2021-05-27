use crate::query::{Aggregation, QueryBuilder, Row, Statement, StatementExpr};
use crate::storage::{Entry, SeriesTable};
use chrono::{TimeZone, Utc};
use serde_derive::{Deserialize, Serialize};
use std::convert::TryInto;
use std::sync::Arc;
use warp::reject::Rejection;
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

async fn query(
    name: String,
    statement_expr: StatementExpr,
    series_table: Arc<SeriesTable>,
) -> Result<warp::reply::Json, Rejection> {
    let reader = series_table
        .reader(&name)
        .ok_or_else(|| super::error::not_found(&name))?;
    let statement: Statement = statement_expr
        .try_into()
        .map_err(|err| super::error::bad_request(format!("can not parse expression: {:?}", err)))?;
    reader
        .query(statement)
        .rows_async()
        .await
        .map(|rows| warp::reply::json(&JsonRows::from_rows(rows)))
        .map_err(|e| super::error::internal(e))
}

pub fn filter(series_table: Arc<SeriesTable>) -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
    warp::path!("series" / String)
        .and(warp::get())
        .and(warp::query::<StatementExpr>())
        .and(super::with_series_table(series_table.clone()))
        .and_then(self::query)
        .recover(super::error::handle)
        .boxed()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::failpoints::Failpoints;
    use crate::storage::error::Error;
    use crate::storage::series_table;
    use warp::http::StatusCode;

    #[tokio::test]
    async fn test_query() -> Result<(), Error> {
        let fp = Arc::new(Failpoints::create());
        let series_table = series_table::test::create_with_failpoints(fp.clone())?;

        let resp = warp::test::request()
            .method("GET")
            .path("/series/t?from=2019-08-01&group_by=hour&aggregators=mean&limit=1000")
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::NOT_FOUND, resp.status());

        series_table.create("t")?;

        let resp = warp::test::request()
            .method("GET")
            .path("/series/t?from=2019-08-01&group_by=hour&aggregators=mean&limit=1000")
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::OK, resp.status());

        let resp = warp::test::request()
            .method("GET")
            .path("/series/t?from=2019-08-01&group_by=milli&aggregators=mean&limit=1000")
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::BAD_REQUEST, resp.status());

        Ok(())
    }
}
