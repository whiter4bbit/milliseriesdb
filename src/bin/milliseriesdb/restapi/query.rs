use milliseriesdb::storage::SeriesTable;
use milliseriesdb::query::{Aggregation, Row, Statement, StatementExpr, QueryBuilder};
use warp::{Reply, http::StatusCode};
use serde_derive::Serialize;
use std::sync::Arc;
use std::convert::{TryFrom, Infallible};
use chrono::{TimeZone, Utc};

#[derive(Serialize)]
pub struct JsonRow {
    pub timestamp: String,
    pub values: Vec<Aggregation>,
}

impl From<Row> for JsonRow {
    fn from(row: Row) -> JsonRow {
        JsonRow {
            timestamp: Utc.timestamp_millis(row.ts as i64).to_rfc3339(),
            values: row.values,
        }
    }
}

#[derive(Serialize)]
pub struct JsonRows {
    pub rows: Vec<JsonRow>,
}

impl From<Vec<Row>> for JsonRows {
    fn from(rows: Vec<Row>) -> JsonRows {
        JsonRows {
            rows: rows.into_iter().map(|row| row.into()).collect(),
        }
    }
}

pub async fn query(
    name: String,
    statement_expr: StatementExpr,
    series_table: Arc<SeriesTable>,
) -> Result<Box<dyn Reply>, Infallible> {
    Ok(match series_table.reader(name) {
        Some(reader) => match Statement::try_from(statement_expr) {
            Ok(statement) => match reader.query(statement).rows_async().await {
                Ok(rows) => Box::new(warp::reply::json::<JsonRows>(&rows.into())),
                _ => Box::new(StatusCode::INTERNAL_SERVER_ERROR),
            },
            _ => Box::new(StatusCode::BAD_REQUEST),
        },
        _ => Box::new(StatusCode::NOT_FOUND),
    })
}