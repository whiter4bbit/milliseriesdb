use chrono::{TimeZone, Utc};
use milliseriesdb::db::{
    execute_query_async, Aggregation, Compression, Entry, Query, QueryExpr, Row, SeriesTable,
};
use serde_derive::{Deserialize, Serialize};
use std::convert::{Infallible, TryFrom};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use warp::{http::StatusCode, Filter};

mod restapi {
    use super::*;
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
                        timestamp: Utc.timestamp_millis(row.ts as i64).to_string(),
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

    pub fn with_series_table(
        series_table: Arc<SeriesTable>,
    ) -> impl Filter<Extract = (Arc<SeriesTable>,), Error = Infallible> + Clone {
        warp::any().map(move || series_table.clone())
    }

    pub async fn create(
        id: String,
        series_table: Arc<SeriesTable>,
    ) -> Result<impl warp::Reply, Infallible> {
        Ok(match series_table.create(id) {
            Ok(()) => StatusCode::CREATED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })
    }

    pub async fn append(
        id: String,
        entries: JsonEntries,
        series_table: Arc<SeriesTable>,
    ) -> Result<impl warp::Reply, Infallible> {
        Ok(match series_table.writer(id) {
            Some(writer) => {
                let result = writer.append_async(entries.entries, Compression::Delta);
                match result.await {
                    Ok(()) => StatusCode::OK,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                }
            }
            _ => StatusCode::NOT_FOUND,
        })
    }

    pub async fn query(
        id: String,
        query_expr: QueryExpr,
        series_table: Arc<SeriesTable>,
    ) -> Result<Box<dyn warp::Reply>, Infallible> {
        Ok(match series_table.reader(id) {
            Some(reader) => match Query::try_from(query_expr) {
                Ok(query) => match execute_query_async(query, reader).await {
                    Ok(rows) => Box::new(warp::reply::json(&JsonRows::from_rows(rows))),
                    _ => Box::new(StatusCode::INTERNAL_SERVER_ERROR),
                },
                _ => Box::new(StatusCode::BAD_REQUEST),
            },
            _ => Box::new(StatusCode::NOT_FOUND),
        })
    }
}

pub async fn start_server(series_table: Arc<SeriesTable>, addr: SocketAddr) -> io::Result<()> {
    let create_series = warp::path!("series" / String)
        .and(warp::put())
        .and(restapi::with_series_table(series_table.clone()))
        .and_then(restapi::create);

    let append_to_series = warp::path!("series" / String)
        .and(warp::post())
        .and(warp::body::json())
        .and(restapi::with_series_table(series_table.clone()))
        .and_then(restapi::append);

    let query_series = warp::path!("series" / String)
        .and(warp::get())
        .and(warp::query::<QueryExpr>())
        .and(restapi::with_series_table(series_table.clone()))
        .and_then(restapi::query);

    let server_api = create_series.or(append_to_series).or(query_series);

    Ok(warp::serve(server_api).run(addr).await)
}
