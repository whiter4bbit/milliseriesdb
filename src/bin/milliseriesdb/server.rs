use milliseriesdb::db::{Aggregation, Entry, Executor, Query, QueryExpr, Row, DB, AsyncDB, Compression};
use serde_derive::{Deserialize, Serialize};
use std::convert::{Infallible, TryFrom};
use std::io;
use std::net::SocketAddr;
use warp::{http::StatusCode, Filter};
use chrono::{Utc, TimeZone};

struct DBVar {
    db: AsyncDB,
}

impl DBVar {
    fn with_db(&self) -> impl Filter<Extract = (AsyncDB,), Error = Infallible> + Clone {
        let db = self.db.clone();
        warp::any().map(move || db.clone())
    }
}

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

mod handlers {
    use super::*;
    pub async fn create_handler(id: String, db: AsyncDB) -> Result<impl warp::Reply, Infallible> {
        Ok(match db.clone().create_series(id).await {
            Ok(()) => StatusCode::CREATED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })
    }
    pub async fn append_handler(id: String, entries: JsonEntries, db: AsyncDB) -> Result<impl warp::Reply, Infallible> {
        Ok(match db.writer(id) {
            Some(writer) => match writer.append_async(entries.entries, Compression::Delta).await {
                Ok(()) => StatusCode::OK,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            _ => StatusCode::NOT_FOUND
        })
    }
    pub async fn query_handler(id: String, query_expr: QueryExpr, db: AsyncDB) -> Result<Box<dyn warp::Reply>, Infallible> {
        fn query_internal(id: String, query: Query, db: AsyncDB) -> io::Result<Option<Vec<Row>>> {
            match db.clone().reader(id) {
                Some(reader) => Ok(Some(Executor::new(&query).execute(reader)?)),
                None => Ok(None),
            }
        }
        Ok(match Query::try_from(query_expr) {
            Ok(query) => {
                let result = tokio::task::spawn_blocking(move || query_internal(id, query, db));
                match result.await {
                    Ok(Ok(None)) => Box::new(StatusCode::NOT_FOUND),
                    Ok(Ok(Some(rows))) => Box::new(warp::reply::json(&JsonRows::from_rows(rows))),
                    _ => Box::new(StatusCode::INTERNAL_SERVER_ERROR),
                }
            }
            _ => Box::new(StatusCode::BAD_REQUEST),
        })
    }
}

pub async fn start_server(db: DB, addr: SocketAddr) -> io::Result<()> {
    let db = DBVar { db: AsyncDB::create(db) };

    let create_series = warp::path!("series" / String)
        .and(warp::put())
        .and(db.with_db())
        .and_then(handlers::create_handler);

    let append_to_series = warp::path!("series" / String)
        .and(warp::post())
        .and(warp::body::json())
        .and(db.with_db())
        .and_then(handlers::append_handler);

    let query_series = warp::path!("series" / String)
        .and(warp::get())
        .and(warp::query::<QueryExpr>())
        .and(db.with_db())
        .and_then(handlers::query_handler);

    let server_api = create_series.or(append_to_series).or(query_series);

    Ok(warp::serve(server_api).run(addr).await)
}
