use milliseriesdb::db::{Entry, DB};
use serde_derive::{Deserialize, Serialize};
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use warp::{http::StatusCode, Filter};

struct DBGuard {
    db: DB,
}

impl DBGuard {
    fn with_db(&self) -> impl Filter<Extract = (DB,), Error = Infallible> + Clone {
        let db = self.db.clone();
        warp::any().map(move || db.clone())
    }
}

#[derive(Deserialize, Serialize)]
pub struct JsonEntry {
    pub timestamp: u64,
    pub value: f64,
}

#[derive(Deserialize, Serialize)]
pub struct JsonEntries {
    pub entries: Vec<JsonEntry>,
}

impl JsonEntries {
    pub fn to_entries(&self) -> Vec<Entry> {
        self.entries
            .iter()
            .map(|json| Entry {
                ts: json.timestamp,
                value: json.value,
            })
            .collect()
    }
}

#[derive(Deserialize, Serialize)]
pub struct JsonQuery {
    pub from: String,
    pub limit: Option<usize>,
}

impl JsonQuery {
    pub fn from_timestamp(&self) -> Option<u64> {
        self.from.parse::<u64>().ok()
    }
}

mod handlers {
    use super::*;
    pub async fn create_handler(id: String, db: DB) -> Result<impl warp::Reply, Infallible> {
        let result = tokio::task::spawn_blocking(move || db.clone().create_series(&id).map(|_| ()));

        Ok(match result.await {
            Ok(Ok(())) => StatusCode::CREATED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })
    }
    pub async fn append_handler(id: String, entries: JsonEntries, db: DB) -> Result<impl warp::Reply, Infallible> {
        let result = tokio::task::spawn_blocking(move || match db.clone().get_series(id) {
            Ok(Some(series)) => {
                let mut writer = series.writer();
                writer.append(&entries.to_entries())?;
                Ok(Some(()))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        });

        Ok(match result.await {
            Ok(Ok(Some(_))) => StatusCode::OK,
            Ok(Ok(None)) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })
    }
    pub async fn query_handler(id: String, query: JsonQuery, db: DB) -> Result<Box<dyn warp::Reply>, Infallible> {
        fn query_internal(id: String, from_timestamp: u64, limit: usize, db: DB) -> io::Result<Option<JsonEntries>> {
            let series = { db.clone().get_series(id) }?;
            match series {
                Some(series) => {
                    let iterator = series.iterator(from_timestamp)?;
                    let mut entries: Vec<JsonEntry> = Vec::new();
                    for entry in iterator.take(limit) {
                        let entry = entry?;
                        entries.push(JsonEntry {
                            timestamp: entry.ts,
                            value: entry.value,
                        });
                    }
                    Ok(Some(JsonEntries { entries: entries }))
                }
                None => Ok(None),
            }
        }
        Ok(match (query.from_timestamp(), query.limit.unwrap_or(1000)) {
            (Some(from_timestamp), limit) => {
                let result = tokio::task::spawn_blocking(move || query_internal(id, from_timestamp, limit, db));
                match result.await {
                    Ok(Ok(None)) => Box::new(StatusCode::NOT_FOUND),
                    Ok(Ok(Some(entries))) => Box::new(warp::reply::json(&entries)),
                    _ => Box::new(StatusCode::INTERNAL_SERVER_ERROR),
                }
            }
            _ => Box::new(StatusCode::BAD_REQUEST),
        })
    }
}

pub async fn start_server(db: DB, addr: SocketAddr) -> io::Result<()> {
    let db = DBGuard { db: db };

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
        .and(warp::query::<JsonQuery>())
        .and(db.with_db())
        .and_then(handlers::query_handler);

    let server_api = create_series.or(append_to_series).or(query_series);

    Ok(warp::serve(server_api).run(addr).await)
}
