use milliseriesdb::db::{Entry, DB, Executor, Row, Query, QueryExpression};
use serde_derive::{Deserialize, Serialize};
use std::convert::{TryFrom, Infallible};
use std::io;
use std::net::SocketAddr;
use warp::{http::StatusCode, Filter};

struct DBVar {
    db: DB,
}

impl DBVar {
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
    pub async fn query_handler(id: String, query_expr: QueryExpression, db: DB) -> Result<Box<dyn warp::Reply>, Infallible> {
        fn query_internal(id: String, query: Query, db: DB) -> io::Result<Option<Vec<Row>>> {
            match db.clone().get_series(id)? {
                Some(series) => Ok(Some(Executor::new(&query).execute(series)?)),
                None => Ok(None),
            }
        }
        Ok(match Query::try_from(query_expr) {
            Ok(query) => {
                let result = tokio::task::spawn_blocking(move || query_internal(id, query, db));
                match result.await {
                    Ok(Ok(None)) => Box::new(StatusCode::NOT_FOUND),
                    Ok(Ok(Some(rows))) => Box::new(warp::reply::json(&rows)),
                    _ => Box::new(StatusCode::INTERNAL_SERVER_ERROR),
                }
            }
            _ => Box::new(StatusCode::BAD_REQUEST),
        })
    }
}

pub async fn start_server(db: DB, addr: SocketAddr) -> io::Result<()> {
    let db = DBVar { db: db };

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
        .and(warp::query::<QueryExpression>())
        .and(db.with_db())
        .and_then(handlers::query_handler);

    let server_api = create_series.or(append_to_series).or(query_series);

    Ok(warp::serve(server_api).run(addr).await)
}