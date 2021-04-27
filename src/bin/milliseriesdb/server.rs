use milliseriesdb::db::{Entry, DB};
use serde_derive::{Deserialize, Serialize};
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use warp::{http::StatusCode, Filter};

struct DBGuard {
    db: Arc<Mutex<DB>>,
}

impl DBGuard {
    fn with_db(&self) -> impl Filter<Extract = (Arc<Mutex<DB>>,), Error = Infallible> + Clone {
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
    pub fn create_handler(id: String, db: Arc<Mutex<DB>>) -> impl warp::Reply {
        let mut db = db.lock().unwrap();

        match db.create_series(&id) {
            Ok(_) => StatusCode::CREATED,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
    pub fn append_handler(id: String, entries: JsonEntries, db: Arc<Mutex<DB>>) -> impl warp::Reply {
        fn append(id: String, entries: JsonEntries, db: Arc<Mutex<DB>>) -> io::Result<Option<()>> {
            let series = {
                let mut db = db.lock().unwrap();
                db.get_series(id)
            }?;
            match series {
                Some(series) => {
                    let mut series = series.lock().unwrap();
                    series.append(&entries.to_entries())?;
                    Ok(Some(()))
                }
                None => Ok(None),
            }
        }

        match append(id, entries, db) {
            Ok(None) => StatusCode::NOT_FOUND,
            Ok(Some(_)) => StatusCode::OK,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
    pub fn query_handler(id: String, query: JsonQuery, db: Arc<Mutex<DB>>) -> Box<dyn warp::Reply> {
        fn query_internal(id: String, from_timestamp: u64, limit: usize, db: Arc<Mutex<DB>>) -> io::Result<Option<JsonEntries>> {
            let series = {
                let mut db = db.lock().unwrap();
                db.get_series(id)
            }?;
            match series {
                Some(series) => {
                    let iterator = {
                        let mut series = series.lock().unwrap();
                        series.iterator(from_timestamp)
                    }?;
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
        
        match (query.from_timestamp(), query.limit.unwrap_or(1000)) {
            (Some(from_timestamp), limit) => match query_internal(id, from_timestamp, limit, db) {
                Ok(None) => Box::new(StatusCode::NOT_FOUND),
                Ok(Some(entries)) => Box::new(warp::reply::json(&entries)),
                Err(_) => Box::new(StatusCode::INTERNAL_SERVER_ERROR),
            },
            _ => Box::new(StatusCode::BAD_REQUEST)
        }
    }
}

pub async fn start_server(db: DB, addr: SocketAddr) -> io::Result<()> {
    let db = DBGuard {
        db: Arc::new(Mutex::new(db)),
    };

    let create_series = warp::path!("series" / String)
        .and(warp::put())
        .and(db.with_db())
        .map(handlers::create_handler);

    let append_to_series = warp::path!("series" / String)
        .and(warp::post())
        .and(warp::body::json())
        .and(db.with_db())
        .map(handlers::append_handler);

    let query_series = warp::path!("series" / String)
        .and(warp::get())
        .and(warp::query::<JsonQuery>())
        .and(db.with_db())
        .map(handlers::query_handler);

    let server_api = create_series.or(append_to_series).or(query_series);

    Ok(warp::serve(server_api).run(addr).await)
}

// #[tokio::main]
// async fn main() {
//     let mut args = env::args();
//     args.next();

//     match (args.next(), args.next().and_then(|port| port.parse::<u16>().ok())) {
//         (Some(base_path), Some(port)) => start_server(base_path, port).await.unwrap(),
//         _ => exit(1),
//     }
// }
