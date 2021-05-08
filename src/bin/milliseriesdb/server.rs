use chrono::{TimeZone, Utc};
use hyper::body::{Body, Bytes, Sender};
use milliseriesdb::query::{Aggregation, QueryBuilder, Row, Statement, StatementExpr};
use milliseriesdb::storage::{Compression, Entry, SeriesReader, SeriesTable};
use serde_derive::{Deserialize, Serialize};
use std::convert::{Infallible, TryFrom};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use warp::{http::Response, http::StatusCode, Filter};

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
        statement_expr: StatementExpr,
        series_table: Arc<SeriesTable>,
    ) -> Result<Box<dyn warp::Reply>, Infallible> {
        Ok(match series_table.reader(id) {
            Some(reader) => match Statement::try_from(statement_expr) {
                Ok(statement) => match reader.query(statement).rows_async().await {
                    Ok(rows) => Box::new(warp::reply::json(&JsonRows::from_rows(rows))),
                    _ => Box::new(StatusCode::INTERNAL_SERVER_ERROR),
                },
                _ => Box::new(StatusCode::BAD_REQUEST),
            },
            _ => Box::new(StatusCode::NOT_FOUND),
        })
    }

    async fn export_entries(reader: Arc<SeriesReader>, sender: &mut Sender) -> io::Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Entry>>(10);

        tokio::task::spawn_blocking(move || {
            let iter = &mut reader.iterator(0)?;

            loop {
                let buf = iter.take(1024).collect::<io::Result<Vec<Entry>>>()?;

                if buf.is_empty() {
                    break;
                } else {
                    tx.blocking_send(buf).map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("can not send the data from the reading thread {:?}", e),
                        )
                    })?;
                }
            }

            Ok::<(), io::Error>(())
        });

        while let Some(entries) = rx.recv().await {
            let format = entries
                .iter()
                .map(|entry| format!("{}; {:.2}\n", entry.ts, entry.value))
                .collect::<Vec<String>>()
                .join("");

            sender.send_data(Bytes::from(format)).await.map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("can not send the data chunk {:?}", e),
                )
            })?
        }

        Ok(())
    }

    pub async fn export(
        name: String,
        series_table: Arc<SeriesTable>,
    ) -> Result<impl warp::Reply, Infallible> {
        series_table
            .reader(name)
            .map(|reader| {
                let (mut sender, body) = Body::channel();

                tokio::spawn(async move {
                    restapi::export_entries(reader, &mut sender)
                        .await
                        .unwrap_or_else(|e| {
                            sender.abort();
                            log::warn!("Can not export the entries: {:?}", e);
                            ()
                        })
                });
                Ok(Response::builder().body(body))
            })
            .unwrap_or_else(|| {
                Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty()))
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
        .and(warp::query::<StatementExpr>())
        .and(restapi::with_series_table(series_table.clone()))
        .and_then(restapi::query);

    let export_series = warp::path!("series" / String / "export")
        .and(warp::get())
        .and(restapi::with_series_table(series_table.clone()))
        .and_then(restapi::export);

    let server_api = create_series
        .or(append_to_series)
        .or(query_series)
        .or(export_series);

    warp::serve(server_api).run(addr).await;
    Ok(())
}
