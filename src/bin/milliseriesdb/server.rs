use bytes::buf::Buf;
use chrono::{TimeZone, Utc};
use futures::{Stream, StreamExt};
use hyper::body::{Body, Bytes, Sender};
use milliseriesdb::buffering::BufferingBuilder;
use milliseriesdb::csv;
use milliseriesdb::query::{Aggregation, QueryBuilder, Row, Statement, StatementExpr};
use milliseriesdb::storage::{error::Error, Entry, SeriesReader, SeriesTable, SeriesWriter};
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
        name: String,
        series_table: Arc<SeriesTable>,
    ) -> Result<impl warp::Reply, Infallible> {
        Ok(match series_table.create(name) {
            Ok(()) => StatusCode::CREATED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })
    }

    pub async fn append(
        name: String,
        entries: JsonEntries,
        series_table: Arc<SeriesTable>,
    ) -> Result<impl warp::Reply, Infallible> {
        Ok(match series_table.writer(name) {
            Some(writer) => match writer.append_async(entries.entries).await {
                Ok(()) => StatusCode::OK,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            _ => StatusCode::NOT_FOUND,
        })
    }

    pub async fn query(
        name: String,
        statement_expr: StatementExpr,
        series_table: Arc<SeriesTable>,
    ) -> Result<Box<dyn warp::Reply>, Infallible> {
        Ok(match series_table.reader(name) {
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Entry>>(1);

        tokio::task::spawn_blocking(move || {
            for batch in reader
                .iterator(0)?
                .buffering::<Result<Vec<Entry>, Error>>(1024)
            {
                tx.blocking_send(batch?).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("can not send the data from the reading thread {:?}", e),
                    )
                })?;
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

    async fn import_entries<S, B>(body: S, writer: Arc<SeriesWriter>) -> io::Result<()>
    where
        S: Stream<Item = Result<B, warp::Error>> + Send + 'static + Unpin,
        B: Buf + Send,
    {
        let mut csv = csv::ChunkedReader::new();
        let mut body = body.boxed();
        let mut entries_count = 0usize;
        while let Some(Ok(mut chunk)) = body.next().await {
            for batch in csv
                .read(&mut chunk)
                .buffering::<Result<Vec<Entry>, ()>>(1024 * 1024)
            {
                let batch = batch
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "Can not read entries"))?;

                entries_count += batch.len();

                writer.append_with_batch_size_async(10, batch).await?;

                log::debug!("Imported {} entries", entries_count);
            }
        }
        log::debug!("Import completed, imported {} entries", entries_count);
        Ok(())
    }

    pub async fn restore<S, B>(
        name: String,
        series_table: Arc<SeriesTable>,
        body: S,
    ) -> Result<impl warp::Reply, Infallible>
    where
        S: Stream<Item = Result<B, warp::Error>> + Send + 'static + Unpin,
        B: Buf + Send,
    {
        let series_name = match series_table.create_temp() {
            Ok(series_name) => series_name,
            Err(_) => return Ok(StatusCode::INTERNAL_SERVER_ERROR),
        };

        let writer = match series_table.writer(&series_name) {
            Some(writer) => writer,
            None => return Ok(StatusCode::INTERNAL_SERVER_ERROR),
        };

        if let Err(err) = restapi::import_entries(body, writer).await {
            log::warn!("can not import series: {:?}", err);
            return Ok(StatusCode::INTERNAL_SERVER_ERROR);
        }

        match series_table.rename(&series_name, &name) {
            Ok(false) => {
                log::warn!(
                    "can not restore series '{}' -> '{}', conflict",
                    &series_name,
                    &name
                );
                return Ok(StatusCode::CONFLICT);
            }
            Err(err) => {
                log::warn!(
                    "can not restore series '{}' -> '{}': {:?}",
                    &series_name,
                    &name,
                    err
                );
                return Ok(StatusCode::INTERNAL_SERVER_ERROR);
            }
            _ => (),
        };

        Ok(StatusCode::OK)
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

    let restore_series = warp::path!("series" / String / "restore")
        .and(warp::post())
        .and(restapi::with_series_table(series_table.clone()))
        .and(warp::body::stream())
        .and_then(restapi::restore);

    let server_api = create_series
        .or(append_to_series)
        .or(query_series)
        .or(export_series)
        .or(restore_series);

    warp::serve(server_api).run(addr).await;
    Ok(())
}
