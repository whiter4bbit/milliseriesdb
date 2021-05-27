use hyper::body::{Body, Bytes, Sender};
use milliseriesdb::buffering::BufferingBuilder;
use milliseriesdb::storage::{error::Error, Entry, SeriesReader, SeriesTable};
use std::io;
use std::sync::Arc;
use warp::reject::{Reject, Rejection};
use warp::Filter;
use warp::http::Response;

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

#[derive(Debug)]
struct UnexpectedError {}

impl Reject for UnexpectedError {}

async fn export(
    name: String,
    series_table: Arc<SeriesTable>,
) -> Result<Response<Body>, Rejection> {
    let reader = series_table
        .reader(name)
        .ok_or_else(|| warp::reject::not_found())?;

    let (mut sender, body) = Body::channel();

    tokio::spawn(async move {
        export_entries(reader, &mut sender)
            .await
            .unwrap_or_else(|e| {
                sender.abort();
                log::warn!("Can not export the entries: {:?}", e);
                ()
            })
    });

    Response::builder()
        .body(body)
        .map_err(|_| warp::reject::custom(UnexpectedError {}))
}

pub fn filter(series_table: Arc<SeriesTable>) -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
    warp::path!("series" / String / "export")
        .and(warp::get())
        .and(super::with_series_table(series_table.clone()))
        .and_then(self::export)
        .boxed()
}
