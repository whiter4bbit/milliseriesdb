use milliseriesdb::storage::{Entry, SeriesReader, SeriesTable};
use milliseriesdb::csv;
use hyper::body::{Body, Bytes, Sender};
use warp::{Reply, http::StatusCode, http::Response};
use std::sync::Arc;
use std::convert::Infallible;
use std::io;

async fn export_entries(reader: Arc<SeriesReader>, sender: &mut Sender) -> io::Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Entry>>(1);

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
        let data= Bytes::from(csv::to_csv(&entries));
        
        sender.send_data(data).await.map_err(|e| {
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
) -> Result<impl Reply, Infallible> {
    series_table
        .reader(name)
        .map(|reader| {
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
            Ok(Response::builder().body(body))
        })
        .unwrap_or_else(|| {
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty()))
        })
}