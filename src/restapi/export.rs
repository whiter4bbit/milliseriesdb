use crate::buffering::BufferingBuilder;
use crate::storage::{error::Error, Entry, SeriesReader, SeriesTable};
use hyper::body::{Body, Bytes, Sender};
use std::io;
use std::sync::Arc;
use warp::http::Response;
use warp::reject::Rejection;
use warp::Filter;

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

async fn export(name: String, series_table: Arc<SeriesTable>) -> Result<Response<Body>, Rejection> {
    let reader = series_table
        .reader(&name)
        .ok_or_else(|| super::error::not_found(&name))?;

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
        .map_err(|_| super::error::internal(Error::Other("can not build the request".to_owned())))
}

pub fn filter(series_table: Arc<SeriesTable>) -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
    warp::path!("series" / String / "export")
        .and(warp::get())
        .and(super::with_series_table(series_table.clone()))
        .and_then(self::export)
        .recover(super::error::handle)
        .boxed()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::failpoints::Failpoints;
    use crate::storage::error::Error;
    use crate::storage::series_table;
    use warp::http::StatusCode;

    #[tokio::test]
    async fn test_export() -> Result<(), Error> {
        let fp = Arc::new(Failpoints::create());
        let series_table = series_table::test::create_with_failpoints(fp.clone())?;

        let resp = warp::test::request()
            .method("GET")
            .path("/series/t/export")
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::NOT_FOUND, resp.status());

        series_table.create("t")?;

        series_table.writer("t").unwrap().append(&vec![
            Entry {ts: 1, value: 1.2},
            Entry {ts: 2, value: 3.1},
        ])?;

        let resp = warp::test::request()
            .method("GET")
            .path("/series/t/export")
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::OK, resp.status());
        assert_eq!("1; 1.20\n2; 3.10\n", std::str::from_utf8(&resp.body()).unwrap());

        Ok(())
    }
}
