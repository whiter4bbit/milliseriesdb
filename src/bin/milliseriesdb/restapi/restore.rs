use bytes::buf::Buf;
use futures::{Stream, StreamExt};
use milliseriesdb::csv;
use milliseriesdb::storage::{Compression, Entry, SeriesTable, SeriesWriterGuard};
use std::convert::Infallible;
use std::sync::Arc;
use std::io;
use warp::{Reply, http::StatusCode};

async fn import_entries<S, B>(body: S, writer: Arc<SeriesWriterGuard>) -> io::Result<()>
where
    S: Stream<Item = Result<B, warp::Error>> + Send + 'static + Unpin,
    B: Buf + Send,
{
    let mut csv = csv::ChunkedReader::new();
    let mut body = body.boxed();
    while let Some(Ok(mut chunk)) = body.next().await {
        let entries = &mut csv.read(&mut chunk);

        loop {
            let batch = entries
                .take(100)
                .collect::<Result<Vec<Entry>, ()>>()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Can not read entries"))?;

            if batch.is_empty() {
                break;
            }

            writer.append_async(batch, Compression::Delta).await?;
        }
    }
    Ok(())
}

pub async fn restore<S, B>(
    name: String,
    series_table: Arc<SeriesTable>,
    body: S,
) -> Result<impl Reply, Infallible>
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

    if let Err(err) = import_entries(body, writer).await {
        log::warn!("can not import series: {:?}", err);
        return Ok(StatusCode::INTERNAL_SERVER_ERROR);
    }

    match series_table.rename(&series_name, &name) {
        Ok(false) => {
            log::warn!("can not restore series '{}' -> '{}', conflict", &series_name, &name);
            return Ok(StatusCode::CONFLICT);
        },
        Err(err) => {
            log::warn!("can not restore series '{}' -> '{}': {:?}", &series_name, &name, err);
            return Ok(StatusCode::INTERNAL_SERVER_ERROR);
        },
        _ => (),
    };

    Ok(StatusCode::OK)
}