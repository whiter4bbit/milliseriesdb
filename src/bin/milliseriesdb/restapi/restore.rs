use bytes::buf::Buf;
use futures::{Stream, StreamExt};
use milliseriesdb::buffering::BufferingBuilder;
use milliseriesdb::csv;
use milliseriesdb::storage::{Entry, SeriesTable, SeriesWriter};
use std::io;
use std::sync::Arc;
use warp::reject::{Reject, Rejection};
use warp::{http::StatusCode, Filter};

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
            let batch =
                batch.map_err(|_| io::Error::new(io::ErrorKind::Other, "Can not read entries"))?;

            entries_count += batch.len();

            writer.append_with_batch_size_async(10, batch).await?;

            log::debug!("Imported {} entries", entries_count);
        }
    }
    log::debug!("Import completed, imported {} entries", entries_count);
    Ok(())
}

#[derive(Debug)]
struct CanNotRestoreSeries {}

impl Reject for CanNotRestoreSeries {}

#[derive(Debug)]
struct SeriesAlreadyExist {}

impl Reject for SeriesAlreadyExist {}

async fn restore<S, B>(
    name: String,
    series_table: Arc<SeriesTable>,
    body: S,
) -> Result<StatusCode, Rejection>
where
    S: Stream<Item = Result<B, warp::Error>> + Send + 'static + Unpin,
    B: Buf + Send,
{
    let series_name = series_table
        .create_temp()
        .map_err(|_| warp::reject::custom(CanNotRestoreSeries {}))?;

    let writer = series_table
        .writer(&series_name)
        .ok_or_else(|| warp::reject::custom(CanNotRestoreSeries {}))?;

    import_entries(body, writer)
        .await
        .map_err(|_| warp::reject::custom(CanNotRestoreSeries {}))?;

    let renamed = series_table
        .rename(&series_name, &name)
        .map_err(|_| warp::reject::custom(CanNotRestoreSeries {}))?;

    if !renamed {
        log::warn!(
            "can not restore series '{}' -> '{}', conflict",
            &series_name,
            &name
        );
        return Err(warp::reject::custom(SeriesAlreadyExist {}));
    }

    Ok(StatusCode::OK)
}

pub fn filter(series_table: Arc<SeriesTable>) -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
    warp::path!("series" / String / "restore")
        .and(warp::post())
        .and(super::with_series_table(series_table.clone()))
        .and(warp::body::stream())
        .and_then(self::restore)
        .boxed()
}
