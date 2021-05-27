use crate::buffering::BufferingBuilder;
use crate::csv;
use crate::storage::error::Error;
use crate::storage::{Entry, SeriesTable, SeriesWriter};
use bytes::buf::Buf;
use futures::{Stream, StreamExt};
use std::io;
use std::sync::Arc;
use warp::reject::Rejection;
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
        .map_err(|err| super::error::internal(err))?;

    let writer = series_table.writer(&series_name).ok_or_else(|| {
        super::error::internal(Error::Other(format!(
            "can not open temp series: {}",
            &series_name
        )))
    })?;

    import_entries(body, writer)
        .await
        .map_err(|err| super::error::internal(Error::Io(err)))?;

    let renamed = series_table
        .rename(&series_name, &name)
        .map_err(|err| super::error::internal(err))?;

    if !renamed {
        #[rustfmt::skip]
        log::warn!("can not restore series '{}' -> '{}', conflict", &series_name, &name);
        return Err(super::error::conflict(&name));
    }

    Ok(StatusCode::OK)
}

pub fn filter(series_table: Arc<SeriesTable>) -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
    warp::path!("series" / String / "restore")
        .and(warp::post())
        .and(super::with_series_table(series_table.clone()))
        .and(warp::body::stream())
        .and_then(self::restore)
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

        let valid_csv = "1; 12.3\n3; 13.4\n";

        let resp = warp::test::request()
            .method("POST")
            .path("/series/t/restore")
            .body(valid_csv)
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::OK, resp.status());

        let entries = series_table
            .reader("t")
            .unwrap()
            .iterator(0)?
            .collect::<Result<Vec<Entry>, Error>>()?;

        #[rustfmt::skip]
        assert_eq!(
            vec![
                Entry { ts: 1, value: 12.3 }, 
                Entry { ts: 3, value: 13.4 },
            ],
            entries
        );

        Ok(())
    }
}
