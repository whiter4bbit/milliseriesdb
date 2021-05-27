use crate::buffering::BufferingBuilder;
use crate::csv;
use crate::storage::error::Error;
use crate::storage::{Entry, SeriesTable, SeriesWriter};
use bytes::buf::Buf;
use futures::{Stream, StreamExt};
use std::sync::Arc;
use warp::reject::Rejection;
use warp::{http::StatusCode, Filter};

enum ImportError {
    Parse(String),
    Internal(Error),
}

impl From<Error> for ImportError {
    fn from(err: Error) -> ImportError {
        ImportError::Internal(err)
    }
}

impl From<Error> for Rejection {
    fn from(err: Error) -> Rejection {
        super::error::internal(err)
    }
}

impl From<ImportError> for Rejection {
    fn from(err: ImportError) -> Rejection {
        match err {
            ImportError::Parse(reason) => super::error::bad_request(reason),
            ImportError::Internal(reason) => super::error::internal(reason),
        }
    }
}

async fn import_entries<S, B>(body: S, writer: Arc<SeriesWriter>) -> Result<(), ImportError>
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
            let batch = batch.map_err(|_| ImportError::Parse("invalid csv".to_owned()))?;

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
    let series_name = series_table.create_temp()?;

    let writer = series_table.writer(&series_name).ok_or_else(|| {
        Error::Other(format!(
            "can not open temp series: {}",
            &series_name
        ))
    })?;

    import_entries(body, writer).await?;

    if !series_table.rename(&series_name, &name)? {
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

        let resp = warp::test::request()
            .method("POST")
            .path("/series/t/restore")
            .body("1; 12.3\n3; 13.4\n")
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

        let resp = warp::test::request()
            .method("POST")
            .path("/series/t/restore")
            .body("1xx 12.3\n3; 13.4\n")
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::BAD_REQUEST, resp.status());

        Ok(())
    }
}
