use crate::storage::{Entry, SeriesTable};
use serde_derive::Deserialize;
use std::sync::Arc;
use warp::http::StatusCode;
use warp::reject::Rejection;
use warp::Filter;

#[derive(Deserialize)]
pub struct JsonEntries {
    pub entries: Vec<Entry>,
}

async fn append(
    name: String,
    entries: JsonEntries,
    series_table: Arc<SeriesTable>,
) -> Result<StatusCode, Rejection> {
    let writer = series_table
        .writer(&name)
        .ok_or_else(|| super::error::not_found(&name))?;
    writer
        .append_async(entries.entries)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|err| super::error::internal(err))
}

pub fn filter(series_table: Arc<SeriesTable>) -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
    warp::path!("series" / String)
        .and(warp::post())
        .and(warp::body::json())
        .and(super::with_series_table(series_table.clone()))
        .and_then(self::append)
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
    async fn test_append() -> Result<(), Error> {
        let fp = Arc::new(Failpoints::create());
        let series_table = series_table::test::create_with_failpoints(fp.clone())?;

        let json_valid = "
        {
            \"entries\": [
                {
                    \"ts\": 21,
                    \"value\": 81.0
                },
                {
                    \"ts\": 23,
                    \"value\": 84.0
                },
                {
                    \"ts\": 26,
                    \"value\": 90.0
                }
            ]
        }
        ";

        let json_invalid = "
        {
            \"entries\": [
                {
                    \"ts\": 21,
                    \"value\": 81.0
                },
        }
        ";

        let resp = warp::test::request()
            .method("POST")
            .path("/series/t")
            .body(json_valid)
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::NOT_FOUND, resp.status());

        series_table.create("t")?;

        let resp = warp::test::request()
            .method("POST")
            .path("/series/t")
            .body(json_valid)
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::OK, resp.status());

        let resp = warp::test::request()
            .method("POST")
            .path("/series/t")
            .body(json_invalid)
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::BAD_REQUEST, resp.status());

        Ok(())
    }
}
