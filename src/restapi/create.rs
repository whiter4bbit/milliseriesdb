use crate::storage::SeriesTable;
use std::sync::Arc;
use warp::http::StatusCode;
use warp::reject::Rejection;
use warp::Filter;

async fn create(name: String, series_table: Arc<SeriesTable>) -> Result<StatusCode, Rejection> {
    series_table
        .create(&name)
        .map(|_| StatusCode::CREATED)
        .map_err(|e| super::error::internal(e))
}

pub fn filter(series_table: Arc<SeriesTable>) -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
    warp::path!("series" / String)
        .and(warp::put())
        .and(super::with_series_table(series_table.clone()))
        .and_then(self::create)
        .recover(super::error::handle)
        .boxed()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::failpoints::Failpoints;
    use crate::storage::error::Error;
    use crate::storage::series_table;

    #[tokio::test]
    async fn test_create() -> Result<(), Error> {
        let fp = Arc::new(Failpoints::create());
        let series_table = series_table::test::create_with_failpoints(fp.clone())?;

        let resp = warp::test::request()
            .method("PUT")
            .path("/series/t")
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::CREATED, resp.status());

        fp.on("series_table::create");

        let resp = warp::test::request()
            .method("PUT")
            .path("/series/co2")
            .reply(&super::filter(series_table.series_table.clone()))
            .await;

        assert_eq!(StatusCode::INTERNAL_SERVER_ERROR, resp.status());

        fp.off("series_table::create");

        Ok(())
    }
}
