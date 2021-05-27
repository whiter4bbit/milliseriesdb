use milliseriesdb::storage::error::Error;
use milliseriesdb::storage::SeriesTable;
use std::sync::Arc;
use warp::http::StatusCode;
use warp::reject::{Reject, Rejection};
use warp::Filter;

#[derive(Debug)]
struct CanNotCreateSeries {
    reason: Error,
}

impl Reject for CanNotCreateSeries {}

async fn create(name: String, series_table: Arc<SeriesTable>) -> Result<StatusCode, Rejection> {
    series_table
        .create(&name)
        .map(|_| StatusCode::CREATED)
        .map_err(|e| warp::reject::custom(CanNotCreateSeries { reason: e }))
}

pub fn filter(series_table: Arc<SeriesTable>) -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
    warp::path!("series" / String)
        .and(warp::put())
        .and(super::with_series_table(series_table.clone()))
        .and_then(self::create)
        .boxed()
}
