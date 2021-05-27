use milliseriesdb::storage::{Entry, SeriesTable};
use serde_derive::Deserialize;
use std::sync::Arc;
use warp::http::StatusCode;
use warp::reject::{Reject, Rejection};
use warp::Filter;

#[derive(Deserialize)]
pub struct JsonEntries {
    pub entries: Vec<Entry>,
}

#[derive(Debug)]
struct AppendError {}

impl Reject for AppendError {}

async fn append(
    name: String,
    entries: JsonEntries,
    series_table: Arc<SeriesTable>,
) -> Result<StatusCode, Rejection> {
    let writer = series_table.writer(name).ok_or(warp::reject::not_found())?;
    writer
        .append_async(entries.entries)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|_| warp::reject::custom(AppendError {}))
}

pub fn filter(series_table: Arc<SeriesTable>) -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
    warp::path!("series" / String)
        .and(warp::post())
        .and(warp::body::json())
        .and(super::with_series_table(series_table.clone()))
        .and_then(self::append)
        .boxed()
}
