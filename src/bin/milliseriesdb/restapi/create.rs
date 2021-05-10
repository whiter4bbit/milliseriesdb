use milliseriesdb::storage::SeriesTable;
use warp::{Reply, http::StatusCode};
use std::sync::Arc;
use std::convert::Infallible;

pub async fn create(
    name: String,
    series_table: Arc<SeriesTable>,
) -> Result<impl Reply, Infallible> {
    Ok(match series_table.create(name) {
        Ok(()) => StatusCode::CREATED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    })
}