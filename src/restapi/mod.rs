use crate::storage::SeriesTable;
use std::convert::Infallible;
use std::sync::Arc;
use warp::Filter;

pub mod create;
pub mod append;
pub mod query;
pub mod export;
pub mod restore;
mod error;

pub fn with_series_table(
    series_table: Arc<SeriesTable>,
) -> impl Filter<Extract = (Arc<SeriesTable>,), Error = Infallible> + Clone {
    warp::any().map(move || series_table.clone())
}