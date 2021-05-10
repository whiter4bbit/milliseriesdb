use milliseriesdb::storage::SeriesTable;
use std::convert::Infallible;
use std::sync::Arc;
use warp::Filter;

mod create;
mod append;
mod query;
mod export;
mod restore;

pub use create::create;
pub use append::append;
pub use query::query;
pub use export::export;
pub use restore::restore;

pub fn with_series_table(
    series_table: Arc<SeriesTable>,
) -> impl Filter<Extract = (Arc<SeriesTable>,), Error = Infallible> + Clone {
    warp::any().map(move || series_table.clone())
}