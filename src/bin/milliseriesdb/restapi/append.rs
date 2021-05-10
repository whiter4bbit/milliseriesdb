use milliseriesdb::storage::{Entry, SeriesTable, Compression};
use warp::{Reply, http::StatusCode};
use serde_derive::Deserialize;
use std::sync::Arc;
use std::convert::Infallible;

#[derive(Deserialize)]
pub struct JsonEntries {
    pub entries: Vec<Entry>,
}

pub async fn append(
    name: String,
    entries: JsonEntries,
    series_table: Arc<SeriesTable>,
) -> Result<impl Reply, Infallible> {
    Ok(match series_table.writer(name) {
        Some(writer) => {
            let result = writer.append_async(entries.entries, Compression::Delta);
            match result.await {
                Ok(()) => StatusCode::OK,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }
        _ => StatusCode::NOT_FOUND,
    })
}