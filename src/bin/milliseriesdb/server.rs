use milliseriesdb::query::StatementExpr;
use milliseriesdb::storage::SeriesTable;
use std::net::SocketAddr;
use std::sync::Arc;
use std::io;
use warp::Filter;
use super::restapi;

pub async fn start_server(series_table: Arc<SeriesTable>, addr: SocketAddr) -> io::Result<()> {
    let create_series = warp::path!("series" / String)
        .and(warp::put())
        .and(restapi::with_series_table(series_table.clone()))
        .and_then(restapi::create);

    let append_to_series = warp::path!("series" / String)
        .and(warp::post())
        .and(warp::body::json())
        .and(restapi::with_series_table(series_table.clone()))
        .and_then(restapi::append);

    let query_series = warp::path!("series" / String)
        .and(warp::get())
        .and(warp::query::<StatementExpr>())
        .and(restapi::with_series_table(series_table.clone()))
        .and_then(restapi::query);

    let export_series = warp::path!("series" / String / "export")
        .and(warp::get())
        .and(restapi::with_series_table(series_table.clone()))
        .and_then(restapi::export);

    let restore_series = warp::path!("series" / String / "restore")
        .and(warp::post())
        .and(restapi::with_series_table(series_table.clone()))
        .and(warp::body::stream())
        .and_then(restapi::restore);

    let server_api = create_series
        .or(append_to_series)
        .or(query_series)
        .or(export_series)
        .or(restore_series);

    warp::serve(server_api).run(addr).await;
    Ok(())
}
