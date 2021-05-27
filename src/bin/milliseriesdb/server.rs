use milliseriesdb::storage::SeriesTable;
use milliseriesdb::restapi;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use warp::Filter;

pub async fn start_server(series_table: Arc<SeriesTable>, addr: SocketAddr) -> io::Result<()> {
    let server_api = restapi::create::filter(series_table.clone())
        .or(restapi::append::filter(series_table.clone()))
        .or(restapi::query::filter(series_table.clone()))
        .or(restapi::export::filter(series_table.clone()))
        .or(restapi::restore::filter(series_table.clone()));

    warp::serve(server_api).run(addr).await;
    Ok(())
}
