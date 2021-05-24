use clap::clap_app;
use milliseriesdb::storage::{file_system, env, series_table};
use std::sync::Arc;

mod server;
mod restapi;

#[tokio::main]
async fn main() {
    stderrlog::new()
        .module(module_path!())
        .verbosity(4)
        .init()
        .unwrap();

    let matches = clap_app!(milliseriesdb =>
        (@setting SubcommandRequiredElseHelp)
        (@arg path: -p <PATH> --path "path to database")        
        (@subcommand server =>
            (about: "start the server")
            (@arg addr: -a <ADDR> --addr default_value("127.0.0.1:8080") "listen address, like 0.0.0.0:8080")
        )
    )
    .get_matches();

    let fs = file_system::open(matches.value_of("path").unwrap()).unwrap();

    let env = env::create(fs);
    let series_table = series_table::create(env).unwrap();

    match matches.subcommand() {
        ("server", Some(sub_match)) => server::start_server(
            Arc::new(series_table),
            sub_match.value_of("addr").unwrap().parse().unwrap(),
        )
        .await
        .unwrap(),
        _ => unreachable!(),
    }
}
