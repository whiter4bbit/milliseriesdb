use clap::clap_app;
use milliseriesdb::storage::{file_system, series_table, Compression, SyncMode};
use milliseriesdb::query::StatementExpr;
use std::sync::Arc;

mod append;
mod export;
mod server;
mod query;

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
        (@subcommand append =>
            (about: "append entries to the series")
            (@arg series: -s <SERIES> --series "id of the series")
            (@arg csv: -c <CSV> --csv "path to csv (timestamp; value)")
            (@arg batch_size: -b <BATCH> --batch default_value("100") "batch size")
            (@arg compression: -i <COMPRESSION> --compression default_value("delta") possible_value[none deflate delta] "compression")
        )
        (@subcommand export =>
            (about: "export entries from the series")
            (@arg series: -s <SERIES> --series "id of the series")
            (@arg csv: -c <CSV> --csv "path to destination csv (timestamp; value)")
            (@arg from: -f <FROM> --from "start timestamp")
        )
        (@subcommand query =>
            (about: "execute query on series")
            (@arg series: -s <SERIES> --series "name of the series")
            (@arg from: -f <FROM> --from "start timestamp expression")
            (@arg groupby: -g <GROUPBY> --groupby "group by")
            (@arg aggregators: -m <AGGREGATORS> --aggregators "aggregators")
            (@arg limit: -l <LIMIT> --limit "entries limit")
        )            
        (@subcommand server =>
            (about: "start the server")
            (@arg addr: -a <ADDR> --addr default_value("127.0.0.1:8080") "listen address, like 0.0.0.0:8080")
        )
    )
    .get_matches();

    let fs = file_system::open(matches.value_of("path").unwrap()).unwrap();

    let series_table = series_table::create(fs, SyncMode::Every(100)).unwrap();

    match matches.subcommand() {
        ("append", Some(sub_match)) => append::append(
            series_table,
            sub_match.value_of("series").unwrap(),
            sub_match.value_of("csv").unwrap(),
            sub_match
                .value_of("batch_size")
                .and_then(|from| from.parse::<usize>().ok())
                .unwrap(),
            match sub_match.value_of("compression").unwrap() {
                "deflate" => Compression::Deflate,
                "delta" => Compression::Delta,
                _ => Compression::None,
            },
        )
        .unwrap(),
                ("query", Some(sub_match)) => {
                        query::query(series_table, sub_match.value_of("series").unwrap(), StatementExpr {
                            from: sub_match.value_of("from").unwrap().to_string(),
                            group_by: sub_match.value_of("groupby").unwrap().to_string(),
                            aggregators: sub_match.value_of("aggregators").unwrap().to_string(),
                            limit: sub_match.value_of("limit").unwrap().to_string(),
                        }).unwrap()
                    }                    
        ("export", Some(sub_match)) => {
            export::export(
                series_table,
                sub_match.value_of("series").unwrap(),
                sub_match.value_of("csv").unwrap(),
                sub_match
                    .value_of("from")
                    .and_then(|from| from.parse::<u64>().ok())
                    .unwrap(),
            )
            .unwrap();
        }
        ("server", Some(sub_match)) => server::start_server(
            Arc::new(series_table),
            sub_match.value_of("addr").unwrap().parse().unwrap(),
        )
        .await
        .unwrap(),
        _ => unreachable!(),
    }
}
