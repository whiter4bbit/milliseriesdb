use clap::{App, AppSettings, Arg, SubCommand};
use milliseriesdb::db::{SyncMode, DB};

mod append;
mod export;
mod server;

#[tokio::main]
async fn main() {
    let matches = App::new("milliseriesdb")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("path")
                .short("p")
                .required(true)
                .takes_value(true)
                .help("path to database"),
        )
        .subcommand(
            SubCommand::with_name("append")
                .about("appends entries to the series")
                .arg(
                    Arg::with_name("series")
                        .short("s")
                        .required(true)
                        .takes_value(true)
                        .help("id of the series"),
                )
                .arg(
                    Arg::with_name("csv")
                        .short("c")
                        .required(true)
                        .takes_value(true)
                        .help("path to csv (timestamp; value)"),
                ),
        )
        .subcommand(
            SubCommand::with_name("export")
                .about("export entries from series")
                .arg(
                    Arg::with_name("series")
                        .short("s")
                        .required(true)
                        .takes_value(true)
                        .help("id of the series"),
                )
                .arg(
                    Arg::with_name("csv")
                        .short("c")
                        .required(true)
                        .takes_value(true)
                        .help("export destination"),
                )
                .arg(
                    Arg::with_name("from")
                        .short("f")
                        .required(true)
                        .takes_value(true)
                        .default_value("0")
                        .help("from timestamp"),
                ),
        )
        .subcommand(
            SubCommand::with_name("server").about("start rest server").arg(
                Arg::with_name("addr")
                    .short("a")
                    .required(true)
                    .takes_value(true)
                    .help("listen address '127.0.0.1:8080'"),
            ),
        )
        .get_matches();

    let mut db = DB::open(matches.value_of("path").unwrap(), SyncMode::Every(100)).unwrap();

    match matches.subcommand() {
        ("append", Some(sub_match)) => {
            append::append(&mut db, sub_match.value_of("series").unwrap(), sub_match.value_of("csv").unwrap()).unwrap()
        }
        ("export", Some(sub_match)) => {
            export::export(
                &mut db,
                sub_match.value_of("series").unwrap(),
                sub_match.value_of("csv").unwrap(),
                sub_match.value_of("from").and_then(|from| from.parse::<u64>().ok()).unwrap(),
            )
            .unwrap();
        }
        ("server", Some(sub_match)) => server::start_server(db, sub_match.value_of("addr").unwrap().parse().unwrap())
            .await
            .unwrap(),
        _ => unreachable!(),
    }
}
