use clap::clap_app;
use milliseriesdb::query::{QueryBuilder, StatementExpr};
use milliseriesdb::storage::{env, error::Error, file_system, series_table, SeriesReader};
use std::convert::TryInto;
use std::sync::Arc;
use std::time;

fn query(reader: Arc<SeriesReader>, group_by: &str, limit: &str) -> Result<usize, Error> {
    Ok(reader
        .query(
            StatementExpr {
                from: "-262000-01-01".to_string(),
                group_by: group_by.to_owned(),
                aggregators: "mean".to_string(),
                limit: limit.to_owned(),
            }
            .try_into()
            .unwrap(),
        )
        .rows()?
        .len())
}

fn main() -> Result<(), Error> {
    stderrlog::new().verbosity(4).init().unwrap();

    let matches = clap_app!(milliseriesdb =>
        (@arg path: -p <PATH> --path default_value("playground/examples") "path to database")
        (@arg group_by: -g <GROUP_BY> --group_by default_value("day") "group by")
        (@arg limit: -l <LIMIT> --limit default_value("1000") "max number of rows")
    )
    .get_matches();

    let path = matches.value_of("path").unwrap();

    let series_table = series_table::create(env::create(file_system::open(path)?))?;

    let start_ts = time::Instant::now();
    let rows = query(
        series_table.reader("t").unwrap(),
        matches.value_of("group_by").unwrap(),
        matches.value_of("limit").unwrap(),
    )?;
    log::debug!("Rows {} in {}ms", rows, start_ts.elapsed().as_millis());

    Ok(())
}
