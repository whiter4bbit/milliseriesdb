use clap::clap_app;
use milliseriesdb::query::{QueryBuilder, Statement, StatementExpr};
use milliseriesdb::storage::{
    env, error::Error, file_system, series_table, Entry, SeriesReader, SeriesWriter,
};
use std::convert::TryFrom;
use std::sync::Arc;
use std::{fs, time};

fn insert(count: usize, batch_size: usize, writer: Arc<SeriesWriter>) -> Result<(), Error> {
    let mut appender = writer.appender()?;

    let start_ts = i64::MIN;
    let values = [1.0, 2.0, 3.0];
    let mut entries = 0usize;
    while entries < count {
        let batch = (0..batch_size)
            .into_iter()
            .map(|i| Entry {
                ts: start_ts + (entries + i) as i64 * 1000,
                value: values[i % values.len()],
            })
            .collect::<Vec<Entry>>();

        appender.append(&batch)?;

        entries += batch.len();
    }

    appender.done()
}

fn query(reader: Arc<SeriesReader>) -> Result<(), Error> {
    let rows = reader
        .query(
            Statement::try_from(StatementExpr {
                from: "-9223372036854775808".to_string(),
                group_by: "day".to_string(),
                aggregators: "mean".to_string(),
                limit: "10000".to_string(),
            })
            .unwrap(),
        )
        .rows()?;
    log::debug!("query rows: {}", rows.len());
    Ok(())
}

fn main() -> Result<(), Error> {
    stderrlog::new().verbosity(4).init().unwrap();

    let matches = clap_app!(milliseriesdb =>
        (@arg drop_path: -d <DROP> --drop default_value("false") "drop path")
        (@arg path: -p <PATH> --path default_value("playground/examples") "path to database")
        (@arg entries: -e <ENTRIES> --entries default_value("100000000") "entries to inserts")
        (@arg batch: -b <BATCH> --batch default_value("1000") "batch size")
    )
    .get_matches();

    let path = matches.value_of("path").unwrap();
    
    if matches.value_of("drop_path").unwrap().parse::<bool>().unwrap() {
        log::debug!("dropping the path: {}", &path);

        fs::remove_dir_all(path)?;
    }

    let series_table = series_table::create(env::create(file_system::open(
        path,
    )?))?;
    
    series_table.create("t")?;

    let entries = matches.value_of("entries").unwrap().parse::<usize>().unwrap();
    let batch = matches.value_of("batch").unwrap().parse::<usize>().unwrap();

    let start_ts = time::Instant::now();
    insert(entries, batch, series_table.writer("t").unwrap())?;
    log::debug!("Inserted in {}ms", start_ts.elapsed().as_millis());

    let start_ts = time::Instant::now();
    query(series_table.reader("t").unwrap())?;
    log::debug!("Query in {}ms", start_ts.elapsed().as_millis());

    Ok(())
}
