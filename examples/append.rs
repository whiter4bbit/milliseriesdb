use clap::clap_app;
use milliseriesdb::buffering::BufferingBuilder;
use milliseriesdb::storage::{
    env, error::Error, file_system, series_table, Entry, SeriesWriter,
};
use std::sync::Arc;
use std::{fs, time};
use chrono::{TimeZone, Utc};

fn utc_millis(ts: &str) -> i64 {
    Utc.datetime_from_str(ts, "%F %H:%M")
        .unwrap()
        .timestamp_millis()
}

fn append(count: usize, batch_size: usize, writer: Arc<SeriesWriter>) -> Result<usize, Error> {
    let mut appender = writer.appender()?;
    let mut entries = 0usize;
    for batch in (utc_millis("-262000-01-01 00:00")..)
        .step_by(1000)
        .into_iter()
        .zip([1.0f64, 2.0f64, 3.0f64].iter().cycle())
        .map(|(ts, value)| Entry {
            ts: ts,
            value: *value,
        })
        .take(count)
        .buffering::<Vec<Entry>>(batch_size)
    {
        entries += batch.len();
        appender.append(&batch)?;
    }

    appender.done()?;
    Ok(entries)
}

fn main() -> Result<(), Error> {
    stderrlog::new().verbosity(4).init().unwrap();

    let matches = clap_app!(milliseriesdb =>
        (@arg drop_path: -d <DROP> --drop default_value("false") "drop path")
        (@arg path: -p <PATH> --path default_value("playground/examples") "path to database")
        (@arg entries: -e <ENTRIES> --entries default_value("100000000") "entries to append")
        (@arg batch: -b <BATCH> --batch default_value("1000") "batch size")
    )
    .get_matches();

    let path = matches.value_of("path").unwrap();

    if matches
        .value_of("drop_path")
        .unwrap()
        .parse::<bool>()
        .unwrap()
    {
        log::debug!("Dropping the path: {}", &path);

        fs::remove_dir_all(path)?;
    }

    let series_table = series_table::create(env::create(file_system::open(path)?))?;
    series_table.create("t")?;

    let entries = matches
        .value_of("entries")
        .unwrap()
        .parse::<usize>()
        .unwrap();

    let batch = matches.value_of("batch").unwrap().parse::<usize>().unwrap();

    let start_ts = time::Instant::now();
    let result = append(entries, batch, series_table.writer("t").unwrap())?;
    log::debug!("Inserted {} in {}ms", result, start_ts.elapsed().as_millis());

    Ok(())
}
