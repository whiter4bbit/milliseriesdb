use milliseriesdb::storage::{env, error::Error, file_system, series_table};

fn main() -> Result<(), Error> {
    stderrlog::new().verbosity(4).init().unwrap();

    let series_table = series_table::create(env::create(file_system::open("playground/example")?))?;
    series_table.create("t")?;

    let writer = series_table.writer("t").expect("table should exist");

    Ok(())
}
