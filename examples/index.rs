use milliseriesdb::storage::{env, error::Error, file_system};

fn main() -> Result<(), Error> {
    stderrlog::new()
        .verbosity(4)
        .init()
        .unwrap();

    let env = env::create(file_system::open("playground/example")?);
    let series_env = env.series("series1")?;

    for _ in 1..3000 {
        series_env.index().append(1, 100)?;
    }
    Ok(())
}
