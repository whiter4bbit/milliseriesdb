use milliseriesdb::query::{QueryBuilder, Statement, StatementExpr};
use milliseriesdb::storage::SeriesTable;
use std::convert::TryFrom;
use std::io;

pub fn query(series_table: SeriesTable, series: &str, expr: StatementExpr) -> io::Result<()> {
    let reader = series_table
        .reader(series)
        .ok_or(io::Error::new(io::ErrorKind::Other, "can not find series"))?;
    let statement = Statement::try_from(expr)
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "can not parse expression"))?;
    for row in reader.query(statement).rows()? {
        println!("{:?}", row);
    }
    Ok(())
}