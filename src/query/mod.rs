mod aggregation;
mod group_by;
mod into_entries_iter;
mod query;
mod statement;
mod statement_expr;
mod round;

pub use aggregation::Aggregation;
pub use query::{QueryBuilder, Row};
pub use statement::Statement;
pub use statement_expr::StatementExpr;

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::{error::Error, series_table, Entry};
    use chrono::{TimeZone, Utc};
    use std::convert::TryInto;

    fn utc_millis(ts: &str) -> i64 {
        Utc.datetime_from_str(ts, "%F %H:%M")
            .unwrap()
            .timestamp_millis()
    }

    fn entry(ts: &str, value: f64) -> Entry {
        Entry {
            ts: utc_millis(ts),
            value: value,
        }
    }

    fn row(ts: &str, agg: Aggregation) -> Row {
        Row {
            ts: utc_millis(ts),
            values: vec![agg],
        }
    }

    #[test]
    fn test_group_by_query() -> Result<(), Error> {
        let table = series_table::test::create()?;
        table.create("series-1")?;

        let writer = table.writer("series-1").unwrap();
        writer.append(&vec![
            entry("1961-01-02 11:00", 3.0),
            entry("1961-01-02 11:02", 2.0),
            entry("1961-01-02 11:04", 4.0),
            entry("1961-01-02 12:02", 5.0),
            entry("1961-01-02 12:04", 7.0),
            entry("1961-01-02 12:02", 5.0),
            entry("1961-01-02 12:04", 7.0),
            entry("1971-01-02 12:02", 5.0),
            entry("1971-01-02 12:04", 7.0),
        ])?;

        let reader = table.reader("series-1").unwrap();

        let rows: Vec<Row> = reader
            .query(
                StatementExpr {
                    from: "1961-01-02".to_string(),
                    group_by: "hour".to_string(),
                    aggregators: "mean".to_string(),
                    limit: "1000".to_string(),
                }
                .try_into()
                .unwrap(),
            )
            .rows()?
            .into_iter()
            .collect();

        assert_eq!(
            vec![
                row("1961-01-02 11:00", Aggregation::Mean(3.0)),
                row("1961-01-02 12:00", Aggregation::Mean(6.0)),                
                row("1971-01-02 12:00", Aggregation::Mean(6.0)),
            ],
            rows
        );

        Ok(())
    }
}
