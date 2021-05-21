mod aggregation;
mod group_by;
mod into_entries_iter;
mod query;
mod statement;
mod statement_expr;

pub use aggregation::Aggregation;
pub use query::{QueryBuilder, Row};
pub use statement::Statement;
pub use statement_expr::StatementExpr;
//   |       |       |        |      |
// -20      -10      0       10      20
//              ^
//              6
//
#[cfg(test)]
mod test_queries {
    use super::*;
    use crate::storage::{error::Error, series_table, Entry};
    use chrono::{TimeZone, Utc};
    use std::convert::TryInto;

    fn utc_millis(ts: &str) -> i64 {
        Utc.datetime_from_str(ts, "%F %H:%M")
            .unwrap()
            .timestamp_millis()
    }

    fn from_utc_millis(ts: i64) -> String {
        Utc.timestamp_millis(ts).format("%F %H:%M").to_string()
    }

    fn round_millis(ts: i64, to: i64) -> i64 {
        if ts < 0 {
            -round_millis(ts.abs() + to, to)
        } else {
            (ts / to) * to
        }
    }

    fn group_key(ts: &str, by: i64) -> String {
        from_utc_millis(round_millis(utc_millis(ts), by))
    }

    #[test]
    fn test_the_first_day() {
        println!("{:?}", from_utc_millis(i64::MIN / 2));
    }

    #[test]
    #[rustfmt::skip]
    fn test_group_key() {
        assert_eq!("1972-01-01 22:00", &group_key("1972-01-01 22:00", 60 * 60 * 1000));
        assert_eq!("1972-01-01 22:00", &group_key("1972-01-01 22:33", 60 * 60 * 1000));
        assert_eq!("1972-01-01 22:00", &group_key("1972-01-01 22:59", 60 * 60 * 1000));

        assert_eq!("1962-01-01 22:00", &group_key("1962-01-01 22:00", 60 * 60 * 1000));
        assert_eq!("1962-01-01 22:00", &group_key("1962-01-01 22:33", 60 * 60 * 1000));        
        assert_eq!("1962-01-01 22:00", &group_key("1962-01-01 22:59", 60 * 60 * 1000));
    }

    fn entry(ts: &str, value: f64) -> Entry {
        Entry {
            ts: utc_millis(ts),
            value: value,
        }
    }

    fn as_tuple(row: &Row) -> (String, Aggregation) {
        (from_utc_millis(row.ts), row.values[0].clone())
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

        let rows = reader
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
            .iter()
            .map(|row| as_tuple(row))
            .collect::<Vec<(String, Aggregation)>>();

        assert_eq!(
            vec![
                ("1961-01-02 11:00".to_string(), Aggregation::Mean(3.0)),
                ("1961-01-02 12:00".to_string(), Aggregation::Mean(6.0)),
                ("1971-01-02 12:00".to_string(), Aggregation::Mean(6.0)),
            ],
            rows
        );

        Ok(())
    }
}
