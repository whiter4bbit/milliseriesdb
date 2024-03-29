use super::aggregation::Aggregator;
use super::statement::Statement;
use chrono::{TimeZone, Utc};
use serde_derive::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::str::FromStr;

#[derive(Deserialize, Serialize, Debug)]
pub struct StatementExpr {
    pub from: String,
    pub group_by: String,
    pub aggregators: String,
    pub limit: String,
}

fn parse_date_time(s: &str, format: &str, s_suffix: &str) -> Result<i64, ()> {
    Utc.datetime_from_str((s.to_owned() + s_suffix).as_ref(), format)
        .map_err(|_| ())
        .map(|dt| dt.timestamp_millis())
}

fn parse_millis(s: &str) -> Result<i64, ()> {
    s.parse::<i64>().map_err(|_| ())
}

#[derive(Debug, Eq, PartialEq)]
struct FromTimestamp(i64);

impl FromStr for FromTimestamp {
    type Err = ();

    fn from_str(s: &str) -> Result<FromTimestamp, Self::Err> {
        parse_date_time(s, "%F %H:%M", "00:00")
            .or_else(|_| parse_millis(s))
            .map(FromTimestamp)
    }
}

#[test]
fn test_timestamp_from_str() {
    assert_eq!(FromTimestamp(1234), "1234".parse().unwrap());

    println!(
        "{:?}",
        Utc.datetime_from_str("2020-07-16 10:00", "%F %H:%M")
    );
}

struct GroupByMillis(u64);

impl FromStr for GroupByMillis {
    type Err = ();

    fn from_str(s: &str) -> Result<GroupByMillis, Self::Err> {
        match s {
            "day" => Ok(GroupByMillis(24 * 60 * 60 * 1000)),
            "hour" => Ok(GroupByMillis(60 * 60 * 1000)),
            "minute" => Ok(GroupByMillis(60 * 1000)),
            _ => Err(()),
        }
    }
}

impl FromStr for Aggregator {
    type Err = ();

    fn from_str(s: &str) -> Result<Aggregator, Self::Err> {
        match s {
            "mean" => Ok(Aggregator::Mean),
            "min" => Ok(Aggregator::Min),
            "max" => Ok(Aggregator::Max),
            _ => Err(()),
        }
    }
}

impl TryFrom<StatementExpr> for Statement {
    type Error = ();
    fn try_from(source: StatementExpr) -> Result<Statement, Self::Error> {
        let FromTimestamp(from) = source.from.parse()?;
        let GroupByMillis(group_by) = source.group_by.parse()?;
        let aggregators = source
            .aggregators
            .split(',')
            .map(|s| s.parse())
            .collect::<Result<Vec<Aggregator>, ()>>()?;
        let limit = source.limit.parse::<usize>().map_err(|_| ())?;

        Ok(Statement {
            from,
            group_by,
            aggregators,
            limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test() {
        let expr = StatementExpr {
            from: "10".to_string(),
            group_by: "hour".to_string(),
            aggregators: "mean,min,max,min".to_string(),
            limit: "1000".to_string(),
        };

        assert_eq!(
            Statement {
                from: 10,
                group_by: 60 * 60 * 1000,
                aggregators: vec![
                    Aggregator::Mean,
                    Aggregator::Min,
                    Aggregator::Max,
                    Aggregator::Min
                ],
                limit: 1000,
            },
            Statement::try_from(expr).unwrap()
        );
    }
}
