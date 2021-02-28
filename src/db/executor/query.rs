use super::agg::Aggregator;
use super::Query;
use std::convert::TryFrom;
use std::str::FromStr;
use serde_derive::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[derive(Debug)]
pub struct QueryExpression {
    pub from: String,
    pub group_by: String,
    pub aggregators: String,
    pub limit: String,
}

struct FromTimestamp(u64);

impl FromStr for FromTimestamp {
    type Err = ();

    fn from_str(s: &str) -> Result<FromTimestamp, Self::Err> {
        Ok(FromTimestamp(s.parse::<u64>().map_err(|_| ())?))
    }
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
            _ => Err(()),
        }
    }
}

impl TryFrom<QueryExpression> for Query {
    type Error = ();
    fn try_from(source: QueryExpression) -> Result<Query, Self::Error> {
        let FromTimestamp(from) = source.from.parse()?;
        let GroupByMillis(group_by) = source.group_by.parse()?;
        let aggregators = source
            .aggregators
            .split(',')
            .map(|s| s.parse())
            .collect::<Result<Vec<Aggregator>, ()>>()?;
        let limit = source.limit.parse::<usize>().map_err(|_| ())?;

        Ok(Query {
            from: from,
            group_by: group_by,
            aggregators: aggregators,
            limit: limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test() {
        let expr = QueryExpression {
            from: "10".to_string(),
            group_by: "hour".to_string(),
            aggregators: "mean".to_string(),
            limit: "1000".to_string(),
        };

        assert_eq!(Query {
            from: 10,
            group_by: 60 * 60 * 1000,
            aggregators: vec![Aggregator::Mean],
            limit: 1000,
        }, Query::try_from(expr).unwrap());
    }
}