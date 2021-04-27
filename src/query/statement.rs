use super::aggregation::Aggregator;

#[derive(Debug, PartialEq, Eq)]
pub struct Statement {
    pub aggregators: Vec<Aggregator>,
    pub group_by: u64,
    pub limit: usize,
    pub from: u64,
}