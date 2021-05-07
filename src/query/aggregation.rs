use super::group_by::Folder;
use serde_derive::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub enum Aggregator {
    Mean, Min, Max
}

impl Aggregator {
    fn seed_state(&self) -> State {
        match self {
            Aggregator::Mean => State::Mean { count: 0, sum: 0.0 },
            Aggregator::Min => State::Min { min: f64::MAX },
            Aggregator::Max => State::Max { max: f64::MIN },
        }
    }
}

#[allow(dead_code)]
pub enum State {
    Mean { count: usize, sum: f64 },
    Min { min: f64 },
    Max { max: f64 },
}

impl State {
    pub fn update(&mut self, value: f64) {
        match self {
            State::Mean { count, sum } => {
                *count += 1;
                *sum += value;
            },
            State::Min { min } => {
                *min = min.min(value);
            },
            State::Max { max } => {
                *max = max.max(value);
            },
        }
    }
    pub fn complete(&mut self) -> Aggregation {
        match self {
            State::Mean { count, sum } => {
                let result = Aggregation::Mean(*sum / *count as f64);
                *count = 0;
                *sum = 0.0;
                result
            },
            State::Min { min } => {
                let result = Aggregation::Min(*min);
                *min = f64::MAX;
                result
            },
            State::Max { max } => {
                let result = Aggregation::Max(*max);
                *max = f64::MIN;
                result
            }
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub enum Aggregation {
    Mean(f64), Min(f64), Max(f64),
}

pub struct AggregatorsFolder {
    states: Vec<State>,
}

impl AggregatorsFolder {
    pub fn new(aggregations: &[Aggregator]) -> AggregatorsFolder {
        AggregatorsFolder {
            states: aggregations.iter().map(|agg| agg.seed_state()).collect(),
        }
    }
}

impl Folder for AggregatorsFolder {
    type Result = Vec<Aggregation>;

    fn fold(&mut self, value: f64) {
        self.states.iter_mut().for_each(|state| state.update(value))
    }

    fn complete(&mut self) -> Self::Result {
        self.states
            .iter_mut()
            .map(|state| state.complete())
            .collect()
    }
}