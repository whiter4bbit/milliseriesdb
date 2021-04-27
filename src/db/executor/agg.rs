use super::Entry;
use serde_derive::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub enum Aggregator {
    Mean,
}

impl Aggregator {
    pub fn default_state(&self) -> AggregatorState {
        match self {
            Aggregator::Mean => AggregatorState::Mean { count: 0, sum: 0.0 },
        }
    }
}

#[allow(dead_code)]
pub enum AggregatorState {
    Mean { count: usize, sum: f64 },
}

impl AggregatorState {
    pub fn update(&mut self, entry: &Entry) {
        match self {
            AggregatorState::Mean { count, sum } => {
                *count += 1;
                *sum += entry.value;
            },
        }
    }
    
    pub fn pop(&mut self) -> Aggregation {
        match self {
            AggregatorState::Mean { count, sum } => {
                let result = Aggregation::Mean(*sum / *count as f64);
                *count = 0;
                *sum = 0.0;
                result
            }
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub enum Aggregation {
    Mean(f64),
}
