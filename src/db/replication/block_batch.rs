use super::super::log::LogEntry;

pub struct BlockBatch {
    pub series: String,
    pub data: Vec<u8>,
    pub index: Vec<u8>,
    pub before: LogEntry,
    pub after: LogEntry,
}