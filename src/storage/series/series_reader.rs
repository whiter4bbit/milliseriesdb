use super::super::data::DataReader;
use super::super::entry::Entry;
use super::super::env::SeriesEnv;
use super::super::error::Error;
use super::super::file_system::{FileKind, OpenMode};
use std::collections::VecDeque;
use std::sync::Arc;

pub struct SeriesReader {
    env: Arc<SeriesEnv>,
}

impl SeriesReader {
    pub fn create(env: Arc<SeriesEnv>) -> Result<SeriesReader, Error> {
        Ok(SeriesReader { env: env.clone() })
    }

    pub fn iterator(&self, from_ts: i64) -> Result<SeriesIterator, Error> {
        let commit = self.env.commit_log().current();

        let start_offset = self
            .env
            .index()
            .ceiling_offset(from_ts, commit.index_offset)?
            .unwrap_or(0);

        Ok(SeriesIterator {
            data_reader: DataReader::create(
                self.env.dir().open(FileKind::Data, OpenMode::Read)?,
                start_offset,
            )?,
            offset: start_offset,
            size: commit.data_offset,
            from_ts,
            buffer: VecDeque::new(),
        })
    }
}

pub struct SeriesIterator {
    data_reader: DataReader,
    offset: u32,
    size: u32,
    from_ts: i64,
    buffer: VecDeque<Entry>,
}

impl SeriesIterator {
    fn fetch_block(&mut self) -> Result<(), Error> {
        if self.offset < self.size {
            let (entries, offset) = self.data_reader.read_block()?;
            self.offset = offset;
            self.buffer = entries.into();

            while self
                .buffer
                .front()
                .filter(|e| e.ts < self.from_ts)
                .is_some()
            {
                self.buffer.pop_front();
            }
        }
        Ok(())
    }
}

impl Iterator for SeriesIterator {
    type Item = Result<Entry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            if let Err(error) = self.fetch_block() {
                return Some(Err(error));
            }
        }

        match self.buffer.pop_front() {
            Some(entry) => Some(Ok(entry)),
            _ => None,
        }
    }
}
