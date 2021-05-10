use std::collections::VecDeque;
use std::sync::Arc;
use super::super::data::DataReader;
use super::super::entry::Entry;
use super::super::file_system::{FileKind, OpenMode, SeriesDir};
use super::super::index::IndexReader;
use super::super::log::LogReader;
use super::super::error::Error;

pub struct SeriesReader {
    dir: Arc<SeriesDir>,
    log_reader: LogReader,
}

impl SeriesReader {
    #[allow(dead_code)]
    pub fn create(dir: Arc<SeriesDir>) -> Result<SeriesReader, Error> {
        Ok(SeriesReader {
            dir: dir.clone(),
            log_reader: LogReader::create(dir),
        })
    }

    #[allow(dead_code)]
    pub fn iterator(&self, from_ts: u64) -> Result<SeriesIterator, Error> {
        let last_log_entry = self.log_reader.get_last_entry_or_default()?;

        let mut index_reader = IndexReader::create(
            self.dir.open(FileKind::Index, OpenMode::Read)?,
            last_log_entry.index_offset,
        )?;

        let start_offset = match index_reader.ceiling_offset(from_ts)? {
            Some(offset) => offset,
            _ => last_log_entry.data_offset,
        };

        Ok(SeriesIterator {
            data_reader: DataReader::create(
                self.dir.open(FileKind::Data, OpenMode::Read)?,
                start_offset,
            )?,
            offset: start_offset,
            size: last_log_entry.data_offset,
            from_ts,
            buffer: VecDeque::new(),
        })
    }
}

pub struct SeriesIterator {
    data_reader: DataReader,
    offset: u64,
    size: u64,
    from_ts: u64,
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