use std::collections::VecDeque;
use std::fs::create_dir_all;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::data::{DataReader, DataWriter};
use super::entry::Entry;
use super::file_system::{FileKind, OpenMode, SeriesDir};
use super::index::{IndexReader, IndexWriter};
use super::log::{self, LogEntry, LogReader, LogWriter};
use super::utils::IntoEntriesIterator;
use super::Compression;

#[derive(Copy, Clone)]
pub enum SyncMode {
    #[allow(dead_code)]
    Paranoid,
    #[allow(dead_code)]
    Never,
    #[allow(dead_code)]
    Every(u16),
}

pub struct SeriesWriter {
    data_writer: DataWriter,
    index_writer: IndexWriter,
    log_writer: LogWriter,
    last_log_entry: LogEntry,
    sync_mode: SyncMode,
    writes: u64,
}

impl SeriesWriter {
    #[allow(dead_code)]
    pub fn create(
        dir: &SeriesDir,
        sync_mode: SyncMode,
    ) -> io::Result<SeriesWriter> {
        let log_reader = LogReader::create(dir.clone());

        let last_entry = log_reader.get_last_entry_or_default()?;

        let mut log_writer = LogWriter::create(dir.clone(), 1024 * 1024)?;
        log_writer.append(&last_entry)?;
        log_writer.sync()?;

        Ok(SeriesWriter {
            data_writer: DataWriter::create(
                dir.open(FileKind::Data, OpenMode::Write)?,
                last_entry.data_offset,
            )?,
            index_writer: IndexWriter::open(
                dir.open(FileKind::Index, OpenMode::Write)?,
                last_entry.index_offset,
            )?,
            log_writer: log_writer,
            last_log_entry: last_entry,
            sync_mode: sync_mode,
            writes: 0,
        })
    }
    fn fsync(&mut self) -> io::Result<()> {
        self.writes += 1;
        let should_sync = match self.sync_mode {
            SyncMode::Paranoid => true,
            SyncMode::Every(p) if p > 0 && self.writes % p as u64 == 0 => true,
            _ => false,
        };
        if should_sync {
            self.data_writer.sync()?;
            self.index_writer.sync()?;
            self.log_writer.sync()?;
        }
        Ok(())
    }
    #[allow(dead_code)]
    pub fn append_batch(&mut self, batch: &[Entry], compression: Compression) -> io::Result<()> {
        let mut ordered: Vec<&Entry> = batch
            .iter()
            .filter(|entry| entry.ts >= self.last_log_entry.highest_ts)
            .collect();
        ordered.sort_by_key(|entry| entry.ts);
        if ordered.is_empty() {
            return Ok(());
        }
        let index_offset = self
            .index_writer
            .append(ordered.last().unwrap().ts, self.last_log_entry.data_offset)?;
        let data_offset = self.data_writer.append(&ordered, compression)?;
        let last_log_entry = LogEntry {
            data_offset: data_offset,
            index_offset: index_offset,
            highest_ts: ordered.last().unwrap().ts,
        };
        self.log_writer.append(&last_log_entry)?;
        self.last_log_entry = last_log_entry;
        self.fsync()
    }
}

pub struct SeriesReader {
    dir: SeriesDir,
    log_reader: LogReader,
}

impl SeriesReader {
    #[allow(dead_code)]
    pub fn create(dir: SeriesDir) -> io::Result<SeriesReader> {
        Ok(SeriesReader {
            dir: dir.clone(),
            log_reader: LogReader::create(dir.clone()),
        })
    }

    #[allow(dead_code)]
    pub fn iterator(&self, from_ts: u64) -> io::Result<SeriesIterator> {
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
            from_ts: from_ts,
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
    fn fetch_block(&mut self) -> io::Result<()> {
        if self.offset < self.size {
            self.offset = self.data_reader.read_block(&mut self.buffer)?;

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
    type Item = io::Result<Entry>;

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

#[derive(Clone)]
pub struct SeriesWriterGuard {
    writer: Arc<Mutex<SeriesWriter>>,
}

impl SeriesWriterGuard {
    pub fn create(
        dir: &SeriesDir,
        sync_mode: SyncMode,
    ) -> io::Result<SeriesWriterGuard> {
        Ok(SeriesWriterGuard {
            writer: Arc::new(Mutex::new(SeriesWriter::create(
                dir,
                sync_mode,
            )?)),
        })
    }

    pub fn append(&self, batch: &[Entry], compression: Compression) -> io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.append_batch(batch, compression)
    }

    pub async fn append_async(
        &self,
        batch: Vec<Entry>,
        compression: Compression,
    ) -> io::Result<()> {
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || {
            let mut writer = writer.lock().unwrap();
            writer.append_batch(&batch, compression)
        })
        .await
        .unwrap()
    }
}

impl IntoEntriesIterator for Arc<SeriesReader> {
    type Iter = SeriesIterator;
    fn into_iter(&self, from: u64) -> io::Result<Self::Iter> {
        self.iterator(from)
    }
}

#[cfg(test)]
mod test {
    use super::super::file_system;
    use super::*;
    use crate::db::test_utils::create_temp_dir;

    #[test]
    fn test_series_read_write() {
        let db_dir = create_temp_dir("test-base").unwrap();
        let file_system = file_system::open(&db_dir.path).unwrap();
        let series_dir = file_system.series("series1").unwrap();

        let entries = [
            Entry { ts: 1, value: 11.0 },
            Entry { ts: 2, value: 12.0 },
            Entry { ts: 3, value: 13.0 },
            Entry { ts: 5, value: 15.0 },
            Entry { ts: 8, value: 18.0 },
            Entry {
                ts: 10,
                value: 110.0,
            },
            Entry {
                ts: 20,
                value: 120.0,
            },
            Entry {
                ts: 21,
                value: 121.0,
            },
            Entry {
                ts: 40,
                value: 140.0,
            },
            Entry {
                ts: 100,
                value: 1100.0,
            },
            Entry {
                ts: 110,
                value: 1110.0,
            },
            Entry {
                ts: 120,
                value: 1120.0,
            },
            Entry {
                ts: 140,
                value: 1140.0,
            },
        ];
        {
            let mut writer = SeriesWriter::create(
                &series_dir,
                SyncMode::Never,
            )
            .unwrap();
            writer
                .append_batch(&entries[0..5], Compression::None)
                .unwrap();
            writer
                .append_batch(&entries[5..8], Compression::None)
                .unwrap();
            writer
                .append_batch(&entries[8..11], Compression::None)
                .unwrap();
        }

        let reader = SeriesReader::create(series_dir.clone()).unwrap();
        assert_eq!(
            entries[3..11].to_vec(),
            reader
                .iterator(4)
                .unwrap()
                .map(|e| e.unwrap())
                .collect::<Vec<Entry>>()
        );
        assert_eq!(
            entries[6..11].to_vec(),
            reader
                .iterator(15)
                .unwrap()
                .map(|e| e.unwrap())
                .collect::<Vec<Entry>>()
        );
        assert_eq!(
            entries[1..11].to_vec(),
            reader
                .iterator(2)
                .unwrap()
                .map(|e| e.unwrap())
                .collect::<Vec<Entry>>()
        );

        {
            let mut writer = SeriesWriter::create(
                &series_dir,
                SyncMode::Never,
            )
            .unwrap();
            writer
                .append_batch(&entries[11..13], Compression::None)
                .unwrap();
        }

        assert_eq!(
            entries[1..13].to_vec(),
            reader
                .iterator(2)
                .unwrap()
                .map(|e| e.unwrap())
                .collect::<Vec<Entry>>()
        );
    }
}
