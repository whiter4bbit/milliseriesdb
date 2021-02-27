use std::collections::VecDeque;
use std::fs::create_dir_all;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::data::{DataReader, DataWriter};
use super::entry::Entry;
use super::index::{IndexReader, IndexWriter};
use super::log::{self, LogEntry, LogWriter};
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
    pub fn create<P: AsRef<Path>>(path: P, sync_mode: SyncMode, compression: Compression) -> io::Result<SeriesWriter> {
        create_dir_all(path.as_ref())?;
        let last_entry = match log::read_last_log_entry(path.as_ref())? {
            Some(entry) => entry,
            _ => LogEntry {
                data_offset: 0,
                index_offset: 0,
                highest_ts: 0,
            },
        };
        let mut log_writer = LogWriter::create(path.as_ref(), 1024 * 1024)?;
        log_writer.append(&last_entry)?;
        log_writer.sync()?;
        Ok(SeriesWriter {
            data_writer: DataWriter::create(path.as_ref(), last_entry.data_offset, compression)?,
            index_writer: IndexWriter::open(path.as_ref(), last_entry.index_offset)?,
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
    pub fn append_batch(&mut self, batch: &[Entry]) -> io::Result<()> {
        let mut ordered: Vec<&Entry> = batch.iter().filter(|entry| entry.ts >= self.last_log_entry.highest_ts).collect();
        ordered.sort_by_key(|entry| entry.ts);
        if ordered.is_empty() {
            return Ok(());
        }
        let index_offset = self
            .index_writer
            .append(ordered.last().unwrap().ts, self.last_log_entry.data_offset)?;
        let data_offset = self.data_writer.append(&ordered)?;
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
    index_reader: IndexReader,
    log_entry: LogEntry,
    path: PathBuf,
}

impl SeriesReader {
    #[allow(dead_code)]
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<SeriesReader> {
        create_dir_all(path.as_ref())?;
        let last_entry = match log::read_last_log_entry(path.as_ref())? {
            Some(entry) => entry,
            _ => LogEntry {
                data_offset: 0,
                index_offset: 0,
                highest_ts: 0,
            },
        };
        Ok(SeriesReader {
            index_reader: IndexReader::create(path.as_ref(), last_entry.index_offset)?,
            log_entry: last_entry,
            path: path.as_ref().to_path_buf(),
        })
    }

    #[allow(dead_code)]
    pub fn iterator(&mut self, from_ts: u64) -> io::Result<SeriesIterator> {
        let data_size = self.log_entry.data_offset;
        let start_offset = match self.index_reader.ceiling_offset(from_ts)? {
            Some(offset) => offset,
            _ => data_size,
        };
        Ok(SeriesIterator {
            data_reader: DataReader::create(self.path.clone(), data_size)?,
            offset: start_offset,
            size: data_size,
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
            let mut block = Vec::new();
            self.offset = self.data_reader.read_block(self.offset, &mut block)?;

            for entry in block {
                if entry.ts >= self.from_ts {
                    self.buffer.push_back(entry);
                }
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
    pub fn append(&mut self, batch: &[Entry]) -> io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.append_batch(batch)
    }
}

#[allow(dead_code)]
pub struct Series {
    writer: SeriesWriterGuard,
    path: PathBuf,
}

impl Series {
    #[allow(dead_code)]
    pub fn open_or_create<P: AsRef<Path>>(path: P, sync_mode: SyncMode, compression: Compression) -> io::Result<Series> {
        Ok(Series {
            writer: SeriesWriterGuard {
                writer: Arc::new(Mutex::new(SeriesWriter::create(path.as_ref(), sync_mode, compression)?)),
            },
            path: path.as_ref().to_path_buf(),
        })
    }
    pub fn writer(&self) -> SeriesWriterGuard {
        self.writer.clone()
    }
    #[allow(dead_code)]
    pub fn iterator(&self, from_ts: u64) -> io::Result<SeriesIterator> {
        let mut reader = SeriesReader::create(&self.path)?;
        reader.iterator(from_ts)
    }
}

impl IntoEntriesIterator for Series {
    type Iter = SeriesIterator;
    fn into_iter(&self, from: u64) -> io::Result<Self::Iter> {
        self.iterator(from)
    }
}

impl IntoEntriesIterator for Arc<Series> {
    type Iter = SeriesIterator;
    fn into_iter(&self, from: u64) -> io::Result<Self::Iter> {
        self.iterator(from)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::test_utils::create_temp_dir;

    #[test]
    fn test_series_read_write() {
        let db_dir = create_temp_dir("test-base").unwrap();

        let entries = [
            Entry { ts: 1, value: 11.0 },
            Entry { ts: 2, value: 12.0 },
            Entry { ts: 3, value: 13.0 },
            Entry { ts: 5, value: 15.0 },
            Entry { ts: 8, value: 18.0 },
            Entry { ts: 10, value: 110.0 },
            Entry { ts: 20, value: 120.0 },
            Entry { ts: 21, value: 121.0 },
            Entry { ts: 40, value: 140.0 },
            Entry { ts: 100, value: 1100.0 },
            Entry { ts: 110, value: 1110.0 },
            Entry { ts: 120, value: 1120.0 },
            Entry { ts: 140, value: 1140.0 },
        ];
        {
            let mut writer = SeriesWriter::create(&db_dir.path, SyncMode::Never, Compression::None).unwrap();
            writer.append_batch(&entries[0..5]).unwrap();
            writer.append_batch(&entries[5..8]).unwrap();
            writer.append_batch(&entries[8..11]).unwrap();
        }

        let mut snapshot_1 = SeriesReader::create(&db_dir.path).unwrap();
        assert_eq!(
            entries[3..11].to_vec(),
            snapshot_1.iterator(4).unwrap().map(|e| e.unwrap()).collect::<Vec<Entry>>()
        );
        assert_eq!(
            entries[6..11].to_vec(),
            snapshot_1.iterator(15).unwrap().map(|e| e.unwrap()).collect::<Vec<Entry>>()
        );
        assert_eq!(
            entries[1..11].to_vec(),
            snapshot_1.iterator(2).unwrap().map(|e| e.unwrap()).collect::<Vec<Entry>>()
        );

        {
            let mut writer = SeriesWriter::create(&db_dir.path, SyncMode::Never, Compression::None).unwrap();
            writer.append_batch(&entries[11..13]).unwrap();
        }

        let mut snapshot_2 = SeriesReader::create(&db_dir.path).unwrap();
        assert_eq!(
            entries[1..13].to_vec(),
            snapshot_2.iterator(2).unwrap().map(|e| e.unwrap()).collect::<Vec<Entry>>()
        );

        assert_eq!(
            entries[1..11].to_vec(),
            snapshot_1.iterator(2).unwrap().map(|e| e.unwrap()).collect::<Vec<Entry>>()
        );
    }
}
