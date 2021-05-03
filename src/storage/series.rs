use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Mutex};

use super::data::{DataReader, DataWriter};
use super::entry::Entry;
use super::file_system::{FileKind, OpenMode, SeriesDir};
use super::index::{IndexReader, IndexWriter};
use super::log::{LogEntry, LogReader, LogWriter};
use super::utils::{LowLevelEntriesIterator, IntoEntriesIterator, IntoLowLevelEntriesIterator};
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
    pub fn create(dir: Arc<SeriesDir>, sync_mode: SyncMode) -> io::Result<SeriesWriter> {
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
            log_writer,
            last_log_entry: last_entry,
            sync_mode,
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
    pub fn append<'a, I>(&mut self, batch: I, compression: Compression) -> io::Result<()>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        let mut ordered: Vec<&Entry> = batch
            .into_iter()
            .filter(|entry| entry.ts >= self.last_log_entry.highest_ts)
            .collect();
        ordered.sort_by_key(|entry| entry.ts);
        if ordered.is_empty() {
            return Ok(());
        }

        let last_entry_ts = ordered.last().unwrap().ts;

        let index_offset = self
            .index_writer
            .append(last_entry_ts, self.last_log_entry.data_offset)?;

        let data_offset = self.data_writer.append(ordered, compression)?;
        let last_log_entry = LogEntry {
            data_offset,
            index_offset,
            highest_ts: last_entry_ts,
        };

        self.log_writer.append(&last_log_entry)?;
        self.last_log_entry = last_log_entry;

        self.fsync()
    }
}

pub struct SeriesReader {
    dir: Arc<SeriesDir>,
    log_reader: LogReader,
}

impl SeriesReader {
    #[allow(dead_code)]
    pub fn create(dir: Arc<SeriesDir>) -> io::Result<SeriesReader> {
        Ok(SeriesReader {
            dir: dir.clone(),
            log_reader: LogReader::create(dir),
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
            from_ts,
            buffer: VecDeque::new(),
        })
    }

    #[allow(dead_code)]
    pub fn low_level_iterator(&self, from_ts: u64) -> io::Result<SeriesLowLevelIterator> {
        let last_log_entry = self.log_reader.get_last_entry_or_default()?;

        let mut index_reader = IndexReader::create(
            self.dir.open(FileKind::Index, OpenMode::Read)?,
            last_log_entry.index_offset,
        )?;

        let start_offset = match index_reader.ceiling_offset(from_ts)? {
            Some(offset) => offset,
            _ => last_log_entry.data_offset,
        };

        Ok(SeriesLowLevelIterator {
            data_reader: DataReader::create(
                self.dir.open(FileKind::Data, OpenMode::Read)?,
                start_offset,
            )?,
            offset: start_offset,
            size: last_log_entry.data_offset,
            from_ts,
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
            let (entries, offset) = self.data_reader.read_block()?;
            self.offset = offset;
            self.buffer.extend(entries);

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

pub struct SeriesLowLevelIterator {
    data_reader: DataReader,
    offset: u64,
    size: u64,
    from_ts: u64,
}

impl SeriesLowLevelIterator {
    fn read_next(&mut self, ts: &mut [u64], values: &mut [f64]) -> io::Result<usize> {
        let mut matching_size = 0usize;

        if self.offset < self.size {
            let (entries_count, next_offset) = self.data_reader.read_block_to_buf(ts, values)?;

            for i in 0..entries_count {
                if ts[i] >= self.from_ts {
                    ts[matching_size] = ts[i];
                    values[matching_size] = values[i];

                    matching_size += 1
                }
            }

            self.offset = next_offset;
        }
        Ok(matching_size)
    }
}

impl LowLevelEntriesIterator for SeriesLowLevelIterator {
    fn next(&mut self, ts: &mut [u64], values: &mut [f64]) -> io::Result<usize> {
        self.read_next(ts, values)
    }
}

impl IntoLowLevelEntriesIterator for Arc<SeriesReader> {
    type Iter = SeriesLowLevelIterator;
    fn into_low_level_iter(&self, from_ts: u64) -> io::Result<Self::Iter> {
        self.low_level_iterator(from_ts)
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

impl IntoEntriesIterator for Arc<SeriesReader> {
    type Iter = SeriesIterator;
    fn into_iter(&self, from: u64) -> io::Result<Self::Iter> {
        self.iterator(from)
    }
}

#[derive(Clone)]
pub struct SeriesWriterGuard {
    writer: Arc<Mutex<SeriesWriter>>,
}

impl SeriesWriterGuard {
    pub fn create(dir: Arc<SeriesDir>, sync_mode: SyncMode) -> io::Result<SeriesWriterGuard> {
        Ok(SeriesWriterGuard {
            writer: Arc::new(Mutex::new(SeriesWriter::create(dir, sync_mode)?)),
        })
    }

    pub fn append<'a, I>(&self, batch: I, compression: Compression) -> io::Result<()>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        let mut writer = self.writer.lock().unwrap();
        writer.append(batch, compression)
    }

    pub async fn append_async(
        &self,
        batch: Vec<Entry>,
        compression: Compression,
    ) -> io::Result<()> {
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || {
            let mut writer = writer.lock().unwrap();
            writer.append(&batch, compression)
        })
        .await
        .unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::super::file_system;
    use super::super::test_utils::create_temp_dir;
    use super::*;

    fn entry(ts: u64, value: f64) -> Entry {
        Entry { ts, value }
    }

    #[test]
    fn test_low_level_itrator() {
        let db_dir = create_temp_dir("test-base").unwrap();
        let file_system = file_system::open(&db_dir.path).unwrap();
        let series_dir = file_system.series("series1").unwrap();

        {
            let mut writer = SeriesWriter::create(series_dir.clone(), SyncMode::Never).unwrap();
            writer.append(&vec![
                entry(1, 10.0),
                entry(2, 20.0),
                entry(3, 30.0),
            ], Compression::Delta).unwrap();

            writer.append(&vec![
                entry(4, 40.0),
                entry(5, 50.0),
                entry(6, 60.0),
            ], Compression::Delta).unwrap();
        }

        {
            let reader = SeriesReader::create(series_dir.clone()).unwrap();
            let mut iter = reader.low_level_iterator(2).unwrap();

            let mut ts = [0u64; 10];
            let mut values = [0f64; 10];

            assert_eq!(2, iter.read_next(&mut ts, &mut values).unwrap());
            assert_eq!(&[2u64, 3u64][..], &ts[0..2]);
            assert_eq!(&[20.0f64, 30.0f64][..], &values[0..2]);

            assert_eq!(3, iter.read_next(&mut ts, &mut values).unwrap());
            assert_eq!(&[4u64, 5u64, 6u64][..], &ts[0..3]);
            assert_eq!(&[40.0f64, 50.0f64, 60.0f64][..], &values[0..3]);

            assert_eq!(0, iter.read_next(&mut ts, &mut values).unwrap());
        }
    }

    #[test]
    fn test_series_read_write() {
        let db_dir = create_temp_dir("test-base").unwrap();
        let file_system = file_system::open(&db_dir.path).unwrap();
        let series_dir = file_system.series("series1").unwrap();

        let compr = Compression::Deflate;

        let entries = [
            entry(1, 11.0),
            entry(2, 12.0),
            entry(3, 13.0),
            entry(5, 15.0),
            entry(8, 18.0),
            entry(10, 110.0),
            entry(20, 120.0),
            entry(21, 121.0),
            entry(40, 140.0),
            entry(100, 1100.0),
            entry(110, 1110.0),
            entry(120, 1120.0),
            entry(140, 1140.0),
        ];
        {
            let mut writer = SeriesWriter::create(series_dir.clone(), SyncMode::Never).unwrap();
            writer.append(&entries[0..5], compr.clone()).unwrap();
            writer.append(&entries[5..8], compr.clone()).unwrap();
            writer.append(&entries[8..11], compr.clone()).unwrap();
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
            let mut writer = SeriesWriter::create(series_dir, SyncMode::Never).unwrap();
            writer.append(&entries[11..13], compr).unwrap();
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
