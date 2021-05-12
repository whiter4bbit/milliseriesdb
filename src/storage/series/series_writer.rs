use super::super::data::{self, DataWriter};
use super::super::entry::Entry;
use super::super::error::Error;
use super::super::file_system::{FileKind, OpenMode, SeriesDir};
use super::super::index::IndexWriter;
use super::super::log::{LogEntry, LogReader, LogWriter};
use super::super::Compression;
use std::sync::{Arc, Mutex};

struct SeriesWriterInterior {
    data_writer: DataWriter,
    index_writer: IndexWriter,
    log_writer: LogWriter,
    data_offset: u64,
    highest_ts: i64,
}

impl SeriesWriterInterior {
    fn create(dir: Arc<SeriesDir>) -> Result<SeriesWriterInterior, Error> {
        SeriesWriterInterior::create_opt(dir, 1024 * 1024)
    }

    fn create_opt(
        dir: Arc<SeriesDir>,
        max_log_segment_size: u32,
    ) -> Result<SeriesWriterInterior, Error> {
        let log_reader = LogReader::create(dir.clone());
        let last_entry = &log_reader.get_last_entry_or_default()?;

        let mut log_writer = LogWriter::create(dir.clone(), max_log_segment_size as u64)?;
        log_writer.append(&last_entry)?;
        log_writer.sync()?;

        Ok(SeriesWriterInterior {
            data_writer: DataWriter::create(
                dir.open(FileKind::Data, OpenMode::Write)?,
                last_entry.data_offset,
            )?,
            index_writer: IndexWriter::open(
                dir.open(FileKind::Index, OpenMode::Write)?,
                last_entry.index_offset,
            )?,
            log_writer,
            data_offset: last_entry.data_offset,
            highest_ts: last_entry.highest_ts,
        })
    }

    fn fsync(&mut self) -> Result<(), Error> {
        self.data_writer.sync()?;
        self.index_writer.sync()?;
        self.log_writer.sync()?;

        Ok(())
    }

    fn process_entries<'a, I>(&mut self, entries: I) -> Vec<&'a Entry>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        let mut entries: Vec<&Entry> = entries
            .into_iter()
            .filter(|entry| entry.ts >= self.highest_ts)
            .collect();
        entries.sort_by_key(|entry| entry.ts);
        entries
    }

    fn append_block<'a>(
        &mut self,
        block: Vec<&'a Entry>,
        compression: Compression,
    ) -> Result<(), Error> {
        let highest_ts = match block.last() {
            Some(entry) => entry.ts,
            _ => return Ok(()),
        };

        let index_offset = self.index_writer.append(highest_ts, self.data_offset)?;
        let data_offset = self.data_writer.append(block, compression)?;

        self.log_writer.append(&LogEntry {
            data_offset,
            index_offset,
            highest_ts,
        })?;

        self.data_offset = data_offset;
        self.highest_ts = highest_ts;

        self.fsync()?;

        Ok(())
    }

    fn append<'a, I>(&mut self, batch: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        self.append_opt(batch, Compression::Delta)
    }

    fn append_opt<'a, I>(&mut self, entries: I, compression: Compression) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        let iter = &mut self.process_entries(entries).into_iter();
        loop {
            let block: Vec<&'a Entry> = iter.take(data::MAX_ENTRIES_PER_BLOCK).collect();

            if block.is_empty() {
                return Ok(());
            }

            self.append_block(block, compression.clone())?;
        }
    }
}

#[derive(Clone)]
pub struct SeriesWriter {
    writer: Arc<Mutex<SeriesWriterInterior>>,
}

impl SeriesWriter {
    pub fn create(dir: Arc<SeriesDir>) -> Result<SeriesWriter, Error> {
        Ok(SeriesWriter {
            writer: Arc::new(Mutex::new(SeriesWriterInterior::create(dir)?)),
        })
    }

    pub fn create_opt(
        dir: Arc<SeriesDir>,
        max_log_segment_size: u32,
    ) -> Result<SeriesWriter, Error> {
        Ok(SeriesWriter {
            writer: Arc::new(Mutex::new(SeriesWriterInterior::create_opt(
                dir,
                max_log_segment_size,
            )?)),
        })
    }

    pub fn append<'a, I>(&self, batch: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        let mut writer = self.writer.lock().unwrap();
        writer.append(batch)
    }

    pub fn append_opt<'a, I>(&self, batch: I, compression: Compression) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        let mut writer = self.writer.lock().unwrap();
        writer.append_opt(batch, compression)
    }

    pub async fn append_async(&self, batch: Vec<Entry>) -> Result<(), Error> {
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || {
            let mut writer = writer.lock().unwrap();
            writer.append(&batch)
        })
        .await
        .unwrap()
    }

    pub async fn append_opt_async(
        &self,
        batch: Vec<Entry>,
        compression: Compression,
    ) -> Result<(), Error> {
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || {
            let mut writer = writer.lock().unwrap();
            writer.append_opt(&batch, compression)
        })
        .await
        .unwrap()
    }
}
