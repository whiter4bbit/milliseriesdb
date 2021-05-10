use super::super::data::DataWriter;
use super::super::entry::Entry;
use super::super::error::Error;
use super::super::file_system::{FileKind, OpenMode, SeriesDir};
use super::super::index::IndexWriter;
use super::super::log::{LogEntry, LogReader, LogWriter};
use super::super::Compression;
use std::sync::{Arc, Mutex};

#[derive(Copy, Clone)]
pub enum SyncMode {
    #[allow(dead_code)]
    Paranoid,
    #[allow(dead_code)]
    Never,
    #[allow(dead_code)]
    Every(u16),
}

struct SeriesWriterInterior {
    data_writer: DataWriter,
    index_writer: IndexWriter,
    log_writer: LogWriter,
    last_log_entry: LogEntry,
    sync_mode: SyncMode,
    writes: u64,
}

impl SeriesWriterInterior {
    fn create(dir: Arc<SeriesDir>) -> Result<SeriesWriterInterior, Error> {
        SeriesWriterInterior::create_opt(dir, SyncMode::Paranoid)
    }
    #[allow(dead_code)]
    fn create_opt(dir: Arc<SeriesDir>, sync_mode: SyncMode) -> Result<SeriesWriterInterior, Error> {
        let log_reader = LogReader::create(dir.clone());

        let last_entry = log_reader.get_last_entry_or_default()?;

        let mut log_writer = LogWriter::create(dir.clone(), 1024 * 1024)?;
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
            last_log_entry: last_entry,
            sync_mode,
            writes: 0,
        })
    }
    fn fsync(&mut self) -> Result<(), Error> {
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

    fn append<'a, I>(&mut self, batch: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        self.append_opt(batch, Compression::Delta)
    }

    #[allow(dead_code)]
    fn append_opt<'a, I>(&mut self, batch: I, compression: Compression) -> Result<(), Error>
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

        self.fsync()?;
        Ok(())
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
        sync_mode: SyncMode,
    ) -> Result<SeriesWriter, Error> {
        Ok(SeriesWriter {
            writer: Arc::new(Mutex::new(SeriesWriterInterior::create_opt(dir, sync_mode)?)),
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

    pub async fn append_async(
        &self,
        batch: Vec<Entry>
    ) -> Result<(), Error> {
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
