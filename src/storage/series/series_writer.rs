use super::super::commit_log::Commit;
use super::super::data::{self, DataWriter};
use super::super::entry::Entry;
use super::super::env::SeriesEnv;
use super::super::error::Error;
use super::super::failpoints;
use super::super::file_system::{FileKind, OpenMode};
use super::super::Compression;
use std::sync::{Arc, Mutex};

struct SeriesWriterInterior {
    data_writer: DataWriter,
    env: Arc<SeriesEnv>,
}

impl SeriesWriterInterior {
    fn create(env: Arc<SeriesEnv>) -> Result<SeriesWriterInterior, Error> {
        let last_commit = env.commit_log().current();

        Ok(SeriesWriterInterior {
            data_writer: DataWriter::create(
                env.dir().open(FileKind::Data, OpenMode::Write)?,
                last_commit.data_offset,
            )?,
            env: env,
        })
    }

    fn fsync(&mut self) -> Result<(), Error> {
        self.data_writer.sync()?;
        self.env.index().sync()?;

        Ok(())
    }

    fn process_entries<'a, I>(&mut self, entries: I) -> Vec<&'a Entry>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        let highest_ts = self.env.commit_log().current().highest_ts;

        let mut entries: Vec<&Entry> = entries
            .into_iter()
            .filter(|entry| entry.ts >= highest_ts)
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

        let commit = self.env.commit_log().current();

        let index_offset = self.env.index().append(highest_ts, commit.data_offset)?;

        let data_offset = self.data_writer.append(block, compression)?;
        failpoints::fail!("series-writer-data", io);

        self.fsync()?;

        self.env.commit_log().commit(Commit {
            data_offset,
            index_offset,
            highest_ts,
        })
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
    pub fn create(env: Arc<SeriesEnv>) -> Result<SeriesWriter, Error> {
        Ok(SeriesWriter {
            writer: Arc::new(Mutex::new(SeriesWriterInterior::create(env)?)),
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
