use super::super::super::failpoints::failpoint;
use super::super::commit_log::Commit;
use super::super::data::{self, DataWriter};
use super::super::entry::Entry;
use super::super::env::SeriesEnv;
use super::super::error::Error;
use super::super::file_system::{FileKind, OpenMode};
use super::super::Compression;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex, MutexGuard};

pub struct Interior {
    data_writer: DataWriter,
    env: Arc<SeriesEnv>,
}

pub struct Appender<I>
where
    I: DerefMut<Target = Interior>,
{
    inter: I,
    data_offset: u32,
    index_offset: u32,
    highest_ts: i64,
}

impl<I> Appender<I>
where
    I: DerefMut<Target = Interior>,
{
    fn create(inter: I) -> Result<Appender<I>, Error> {
        let commit = inter.env.commit_log().current();

        Ok(Appender {
            inter: inter,
            data_offset: commit.data_offset,
            index_offset: commit.index_offset,
            highest_ts: commit.highest_ts,
        })
    }

    pub fn done(mut self) -> Result<(), Error> {
        self.inter.data_writer.sync()?;
        self.inter.env.index().sync()?;

        self.inter.env.commit_log().commit(Commit {
            data_offset: self.data_offset,
            index_offset: self.index_offset,
            highest_ts: self.highest_ts,
        })
    }

    fn process_entries<'a, E>(&mut self, entries: E) -> Vec<&'a Entry>
    where
        E: IntoIterator<Item = &'a Entry> + 'a,
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

        #[rustfmt::skip]
        let index_offset = self.inter.env.index().set(self.index_offset, highest_ts, self.data_offset)?;

        failpoint!(
            self.inter.env.fp(),
            "series_writer::index::set",
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "fp"
            )))
        );

        #[rustfmt::skip]
        let data_offset = self.inter.data_writer.write_block(self.data_offset, block, compression)?;

        failpoint!(
            self.inter.env.fp(),
            "series_writer::data_writer::write_block",
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "fp"
            )))
        );

        self.data_offset = data_offset;
        self.index_offset = index_offset;
        self.highest_ts = highest_ts;

        Ok(())
    }

    pub fn append<'a, E>(&mut self, batch: E) -> Result<(), Error>
    where
        E: IntoIterator<Item = &'a Entry> + 'a,
    {
        self.append_opt(batch, Compression::Delta)
    }

    pub fn append_opt<'a, E>(&mut self, entries: E, compression: Compression) -> Result<(), Error>
    where
        E: IntoIterator<Item = &'a Entry> + 'a,
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

impl Interior {
    fn create(env: Arc<SeriesEnv>) -> Result<Interior, Error> {
        Ok(Interior {
            data_writer: DataWriter::create(env.dir().open(FileKind::Data, OpenMode::Write)?)?,
            env: env,
        })
    }
}

#[derive(Clone)]
pub struct SeriesWriter {
    writer: Arc<Mutex<Interior>>,
}

impl SeriesWriter {
    pub fn create(env: Arc<SeriesEnv>) -> Result<SeriesWriter, Error> {
        Ok(SeriesWriter {
            writer: Arc::new(Mutex::new(Interior::create(env)?)),
        })
    }

    pub fn appender(&self) -> Result<Appender<MutexGuard<'_, Interior>>, Error> {
        Appender::create(self.writer.lock().unwrap())
    }

    pub fn append<'a, I>(&self, batch: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        let mut appender = self.appender()?;
        appender.append(batch)?;
        appender.done()
    }

    pub fn append_opt<'a, I>(&self, batch: I, compression: Compression) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        let mut appender = self.appender()?;
        appender.append_opt(batch, compression)?;
        appender.done()
    }

    pub async fn append_async(&self, batch: Vec<Entry>) -> Result<(), Error> {
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || {
            let mut appender = Appender::create(writer.lock().unwrap())?;
            appender.append(&batch)?;
            appender.done()
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
            let mut appender = Appender::create(writer.lock().unwrap())?;
            appender.append_opt(&batch, compression)?;
            appender.done()
        })
        .await
        .unwrap()
    }
}
