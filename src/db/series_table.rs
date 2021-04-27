use super::file_system::FileSystem;
use super::{SeriesReader, SeriesWriterGuard, SyncMode};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

struct SeriesEntry {
    writer: Arc<SeriesWriterGuard>,
    reader: Arc<SeriesReader>,
}

impl SeriesEntry {
    pub fn open_or_create<S: AsRef<str>>(
        fs: &FileSystem,
        name: S,
        sync_mode: SyncMode,
    ) -> io::Result<SeriesEntry> {
        Ok(SeriesEntry {
            writer: Arc::new(SeriesWriterGuard::create(
                fs.series(name.as_ref())?,
                sync_mode,
            )?),
            reader: Arc::new(SeriesReader::create(fs.series(name.as_ref())?)?),
        })
    }
}

pub struct SeriesTable {
    fs: FileSystem,
    sync_mode: SyncMode,
    entries: Arc<Mutex<HashMap<String, Arc<SeriesEntry>>>>,
}

impl SeriesTable {
    pub fn reader<S: AsRef<str>>(&self, name: S) -> Option<Arc<SeriesReader>> {
        let entries = self.entries.lock().unwrap();
        entries.get(name.as_ref()).map(|entry| entry.reader.clone())
    }
    pub fn writer<S: AsRef<str>>(&self, name: S) -> Option<Arc<SeriesWriterGuard>> {
        let entries = self.entries.lock().unwrap();
        entries.get(name.as_ref()).map(|entry| entry.writer.clone())
    }
    pub fn create<S: AsRef<str>>(&self, name: S) -> io::Result<()> {
        let mut entries = self.entries.lock().unwrap();
        if entries.contains_key(name.as_ref()) {
            return Ok(());
        }

        let entry = SeriesEntry::open_or_create(&self.fs, &name, self.sync_mode)?;
        entries.insert(name.as_ref().to_owned(), Arc::new(entry));

        Ok(())
    }
}

pub fn create(fs: FileSystem, sync_mode: SyncMode) -> io::Result<SeriesTable> {
    let mut entries = HashMap::new();
    for name in fs.get_series()? {
        entries.insert(
            name.to_owned(),
            Arc::new(SeriesEntry::open_or_create(&fs, &name, sync_mode)?),
        );
    }

    Ok(SeriesTable {
        fs: fs,
        sync_mode: sync_mode,
        entries: Arc::new(Mutex::new(entries)),
    })
}