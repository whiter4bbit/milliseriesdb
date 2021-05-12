use super::error::Error;
use super::file_system::FileSystem;
use super::{SeriesReader, SeriesWriter};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time;

struct TableEntry {
    writer: Arc<SeriesWriter>,
    reader: Arc<SeriesReader>,
}

impl TableEntry {
    pub fn open_or_create<S: AsRef<str>>(
        fs: &FileSystem,
        name: S
    ) -> Result<TableEntry, Error> {
        Ok(TableEntry {
            writer: Arc::new(SeriesWriter::create(fs.series(name.as_ref())?)?),
            reader: Arc::new(SeriesReader::create(fs.series(name.as_ref())?)?),
        })
    }
}

pub struct SeriesTable {
    fs: FileSystem,
    entries: Arc<Mutex<HashMap<String, Arc<TableEntry>>>>,
}

impl SeriesTable {
    pub fn reader<S: AsRef<str>>(&self, name: S) -> Option<Arc<SeriesReader>> {
        let entries = self.entries.lock().unwrap();
        entries.get(name.as_ref()).map(|entry| entry.reader.clone())
    }
    pub fn writer<S: AsRef<str>>(&self, name: S) -> Option<Arc<SeriesWriter>> {
        let entries = self.entries.lock().unwrap();
        entries.get(name.as_ref()).map(|entry| entry.writer.clone())
    }
    pub fn create<S: AsRef<str>>(&self, name: S) -> Result<(), Error> {
        let mut entries = self.entries.lock().unwrap();
        if entries.contains_key(name.as_ref()) {
            return Ok(());
        }

        let entry = TableEntry::open_or_create(&self.fs, &name)?;
        entries.insert(name.as_ref().to_owned(), Arc::new(entry));

        Ok(())
    }
    pub fn create_temp(&self) -> Result<String, Error> {
        let name = format!(
            "restore-{}",
            time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        self.create(&name)?;
        Ok(name)
    }
    pub fn rename<S: AsRef<str>>(&self, src: S, dst: S) -> Result<bool, Error> {
        let mut entries = self.entries.lock().unwrap();
        if !entries.contains_key(src.as_ref()) || entries.contains_key(dst.as_ref()) {
            return Ok(false);
        }

        self.fs.rename_series(src.as_ref(), dst.as_ref())?;

        {
            entries.remove(src.as_ref());
        }

        let entry = TableEntry::open_or_create(&self.fs, dst.as_ref())?;
        entries.insert(dst.as_ref().to_owned(), Arc::new(entry));

        Ok(true)
    }
}

pub fn create(fs: FileSystem) -> Result<SeriesTable, Error> {
    let mut entries = HashMap::new();
    for name in fs.get_series()? {
        entries.insert(
            name.to_owned(),
            Arc::new(TableEntry::open_or_create(&fs, &name)?),
        );
    }

    Ok(SeriesTable {
        fs,
        entries: Arc::new(Mutex::new(entries)),
    })
}
