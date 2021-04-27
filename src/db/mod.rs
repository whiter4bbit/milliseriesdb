mod data;
mod entry;
mod index;
mod io_utils;
mod log;
mod table;
mod compression;

#[cfg(test)]
mod test_utils;

pub use entry::Entry;
use std::io;
use std::path::{Path, PathBuf};
pub use table::SyncMode;
use table::{TableIterator, TableReader, TableWriter};

#[allow(dead_code)]
pub struct DB {
    writer: TableWriter,
    path: PathBuf,
}

impl DB {
    #[allow(dead_code)]
    pub fn open_or_create<P: AsRef<Path>>(path: P, sync_mode: SyncMode) -> io::Result<DB> {
        Ok(DB {
            writer: TableWriter::create(path.as_ref(), sync_mode)?,
            path: path.as_ref().to_path_buf(),
        })
    }
    #[allow(dead_code)]
    pub fn append(&mut self, batch: &[Entry]) -> io::Result<()> {
        self.writer.append_batch(batch)
    }
    #[allow(dead_code)]
    pub fn iterator(&mut self, from_ts: u64) -> io::Result<TableIterator> {
        let mut reader = TableReader::create(&self.path)?;
        reader.iterator(from_ts)
    }
}