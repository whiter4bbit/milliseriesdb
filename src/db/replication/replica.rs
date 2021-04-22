use super::block_batch::BlockBatch;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::io;
use super::super::log::{read_last_log_entry, LogEntry};
use super::super::get_series_paths;

struct Updater {

}

pub struct Replica {
    base_path: PathBuf,
}

impl Replica {
    pub fn create<P: AsRef<Path>>(base_path: P) -> io::Result<Replica> {
        Ok(Replica {
            base_path: base_path.as_ref().to_path_buf(),
        })
    }

    pub fn handshake(&self) -> io::Result<HashMap<String, LogEntry>> {
        let mut state = HashMap::new();
        for (name, series_path) in get_series_paths(&self.base_path)? {
            if let Some(last_log_entry) = read_last_log_entry(series_path.clone())? {
                state.insert(name.clone(), last_log_entry);
            }
        }
        Ok(state)
    }

    pub fn accept_block(&self, batch: &BlockBatch) -> io::Result<LogEntry> {
        Ok(batch.after)
    }
}