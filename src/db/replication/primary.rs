use super::super::data::DataReader;
use super::super::get_series_paths;
use super::super::index::IndexReader;
use super::super::log::{read_last_log_entry, LogEntry};
use super::block_batch::BlockBatch;

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

pub struct Session {
    base_path: PathBuf,
    primary: HashMap<String, LogEntry>,
    replica: HashMap<String, LogEntry>,
}

impl Session {
    fn refresh_primary_state(&mut self) -> io::Result<()> {
        for (name, series_path) in get_series_paths(&self.base_path)? {
            if let Some(last_log_entry) = read_last_log_entry(series_path.clone())? {
                self.primary.insert(name.clone(), last_log_entry);
            }
        }

        Ok(())
    }

    fn read_data_block(&self, name: &str, offset: u64) -> io::Result<(Vec<u8>, u64)> {
        let mut reader = DataReader::create(self.base_path.join("series").join(name), offset)?;

        let raw_block = reader.read_raw_block()?;

        reader.seek(offset)?;

        let mut block_entries = Vec::new();
        reader.read_block(&mut block_entries)?;

        if block_entries.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, "Block is empty"));
        }

        Ok((raw_block, block_entries[block_entries.len() - 1].ts))
    }

    fn read_index_block(&self, name: &str, offset: u64) -> io::Result<Vec<u8>> {
        let mut reader = IndexReader::create(self.base_path.join("series").join(name), offset)?;

        reader.read_raw_at(offset)
    }

    fn create_block_batch(&self, name: &str, from: &LogEntry, until: &LogEntry) -> io::Result<BlockBatch> {
        let (data_block, highest_ts) = self.read_data_block(name, from.data_offset)?;
        let next_data_offset = from.data_offset + data_block.len() as u64;

        let index_block = self.read_index_block(name, from.index_offset)?;
        let next_index_offset = from.index_offset + index_block.len() as u64;

        Ok(BlockBatch {
            series: name.to_owned(),
            data: data_block,
            index: index_block,
            before: from.clone(),
            after: LogEntry {
                data_offset: next_data_offset,
                index_offset: next_index_offset,
                highest_ts: highest_ts,
            },
        })
    }
    #[allow(dead_code)]
    pub fn next_batch(&mut self) -> io::Result<Option<BlockBatch>> {
        self.refresh_primary_state()?;

        for (name, primary_log_entry) in self.primary.iter() {
            let replica_log_entry = self
                .replica
                .entry(name.clone())
                .or_insert(LogEntry {
                    data_offset: 0,
                    index_offset: 0,
                    highest_ts: 0,
                })
                .clone();

            if primary_log_entry.data_offset > replica_log_entry.data_offset {
                return Ok(Some(self.create_block_batch(name, &replica_log_entry, primary_log_entry)?));
            }
        }

        Ok(None)
    }
    #[allow(dead_code)]
    pub fn acknowledge(&mut self, name: &str, log_entry: &LogEntry) -> io::Result<()> {
        let current = self
            .replica
            .entry(name.to_owned())
            .or_insert(LogEntry {
                data_offset: 0,
                index_offset: 0,
                highest_ts: 0,
            })
            .clone();

        if current.data_offset < log_entry.data_offset {
            self.replica.insert(name.to_owned(), log_entry.clone());
        }

        Ok(())
    }
}

pub struct Primary {
    base_path: PathBuf,
}

impl Primary {
    #[allow(dead_code)]
    pub fn create<P: AsRef<Path>>(base_path: P) -> io::Result<Primary> {
        Ok(Primary {
            base_path: base_path.as_ref().to_path_buf(),
        })
    }

    #[allow(dead_code)]
    pub fn handshake(&self, replica: HashMap<String, LogEntry>) -> io::Result<Session> {
        Ok(Session {
            base_path: self.base_path.clone(),
            primary: HashMap::new(),
            replica: replica,
        })
    }
}

#[cfg(test)]
mod test {
    use super::super::super::test_utils::create_temp_dir;
    use super::super::super::{Compression, Entry, SyncMode, DB};
    use super::*;

    #[test]
    fn test_replica() {
        let db_dir = create_temp_dir("test-base").unwrap();

        let mut db = DB::open(&db_dir.path, SyncMode::Paranoid).unwrap();
        let mut primary = Primary::create(&db_dir.path).unwrap();

        let mut session1 = primary.handshake(HashMap::new()).unwrap();

        assert!(session1.next_batch().unwrap().is_none());

        db.create_series("series1").unwrap();

        assert!(session1.next_batch().unwrap().is_none());

        db.writer("series1")
            .unwrap()
            .append(&vec![Entry { ts: 1, value: 1.0 }], Compression::Delta)
            .unwrap();

        assert!(session1.next_batch().unwrap().is_some());

        let first_batch = session1.next_batch().unwrap().unwrap();

        session1.acknowledge("series1", &first_batch.after).unwrap();

        assert!(session1.next_batch().unwrap().is_none());

        db.writer("series1")
            .unwrap()
            .append(&vec![Entry { ts: 2, value: 2.0 }], Compression::Delta)
            .unwrap();        

        assert!(session1.next_batch().unwrap().is_some());

        let second_batch = session1.next_batch().unwrap().unwrap();

        assert_eq!(second_batch.before, first_batch.after);

        assert_eq!(second_batch.after.highest_ts, 2);
    }
}
