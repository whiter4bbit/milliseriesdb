use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{self, Cursor};
use std::path::Path;

use crate::db::entry::Entry;
use crate::db::io_utils::{self, ReadBytes, WriteBytes};

pub struct DataWriter {
    file: File,
    offset: u64,
}

impl DataWriter {
    pub fn create<P: AsRef<Path>>(path: P, offset: u64) -> io::Result<DataWriter> {
        let mut file = io_utils::open_writable(path.as_ref().join("series.dat"))?;
        file.seek(SeekFrom::Start(offset))?;
        Ok(DataWriter {
            file: file,
            offset: offset,
        })
    }
    pub fn append(&mut self, block: &[&Entry]) -> io::Result<u64> {
        let mut raw = Vec::new();
        for entry in block {
            raw.write_u64(&entry.ts)?;
            raw.write_f64(&entry.value)?;
        }
        self.offset += raw.len() as u64 + 4 + 4;
        self.file.write_u32(&(block.len() as u32))?;
        self.file.write_u32(&(raw.len() as u32))?;
        self.file.write_all(&raw)?;
        Ok(self.offset)
    }
}

pub struct DataReader {
    file: File,
}

impl DataReader {
    pub fn create<P: AsRef<Path>>(path: P, _: u64) -> io::Result<DataReader> {
        Ok(DataReader {
            file: io_utils::open_readable(path.as_ref().join("series.dat"))?,
        })
    }
    pub fn read_block(&mut self, offset: u64, destination: &mut Vec<Entry>) -> io::Result<u64> {
        self.file.seek(SeekFrom::Start(offset))?;

        let entries_count = self.file.read_u32()? as usize;
        let block_size = self.file.read_u32()? as usize;

        let mut block = vec![0u8; block_size];
        self.file.read_exact(&mut block)?;

        let mut reader = Cursor::new(&block);
        for _ in 0..entries_count {
            destination.push(Entry {
                ts: reader.read_u64()?,
                value: reader.read_f64()?,
            });
        }
        Ok(offset + 4 + 4 + block_size as u64)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::test_utils::create_temp_dir;
    #[test]
    fn test_read_write() {
        let db_dir = create_temp_dir("test-path").unwrap();

        let entries = vec![
            Entry { ts: 1, value: 11.0 },
            Entry { ts: 2, value: 21.0 },
            Entry { ts: 3, value: 31.0 },
            Entry { ts: 4, value: 41.0 },
            Entry { ts: 5, value: 51.0 },
        ];

        let mut writer = DataWriter::create(&db_dir.path, 0).unwrap();
        let offset_block0 = 0u64;
        let offset_block1 = writer.append(&entries[0..3].iter().collect::<Vec<&Entry>>()).unwrap();
        let offset_block2 = writer.append(&entries[3..5].iter().collect::<Vec<&Entry>>()).unwrap();

        {
            let mut reader = DataReader::create(&db_dir.path, offset_block2).unwrap();
            let mut result: Vec<Entry> = Vec::new();

            reader.read_block(offset_block0, &mut result).unwrap();
            assert_eq!(entries[0..3].to_owned(), result);

            result.clear();

            reader.read_block(offset_block1, &mut result).unwrap();
            assert_eq!(entries[3..5].to_owned(), result);
        }
    }
}
