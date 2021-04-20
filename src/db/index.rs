use super::io_utils;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::path::Path;
use std::time::SystemTime;

const INDEX_ENTRY_LENGTH: u64 = 16u64;

pub struct IndexWriter {
    offset: u64,
    file: File,
}

impl IndexWriter {
    pub fn open<P: AsRef<Path>>(path: P, offset: u64) -> io::Result<IndexWriter> {
        let mut file = io_utils::open_writable(path.as_ref().join("series.idx"))?;
        file.seek(SeekFrom::Start(offset))?;
        Ok(IndexWriter {
            offset: offset,
            file: file,
        })
    }
    pub fn append(&mut self, ts: u64, offset: u64) -> io::Result<u64> {
        self.file.write_all(&ts.to_be_bytes())?;
        self.file.write_all(&offset.to_be_bytes())?;
        self.offset += INDEX_ENTRY_LENGTH;
        Ok(self.offset)
    }
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}

pub struct IndexReader {
    file: File,
    entries: u64,
    buf: [u8; 8],
}

impl IndexReader {
    pub fn create<P: AsRef<Path>>(path: P, offset: u64) -> io::Result<IndexReader> {
        Ok(IndexReader {
            file: io_utils::open_readable(path.as_ref().join("series.idx"))?,
            entries: offset / INDEX_ENTRY_LENGTH,
            buf: [0u8; 8],
        })
    }

    pub fn read_raw_at(&mut self, offset: u64) -> io::Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;

        let mut entry = vec![0u8; INDEX_ENTRY_LENGTH as usize];
        self.file.read_exact(&mut entry)?;
        Ok(entry)
    }

    fn read_higher_ts(&mut self, entry_index: u64) -> io::Result<u64> {
        self.file.seek(SeekFrom::Start(entry_index * INDEX_ENTRY_LENGTH))?;
        self.file.read_exact(&mut self.buf)?;

        Ok(u64::from_be_bytes(self.buf))
    }

    fn read_offset(&mut self, entry_index: u64) -> io::Result<Option<u64>> {
        if entry_index >= self.entries {
            return Ok(None)
        }

        self.file.seek(SeekFrom::Start(entry_index * INDEX_ENTRY_LENGTH + 8))?;
        self.file.read_exact(&mut self.buf)?;

        Ok(Some(u64::from_be_bytes(self.buf)))
    }

    pub fn ceiling_offset(&mut self, target_ts: u64) -> io::Result<Option<u64>> {
        let start_ts = SystemTime::now();
        let mut scanned = 0usize;

        let mut lo = 0i128;
        let mut hi = (self.entries - 1) as i128;
        while lo <= hi {
            let m = lo + (hi - lo) / 2;

            if self.read_higher_ts(m as u64)? < target_ts {
                lo = m + 1;
            } else {
                hi = m - 1;
            }

            scanned += 1;
        }

        let result = self.read_offset(lo as u64);

        log::debug!("Index scanned {} entries took {}us", scanned, start_ts.elapsed().unwrap().as_micros());
        
        result
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::test_utils::create_temp_dir;

    #[test]
    fn test_ceiling() {
        let db_dir = create_temp_dir("test-dir").unwrap();

        let offset = {
            let mut writer = IndexWriter::open(&db_dir.path, 0).unwrap();
            writer.append(1, 11).unwrap();
            writer.append(4, 44).unwrap();
            writer.append(9, 99).unwrap()
        };

        {
            let mut reader = IndexReader::create(&db_dir.path, offset).unwrap();
            assert_eq!(Some(11), reader.ceiling_offset(0).unwrap());
            assert_eq!(Some(11), reader.ceiling_offset(1).unwrap());
            assert_eq!(Some(44), reader.ceiling_offset(3).unwrap());
            assert_eq!(Some(99), reader.ceiling_offset(5).unwrap());
            assert_eq!(Some(99), reader.ceiling_offset(9).unwrap());
            assert_eq!(None, reader.ceiling_offset(10).unwrap());
        }
    }
}
