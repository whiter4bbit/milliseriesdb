use crate::db::io_utils;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::path::Path;

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
}

pub struct IndexReader {
    file: File,
    entries: u64,
}

impl IndexReader {
    pub fn create<P: AsRef<Path>>(path: P, offset: u64) -> io::Result<IndexReader> {
        Ok(IndexReader {
            file: io_utils::open_readable(path.as_ref().join("series.idx"))?,
            entries: offset / INDEX_ENTRY_LENGTH,
        })
    }

    pub fn ceiling_offset(&mut self, target_ts: u64) -> io::Result<Option<u64>> {
        let mut buf = [8u8; 8];

        let mut lo = 0i128;
        let mut hi = (self.entries - 1) as i128;
        while lo <= hi {
            let m = lo + (hi - lo) / 2;

            let ts = {
                self.file.seek(SeekFrom::Start(m as u64 * INDEX_ENTRY_LENGTH))?;
                self.file.read_exact(&mut buf)?;

                u64::from_be_bytes(buf)
            };

            if ts < target_ts {
                lo = m + 1;
            } else {
                hi = m - 1;
            }
        }

        Ok(match (lo as u64) < self.entries {
            true => {
                self.file.seek(SeekFrom::Start(lo as u64 * INDEX_ENTRY_LENGTH + 8))?;
                self.file.read_exact(&mut buf)?;

                Some(u64::from_be_bytes(buf))
            }
            _ => None,
        })
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
