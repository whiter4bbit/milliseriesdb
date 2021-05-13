use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

use super::error::Error;

const INDEX_ENTRY_LENGTH: u64 = 16u64;

pub struct IndexWriter {
    offset: u64,
    file: File,
}

impl IndexWriter {
    pub fn open(file: File, offset: u64) -> Result<IndexWriter, Error> {
        let mut writer = IndexWriter { offset, file };
        writer.file.seek(SeekFrom::Start(offset))?;
        Ok(writer)
    }
    pub fn append(&mut self, ts: i64, offset: u64) -> Result<u64, Error> {
        self.file.write_all(&ts.to_be_bytes())?;
        self.file.write_all(&offset.to_be_bytes())?;
        self.offset += INDEX_ENTRY_LENGTH;
        Ok(self.offset)
    }
    pub fn sync(&mut self) -> Result<(), Error> {
        self.file.sync_data()?;
        Ok(())
    }
}

pub struct IndexReader {
    file: File,
    entries: u64,
    buf: [u8; 8],
}

impl IndexReader {
    pub fn create(file: File, offset: u64) -> Result<IndexReader, Error> {
        Ok(IndexReader {
            file,
            entries: offset / INDEX_ENTRY_LENGTH,
            buf: [0u8; 8],
        })
    }

    fn read_higher_ts(&mut self, entry_index: u64) -> Result<i64, Error> {
        self.file
            .seek(SeekFrom::Start(entry_index * INDEX_ENTRY_LENGTH))?;
        self.file.read_exact(&mut self.buf)?;

        Ok(i64::from_be_bytes(self.buf))
    }

    fn read_offset(&mut self, entry_index: u64) -> Result<Option<u64>, Error> {
        if entry_index >= self.entries {
            return Ok(None);
        }

        let offset = entry_index * INDEX_ENTRY_LENGTH + 8;

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut self.buf)?;

        Ok(Some(u64::from_be_bytes(self.buf)))
    }

    pub fn ceiling_offset(&mut self, target_ts: i64) -> Result<Option<u64>, Error> {
        let mut lo = 0i128;
        let mut hi = (self.entries as i128 - 1) as i128;
        while lo <= hi {
            let m = lo + (hi - lo) / 2;

            if self.read_higher_ts(m as u64)? < target_ts {
                lo = m + 1;
            } else {
                hi = m - 1;
            }
        }

        self.read_offset(lo as u64)
    }
}

#[cfg(test)]
mod test {
    use super::super::file_system::{FileKind, OpenMode};
    use super::super::env;
    use super::*;

    #[test]
    fn test_ceiling() -> Result<(), Error> {
        let env = env::test::create()?;
        let series_dir = env.fs().series("series1")?;

        let offset = {
            let file = series_dir.open(FileKind::Index, OpenMode::Write)?;
            let mut writer = IndexWriter::open(file, 0)?;

            writer.append(1, 11)?;
            writer.append(4, 44)?;
            writer.append(9, 99)?
        };

        {
            let file = series_dir.open(FileKind::Index, OpenMode::Read)?;
            let mut reader = IndexReader::create(file, offset)?;

            assert_eq!(Some(11), reader.ceiling_offset(0)?);
            assert_eq!(Some(11), reader.ceiling_offset(1)?);
            assert_eq!(Some(44), reader.ceiling_offset(3)?);
            assert_eq!(Some(99), reader.ceiling_offset(5)?);
            assert_eq!(Some(99), reader.ceiling_offset(9)?);
            assert_eq!(None, reader.ceiling_offset(10)?);
        }

        Ok(())
    }
}
