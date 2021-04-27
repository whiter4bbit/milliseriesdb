use std::fs::File;
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::time::SystemTime;

const INDEX_ENTRY_LENGTH: u64 = 16u64;

pub struct IndexWriter {
    offset: u64,
    file: File,
}

impl IndexWriter {
    pub fn open(file: File, offset: u64) -> io::Result<IndexWriter> {
        let mut writer = IndexWriter {
            offset: offset,
            file: file,
        };
        writer.file.seek(SeekFrom::Start(offset))?;
        Ok(writer)
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
    pub fn create(file: File, offset: u64) -> io::Result<IndexReader> {
        Ok(IndexReader {
            file: file,
            entries: offset / INDEX_ENTRY_LENGTH,
            buf: [0u8; 8],
        })
    }

    fn read_higher_ts(&mut self, entry_index: u64) -> io::Result<u64> {
        self.file
            .seek(SeekFrom::Start(entry_index * INDEX_ENTRY_LENGTH))?;
        self.file.read_exact(&mut self.buf)?;

        Ok(u64::from_be_bytes(self.buf))
    }

    fn read_offset(&mut self, entry_index: u64) -> io::Result<Option<u64>> {
        if entry_index >= self.entries {
            return Ok(None);
        }

        let offset = entry_index * INDEX_ENTRY_LENGTH + 8;

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut self.buf)?;

        Ok(Some(u64::from_be_bytes(self.buf)))
    }

    pub fn ceiling_offset(&mut self, target_ts: u64) -> io::Result<Option<u64>> {
        let start_ts = SystemTime::now();
        let mut scanned = 0usize;

        let mut lo = 0i128;
        let mut hi = (self.entries as i128 - 1) as i128;
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

        log::debug!(
            "Index scanned {} entries took {}us",
            scanned,
            start_ts.elapsed().unwrap().as_micros()
        );
        result
    }
}

#[cfg(test)]
mod test {
    use super::super::file_system::{self, FileKind, OpenMode};
    use super::*;
    use super::super::test_utils::create_temp_dir;

    #[test]
    fn test_ceiling() {
        let db_dir = create_temp_dir("test-dir").unwrap();
        let fs = file_system::open(&db_dir.path).unwrap();
        let series_dir = fs.series("series1").unwrap();

        let offset = {
            let mut writer = IndexWriter::open(
                series_dir.open(FileKind::Index, OpenMode::Write).unwrap(),
                0,
            )
            .unwrap();

            writer.append(1, 11).unwrap();
            writer.append(4, 44).unwrap();
            writer.append(9, 99).unwrap()
        };

        {
            let mut reader = IndexReader::create(
                series_dir.open(FileKind::Index, OpenMode::Read).unwrap(),
                offset,
            )
            .unwrap();

            assert_eq!(Some(11), reader.ceiling_offset(0).unwrap());
            assert_eq!(Some(11), reader.ceiling_offset(1).unwrap());
            assert_eq!(Some(44), reader.ceiling_offset(3).unwrap());
            assert_eq!(Some(99), reader.ceiling_offset(5).unwrap());
            assert_eq!(Some(99), reader.ceiling_offset(9).unwrap());
            assert_eq!(None, reader.ceiling_offset(10).unwrap());
        }
    }
}
