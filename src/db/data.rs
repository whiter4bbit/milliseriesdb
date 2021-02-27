use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{self, Cursor};
use std::path::Path;

use super::compression::Compression;
use super::entry::Entry;
use super::io_utils::{self, ReadBytes, WriteBytes};

const BLOCK_HEADER_SIZE: u64 = 4 + 1 + 4;

struct BlockHeader {
    entries_count: usize,
    compression: Compression,
    payload_size: usize,
}

impl BlockHeader {
    fn read(file: &mut File) -> io::Result<BlockHeader> {
        let entries_count = file.read_u32()? as usize;
        let compression = match Compression::from_marker(file.read_u8()?) {
            Some(compression) => compression,
            None => return Err(io::Error::new(io::ErrorKind::Other, "Unknown compression format")),
        };
        let payload_size = file.read_u32()? as usize;
        Ok(BlockHeader {
            entries_count: entries_count,
            compression: compression,
            payload_size: payload_size,
        })
    }
    fn write(&self, file: &mut File) -> io::Result<()> {
        file.write_u32(&(self.entries_count as u32))?;
        file.write_u8(&(self.compression.marker()))?;
        file.write_u32(&(self.payload_size as u32))?;
        Ok(())
    }
}

pub struct DataWriter {
    file: File,
    offset: u64,
    buffer: Cursor<Vec<u8>>,
    compression: Compression,
}

impl DataWriter {
    pub fn create<P: AsRef<Path>>(path: P, offset: u64, compression: Compression) -> io::Result<DataWriter> {
        let mut file = io_utils::open_writable(path.as_ref().join("series.dat"))?;
        file.seek(SeekFrom::Start(offset))?;

        Ok(DataWriter {
            file: file,
            offset: offset,
            buffer: Cursor::new(Vec::new()),
            compression: compression,
        })
    }
    pub fn append(&mut self, block: &[&Entry]) -> io::Result<u64> {
        self.buffer.set_position(0);
        self.compression.write(block, &mut self.buffer)?;

        let block_size = self.buffer.position();

        BlockHeader {
            entries_count: block.len(),
            compression: self.compression.clone(),
            payload_size: block_size as usize,
        }.write(&mut self.file)?;
        
        self.file.write_all(&self.buffer.get_ref()[0..block_size as usize])?;

        self.offset += block_size + BLOCK_HEADER_SIZE;
        Ok(self.offset)
    }
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}

pub struct DataReader {
    file: File,
    buffer: Vec<u8>,
}

impl DataReader {
    pub fn create<P: AsRef<Path>>(path: P, _: u64) -> io::Result<DataReader> {
        Ok(DataReader {
            file: io_utils::open_readable(path.as_ref().join("series.dat"))?,
            buffer: Vec::new(),
        })
    }
    pub fn read_block(&mut self, offset: u64, destination: &mut Vec<Entry>) -> io::Result<u64> {
        self.file.seek(SeekFrom::Start(offset))?;
        
        let header = BlockHeader::read(&mut self.file)?;

        while self.buffer.len() < header.payload_size {
            self.buffer.push(0u8);
        }

        self.file.read_exact(&mut self.buffer[0..header.payload_size])?;
        
        let mut reader = Cursor::new(&self.buffer);
        for entry in header.compression.read(&mut reader, header.entries_count)? {
            destination.push(entry);
        }
        Ok(offset + header.payload_size as u64 + BLOCK_HEADER_SIZE)
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

        let mut writer = DataWriter::create(&db_dir.path, 0, Compression::None).unwrap();
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
