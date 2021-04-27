use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader, Cursor, SeekFrom};

use super::compression::Compression;
use super::entry::Entry;
use super::io_utils::{ReadBytes, WriteBytes};

const BLOCK_HEADER_SIZE: u64 = 4 + 1 + 4;

struct BlockHeader {
    entries_count: usize,
    compression: Compression,
    payload_size: usize,
}

impl BlockHeader {
    fn read<R: Read>(file: &mut R) -> io::Result<BlockHeader> {
        let entries_count = file.read_u32()? as usize;
        let compression = match Compression::from_marker(file.read_u8()?) {
            Some(compression) => compression,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unknown compression format",
                ))
            }
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
}

impl DataWriter {
    pub fn create(file: File, offset: u64) -> io::Result<DataWriter> {
        let mut writer = DataWriter {
            file: file,
            offset: offset,
            buffer: Cursor::new(Vec::new()),
        };

        writer.file.seek(SeekFrom::Start(offset))?;

        Ok(writer)
    }
    pub fn append(&mut self, block: &[&Entry], compression: Compression) -> io::Result<u64> {
        self.buffer.set_position(0);

        compression.write(block, &mut self.buffer)?;

        let block_size = self.buffer.position();

        BlockHeader {
            entries_count: block.len(),
            compression: compression.clone(),
            payload_size: block_size as usize,
        }
        .write(&mut self.file)?;

        let payload = &self.buffer.get_ref()[0..block_size as usize];

        self.file.write_all(payload)?;

        self.offset += block_size + BLOCK_HEADER_SIZE;

        Ok(self.offset)
    }
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}

pub struct DataReader {
    file: BufReader<File>,
    offset: u64,
}

impl DataReader {
    pub fn create(file: File, start_offset: u64) -> io::Result<DataReader> {
        let mut reader = DataReader {
            file: BufReader::with_capacity(2 * 1024 * 1024, file),
            offset: start_offset,
        };

        reader.file.seek(SeekFrom::Start(start_offset))?;

        Ok(reader)
    }

    pub fn read_block<D: Extend<Entry>>(&mut self, destination: &mut D) -> io::Result<u64> {
        let header = BlockHeader::read(&mut self.file)?;
        let mut payload = self.file.by_ref().take(header.payload_size as u64);
        destination.extend(
            header
                .compression
                .read(&mut payload, header.entries_count)?,
        );
        self.offset += header.payload_size as u64 + BLOCK_HEADER_SIZE;
        Ok(self.offset)
    }
}

#[cfg(test)]
mod test {
    use super::super::file_system::{self, FileKind, OpenMode};
    use super::super::test_utils::create_temp_dir;
    use super::*;
        
    #[test]
    fn test_read_write() {
        let db_dir = create_temp_dir("test-path").unwrap();
        let fs = file_system::open(&db_dir.path).unwrap();
        let series_dir = fs.series("series1").unwrap();

        let entries = vec![
            Entry { ts: 1, value: 11.0 },
            Entry { ts: 2, value: 21.0 },
            Entry { ts: 3, value: 31.0 },
            Entry { ts: 4, value: 41.0 },
            Entry { ts: 5, value: 51.0 },
        ];

        let mut writer =
            DataWriter::create(series_dir.open(FileKind::Data, OpenMode::Write).unwrap(), 0)
                .unwrap();
        writer
            .append(
                &entries[0..3].iter().collect::<Vec<&Entry>>(),
                Compression::Deflate,
            )
            .unwrap();
        writer
            .append(
                &entries[3..5].iter().collect::<Vec<&Entry>>(),
                Compression::Deflate,
            )
            .unwrap();

        {
            let mut reader =
                DataReader::create(series_dir.open(FileKind::Data, OpenMode::Read).unwrap(), 0)
                    .unwrap();
            let mut result: Vec<Entry> = Vec::new();

            reader.read_block(&mut result).unwrap();
            assert_eq!(entries[0..3].to_owned(), result);

            result.clear();

            reader.read_block(&mut result).unwrap();
            assert_eq!(entries[3..5].to_owned(), result);
        }
    }
}
