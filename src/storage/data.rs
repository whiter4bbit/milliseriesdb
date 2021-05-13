use crc::crc16;
use std::convert::TryInto;
use std::fs::File;
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom};

use super::compression::Compression;
use super::entry::Entry;
use super::error::Error;
use super::io_utils::WriteBytes;

const BLOCK_HEADER_SIZE: u64 = 2 + 1 + 4 + 2;

#[cfg(not(test))]
const MAX_DATA_FILE_SIZE: u32 = u32::MAX;

#[cfg(test)]
const MAX_DATA_FILE_SIZE: u32 = 10 * 1024 * 1024;

const MAX_BLOCK_SIZE: u32 = 2 * 1024 * 1024;

pub const MAX_ENTRIES_PER_BLOCK: usize = u16::MAX as usize;

struct BlockHeader {
    entries_count: u16,
    compression: Compression,
    payload_size: u32,
}

impl BlockHeader {
    fn checksum(&self) -> u16 {
        let table = &crc16::USB_TABLE;
        let mut checksum = 0u16;

        checksum = crc16::update(checksum, table, &(self.entries_count).to_be_bytes());
        checksum = crc16::update(checksum, table, &[self.compression.marker()]);
        checksum = crc16::update(checksum, table, &(self.payload_size).to_be_bytes());

        checksum
    }
    fn read(bytes: &[u8]) -> Result<BlockHeader, Error> {
        let header = BlockHeader {
            entries_count: u16::from_be_bytes(bytes[..2].try_into()?),
            compression: {
                let marker = bytes[2];

                match Compression::from_marker(marker) {
                    Some(compression) => compression,
                    None => return Err(Error::UnknownCompression),
                }
            },
            payload_size: u32::from_be_bytes(bytes[3..7].try_into()?),
        };

        let checksum = u16::from_be_bytes(bytes[7..9].try_into()?);

        if checksum != header.checksum() {
            return Err(Error::Crc16Mismatch);
        }

        Ok(header)
    }
    fn write(&self, file: &mut File) -> Result<(), Error> {
        file.write_u16(&self.entries_count)?;
        file.write_u8(&(self.compression.marker()))?;
        file.write_u32(&self.payload_size)?;

        file.write_u16(&self.checksum())?;
        Ok(())
    }
}

pub struct DataWriter {
    file: File,
    offset: u64,
    buffer: Cursor<Vec<u8>>,
}

impl DataWriter {
    pub fn create(file: File, offset: u32) -> Result<DataWriter, Error> {
        let mut writer = DataWriter {
            file,
            offset: offset as u64,
            buffer: Cursor::new(Vec::with_capacity(MAX_BLOCK_SIZE as usize)),
        };

        writer.file.seek(SeekFrom::Start(offset as u64))?;

        Ok(writer)
    }

    pub fn append<'a, I>(&mut self, entries: I, compression: Compression) -> Result<u32, Error>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        let entries: Vec<&Entry> = entries.into_iter().collect();

        if entries.len() > MAX_ENTRIES_PER_BLOCK {
            return Err(Error::TooManyEntries);
        }

        self.buffer.set_position(0);

        compression.write(&entries, &mut self.buffer)?;

        let payload_size = self.buffer.position();

        let next_offset = self.offset + payload_size + BLOCK_HEADER_SIZE;

        if next_offset > MAX_DATA_FILE_SIZE as u64 {
            return Err(Error::DataFileTooBig);
        }

        let block_header = BlockHeader {
            entries_count: entries.len() as u16,
            compression,
            payload_size: payload_size as u32,
        };

        block_header.write(&mut self.file)?;

        let block_payload = &self.buffer.get_ref()[0..payload_size as usize];

        self.file.write_all(block_payload)?;

        self.offset = next_offset;

        Ok(self.offset as u32)
    }
    pub fn sync(&mut self) -> Result<(), Error> {
        self.file.sync_data()?;
        Ok(())
    }
}

pub struct DataReader {
    file: File,
    buf: Vec<u8>,
    buf_pos: usize,
    buf_len: usize,
    offset: u64,
}

impl DataReader {
    pub fn create(file: File, start_offset: u32) -> Result<DataReader, Error> {
        let mut reader = DataReader {
            file: file,
            buf: vec![0u8; 5 * 1024 * 1024],
            buf_pos: 0,
            buf_len: 0,
            offset: start_offset as u64,
        };

        reader.file.seek(SeekFrom::Start(start_offset as u64))?;

        Ok(reader)
    }

    fn refill(&mut self) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(self.offset))?;

        self.buf_pos = 0;
        self.buf_len = 0;

        while self.buf_len < self.buf.len() {
            let read = self.file.read(&mut self.buf[self.buf_len..])?;

            if read == 0 {
                break;
            }

            self.buf_len += read;
        }

        Ok(())
    }

    pub fn read_block(&mut self) -> Result<(Vec<Entry>, u32), Error> {
        if self.buf_len - self.buf_pos < BLOCK_HEADER_SIZE as usize {
            self.refill()?;
        }

        let header = BlockHeader::read(&self.buf[self.buf_pos..])?;

        self.buf_pos += BLOCK_HEADER_SIZE as usize;

        let payload_size = header.payload_size as usize;

        if self.buf_len - self.buf_pos < payload_size {
            self.refill()?;

            self.buf_pos += BLOCK_HEADER_SIZE as usize;
        }

        let compression = header.compression;

        let entries = compression.read(
            &self.buf[self.buf_pos..self.buf_pos + payload_size],
            header.entries_count as usize,
        )?;

        self.buf_pos += payload_size;

        self.offset += header.payload_size as u64 + BLOCK_HEADER_SIZE;

        Ok((entries, self.offset as u32))
    }
}

#[cfg(test)]
mod test {
    use super::super::file_system::{FileKind, OpenMode};
    use super::super::env;
    use super::*;

    #[test]
    fn test_read_write() -> Result<(), Error> {
        let env = env::test::create()?;        
        let series_dir = env.fs().series("series1")?;

        let entries = vec![
            Entry { ts: 1, value: 11.0 },
            Entry { ts: 2, value: 21.0 },
            Entry { ts: 3, value: 31.0 },
            Entry { ts: 4, value: 41.0 },
            Entry { ts: 5, value: 51.0 },
        ];

        {
            let file = series_dir.open(FileKind::Data, OpenMode::Write)?;
            let mut writer = DataWriter::create(file, 0)?;
            writer.append(&entries[0..3], Compression::Deflate)?;
            writer.append(&entries[3..5], Compression::Deflate)?;
        }

        {
            let file = series_dir.open(FileKind::Data, OpenMode::Read)?;
            let mut reader = DataReader::create(file, 0)?;

            let (result, _) = reader.read_block()?;
            assert_eq!(entries[0..3].to_owned(), result);

            let (result, _) = reader.read_block()?;
            assert_eq!(entries[3..5].to_owned(), result);
        }

        Ok(())
    }

    fn entries(count: usize) -> Vec<Entry> {
        (0..count)
            .into_iter()
            .map(|_| Entry { ts: 1, value: 0.0 })
            .collect::<Vec<Entry>>()
    }

    #[test]
    fn test_max_entries() -> Result<(), Error> {
        let env = env::test::create()?;        
        let series_dir = env.fs().series("series1")?;

        {
            let file = series_dir.open(FileKind::Data, OpenMode::Write)?;
            let mut writer = DataWriter::create(file, 0)?;

            assert!(
                match writer.append(&entries(MAX_ENTRIES_PER_BLOCK), Compression::None) {
                    Ok(_) => true,
                    _ => false,
                }
            );

            assert!(
                match writer.append(&entries(MAX_ENTRIES_PER_BLOCK + 1), Compression::None) {
                    Err(Error::TooManyEntries) => true,
                    _ => false,
                }
            );
        }

        Ok(())
    }

    #[test]
    fn test_max_data_file_size() -> Result<(), Error> {
        let env = env::test::create()?;        
        let series_dir = env.fs().series("series1")?;

        {
            let file = series_dir.open(FileKind::Data, OpenMode::Write)?;
            let mut writer = DataWriter::create(file, 0)?;

            let entries = entries(MAX_ENTRIES_PER_BLOCK);

            for _ in 1..=10 {
                assert!(match writer.append(&entries, Compression::None) {
                    Ok(_) => true,
                    Err(Error::DataFileTooBig) => true,
                    _ => false,
                });
            }

            assert!(match writer.append(&entries, Compression::None) {
                Err(Error::DataFileTooBig) => true,
                _ => false,
            });
        }

        Ok(())
    }
}
