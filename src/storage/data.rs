use crc::crc32;
use std::convert::TryInto;
use std::fs::File;
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom};

use super::compression::Compression;
use super::entry::Entry;
use super::io_utils::WriteBytes;
use super::error::Error;

const BLOCK_HEADER_SIZE: u64 = 4 + 1 + 4 + 4;

struct BlockHeader {
    entries_count: u32,
    compression: Compression,
    payload_size: u32,
}

impl BlockHeader {
    fn checksum(&self) -> u32 {
        let table = &crc32::IEEE_TABLE;
        let mut checksum = 0u32;

        checksum = crc32::update(checksum, table, &(self.entries_count as u64).to_le_bytes());
        checksum = crc32::update(checksum, table, &[self.compression.marker()]);
        checksum = crc32::update(checksum, table, &(self.payload_size as u64).to_le_bytes());

        checksum
    }
    fn read(bytes: &[u8]) -> Result<BlockHeader, Error> {
        let header = BlockHeader {
            entries_count: u32::from_be_bytes(bytes[..4].try_into()?),
            compression: {
                let marker = bytes[4];

                match Compression::from_marker(marker) {
                    Some(compression) => compression,
                    None => return Err(Error::UnknownCompression),
                }
            },
            payload_size: u32::from_be_bytes(bytes[5..9].try_into()?),
        };

        let checksum = u32::from_be_bytes(bytes[9..13].try_into()?);

        if checksum != header.checksum() {
            return Err(Error::Crc32Mismatch);
        }

        Ok(header)
    }
    fn write(&self, file: &mut File) -> Result<(), Error> {
        file.write_u32(&self.entries_count)?;
        file.write_u8(&(self.compression.marker()))?;
        file.write_u32(&self.payload_size)?;

        file.write_u32(&self.checksum())?;
        Ok(())
    }
}

pub struct DataWriter {
    file: File,
    offset: u64,
    buffer: Cursor<Vec<u8>>,
}

impl DataWriter {
    pub fn create(file: File, offset: u64) -> Result<DataWriter, Error> {
        let mut writer = DataWriter {
            file,
            offset,
            buffer: Cursor::new(Vec::new()),
        };

        writer.file.seek(SeekFrom::Start(offset))?;

        Ok(writer)
    }

    pub fn append<'a, I>(&mut self, block: I, compression: Compression) -> Result<u64, Error>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        self.buffer.set_position(0);

        let entries: Vec<&Entry> = block.into_iter().collect();

        compression.write(&entries, &mut self.buffer)?;

        let block_size = self.buffer.position();

        let block_header = BlockHeader {
            entries_count: entries.len() as u32,
            compression,
            payload_size: block_size as u32,
        };

        block_header.write(&mut self.file)?;

        let block_payload = &self.buffer.get_ref()[0..block_size as usize];

        self.file.write_all(block_payload)?;

        self.offset += block_size + BLOCK_HEADER_SIZE;

        Ok(self.offset)
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
    pub fn create(file: File, start_offset: u64) -> Result<DataReader, Error> {
        let mut reader = DataReader {
            file: file,
            buf: vec![0u8; 5 * 1024 * 1024],
            buf_pos: 0,
            buf_len: 0,
            offset: start_offset,
        };

        reader.file.seek(SeekFrom::Start(start_offset))?;

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

    pub fn read_block(&mut self) -> Result<(Vec<Entry>, u64), Error> {
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

        Ok((entries, self.offset))
    }
}

#[cfg(test)]
mod test {
    use super::super::file_system::{self, FileKind, OpenMode};
    use super::*;

    #[test]
    fn test_read_write() -> Result<(), Error>{
        let fs = &file_system::open_temp()?;
        let series_dir = fs.series("series1")?;

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

    // #[test]
    // fn test_small_buf() -> Result<(), Error> {
    //     let fs = file_system::open_temp()?;
    //     let series_dir = fs.series("series1")?;

    //     {
    //         let file = series_dir.open(FileKind::Data, OpenMode::Write)?;
    //         let mut writer = DataWriter::create(file, 0)?;
            
    //     }
    // }
}
