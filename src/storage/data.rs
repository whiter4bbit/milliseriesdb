use crc::crc32::{self, Hasher32};
use std::fs::File;
use std::hash::Hasher;
use std::io::prelude::*;
use std::io::{self, Cursor, SeekFrom};

use super::compression::Compression;
use super::entry::Entry;
use super::io_utils::{ReadBytes, WriteBytes};

const BLOCK_HEADER_SIZE: u64 = 4 + 1 + 4 + 4;

struct BlockHeader {
    entries_count: usize,
    compression: Compression,
    payload_size: usize,
}

fn header_checksum(entries_count: usize, compression: &Compression, payload_size: usize) -> u32 {
    let mut digest = crc32::Digest::new(crc32::IEEE);
    digest.write_usize(entries_count);
    digest.write_u8(compression.marker());
    digest.write_usize(payload_size);
    digest.sum32()
}

impl BlockHeader {
    fn read<R: Read>(file: &mut R) -> io::Result<BlockHeader> {
        let entries_count = file.read_u32()? as usize;
        let compr_marker = file.read_u8()?;
        let compression = match Compression::from_marker(compr_marker) {
            Some(compression) => compression,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown compression format: {}", compr_marker),
                ))
            }
        };
        let payload_size = file.read_u32()? as usize;
        let target_checksum = file.read_u32()?;
        let checksum = header_checksum(entries_count, &compression, payload_size);

        if target_checksum != checksum {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "crc32 mismatch"));
        }

        Ok(BlockHeader {
            entries_count,
            compression,
            payload_size,
        })
    }
    fn write(&self, file: &mut File) -> io::Result<()> {
        file.write_u32(&(self.entries_count as u32))?;
        file.write_u8(&(self.compression.marker()))?;
        file.write_u32(&(self.payload_size as u32))?;

        let checksum = header_checksum(self.entries_count, &self.compression, self.payload_size);
        file.write_u32(&checksum)?;
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
            file,
            offset,
            buffer: Cursor::new(Vec::new()),
        };

        writer.file.seek(SeekFrom::Start(offset))?;

        Ok(writer)
    }

    pub fn append<'a, I>(&mut self, block: I, compression: Compression) -> io::Result<u64>
    where
        I: IntoIterator<Item = &'a Entry> + 'a,
    {
        self.buffer.set_position(0);

        let entries: Vec<&Entry> = block.into_iter().collect();

        compression.write(&entries, &mut self.buffer)?;

        let block_size = self.buffer.position();

        let block_header = BlockHeader {
            entries_count: entries.len(),
            compression,
            payload_size: block_size as usize,
        };

        block_header.write(&mut self.file)?;

        let block_payload = &self.buffer.get_ref()[0..block_size as usize];

        self.file.write_all(block_payload)?;

        self.offset += block_size + BLOCK_HEADER_SIZE;

        Ok(self.offset)
    }
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}

pub struct BufDataReader {
    file: File,
    buf: Vec<u8>,
    buf_pos: usize,
    buf_len: usize,
    offset: u64,
}

impl BufDataReader {
    pub fn create(file: File, start_offset: u64) -> io::Result<BufDataReader> {
        let mut reader = BufDataReader {
            file: file,
            buf: vec![0u8; 5 * 1024 * 1024],
            buf_pos: 0,
            buf_len: 0,
            offset: start_offset,
        };

        reader.file.seek(SeekFrom::Start(start_offset))?;

        Ok(reader)
    }

    fn refill(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(self.offset))?;

        self.buf_pos = 0;
        self.buf_len = 0;

        while self.buf_len < self.buf.len() {
            let read = self.file.read(&mut self.buf[self.buf_len..])?;

            if read == 0 {
                break
            }

            self.buf_len += read;
        }

        Ok(())
    }

    pub fn read_block(&mut self) -> io::Result<(Vec<Entry>, u64)> {
        if self.buf_len - self.buf_pos < BLOCK_HEADER_SIZE as usize {
            self.refill()?;
        }

        let header = BlockHeader::read(&mut &self.buf[self.buf_pos..])?;

        self.buf_pos += BLOCK_HEADER_SIZE as usize;

        if self.buf_len - self.buf_pos < header.payload_size {
            self.refill()?;

            self.buf_pos += BLOCK_HEADER_SIZE as usize;
        }

        let compression = header.compression;

        let entries = compression.read(
            &mut &self.buf[self.buf_pos..self.buf_pos + header.payload_size],
            header.entries_count,
        )?;

        self.buf_pos += header.payload_size;

        self.offset += header.payload_size as u64 + BLOCK_HEADER_SIZE;

        Ok((entries, self.offset))
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

        {
            let file = series_dir.open(FileKind::Data, OpenMode::Write).unwrap();
            let mut writer = DataWriter::create(file, 0).unwrap();
            writer.append(&entries[0..3], Compression::Deflate).unwrap();
            writer.append(&entries[3..5], Compression::Deflate).unwrap();
        }

        {
            let file = series_dir.open(FileKind::Data, OpenMode::Read).unwrap();
            let mut reader = BufDataReader::create(file, 0).unwrap();

            let (result, _) = reader.read_block().unwrap();
            assert_eq!(entries[0..3].to_owned(), result);

            let (result, _) = reader.read_block().unwrap();
            assert_eq!(entries[3..5].to_owned(), result);
        }
    }
}
