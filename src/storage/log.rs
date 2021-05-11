use crc::crc32;
use std::collections::VecDeque;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::sync::Arc;

use super::error::Error;
use super::file_system::{FileKind, OpenMode, SeriesDir};
use super::io_utils::{ReadBytes, WriteBytes};

#[derive(Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub data_offset: u64,
    pub index_offset: u64,
    pub highest_ts: u64,
}

#[allow(dead_code)]
const LOG_ENTRY_SIZE: u64 = 8 + 8 + 8 + 4;

const ZERO_ENTRY: LogEntry = LogEntry {
    data_offset: 0,
    index_offset: 0,
    highest_ts: 0,
};

impl LogEntry {
    fn checksum(&self) -> u32 {
        let table = &crc32::IEEE_TABLE;
        let mut checksum = 0u32;

        checksum = crc32::update(checksum, table, &self.data_offset.to_le_bytes());
        checksum = crc32::update(checksum, table, &self.index_offset.to_le_bytes());
        checksum = crc32::update(checksum, table, &self.highest_ts.to_le_bytes());

        checksum
    }
    fn read_entry<R: Read>(read: &mut R) -> Result<LogEntry, Error> {
        let entry = LogEntry {
            data_offset: read.read_u64()?,
            index_offset: read.read_u64()?,
            highest_ts: read.read_u64()?,
        };

        let checksum = read.read_u32()?;

        if checksum != entry.checksum() {
            return Err(Error::Crc32Mismatch);
        }

        Ok(entry)
    }
    fn write_entry<W: Write>(&self, write: &mut W) -> Result<(), Error> {
        write.write_u64(&self.data_offset)?;
        write.write_u64(&self.index_offset)?;
        write.write_u64(&self.highest_ts)?;
        write.write_u32(&self.checksum())?;
        Ok(())
    }
}

pub struct LogReader {
    dir: Arc<SeriesDir>,
}

impl LogReader {
    pub fn create(dir: Arc<SeriesDir>) -> LogReader {
        LogReader { dir }
    }

    fn read_last_entry(&self, seq: u64) -> Result<Option<LogEntry>, Error> {
        let mut file = BufReader::new(self.dir.open(FileKind::Log(seq), OpenMode::Read)?);
        let mut last: Option<LogEntry> = None;
        loop {
            match LogEntry::read_entry(&mut file) {
                Err(Error::Io(error)) => match error.kind() {
                    io::ErrorKind::UnexpectedEof => break,
                    _ => return Err(Error::Io(error)),
                },
                Err(Error::Crc32Mismatch) => break,
                Err(error) => return Err(error),
                Ok(entry) => last = Some(entry),
            }
        }
        Ok(last)
    }

    pub fn get_last_entry_or_default(&self) -> Result<LogEntry, Error> {
        for seq in self.dir.read_log_sequences()? {
            if let Some(entry) = self.read_last_entry(seq)? {
                return Ok(entry);
            }
        }
        Ok(ZERO_ENTRY)
    }
}

pub struct LogWriter {
    file: File,
    sequence: u64,
    max_size: u64,
    current_size: u64,
    sequences: VecDeque<u64>,
    dir: Arc<SeriesDir>,
}

impl LogWriter {
    pub fn create(dir: Arc<SeriesDir>, max_size: u64) -> Result<LogWriter, Error> {
        if max_size < LOG_ENTRY_SIZE {
            return Err(Error::ArgTooSmall);
        }

        let mut sequences: VecDeque<u64> = dir.read_log_sequences()?.into_iter().collect();

        let sequence = match sequences.front() {
            Some(s) => s + 1,
            _ => 0,
        };

        sequences.push_front(sequence);

        let mut writer = LogWriter {
            file: dir.open(FileKind::Log(sequence), OpenMode::Write)?,
            sequence,
            sequences,
            max_size,
            current_size: 0,
            dir,
        };

        writer.cleanup()?;

        Ok(writer)
    }
    fn cleanup(&mut self) -> Result<(), Error> {
        while self.sequences.len() > 2 {
            if let Some(s) = self.sequences.pop_back() {
                self.dir.remove_log(s)?;
            }
        }
        Ok(())
    }
    fn rotate_if_needed(&mut self) -> Result<(), Error> {
        if self.current_size + LOG_ENTRY_SIZE < self.max_size {
            return Ok(());
        }

        self.file.sync_data()?;

        let next_sequence = self.sequence + 1;

        self.file = self
            .dir
            .open(FileKind::Log(next_sequence), OpenMode::Write)?;

        self.sequence = next_sequence;

        self.current_size = 0;

        self.sequences.push_front(next_sequence);

        self.cleanup()
    }
    pub fn append(&mut self, entry: &LogEntry) -> Result<(), Error> {
        self.rotate_if_needed()?;

        entry.write_entry(&mut self.file)?;

        self.current_size += LOG_ENTRY_SIZE;
        Ok(())
    }
    pub fn sync(&mut self) -> Result<(), Error> {
        self.file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::super::file_system;
    use super::*;
    use std::io::{Cursor, SeekFrom};

    #[test]
    fn test_log_entry_read_write() -> Result<(), Error> {
        let mut cursor = Cursor::new(Vec::new());

        let entry = LogEntry {
            data_offset: 123 as u64,
            index_offset: 321 as u64,
            highest_ts: 110,
        };

        {
            entry.write_entry(&mut cursor)?;
            cursor.set_position(0);
        }
        assert_eq!(entry, LogEntry::read_entry(&mut cursor)?);

        {
            cursor.set_position(0);
            cursor.write(&[1, 2, 3])?;
            cursor.set_position(0);
        }
        assert_eq!(
            true,
            match LogEntry::read_entry(&mut cursor) {
                Err(Error::Crc32Mismatch) => true,
                _ => false,
            }
        );

        {
            cursor.set_position(0);
            cursor.write_u64(&321)?;
            cursor.write_u64(&123)?;
            cursor.set_position(0);
        }
        assert_eq!(
            true,
            match LogEntry::read_entry(&mut cursor) {
                Err(Error::Crc32Mismatch) => true,
                _ => false,
            }
        );

        Ok(())
    }

    #[test]
    fn test_writer() -> Result<(), Error> {
        let fs = &file_system::open_temp()?;
        let series_dir = fs.series("series1")?;

        let entry1 = LogEntry {
            data_offset: 11,
            index_offset: 22,
            highest_ts: 33,
        };
        let entry2 = LogEntry {
            data_offset: 44,
            index_offset: 55,
            highest_ts: 66,
        };
        let entry3 = LogEntry {
            data_offset: 77,
            index_offset: 88,
            highest_ts: 99,
        };
        let entry4 = LogEntry {
            data_offset: 111,
            index_offset: 222,
            highest_ts: 333,
        };
        let entry5 = LogEntry {
            data_offset: 444,
            index_offset: 555,
            highest_ts: 666,
        };
        let entry6 = LogEntry {
            data_offset: 777,
            index_offset: 888,
            highest_ts: 999,
        };
        {
            let mut writer = LogWriter::create(series_dir.clone(), 1024)?;
            writer.append(&entry1)?;
            writer.append(&entry2)?;
            writer.append(&entry3)?;
        }

        {
            let reader = LogReader::create(series_dir.clone());
            assert_eq!(entry3, reader.get_last_entry_or_default()?);
        }
        {
            let mut writer = LogWriter::create(series_dir.clone(), 1024)?;
            writer.append(&entry4)?;
            writer.append(&entry5)?;
            writer.append(&entry6)?;
        }

        {
            let reader = LogReader::create(series_dir.clone());
            assert_eq!(entry6, reader.get_last_entry_or_default()?);
        }

        {
            let mut file = series_dir.open(FileKind::Log(1), OpenMode::Write)?;
            file.seek(SeekFrom::Start(LOG_ENTRY_SIZE + 1))?;
            file.write_all(&[0, 1, 2, 3])?;
        }

        {
            let reader = LogReader::create(series_dir);
            assert_eq!(entry4, reader.get_last_entry_or_default()?);
        }

        Ok(())
    }

    fn gen_entry(seq: u64) -> LogEntry {
        LogEntry {
            data_offset: seq,
            index_offset: 1000 + seq,
            highest_ts: 2000 + seq,
        }
    }

    #[test]
    fn test_rotate() -> Result<(), Error> {
        let fs = &file_system::open_temp()?;
        let series_dir = fs.series("series1")?;

        {
            let mut writer = LogWriter::create(series_dir.clone(), LOG_ENTRY_SIZE * 10)?;
            for i in 1..=34 {
                writer.append(&gen_entry(i as u64))?;
            }
        }
        assert_eq!(vec![3, 2], series_dir.clone().read_log_sequences()?);
        Ok(())
    }
}
