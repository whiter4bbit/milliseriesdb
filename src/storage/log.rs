use crc::crc32::{self, Hasher32};
use std::collections::VecDeque;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::sync::Arc;
use std::hash::Hasher;

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

fn entry_checksum(data_offset: u64, index_offset: u64, highest_ts: u64) -> u32 {
    let mut digest = crc32::Digest::new(crc32::IEEE);
    digest.write_u64(data_offset);
    digest.write_u64(index_offset);
    digest.write_u64(highest_ts);
    digest.sum32()
}

impl LogEntry {
    fn read_entry<R: Read>(read: &mut R) -> io::Result<LogEntry> {
        let data_offset = read.read_u64()?;
        let index_offset = read.read_u64()?;
        let highest_ts = read.read_u64()?;
        let target_checksum = read.read_u32()?;
        let actual_checksum = entry_checksum(data_offset, index_offset, highest_ts);

        match target_checksum == actual_checksum {
            true => Ok(LogEntry {
                data_offset,
                index_offset,
                highest_ts,
            }),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "crc32 mismatch")),
        }
    }
    fn write_entry<W: Write>(&self, write: &mut W) -> io::Result<()> {
        write.write_u64(&self.data_offset)?;
        write.write_u64(&self.index_offset)?;
        write.write_u64(&self.highest_ts)?;
        write.write_u32(&entry_checksum(
            self.data_offset,
            self.index_offset,
            self.highest_ts,
        ))?;
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

    fn read_last_entry(&self, seq: u64) -> io::Result<Option<LogEntry>> {
        let mut file = BufReader::new(self.dir.open(FileKind::Log(seq), OpenMode::Read)?);
        let mut last: Option<LogEntry> = None;
        loop {
            match LogEntry::read_entry(&mut file) {
                Err(error) => match error.kind() {
                    io::ErrorKind::UnexpectedEof => break,
                    io::ErrorKind::InvalidData => break,
                    _ => return Err(error),
                },
                Ok(entry) => last = Some(entry),
            }
        }
        Ok(last)
    }

    pub fn get_last_entry_or_default(&self) -> io::Result<LogEntry> {
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
    pub fn create(dir: Arc<SeriesDir>, max_size: u64) -> io::Result<LogWriter> {
        if max_size < LOG_ENTRY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "max_size should be at least {} ({})",
                    LOG_ENTRY_SIZE, max_size
                ),
            ));
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
    fn cleanup(&mut self) -> io::Result<()> {
        while self.sequences.len() > 2 {
            if let Some(s) = self.sequences.pop_back() {
                self.dir.remove_log(s)?;
            }
        }
        Ok(())
    }
    fn rotate_if_needed(&mut self) -> io::Result<()> {
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
    pub fn append(&mut self, entry: &LogEntry) -> io::Result<()> {
        self.rotate_if_needed()?;

        entry.write_entry(&mut self.file)?;

        self.current_size += LOG_ENTRY_SIZE;
        Ok(())
    }
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}

#[cfg(test)]
mod test {
    use super::super::file_system;
    use super::super::test_utils::create_temp_dir;
    use super::*;
    use std::io::{Cursor, SeekFrom};

    #[test]
    fn test_log_entry_read_write() {
        let mut cursor = Cursor::new(Vec::new());
        let entry = LogEntry {
            data_offset: 123 as u64,
            index_offset: 321 as u64,
            highest_ts: 110,
        };
        {
            entry.write_entry(&mut cursor).unwrap();
            cursor.set_position(0);
        }
        assert_eq!(entry, LogEntry::read_entry(&mut cursor).unwrap());

        {
            cursor.set_position(0);
            cursor.write(&[1, 2, 3]).unwrap();
            cursor.set_position(0);
        }
        assert_eq!(
            true,
            match LogEntry::read_entry(&mut cursor) {
                Err(error) => match error.kind() {
                    io::ErrorKind::InvalidData => true,
                    _ => false,
                },
                _ => false,
            }
        );
        {
            cursor.set_position(0);
            cursor.write_u64(&321).unwrap();
            cursor.write_u64(&123).unwrap();
            cursor.set_position(0);
        }
        assert_eq!(
            true,
            match LogEntry::read_entry(&mut cursor) {
                Err(error) => match error.kind() {
                    io::ErrorKind::InvalidData => true,
                    _ => false,
                },
                _ => false,
            }
        );
    }

    #[test]
    fn test_writer() {
        let db_dir = create_temp_dir("test-path").unwrap();
        let fs = file_system::open(&db_dir.path).unwrap();
        let series_dir = fs.series("series1").unwrap();

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
            let mut writer = LogWriter::create(series_dir.clone(), 1024).unwrap();
            writer.append(&entry1).unwrap();
            writer.append(&entry2).unwrap();
            writer.append(&entry3).unwrap();
        }

        {
            let reader = LogReader::create(series_dir.clone());
            assert_eq!(entry3, reader.get_last_entry_or_default().unwrap());
        }
        {
            let mut writer = LogWriter::create(series_dir.clone(), 1024).unwrap();
            writer.append(&entry4).unwrap();
            writer.append(&entry5).unwrap();
            writer.append(&entry6).unwrap();
        }

        {
            let reader = LogReader::create(series_dir.clone());
            assert_eq!(entry6, reader.get_last_entry_or_default().unwrap());
        }

        {
            let mut file = series_dir.open(FileKind::Log(1), OpenMode::Write).unwrap();
            file.seek(SeekFrom::Start(LOG_ENTRY_SIZE + 1)).unwrap();
            file.write_all(&[0, 1, 2, 3]).unwrap();
        }

        {
            let reader = LogReader::create(series_dir);
            assert_eq!(entry4, reader.get_last_entry_or_default().unwrap());
        }
    }

    fn gen_entry(seq: u64) -> LogEntry {
        LogEntry {
            data_offset: seq,
            index_offset: 1000 + seq,
            highest_ts: 2000 + seq,
        }
    }

    #[test]
    fn test_rotate() {
        let db_dir = create_temp_dir("test-path").unwrap();
        let fs = file_system::open(&db_dir.path).unwrap();
        let series_dir = fs.series("series1").unwrap();

        {
            let mut writer = LogWriter::create(series_dir.clone(), LOG_ENTRY_SIZE * 10).unwrap();
            for i in 1..=34 {
                writer.append(&gen_entry(i as u64)).unwrap();
            }
        }
        assert_eq!(vec![3, 2], series_dir.clone().read_log_sequences().unwrap());
    }
}