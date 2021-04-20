use super::io_utils::{self, checksum_u64, ReadBytes, ReadError, ReadResult, WriteBytes};
use std::collections::VecDeque;
use std::fs::{read_dir, remove_file, File};
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct LogEntry {
    pub data_offset: u64,
    pub index_offset: u64,
    pub highest_ts: u64,
}

#[allow(dead_code)]
const LOG_ENTRY_SIZE: u64 = 8 + 8 + 8 + 8;

impl LogEntry {
    fn read_entry<R: Read>(read: &mut R) -> ReadResult<LogEntry> {
        let data_offset = read.read_u64().map_err(|e| ReadError::Other(e))?;
        let index_offset = read.read_u64().map_err(|e| ReadError::Other(e))?;
        let highest_ts = read.read_u64().map_err(|e| ReadError::Other(e))?;
        let target_checksum = read.read_u64().map_err(|e| ReadError::Other(e))?;
        let actual_checksum = checksum_u64(&[data_offset, index_offset, highest_ts]);

        match target_checksum == actual_checksum {
            true => Ok(LogEntry {
                data_offset: data_offset,
                index_offset: index_offset,
                highest_ts: highest_ts,
            }),
            _ => Err(ReadError::CorruptedBlock),
        }
    }
    fn write_entry<W: Write>(&self, write: &mut W) -> io::Result<()> {
        write.write_u64(&self.data_offset)?;
        write.write_u64(&self.index_offset)?;
        write.write_u64(&self.highest_ts)?;
        write.write_u64(&checksum_u64(&[self.data_offset, self.index_offset, self.highest_ts]))?;
        Ok(())
    }
}

fn log_filename(sequence: u64) -> String {
    return format!("series.log.{}", sequence);
}

fn parse_log_filename(base: &Path, s: &str) -> Option<(PathBuf, u64)> {
    s.strip_prefix("series.log.")
        .and_then(|suffix| suffix.parse::<u64>().ok().map(|seq| (base.join(s), seq)))
}

pub fn read_log_filenames<P: AsRef<Path>>(path: P) -> io::Result<Vec<(PathBuf, u64)>> {
    let mut filenames = read_dir(path.as_ref())?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter_map(|entry| parse_log_filename(path.as_ref(), &entry))
        .collect::<Vec<(PathBuf, u64)>>();
    filenames.sort_by_key(|(_, seq)| *seq);
    filenames.reverse();
    Ok(filenames)
}

fn read_last_log_entry_of(path: PathBuf) -> io::Result<Option<LogEntry>> {
    let mut file = BufReader::new(io_utils::open_readable(path)?);
    let mut last: Option<LogEntry> = None;
    loop {
        match LogEntry::read_entry(&mut file) {
            Err(error) => match error {
                ReadError::Other(other) => match other.kind() {
                    io::ErrorKind::UnexpectedEof => break,
                    _ => return Err(other),
                },
                ReadError::CorruptedBlock => break,
            },
            Ok(entry) => last = Some(entry),
        }
    }
    Ok(last)
}

pub fn read_last_log_entry<P: AsRef<Path>>(path: P) -> io::Result<Option<LogEntry>> {
    for (log_path, _) in read_log_filenames(path)? {
        match read_last_log_entry_of(log_path)? {
            Some(entry) => return Ok(Some(entry)),
            _ => continue,
        }
    }
    Ok(None)
}

pub struct LogWriter {
    file: File,
    sequence: u64,
    path: PathBuf,
    max_size: u64,
    current_size: u64,
    log_paths: VecDeque<(PathBuf, u64)>,
}

impl LogWriter {
    pub fn create<P: AsRef<Path>>(path: P, max_size: u64) -> io::Result<LogWriter> {
        if max_size < LOG_ENTRY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("max_size should be at least {} ({})", LOG_ENTRY_SIZE, max_size),
            ));
        }

        let mut log_paths: VecDeque<(PathBuf, u64)> = read_log_filenames(path.as_ref())?.into_iter().collect();

        let (current_log_path, sequence) = match log_paths.front() {
            Some((_, seq)) => (path.as_ref().join(log_filename(seq + 1)), seq + 1),
            _ => (path.as_ref().join(log_filename(0)), 0),
        };
        log_paths.push_front((current_log_path.clone(), sequence));

        let mut writer = LogWriter {
            file: io_utils::open_writable(current_log_path.clone())?,
            sequence: sequence,
            path: path.as_ref().to_path_buf(),
            max_size: max_size,
            current_size: 0,
            log_paths: log_paths,
        };

        writer.cleanup()?;

        Ok(writer)
    }
    fn cleanup(&mut self) -> io::Result<()> {
        while self.log_paths.len() > 2 {
            if let Some((path, _)) = self.log_paths.pop_back() {
                remove_file(path.clone())?;
                log::debug!("Removed {:?}", path.clone());
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
        let next_log_path = self.path.clone().join(&log_filename(next_sequence));

        self.file = io_utils::open_writable(next_log_path.clone())?;

        self.sequence = next_sequence;
        self.current_size = 0;

        self.log_paths.push_front((next_log_path.clone(), self.sequence));

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
    use super::*;
    use crate::db::test_utils::create_temp_dir;
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
                Err(ReadError::CorruptedBlock) => true,
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
                Err(ReadError::CorruptedBlock) => true,
                _ => false,
            }
        );
    }

    #[test]
    fn test_writer() {
        let db_dir = create_temp_dir("test-path").unwrap();
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
            let mut writer = LogWriter::create(&db_dir.path, 1024).unwrap();
            writer.append(&entry1).unwrap();
            writer.append(&entry2).unwrap();
            writer.append(&entry3).unwrap();
        }

        assert_eq!(Some(entry3), read_last_log_entry(&db_dir.path).unwrap());

        {
            let mut writer = LogWriter::create(&db_dir.path, 1024).unwrap();
            writer.append(&entry4).unwrap();
            writer.append(&entry5).unwrap();
            writer.append(&entry6).unwrap();
        }

        assert_eq!(Some(entry6), read_last_log_entry(&db_dir.path).unwrap());

        {
            let mut file = io_utils::open_writable((&db_dir.path).join(log_filename(1))).unwrap();
            file.seek(SeekFrom::Start(LOG_ENTRY_SIZE + 1)).unwrap();
            file.write_all(&[0, 1, 2, 3]).unwrap();
        }

        assert_eq!(Some(entry4), read_last_log_entry(&db_dir.path).unwrap());
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
        {
            let mut writer = LogWriter::create(&db_dir.path, LOG_ENTRY_SIZE * 10).unwrap();
            for i in 1..=34 {
                writer.append(&gen_entry(i as u64)).unwrap();
            }
        }
        assert_eq!(
            vec![
                ((&db_dir.path.join("series.log.3")).to_path_buf(), 3),
                ((&db_dir.path.join("series.log.2")).to_path_buf(), 2),
            ],
            read_log_filenames(&db_dir.path).unwrap()
        );
    }
}
