use crate::db::io_utils::{self, checksum_u64, ReadBytes, ReadError, ReadResult, WriteBytes};
use std::fs::{read_dir, File};
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub data_offset: u64,
    pub index_offset: u64,
    pub highest_ts: u64,
}

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

fn parse_log_filename(base: &Path, s: &str) -> Option<(PathBuf, u8)> {
    s.strip_prefix("series.log.")
        .and_then(|suffix| suffix.parse::<u8>().ok().map(|seq| (base.join(s), seq)))
}

pub fn read_log_filenames<P: AsRef<Path>>(path: P) -> io::Result<Vec<(PathBuf, u8)>> {
    let mut filenames = read_dir(path.as_ref())?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter_map(|entry| parse_log_filename(path.as_ref(), &entry))
        .collect::<Vec<(PathBuf, u8)>>();
    filenames.sort_by_key(|(_, seq)| *seq);
    filenames.reverse();
    Ok(filenames)
}

fn read_last_log_entry_of(path: PathBuf) -> io::Result<Option<LogEntry>> {
    let mut file = io_utils::open_readable(path)?;
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
}

impl LogWriter {
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<LogWriter> {
        let filename = match read_log_filenames(path.as_ref())?.first() {
            Some((_, seq)) => format!("series.log.{:03}", seq + 1),
            _ => "series.log.000".to_string(),
        };

        Ok(LogWriter {
            file: io_utils::open_writable(path.as_ref().join(&filename))?,
        })
    }
    pub fn append(&mut self, entry: &LogEntry) -> io::Result<()> {
        entry.write_entry(&mut self.file)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::test_utils::create_temp_dir;
    use std::io::Cursor;

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
        {
            let mut writer = LogWriter::create(&db_dir.path).unwrap();
            writer
                .append(&LogEntry {
                    data_offset: 11,
                    index_offset: 22,
                    highest_ts: 33,
                })
                .unwrap();
            writer
                .append(&LogEntry {
                    data_offset: 44,
                    index_offset: 55,
                    highest_ts: 66,
                })
                .unwrap();
            writer
                .append(&LogEntry {
                    data_offset: 44,
                    index_offset: 55,
                    highest_ts: 66,
                })
                .unwrap();
        }

        assert_eq!(
            Some(LogEntry {
                data_offset: 44,
                index_offset: 55,
                highest_ts: 66,
            }),
            read_last_log_entry(&db_dir.path).unwrap()
        );

        {
            let mut writer = LogWriter::create(&db_dir.path).unwrap();
            writer
                .append(&LogEntry {
                    data_offset: 77,
                    index_offset: 88,
                    highest_ts: 99,
                })
                .unwrap();
        }

        assert_eq!(
            Some(LogEntry {
                data_offset: 77,
                index_offset: 88,
                highest_ts: 99,
            }),
            read_last_log_entry(&db_dir.path).unwrap()
        );
    }
}
