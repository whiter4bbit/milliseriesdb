use super::error::Error;
use super::file_system::{FileKind, OpenMode, SeriesDir};
use super::io_utils::{ReadBytes, WriteBytes};
use crc::crc16;
use std::collections::VecDeque;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufWriter};
use std::sync::{Arc, RwLock};

const COMMIT_SIZE: usize = 4 + 4 + 8 + 2;

#[cfg(not(test))]
const MAX_LOG_SIZE: usize = 2 * 1024 * 1024;

#[cfg(test)]
const MAX_LOG_SIZE: usize = 80;

#[derive(Debug, PartialEq, Clone)]
pub struct Commit {
    pub data_offset: u32,
    pub index_offset: u32,
    pub highest_ts: i64,
}

impl Commit {
    fn checksum(&self) -> u16 {
        let table = &crc16::USB_TABLE;
        let mut checksum = 0u16;

        checksum = crc16::update(checksum, table, &self.data_offset.to_be_bytes());
        checksum = crc16::update(checksum, table, &self.index_offset.to_be_bytes());
        checksum = crc16::update(checksum, table, &self.highest_ts.to_be_bytes());

        checksum
    }
    fn read<R: Read>(read: &mut R) -> Result<Commit, Error> {
        let commit = Commit {
            data_offset: read.read_u32()?,
            index_offset: read.read_u32()?,
            highest_ts: read.read_i64()?,
        };

        let checksum = read.read_u16()?;

        if checksum != commit.checksum() {
            return Err(Error::Crc32Mismatch);
        }

        Ok(commit)
    }
    fn write<W: Write>(&self, write: &mut W) -> Result<(), Error> {
        write.write_u32(&self.data_offset)?;
        write.write_u32(&self.index_offset)?;
        write.write_i64(&self.highest_ts)?;
        write.write_u16(&self.checksum())?;
        Ok(())
    }
}

#[cfg(test)]
mod test_commit {
    use super::*;

    #[test]
    fn test_read_write() -> Result<(), Error> {
        let commit = Commit {
            data_offset: 123,
            index_offset: 321,
            highest_ts: 110,
        };

        let mut buf = Vec::new();

        commit.write(&mut buf)?;

        assert_eq!(commit, Commit::read(&mut &buf[..])?);

        buf[COMMIT_SIZE - 2] = 23;
        buf[COMMIT_SIZE - 1] = 21;

        assert!(match Commit::read(&mut &buf[..]) {
            Err(Error::Crc32Mismatch) => true,
            _ => false,
        });

        Ok(())
    }
}

const FIRST: Commit = Commit {
    data_offset: 0,
    index_offset: 0,
    highest_ts: i64::MIN,
};

struct Interior {
    current: Arc<Commit>,
    dir: Arc<SeriesDir>,
    seqs: VecDeque<u64>,
    current_seq: u64,
    current_size: usize,
    writer: BufWriter<File>,
}

impl Interior {
    fn open(dir: Arc<SeriesDir>) -> Result<Interior, Error> {
        let mut seqs: VecDeque<u64> = dir.read_log_sequences()?.into();

        let mut current: Option<Commit> = None;
        for seq in seqs.iter() {
            let mut file = dir.open(FileKind::Log(*seq), OpenMode::Write)?;
            loop {
                match Commit::read(&mut file) {
                    Err(Error::Crc32Mismatch) => {
                        log::warn!("crc32 mismatch in log {:?}", &file);
                        break;
                    }
                    Err(Error::Io(error)) => match error.kind() {
                        io::ErrorKind::UnexpectedEof => break,
                        _ => return Err(Error::Io(error)),
                    },
                    Err(error) => return Err(error),
                    Ok(entry) => current = Some(entry),
                }
            }
        }

        let current = current.unwrap_or(FIRST);

        let current_seq = seqs.front().map(|seq| seq + 1).unwrap_or(0);

        seqs.push_front(current_seq);

        let mut commit_log = Interior {
            current: Arc::new(current.clone()),
            dir: dir.clone(),
            current_seq: current_seq,
            current_size: 0,
            seqs: seqs,
            writer: BufWriter::new(dir.open(FileKind::Log(current_seq), OpenMode::Write)?),
        };

        commit_log.commit(current)?;

        Ok(commit_log)
    }
}

impl Interior {
    fn cleanup(&mut self) -> Result<(), Error> {
        while self.seqs.len() > 2 {
            if let Some(seq) = self.seqs.back() {
                self.dir.remove_log(*seq)?;
                self.seqs.pop_back();
            }
        }
        Ok(())
    }
    fn rotate_if_needed(&mut self) -> Result<(), Error> {
        if self.current_size < MAX_LOG_SIZE {
            return Ok(());
        }

        self.current_seq += 1;
        self.current_size = 0;
        self.seqs.push_front(self.current_seq);

        self.writer = BufWriter::new(
            self.dir
                .open(FileKind::Log(self.current_seq), OpenMode::Write)?,
        );

        self.cleanup()?;

        Ok(())
    }
    fn commit(&mut self, commit: Commit) -> Result<(), Error> {
        self.rotate_if_needed()?;

        commit.write(&mut self.writer)?;

        self.writer.get_ref().sync_data()?;

        self.current = Arc::new(commit);
        self.current_size += COMMIT_SIZE;

        Ok(())
    }
    fn current(&self) -> Arc<Commit> {
        self.current.clone()
    }
}

#[cfg(test)]
mod test_interior {
    use super::super::file_system;
    use super::*;
    use std::io::{Seek, SeekFrom};

    fn commit(i: usize) -> Commit {
        Commit {
            data_offset: i as u32,
            index_offset: i as u32,
            highest_ts: i as i64,
        }
    }

    #[test]
    fn test_basic() -> Result<(), Error> {
        let fs = file_system::open_temp()?;
        let dir = fs.series("series1")?;

        {
            let mut log = Interior::open(dir.clone())?;

            assert_eq!(Arc::new(FIRST), log.current());

            log.commit(commit(1))?;
            log.commit(commit(2))?;
            log.commit(commit(3))?;

            assert_eq!(Arc::new(commit(3)), log.current());

            log.commit(commit(4))?;

            assert_eq!(Arc::new(commit(4)), log.current());
        }

        {
            let mut log = Interior::open(dir.clone())?;
            assert_eq!(Arc::new(commit(4)), log.current());
            log.commit(commit(5))?;
            log.commit(commit(6))?;
        }

        assert_eq!(vec![1u64, 0u64], dir.read_log_sequences()?);

        {
            let mut file = dir.open(FileKind::Log(1), OpenMode::Write)?;
            file.seek(SeekFrom::Start(COMMIT_SIZE as u64 + 1))?;
            file.write(&[1, 2, 3])?;
        }

        {
            let log = Interior::open(dir.clone())?;
            assert_eq!(Arc::new(commit(4)), log.current());
        }

        Ok(())
    }

    #[test]
    fn test_rotate() -> Result<(), Error> {
        let fs = file_system::open_temp()?;
        let dir = fs.series("series1")?;

        let mut log = Interior::open(dir.clone())?;

        for _ in 0..19 {
            log.commit(commit(0))?;
        }

        assert_eq!(vec![3u64,2u64], dir.read_log_sequences()?);

        Ok(())
    }
}

pub struct CommitLog {
    inter: Arc<RwLock<Interior>>,
}

impl CommitLog {
    pub fn open(dir: Arc<SeriesDir>) -> Result<CommitLog, Error> {
        Ok(CommitLog {
            inter: Arc::new(RwLock::new(Interior::open(dir)?)),
        })
    }
    pub fn commit(&self, commit: Commit) -> Result<(), Error> {
        let mut inter = self.inter.write().unwrap();
        inter.commit(commit)
    }
    pub fn current(&self) -> Arc<Commit> {
        let inter = self.inter.read().unwrap();
        inter.current()
    }
}
