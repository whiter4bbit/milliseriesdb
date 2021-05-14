use memmap::{MmapMut, MmapOptions};
use std::convert::TryInto;
use std::fs::File;
use std::sync::{Arc, RwLock};

use super::error::Error;

const MAX_INDEX_SIZE: u32 = 2 * 1024 * 1024 * 1024;

const INDEX_BLOCK_SIZE: u32 = ENTRY_SIZE * 1024;

const ENTRY_SIZE: u32 = 8 + 4;

struct Interior {
    mmap: MmapMut,
    file: File,
    offset: usize,
    len: usize,
}

impl Interior {
    fn open(file: File, offset: u32) -> Result<Interior, Error> {
        if offset % ENTRY_SIZE != 0 {
            return Err(Error::InvalidOffset);
        }
        if offset > MAX_INDEX_SIZE {
            return Err(Error::IndexFileTooBig);
        }

        let len = INDEX_BLOCK_SIZE.min((offset / INDEX_BLOCK_SIZE + 1) * INDEX_BLOCK_SIZE);

        file.set_len(len as u64)?;

        Ok(Interior {
            mmap: unsafe { MmapOptions::new().map_mut(&file)? },
            file: file,
            offset: offset as usize,
            len: len as usize,
        })
    }
    fn remap_if_needed(&mut self) -> Result<(), Error> {
        if self.offset as u64 + ENTRY_SIZE as u64 > MAX_INDEX_SIZE as u64 {
            return Err(Error::IndexFileTooBig);
        }
        if self.offset + ENTRY_SIZE as usize <= self.len {
            return Ok(());
        }

        let len = self.len + INDEX_BLOCK_SIZE as usize;

        self.file.set_len(len as u64)?;
        self.mmap = unsafe { MmapOptions::new().map_mut(&self.file)? };

        log::debug!("index {:?} remapped {} -> {}", &self.file, self.len, len,);

        self.len = len;

        Ok(())
    }
    fn append(&mut self, ts: i64, block_offset: u32) -> Result<u32, Error> {
        self.remap_if_needed()?;

        self.mmap[self.offset..self.offset + 8].copy_from_slice(&ts.to_be_bytes());
        self.mmap[self.offset + 8..self.offset + 12].copy_from_slice(&block_offset.to_be_bytes());

        self.offset += ENTRY_SIZE as usize;

        Ok(self.offset as u32)
    }
    fn sync(&mut self) -> Result<(), Error> {
        Ok(self.mmap.flush()?)
    }
}

impl Interior {
    fn nth_ts(&self, nth: usize) -> Result<i64, Error> {
        let start = ENTRY_SIZE as usize * nth;
        Ok(i64::from_be_bytes(
            (&self.mmap[start..start + 8]).try_into()?,
        ))
    }
    fn nth_offset(&self, nth: usize, upper_offset: usize) -> Result<Option<u32>, Error> {
        let start = ENTRY_SIZE as usize * nth + 8;
        if start + 4 > upper_offset {
            return Ok(None);
        }
        Ok(Some(u32::from_be_bytes(
            (&self.mmap[start..start + 4]).try_into()?,
        )))
    }
    fn ceiling_offset(&self, ts: i64, upper_offset: u32) -> Result<Option<u32>, Error> {
        if upper_offset as usize > self.offset {
            return Err(Error::OffsetOutsideTheRange);
        }
        if (upper_offset as u32) % ENTRY_SIZE != 0 {
            return Err(Error::OffsetIsNotAligned);
        }

        let entries = upper_offset / ENTRY_SIZE;

        let mut lo = 0usize;
        let mut hi = entries as usize;

        while lo <= hi {
            let m = lo + (hi - lo) / 2;

            if self.nth_ts(m)? < ts {
                lo = m + 1;
            } else {
                if m == 0 {
                    break;
                }

                hi = m - 1;
            }
        }

        self.nth_offset(lo, upper_offset as usize)
    }
}

#[cfg(test)]
mod test_index {
    use super::super::file_system::{self, FileKind, OpenMode};
    use super::*;
    #[test]
    fn test_basic() -> Result<(), Error> {
        let fs = file_system::test::open()?;
        let dir = fs.series("series1")?;
        {
            let mut index = Interior::open(dir.open(FileKind::Index, OpenMode::Write)?, 0)?;
            index.append(-10, 0)?;
            index.append(-2, 1)?;
            index.append(-1, 4)?;
            index.append(4, 5)?;
            let upper = index.append(6, 7)?;

            assert_eq!(Some(0), index.ceiling_offset(-10, upper)?);
            assert_eq!(Some(4), index.ceiling_offset(-1, upper)?);
            assert_eq!(Some(5), index.ceiling_offset(4, upper)?);
            assert_eq!(Some(5), index.ceiling_offset(0, upper)?);
            assert_eq!(Some(1), index.ceiling_offset(-5, upper)?);
            assert_eq!(Some(0), index.ceiling_offset(-1000, upper)?);

            assert_eq!(None, index.ceiling_offset(7, upper)?);
        }
        Ok(())
    }
}

pub struct Index {
    inter: Arc<RwLock<Interior>>,
}

impl Index {
    pub fn open(file: File, offset: u32) -> Result<Index, Error> {
        Ok(Index {
            inter: Arc::new(RwLock::new(Interior::open(file, offset)?)),
        })
    }
    pub fn append(&self, ts: i64, offset: u32) -> Result<u32, Error> {
        let mut inter = self.inter.write().unwrap();
        inter.append(ts, offset)
    }
    pub fn sync(&self) -> Result<(), Error> {
        let mut inter = self.inter.write().unwrap();
        inter.sync()
    }
    pub fn ceiling_offset(&self, ts: i64, upper: u32) -> Result<Option<u32>, Error> {
        let inter = self.inter.read().unwrap();
        inter.ceiling_offset(ts, upper)
    }
}
