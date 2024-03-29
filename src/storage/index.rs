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
    len: usize,
}

impl Interior {
    fn open(file: File, upper_offset: u32) -> Result<Interior, Error> {
        if upper_offset % ENTRY_SIZE != 0 {
            return Err(Error::InvalidOffset);
        }
        if upper_offset > MAX_INDEX_SIZE {
            return Err(Error::IndexFileTooBig);
        }

        let len = MAX_INDEX_SIZE.min((upper_offset / INDEX_BLOCK_SIZE + 1) * INDEX_BLOCK_SIZE);

        file.set_len(len as u64)?;

        Ok(Interior {
            mmap: unsafe { MmapOptions::new().map_mut(&file)? },
            file: file,
            len: len as usize,
        })
    }
    fn remap_if_needed(&mut self, offset: u32) -> Result<(), Error> {
        if offset as u64 + ENTRY_SIZE as u64 > MAX_INDEX_SIZE as u64 {
            return Err(Error::IndexFileTooBig);
        }
        if offset as usize + ENTRY_SIZE as usize <= self.len {
            return Ok(());
        }

        let len = self.len + INDEX_BLOCK_SIZE as usize;

        self.file.set_len(len as u64)?;
        self.mmap = unsafe { MmapOptions::new().map_mut(&self.file)? };

        self.len = len;

        Ok(())
    }
    fn set(&mut self, offset: u32, ts: i64, block_offset: u32) -> Result<u32, Error> {
        self.remap_if_needed(offset)?;

        let offset = offset as usize;

        debug_assert!(offset + ENTRY_SIZE as usize <= self.len);

        self.mmap[offset..offset + 8].copy_from_slice(&ts.to_be_bytes());
        self.mmap[offset + 8..offset + 12].copy_from_slice(&block_offset.to_be_bytes());

        Ok(offset as u32 + ENTRY_SIZE)
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
}

#[cfg(test)]
impl Interior {
    fn check_consistency(&self, upper_offset: u32) -> Result<(), Error> {
        let entries = (upper_offset / ENTRY_SIZE) as usize;
        for i in 1..entries {
            if self.nth_ts(i - 1)? > self.nth_ts(i)? {
                return Err(Error::IndexIsNotConsistent)
            }
        }
        Ok(())
    }
}

impl Interior {
    fn ceiling_offset(&self, ts: i64, upper_offset: u32) -> Result<Option<u32>, Error> {
        if upper_offset as usize > self.len {
            return Err(Error::OffsetOutsideTheRange);
        }
        if (upper_offset as u32) % ENTRY_SIZE != 0 {
            return Err(Error::OffsetIsNotAligned);
        }

        #[cfg(test)]
        self.check_consistency(upper_offset)?;

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
            assert_eq!(1 * ENTRY_SIZE, index.set(0 * ENTRY_SIZE, -10, 0)?);
            assert_eq!(2 * ENTRY_SIZE, index.set(1 * ENTRY_SIZE,-2, 1)?);
            assert_eq!(3 * ENTRY_SIZE, index.set(2 * ENTRY_SIZE,-1, 4)?);
            assert_eq!(4 * ENTRY_SIZE, index.set(3 * ENTRY_SIZE,4, 5)?);
            let upper = index.set(4 * ENTRY_SIZE, 6, 7)?;

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
    pub fn set(&self, offset: u32, ts: i64, block_offset: u32) -> Result<u32, Error> {
        let mut inter = self.inter.write().unwrap();
        inter.set(offset, ts, block_offset)
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
