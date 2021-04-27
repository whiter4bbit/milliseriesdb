use std::io::{self, Read, Write};

pub trait WriteBytes: Write {
    fn write_u8(&mut self, v: &u8) -> io::Result<()> {
        self.write_all(&v.to_be_bytes())?;
        return Ok(());
    }
    fn write_u32(&mut self, v: &u32) -> io::Result<()> {
        self.write_all(&v.to_be_bytes())?;
        return Ok(());
    }
    fn write_u64(&mut self, v: &u64) -> io::Result<()> {
        self.write_all(&v.to_be_bytes())?;
        return Ok(());
    }
    fn write_f64(&mut self, v: &f64) -> io::Result<()> {
        self.write_all(&v.to_be_bytes())?;
        return Ok(());
    }
}

impl<W: Write> WriteBytes for W {}

pub trait ReadBytes: Read {
    fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        return Ok(buf[0]);
    }
    fn read_u64(&mut self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        return Ok(u64::from_be_bytes(buf));
    }
    fn read_f64(&mut self) -> io::Result<f64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        return Ok(f64::from_be_bytes(buf));
    }
    fn read_u32(&mut self) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        return Ok(u32::from_be_bytes(buf));
    }
}

impl<R: Read> ReadBytes for R {}

pub fn checksum_u64(p: &[u64]) -> u64 {
    let mut sum = 0u64;
    for x in p {
        sum = sum.overflowing_shl(1).0.overflowing_add(*x).0;
    }
    return sum;
}

#[derive(Debug)]
pub enum ReadError {
    Other(io::Error),
    CorruptedBlock,
}

pub type ReadResult<T> = Result<T, ReadError>;