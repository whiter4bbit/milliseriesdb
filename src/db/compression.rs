use crate::db::io_utils::{ReadBytes, WriteBytes};
use crate::db::Entry;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression as DeflateCompression;
use std::io::{self, Read, Write};

pub enum Compression {
    None,
    Deflate,
}

fn write_raw<W: Write>(block: &[&Entry], to: &mut W) -> io::Result<()> {
    for entry in block {
        to.write_u64(&entry.ts)?;
        to.write_f64(&entry.value)?;
    }
    Ok(())
}

fn write_deflate<W: Write>(block: &[&Entry], to: &mut W) -> io::Result<()> {
    let mut encoder = DeflateEncoder::new(to, DeflateCompression::default());
    write_raw(block, &mut encoder)?;
    encoder.finish()?;
    Ok(())
}

fn read_raw<R: Read>(from: &mut R, size: usize) -> io::Result<Vec<Entry>> {
    let mut entries = Vec::new();
    for _ in 0..size {
        entries.push(Entry {
            ts: from.read_u64()?,
            value: from.read_f64()?,
        });
    }
    Ok(entries)
}

fn read_deflate<R: Read>(from: &mut R, size: usize) -> io::Result<Vec<Entry>> {
    let mut decoder = DeflateDecoder::new(from);
    read_raw(&mut decoder, size)
}

impl Compression {
    pub fn from_marker(b: u8) -> Option<Compression> {
        match b {
            0 => Some(Compression::None),
            1 => Some(Compression::Deflate),
            _ => None,
        }
    }

    pub fn marker(&self) -> u8 {
        match self {
            Compression::None => 0,
            Compression::Deflate => 1,
        }
    }

    pub fn write<W: Write>(&self, block: &[&Entry], to: &mut W) -> io::Result<()> {
        match self {
            Compression::None => write_raw(block, to),
            Compression::Deflate => write_deflate(block, to),
        }
    }

    pub fn read<R: Read>(&self, from: &mut R, size: usize) -> io::Result<Vec<Entry>> {
        match self {
            Compression::None => read_raw(from, size),
            Compression::Deflate => read_deflate(from, size),
        }
    }
}
