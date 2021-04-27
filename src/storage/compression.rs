use super::io_utils::{ReadBytes, WriteBytes};
use super::Entry;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression as DeflateCompression;
use integer_encoding::{VarIntReader, VarIntWriter};
use std::io::{self, Read, Write};

#[derive(Clone)]
pub enum Compression {
    None,
    Deflate,
    Delta,
}

fn write_delta<W: Write>(block: &[&Entry], to: &mut W) -> io::Result<()> {
    let mut last_ts = block[0].ts;
    let mut last_val = block[0].value;

    to.write_u64(&last_ts)?;
    to.write_f64(&last_val)?;

    for entry in &block[1..] {
        to.write_varint(entry.ts - last_ts)?;
        to.write_varint(entry.value.to_bits() ^ last_val.to_bits())?;

        last_ts = entry.ts;
        last_val = entry.value;
    }
    Ok(())
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

fn read_delta<R: Read>(from: &mut R, size: usize) -> io::Result<Vec<Entry>> {
    let mut entries = Vec::new();

    let mut last_ts = from.read_u64()?;
    let mut last_val = from.read_f64()?;

    entries.push(Entry {
        ts: last_ts,
        value: last_val,
    });

    for _ in 1..size {
        last_ts = last_ts + from.read_varint::<u64>()?;
        last_val = f64::from_bits(last_val.to_bits() ^ from.read_varint::<u64>()?);

        entries.push(Entry {
            ts: last_ts,
            value: last_val,
        });
    }

    Ok(entries)
}

impl Compression {
    pub fn from_marker(b: u8) -> Option<Compression> {
        match b {
            0 => Some(Compression::None),
            1 => Some(Compression::Deflate),
            2 => Some(Compression::Delta),
            _ => None,
        }
    }

    pub fn marker(&self) -> u8 {
        match self {
            Compression::None => 0,
            Compression::Deflate => 1,
            Compression::Delta => 2,
        }
    }

    pub fn write<W: Write>(&self, block: &[&Entry], to: &mut W) -> io::Result<()> {
        match self {
            Compression::None => write_raw(block, to),
            Compression::Deflate => write_deflate(block, to),
            Compression::Delta => write_delta(block, to),
        }
    }

    pub fn read<R: Read>(&self, from: &mut R, size: usize) -> io::Result<Vec<Entry>> {
        match self {
            Compression::None => read_raw(from, size),
            Compression::Deflate => read_deflate(from, size),
            Compression::Delta => read_delta(from, size),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Cursor;

    fn check(compression: Compression, entries: &[&Entry]) -> io::Result<()> {
        let mut cursor = Cursor::new(Vec::new());
        compression.write(entries, &mut cursor)?;
        cursor.set_position(0);
        assert_eq!(
            entries.into_iter().cloned().cloned().collect::<Vec<Entry>>(),
            compression.read(&mut cursor, entries.len())?
        );
        Ok(())
    }
    
    #[test]
    fn test_delta() {
        check(Compression::Delta, &[&Entry { ts: 1, value: 10.0 }]).unwrap();
        check(Compression::Delta, &[&Entry { ts: 1, value: 10.0 }, &Entry { ts: 2, value: 20.0 }]).unwrap();
        check(
            Compression::Delta,
            &[
                &Entry { ts: 1, value: 10.0 },
                &Entry { ts: 2, value: 20.0 },
                &Entry { ts: 10, value: 30.0 },
            ],
        )
        .unwrap();
    }

    #[test]
    fn test_deflate() {
        check(Compression::Deflate, &[&Entry { ts: 1, value: 10.0 }, &Entry { ts: 2, value: 20.0 }]).unwrap();
    }
}