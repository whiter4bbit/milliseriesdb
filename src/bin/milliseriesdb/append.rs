use flate2::read::GzDecoder;
use milliseriesdb::storage::{Compression, Entry, SeriesTable};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::str::FromStr;

struct CsvEntry(u64, f64);

impl FromStr for CsvEntry {
    type Err = io::Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut parts = input.split(';').map(|p| p.trim());
        match (
            parts.next().and_then(|ts| ts.parse::<u64>().ok()),
            parts.next().and_then(|val| val.parse::<f64>().ok()),
        ) {
            (Some(ts), Some(val)) => Ok(CsvEntry(ts, val)),
            _ => Err(io::Error::new(io::ErrorKind::Other, "can not parse a line")),
        }
    }
}

pub fn append(
    series_table: SeriesTable,
    series_id: &str,
    input_csv: &str,
    batch_size: usize,
    compression: Compression,
) -> io::Result<()> {
    let reader: Box<dyn BufRead> = if input_csv.ends_with(".gz") {
        Box::new(BufReader::new(GzDecoder::new(File::open(input_csv)?)?))
    } else {
        Box::new(BufReader::new(File::open(input_csv)?))
    };

    series_table.create(series_id)?;
    let writer = series_table.writer(series_id).unwrap();
    let mut buffer = Vec::new();
    for entry in reader.lines() {
        let CsvEntry(ts, val) = entry?.parse::<CsvEntry>()?;
        buffer.push(Entry { ts, value: val });
        if buffer.len() == batch_size {
            writer.append_opt(&buffer, compression.clone())?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        writer.append_opt(&buffer, compression)?;
    }
    Ok(())
}
