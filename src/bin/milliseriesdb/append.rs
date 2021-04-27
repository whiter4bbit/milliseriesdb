use flate2::read::GzDecoder;
use milliseriesdb::db::{Entry, DB};
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

pub fn append(db: &mut DB, series_id: &str, input_csv: &str) -> io::Result<()> {
    fn append_internal(db: &mut DB, series_id: &str, reader: Box<dyn BufRead>) -> io::Result<()> {
        let series = db.create_series(series_id)?;
        let mut writer = series.writer();
        let mut buffer = Vec::new();
        let batch_size = 100;
        for entry in reader.lines() {
            let CsvEntry(ts, val) = entry?.parse::<CsvEntry>()?;
            buffer.push(Entry { ts: ts, value: val });
            if buffer.len() == batch_size {
                writer.append(&buffer)?;
                buffer.clear();
            }
        }
        if !buffer.is_empty() {
            writer.append(&buffer)?;
        }
        Ok(())
    }

    append_internal(
        db,
        series_id,
        if input_csv.ends_with(".gz") {
            Box::new(BufReader::new(GzDecoder::new(File::open(input_csv)?)?))
        } else {
            Box::new(BufReader::new(File::open(input_csv)?))
        },
    )
}