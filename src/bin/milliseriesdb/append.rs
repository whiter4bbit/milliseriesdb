use flate2::read::GzDecoder;
use milliseriesdb::db::{Entry, DB};
use std::fs::File;
use std::io::{self, BufRead, BufReader};

pub fn append(db: &mut DB, series_id: &str, input_csv: &str) -> io::Result<()> {
    fn append_internal<R: BufRead>(db: &mut DB, series_id: &str, reader: R) -> io::Result<()> {
        let series = db.create_series(series_id)?;
        let mut series = series.lock().unwrap();
        let mut buffer = Vec::new();
        let batch_size = 100;
        for entry in reader.lines() {
            let entry = entry?;
            let mut tokens = entry.split(';');
            match (tokens.next(), tokens.next()) {
                (Some(timestamp), Some(value)) => {
                    let entry = timestamp.trim().parse::<u64>().ok().and_then(|timestamp| {
                        value.trim().parse::<f64>().ok().map(|value| Entry {
                            ts: timestamp,
                            value: value,
                        })
                    });
                    if entry.is_some() {
                        buffer.push(entry.unwrap());
                    }
                    if buffer.len() == batch_size {
                        series.append(&buffer)?;
                        buffer.clear();
                    }
                }
                _ => {}
            }
        }
        if !buffer.is_empty() {
            series.append(&buffer)?;
        }
        Ok(())
    }
    if input_csv.ends_with(".gz") {
        append_internal(db, series_id, BufReader::new(GzDecoder::new(File::open(input_csv)?)?))
    } else {
        append_internal(db, series_id, BufReader::new(File::open(input_csv)?))
    }
}
