use milliseriesdb::db::DB;
use std::fs::File;
use std::io::{self, BufWriter, Write};

pub fn export(db: &mut DB, series_id: &str, output_csv: &str, from_ts: u64) -> io::Result<()> {
    let series = db.create_series(series_id)?;
    let mut series = series.lock().unwrap();
    let mut writer = BufWriter::new(File::create(output_csv)?);
    for entry in series.iterator(from_ts)? {
        let entry = entry?;
        writer.write(format!("{}; {:.2}\n", entry.ts, entry.value).as_bytes())?;
    }
    Ok(())
}
