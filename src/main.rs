mod db;
use db::{Entry, DB, SyncMode};
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::process::exit;

fn append(db_path: &str, input_csv: &str) -> io::Result<()> {
    let mut db = DB::open_or_create(db_path, SyncMode::Every(1000))?;
    let reader = BufReader::new(File::open(input_csv)?);
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
                    db.append(&buffer)?;
                    buffer.clear();
                }
            }
            _ => {}
        }
    }
    if !buffer.is_empty() {
        db.append(&buffer)?;
    }
    Ok(())
}

fn export(db_path: &str, output_csv: &str, from_ts: u64) -> io::Result<()> {
    let mut db = DB::open_or_create(db_path, SyncMode::Never)?;
    let mut writer = BufWriter::new(File::create(output_csv)?);
    for entry in db.iterator(from_ts)? {
        let entry = entry?;
        writer.write(format!("{}; {:.2}\n", entry.ts, entry.value).as_bytes())?;
    }
    Ok(())
}

fn help_and_exit() {
    exit(1);
}

fn main() {
    let mut args = env::args();
    args.next();

    match args.next() {
        Some(command) => match command.as_ref() {
            "append" => match (args.next(), args.next()) {
                (Some(db_path), Some(input_csv)) => append(&db_path, &input_csv).unwrap(),
                _ => help_and_exit(),
            },
            "export" => match (args.next(), args.next(), args.next().and_then(|ts| ts.parse::<u64>().ok())) {
                (Some(db_path), Some(output_csv), Some(from_ts)) => export(&db_path, &output_csv, from_ts).unwrap(),
                _ => help_and_exit(),
            }
            _ => help_and_exit(),
        },
        _ => help_and_exit(),
    }
}
