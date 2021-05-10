mod series_reader;
mod series_writer;

pub use series_reader::{SeriesIterator, SeriesReader};
pub use series_writer::{SeriesWriter, SyncMode};

#[cfg(test)]
mod test {
    use super::super::entry::Entry;
    use super::super::error::Error;
    use super::super::file_system;
    use super::*;

    fn entry(ts: u64, value: f64) -> Entry {
        Entry { ts, value }
    }

    #[test]
    fn test_series_read_write() -> Result<(), Error> {
        let file_system = &file_system::open_temp()?.fs;
        let series_dir = file_system.series("series1")?;

        let entries = [
            entry(1, 11.0),
            entry(2, 12.0),
            entry(3, 13.0),
            entry(5, 15.0),
            entry(8, 18.0),
            entry(10, 110.0),
            entry(20, 120.0),
            entry(21, 121.0),
            entry(40, 140.0),
            entry(100, 1100.0),
            entry(110, 1110.0),
            entry(120, 1120.0),
            entry(140, 1140.0),
        ];
        {
            let writer = SeriesWriter::create(series_dir.clone())?;
            writer.append(&entries[0..5])?;
            writer.append(&entries[5..8])?;
            writer.append(&entries[8..11])?;
        }

        let reader = SeriesReader::create(series_dir.clone())?;
        assert_eq!(
            entries[3..11].to_vec(),
            reader.iterator(4)?.collect::<Result<Vec<Entry>, Error>>()?
        );
        assert_eq!(
            entries[6..11].to_vec(),
            reader
                .iterator(15)?
                .collect::<Result<Vec<Entry>, Error>>()?
        );
        assert_eq!(
            entries[1..11].to_vec(),
            reader.iterator(2)?.collect::<Result<Vec<Entry>, Error>>()?
        );

        {
            let writer = SeriesWriter::create(series_dir)?;
            writer.append(&entries[11..13])?;
        }

        assert_eq!(
            entries[1..13].to_vec(),
            reader.iterator(2)?.collect::<Result<Vec<Entry>, Error>>()?
        );

        Ok(())
    }
}
