mod compression;
mod data;
mod entry;
mod executor;
mod index;
mod io_utils;
mod log;
mod series;
mod utils;

#[cfg(test)]
mod test_utils;

pub use compression::Compression;
pub use entry::Entry;
pub use executor::{Aggregation, execute_query, execute_query_async, Query, QueryExpr, Row};
pub use series::{SeriesReader, SeriesWriterGuard, SyncMode};
use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

fn get_series_paths(base_path: &PathBuf) -> io::Result<Vec<(String, PathBuf)>> {
    let mut series = Vec::new();
    for entry in read_dir(base_path.join("series"))? {
        let series_path = entry?.path().clone();
        if series_path.join("series.dat").is_file() {
            if let Some(filename) = series_path.file_name().and_then(|f| f.to_owned().into_string().ok()) {
                series.push((filename, series_path.to_path_buf()));
            }
        }
    }
    series.sort_by_key(|(id, _)| id.clone());
    Ok(series)
}

#[derive(Clone)]
pub struct DB {
    dir_with_series: PathBuf,
    writers: Arc<Mutex<HashMap<String, Arc<SeriesWriterGuard>>>>,
    readers: Arc<Mutex<HashMap<String, Arc<SeriesReader>>>>,
    sync_mode: SyncMode,
}

impl DB {
    pub fn open<P: AsRef<Path>>(base_path: P, sync_mode: SyncMode) -> io::Result<DB> {
        let dir_with_series = base_path.as_ref().join("series");
        create_dir_all(dir_with_series.clone())?;

        let mut writers = HashMap::new();
        let mut readers = HashMap::new();
        for (name, series_dir) in get_series_paths(&base_path.as_ref().to_path_buf())? {
            writers.insert(name.clone(), Arc::new(SeriesWriterGuard::create(series_dir.clone(), sync_mode)?));
            readers.insert(name.clone(), Arc::new(SeriesReader::create(series_dir.clone())?));
        }
        Ok(DB {
            dir_with_series: dir_with_series.clone(),
            writers: Arc::new(Mutex::new(writers)),
            readers: Arc::new(Mutex::new(readers)),
            sync_mode: sync_mode,
        })
    }

    pub fn writer<N: AsRef<str>>(&self, name: N) -> Option<Arc<SeriesWriterGuard>> {
        let writers = self.writers.lock().unwrap();
        writers.get(name.as_ref()).map(|s| s.clone())
    }

    pub fn reader<N: AsRef<str>>(&self, name: N) -> Option<Arc<SeriesReader>> {
        let readers = self.readers.lock().unwrap();
        readers.get(name.as_ref()).map(|s| s.clone())
    }

    pub fn create_series<N: AsRef<str>>(&self, name: N) -> io::Result<()> {
        let mut writers = self.writers.lock().unwrap();
        let mut readers = self.readers.lock().unwrap();

        match readers.get(name.as_ref()) {
            Some(_) => Ok(()),
            _ => {
                let series_path = self.dir_with_series.join(name.as_ref());

                let writer = Arc::new(SeriesWriterGuard::create(series_path.clone(), self.sync_mode)?);
                writers.insert(name.as_ref().to_owned(), writer.clone());

                let reader = Arc::new(SeriesReader::create(series_path.clone())?);
                readers.insert(name.as_ref().to_owned(), reader.clone());

                Ok(())
            }
        }
    }
}

#[derive(Clone)]
pub struct AsyncDB {
    db: Arc<DB>,
}

impl AsyncDB {
    pub fn create(db: DB) -> AsyncDB {
        AsyncDB {
            db: Arc::new(db),
        }
    }

    pub fn writer<N: AsRef<str>>(&self, name: N) -> Option<Arc<SeriesWriterGuard>> {
        self.db.writer(name)
    }

    pub fn reader<N: AsRef<str>>(&self, name: N) -> Option<Arc<SeriesReader>> {
        self.db.reader(name)
    }

    pub async fn create_series(&self, name: String) -> io::Result<()> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            db.create_series(name)
        }).await.unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::test_utils::create_temp_dir;
    use super::*;
    use std::fs::write;
    #[test]
    fn test_db_basic() {
        let db_dir = create_temp_dir("test-base").unwrap();

        {
            let mut db = DB::open(&db_dir.path, SyncMode::Never).unwrap();
            assert!(db.reader("co2").is_none());
            assert!(db.writer("co2").is_none());

            db.create_series("co2").unwrap();
            assert!(db.reader("co2").is_some());
            assert!(db.writer("co2").is_some());
        }

        {
            let db = DB::open(&db_dir.path, SyncMode::Never).unwrap();
            assert!(db.reader("co2").is_some());
            assert!(db.writer("co2").is_some());
        }
    }

    #[test]
    fn test_get_series() {
        let db_dir = create_temp_dir("test-base").unwrap();

        create_dir_all(&db_dir.path.join("series").join("series1")).unwrap();
        write(&db_dir.path.join("series").join("series1").join("series.dat"), "noop").unwrap();

        create_dir_all(&db_dir.path.join("series").join("series2")).unwrap();
        //

        create_dir_all(&db_dir.path.join("series").join("series3")).unwrap();
        write(&db_dir.path.join("series").join("series3").join("series.dat"), "noop").unwrap();

        assert_eq!(
            vec![
                ("series1".to_owned(), (&db_dir.path.join("series").join("series1")).to_owned()),
                ("series3".to_owned(), (&db_dir.path.join("series").join("series3")).to_owned()),
            ],
            get_series_paths(&db_dir.path).unwrap()
        );
    }
}