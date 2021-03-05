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
pub use executor::{Aggregation, Executor, Query, QueryExpr, Row};
pub use series::{Series, SyncMode, SeriesWriterGuard};
use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub struct Change {
    offset: u64,
    size: u64,
    path: PathBuf,
}

pub struct Changes {
    data: Change,
    index: Change,
    log: Change,
}

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
    series: Arc<Mutex<HashMap<String, Arc<Series>>>>,
    sync_mode: SyncMode,
}

impl DB {
    pub fn open<P: AsRef<Path>>(base_path: P, sync_mode: SyncMode) -> io::Result<DB> {
        let dir_with_series = base_path.as_ref().join("series");
        create_dir_all(dir_with_series.clone())?;

        let mut series = HashMap::new();
        for (id, series_dir) in get_series_paths(&base_path.as_ref().to_path_buf())? {
            series.insert(id, Arc::new(Series::open_or_create(series_dir, sync_mode)?));
        }
        Ok(DB {
            dir_with_series: dir_with_series.clone(),
            series: Arc::new(Mutex::new(series)),
            sync_mode: sync_mode,
        })
    }

    pub fn get_series<N: AsRef<str>>(&self, name: N) -> Option<Arc<Series>> {
        let series = self.series.lock().unwrap();
        series.get(name.as_ref()).map(|s| s.clone())
    }

    pub fn create_series<N: AsRef<str>>(&mut self, name: N) -> io::Result<Arc<Series>> {
        let mut opened_series = self.series.lock().unwrap();

        match opened_series.get(name.as_ref()) {
            Some(series) => Ok(series.clone()),
            _ => {
                let created = Arc::new(Series::open_or_create(self.dir_with_series.join(name.as_ref()), self.sync_mode)?);
                opened_series.insert(name.as_ref().to_owned(), created.clone());
                Ok(created.clone())
            }
        }
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
            assert!(db.get_series("co2").is_none());

            db.create_series("co2").unwrap();
            assert!(db.get_series("co2").is_some());
        }

        {
            let db = DB::open(&db_dir.path, SyncMode::Never).unwrap();
            assert!(db.get_series("co2").is_some());
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
