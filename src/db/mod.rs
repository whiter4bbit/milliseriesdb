mod compression;
mod data;
mod entry;
mod index;
mod io_utils;
mod log;
mod series;

#[cfg(test)]
mod test_utils;

pub use entry::Entry;
pub use series::{Series, SyncMode};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::fs::create_dir_all;
use std::sync::{Arc, Mutex};

pub type SeriesGuard = Arc<Mutex<Series>>;

pub struct DB {
    series_dir: PathBuf,
    series: HashMap<String, SeriesGuard>,
    sync_mode: SyncMode,
}

impl DB {
    pub fn open<P: AsRef<Path>>(base_path: P, sync_mode: SyncMode) -> io::Result<DB> {
        let series_dir = base_path.as_ref().join("series");
        create_dir_all(series_dir.clone())?;
        Ok(DB {
            series_dir: series_dir.clone(),
            series: HashMap::new(),
            sync_mode: sync_mode,
        })
    }

    fn get_series_internal<N: AsRef<str>>(&mut self, name: N, create: bool) -> io::Result<Option<SeriesGuard>> {
        if self.series.contains_key(name.as_ref()) {
            return Ok(Some(self.series.get(name.as_ref()).unwrap().clone()))
        }
        let series_dir = self.series_dir.clone().join(name.as_ref());
        match series_dir.is_dir() || create {
            true => {
                let series = Arc::new(Mutex::new(Series::open_or_create(series_dir, self.sync_mode)?));
                self.series.insert(name.as_ref().to_string(), series.clone());
                Ok(Some(series.clone()))
            },
            _ => Ok(None)
        }
    }

    pub fn get_series<N: AsRef<str>>(&mut self, name: N) -> io::Result<Option<SeriesGuard>> {
        self.get_series_internal(name, false)
    }

    pub fn create_series<N: AsRef<str>>(&mut self, name: N) -> io::Result<SeriesGuard> {
        self.get_series_internal(name, true).map(|result| result.unwrap())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::test_utils::create_temp_dir;
    
    #[test]
    fn test_db_basic() {
        let db_dir = create_temp_dir("test-base").unwrap();

        let mut db = DB::open(&db_dir.path, SyncMode::Never).unwrap();

        assert!(db.get_series("co2").unwrap().is_none());
        
        let _ = db.create_series("co2").unwrap();

        assert!(db.get_series("co2").unwrap().is_some());
    }
}