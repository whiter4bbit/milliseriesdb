mod compression;
mod data;
mod entry;
mod index;
mod io_utils;
mod log;
mod series;
mod executor;
mod utils;

#[cfg(test)]
mod test_utils;

pub use entry::Entry;
pub use series::{Series, SyncMode};
pub use executor::{Query, QueryExpression, Executor, Row};
pub use compression::Compression;
use std::collections::HashMap;
use std::fs::create_dir_all;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct DB {
    series_dir: PathBuf,
    series: Arc<Mutex<HashMap<String, Arc<Series>>>>,
    sync_mode: SyncMode,
    compression: Compression,
}

impl DB {
    pub fn open<P: AsRef<Path>>(base_path: P, sync_mode: SyncMode, compression: Compression) -> io::Result<DB> {
        let series_dir = base_path.as_ref().join("series");
        create_dir_all(series_dir.clone())?;
        Ok(DB {
            series_dir: series_dir.clone(),
            series: Arc::new(Mutex::new(HashMap::new())),
            sync_mode: sync_mode,
            compression: compression,
        })
    }

    fn get_series_internal<N: AsRef<str>>(&mut self, name: N, create: bool) -> io::Result<Option<Arc<Series>>> {
        let mut series = self.series.lock().unwrap();
        if series.contains_key(name.as_ref()) {
            return Ok(Some(series.get(name.as_ref()).unwrap().clone()));
        }
        let series_dir = self.series_dir.clone().join(name.as_ref());
        match series_dir.is_dir() || create {
            true => {
                let new = Arc::new(Series::open_or_create(series_dir, self.sync_mode, self.compression.clone())?);
                series.insert(name.as_ref().to_string(), new.clone());
                Ok(Some(new.clone()))
            }
            _ => Ok(None),
        }
    }

    pub fn get_series<N: AsRef<str>>(&mut self, name: N) -> io::Result<Option<Arc<Series>>> {
        self.get_series_internal(name, false)
    }

    pub fn create_series<N: AsRef<str>>(&mut self, name: N) -> io::Result<Arc<Series>> {
        self.get_series_internal(name, true).map(|result| result.unwrap())
    }
}

#[cfg(test)]
mod test {
    use super::test_utils::create_temp_dir;
    use super::*;
    #[test]
    fn test_db_basic() {
        let db_dir = create_temp_dir("test-base").unwrap();

        let mut db = DB::open(&db_dir.path, SyncMode::Never, Compression::None).unwrap();

        assert!(db.get_series("co2").unwrap().is_none());
        let _ = db.create_series("co2").unwrap();

        assert!(db.get_series("co2").unwrap().is_some());
    }
}
