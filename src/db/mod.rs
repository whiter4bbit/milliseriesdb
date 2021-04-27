mod compression;
mod data;
mod entry;
mod executor;
mod file_system;
mod index;
mod io_utils;
mod log;
mod series;
mod series_table;
mod utils;

#[cfg(test)]
mod test_utils;

pub use compression::Compression;
pub use entry::Entry;
pub use executor::{execute_query, execute_query_async, Aggregation, Query, QueryExpr, Row};
pub use series::{SeriesReader, SeriesWriterGuard, SyncMode};
use series_table::SeriesTable;
use std::io;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct DB {
    table: Arc<SeriesTable>,
}

impl DB {
    pub fn open<P: AsRef<Path>>(base_path: P, sync_mode: SyncMode) -> io::Result<DB> {
        let fs = file_system::open(base_path.as_ref())?;

        Ok(DB {
            table: Arc::new(series_table::create(fs, sync_mode)?),
        })
    }

    pub fn writer<N: AsRef<str>>(&self, name: N) -> Option<Arc<SeriesWriterGuard>> {
        self.table.writer(name.as_ref())
    }

    pub fn reader<N: AsRef<str>>(&self, name: N) -> Option<Arc<SeriesReader>> {
        self.table.reader(name.as_ref())
    }

    pub fn create_series<N: AsRef<str>>(&self, name: N) -> io::Result<()> {
        self.table.create(name.as_ref())
    }
}

#[derive(Clone)]
pub struct AsyncDB {
    db: Arc<DB>,
}

impl AsyncDB {
    pub fn create(db: DB) -> AsyncDB {
        AsyncDB { db: Arc::new(db) }
    }

    pub fn writer<N: AsRef<str>>(&self, name: N) -> Option<Arc<SeriesWriterGuard>> {
        self.db.writer(name)
    }

    pub fn reader<N: AsRef<str>>(&self, name: N) -> Option<Arc<SeriesReader>> {
        self.db.reader(name)
    }

    pub async fn create_series(&self, name: String) -> io::Result<()> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || db.create_series(name))
            .await
            .unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::test_utils::create_temp_dir;
    use super::*;
    #[test]
    fn test_db_basic() {
        let db_dir = create_temp_dir("test-base").unwrap();
        {
            let db = DB::open(&db_dir.path, SyncMode::Never).unwrap();
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
}
