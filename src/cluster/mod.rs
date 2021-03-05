use crate::db as storage;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Node {
    id: String,
}

pub struct PoolConfig {
    series_id_prefix: String,
    primary: String,
    replicas: Vec<String>,
}

pub struct Config {
    epoch: u64,
    nodes: Vec<Node>,
    pools: Vec<PoolConfig>,
}

enum Role {
    Primary,
    Replica,
}

impl Config {
    fn role_in_pool<N: AsRef<str>, S: AsRef<str>>(&self, node_id: N, series_id: S) -> Option<Role> {
        for pool in self.pools.iter() {
            if series_id.as_ref().starts_with(&pool.series_id_prefix) {
                if pool.primary == node_id.as_ref() {
                    return Some(Role::Primary);
                } else if pool.replicas.iter().any(|id| id == node_id.as_ref()) {
                    return Some(Role::Replica);
                }
            }
        }
        return None;
    }
}

struct SeriesReader {}

#[derive(Clone)]
struct SeriesWriter {
    writer: storage::SeriesWriterGuard,
}

impl SeriesWriter {
    async fn create<P: AsRef<Path>>(series_path: P) -> io::Result<SeriesWriter> {
        let path = series_path.as_ref().to_path_buf().clone();
        let writer = tokio::task::spawn_blocking(move || storage::SeriesWriterGuard::create(path, storage::SyncMode::Paranoid))
            .await
            .unwrap()?;
        Ok(SeriesWriter {
            writer: writer,
        })
    }
    async fn append(&mut self, entries: &[storage::Entry], compression: storage::Compression) -> io::Result<()> {
        let mut writer = self.writer.clone();
        let entries = entries.to_owned();

        let changes = tokio::task::spawn_blocking(move || writer.append(&entries, compression))
            .await
            .unwrap()?;
        
        Ok(())
    }
}

struct DB {
    node_id: String,
    config: Config,
    series_path: PathBuf,
    series_writers: Arc<Mutex<HashMap<String, SeriesWriter>>>,
}

impl DB {
    #[allow(dead_code)]
    async fn open<P: AsRef<Path>>(node_id: String, config: Config, db_path: P) -> io::Result<DB> {
        Ok(DB {
            node_id: node_id,
            config: config,
            series_path: db_path.as_ref().join("series"),
            series_writers: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    #[allow(dead_code)]
    async fn reader<S: AsRef<str>>(&mut self, id: S) -> io::Result<Option<SeriesReader>> {
        match self.config.role_in_pool(&self.node_id, id.as_ref()) {
            Some(Role::Primary) | Some(Role::Replica) => Ok(None),
            _ => Ok(None),
        }
    }
    #[allow(dead_code)]
    async fn create<S: AsRef<str>>(&mut self, id: S) -> io::Result<Option<()>> {
        let mut writers = self.series_writers.lock().await;
        if writers.contains_key(id.as_ref()) {
            if let Some(Role::Primary) = self.config.role_in_pool(&self.node_id, id.as_ref()) {
                let writer = SeriesWriter::create(self.series_path.join(id.as_ref())).await?;
                writers.insert(id.as_ref().to_owned(), writer.clone());
                return Ok(Some(()));
            }
        }
        Ok(None)
    }
    #[allow(dead_code)]
    async fn writer<S: AsRef<str>>(&mut self, id: S) -> io::Result<Option<SeriesWriter>> {
        let writers = self.series_writers.lock().await;
        Ok(writers.get(id.as_ref()).map(|w| w.clone()))
    }
}
