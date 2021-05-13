use super::commit_log::CommitLog;
use super::error::Error;
use super::file_system::{FileSystem, SeriesDir};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct SeriesEnv {
    dir: Arc<SeriesDir>,
    commit_log: CommitLog,
}

impl SeriesEnv {
    fn create(dir: Arc<SeriesDir>) -> Result<SeriesEnv, Error> {
        Ok(SeriesEnv {
            dir: dir.clone(),
            commit_log: CommitLog::open(dir.clone())?,
        })
    }
    pub fn dir(&self) -> Arc<SeriesDir> {
        self.dir.clone()
    }
    pub fn commit_log(&self) -> &CommitLog {
        &self.commit_log
    }
}

pub struct Env {
    fs: FileSystem,
    series: Arc<Mutex<HashMap<String, Arc<SeriesEnv>>>>,
}

impl Env {
    pub fn fs(&self) -> &FileSystem {
        &self.fs
    }
    pub fn series<S: AsRef<str>>(&self, name: S) -> Result<Arc<SeriesEnv>, Error> {
        let mut series = self.series.lock().unwrap();
        match series.get(name.as_ref()) {
            Some(env) => Ok(env.clone()),
            _ => {
                let env = Arc::new(SeriesEnv::create(self.fs.series(name.as_ref())?)?);
                series.insert(name.as_ref().to_owned(), env.clone());

                Ok(env.clone())
            }
        }
    }
}

pub fn create(fs: FileSystem) -> Env {
    Env {
        fs: fs,
        series: Arc::new(Mutex::new(HashMap::new()))
    }
}

#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(test)]
use std::path::PathBuf;
#[cfg(test)]
use std::ops::Deref;
#[cfg(test)]
use std::fs;
#[cfg(test)]
use super::file_system;

#[cfg(test)]
pub struct TempEnv {
    pub env: Env,
    path: PathBuf,
}

#[cfg(test)]
impl Drop for TempEnv {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).unwrap();
    }
}

#[cfg(test)]
impl Deref for TempEnv {
    type Target = Env;
    fn deref(&self) -> &Self::Target {
        &self.env
    }
}

#[cfg(test)]
pub fn create_temp() -> Result<TempEnv, Error> {
    let path = PathBuf::from(format!(
        "temp-dir-{:?}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    Ok(TempEnv {        
        env: create(file_system::open(&path)?),
        path: path.clone(),
    })
}