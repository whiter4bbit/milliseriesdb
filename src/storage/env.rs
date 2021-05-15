#[cfg(test)]
use super::super::failpoints::Failpoints;
use super::commit_log::CommitLog;
use super::error::Error;
use super::file_system::{FileKind, FileSystem, OpenMode, SeriesDir};
use super::index::Index;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct SeriesEnv {
    dir: Arc<SeriesDir>,
    commit_log: CommitLog,
    index: Index,
    #[cfg(test)]
    fp: Arc<Failpoints>,
}

impl SeriesEnv {
    fn create(dir: Arc<SeriesDir>, #[cfg(test)] fp: Arc<Failpoints>) -> Result<SeriesEnv, Error> {
        let log = CommitLog::open(
            dir.clone(),
            #[cfg(test)]
            fp.clone(),
        )?;
        let index_offset = log.current().index_offset;
        Ok(SeriesEnv {
            dir: dir.clone(),
            commit_log: log,
            index: Index::open(
                dir.clone().open(FileKind::Index, OpenMode::Write)?,
                index_offset,
            )?,
            #[cfg(test)]
            fp: fp,
        })
    }
    pub fn dir(&self) -> Arc<SeriesDir> {
        self.dir.clone()
    }
    pub fn commit_log(&self) -> &CommitLog {
        &self.commit_log
    }
    #[cfg(test)]
    pub fn fp(&self) -> Arc<Failpoints> {
        self.fp.clone()
    }
    pub fn index(&self) -> &Index {
        &self.index
    }
}

pub struct Env {
    fs: FileSystem,
    series: Arc<Mutex<HashMap<String, Arc<SeriesEnv>>>>,
    #[cfg(test)]
    fp: Arc<Failpoints>,
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
                let env = Arc::new(SeriesEnv::create(
                    self.fs.series(name.as_ref())?,
                    #[cfg(test)]
                    self.fp.clone(),
                )?);
                series.insert(name.as_ref().to_owned(), env.clone());

                Ok(env.clone())
            }
        }
    }
}

pub fn create(
    fs: FileSystem,
    #[cfg(test)] 
    fp: Arc<Failpoints>,
) -> Env {
    Env {
        fs: fs,
        series: Arc::new(Mutex::new(HashMap::new())),
        #[cfg(test)]
        fp,
    }
}

#[cfg(test)]
pub mod test {
    use super::super::file_system;
    use super::*;
    use std::fs;
    use std::ops::Deref;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    pub struct TempEnv {
        pub env: Env,
        path: PathBuf,
    }

    impl Drop for TempEnv {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.path).unwrap();
        }
    }

    impl Deref for TempEnv {
        type Target = Env;
        fn deref(&self) -> &Self::Target {
            &self.env
        }
    }

    pub fn create_with_failpoints(fp: Arc<Failpoints>) -> Result<TempEnv, Error> {
        let path = PathBuf::from(format!(
            "temp-dir-{:?}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        Ok(TempEnv {
            env: super::create(file_system::open(&path)?, fp),
            path: path.clone(),
        })
    }

    pub fn create() -> Result<TempEnv, Error> {
        create_with_failpoints(Arc::new(Failpoints::create()))
    }
}
