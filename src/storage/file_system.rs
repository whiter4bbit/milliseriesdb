use super::error::Error;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub enum FileKind {
    #[allow(dead_code)]
    Data,
    #[allow(dead_code)]
    Index,
    #[allow(dead_code)]
    Log(u64),
}

pub enum OpenMode {
    #[allow(dead_code)]
    Read,
    #[allow(dead_code)]
    Write,
}

pub struct SeriesDir {
    base_path: PathBuf,
}

impl SeriesDir {
    fn file_path(&self, kind: FileKind) -> PathBuf {
        self.base_path.join(match kind {
            FileKind::Data => "series.dat".to_owned(),
            FileKind::Index => "series.idx".to_owned(),
            FileKind::Log(s) => format!("series.log.{}", s),
        })
    }
    pub fn open(&self, kind: FileKind, mode: OpenMode) -> Result<File, Error> {
        let path = self.file_path(kind);
        let mut options = OpenOptions::new();
        let options = match mode {
            OpenMode::Read => options.read(true),
            OpenMode::Write => options.read(true).write(true).create(true),
        };
        Ok(options.open(&path)?)
    }
    fn parse_log_filename(&self, s: &str) -> Option<u64> {
        s.strip_prefix("series.log.")
            .and_then(|suffix| suffix.parse::<u64>().ok())
    }
    pub fn read_log_sequences(&self) -> Result<Vec<u64>, Error> {
        let mut sequences = fs::read_dir(&self.base_path)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter_map(|entry| self.parse_log_filename(&entry))
            .collect::<Vec<u64>>();
        sequences.sort_unstable();
        sequences.reverse();
        Ok(sequences)
    }
    pub fn remove_log(&self, seq: u64) -> Result<(), Error> {
        Ok(fs::remove_file(self.file_path(FileKind::Log(seq)))?)
    }
}

pub struct FileSystem {
    base_path: PathBuf,
}

impl FileSystem {
    pub fn series<S: AsRef<str>>(&self, name: S) -> Result<Arc<SeriesDir>, Error> {
        let base_path = self.base_path.join("series").join(name.as_ref());
        fs::create_dir_all(&base_path)?;

        Ok(Arc::new(SeriesDir { base_path }))
    }

    pub fn rename_series<S: AsRef<str>>(&self, src: S, dst: S) -> Result<(), Error> {
        let src_path = self.base_path.join("series").join(src.as_ref());
        let dst_path = self.base_path.join("series").join(dst.as_ref());

        Ok(fs::rename(src_path, dst_path)?)
    }

    pub fn get_series(&self) -> Result<Vec<String>, Error> {
        let mut series = Vec::new();
        for entry in fs::read_dir(self.base_path.join("series"))? {
            let series_path = entry?.path().clone();
            if series_path.join("series.dat").is_file() {
                if let Some(filename) = series_path
                    .file_name()
                    .and_then(|f| f.to_owned().into_string().ok())
                {
                    series.push(filename);
                }
            }
        }
        series.sort();
        Ok(series)
    }
}

pub fn open<P: AsRef<Path>>(base_path: P) -> Result<FileSystem, Error> {
    fs::create_dir_all(base_path.as_ref().join("series"))?;
    Ok(FileSystem {
        base_path: base_path.as_ref().to_owned(),
    })
}

#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::ops::Deref;

#[cfg(test)]
pub struct TempFS {
    pub fs: FileSystem,
    path: PathBuf,
}

#[cfg(test)]
impl Drop for TempFS {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).unwrap();
    }
}

#[cfg(test)]
impl Deref for TempFS {
    type Target = FileSystem;
    fn deref(&self) -> &Self::Target {
        &self.fs
    }
}

#[cfg(test)]
pub fn open_temp() -> Result<TempFS, Error> {
    let path = PathBuf::from(format!(
        "temp-dir-{:?}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    Ok(TempFS {        
        fs: open(&path)?,
        path: path.clone(),
    })
}