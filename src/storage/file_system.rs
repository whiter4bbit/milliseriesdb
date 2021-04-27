use std::fs::{self, File, OpenOptions};
use std::io;
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
    pub fn open(&self, kind: FileKind, mode: OpenMode) -> io::Result<File> {
        let path = self.file_path(kind);
        let mut options = OpenOptions::new();
        let options = match mode {
            OpenMode::Read => options.read(true),
            OpenMode::Write => options.read(true).write(true).create(true),
        };
        options.open(&path).map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Can not open file: {:?}: {:?}", &path, err),
            )
        })
    }
    fn parse_log_filename(&self, s: &str) -> Option<u64> {
        s.strip_prefix("series.log.")
            .and_then(|suffix| suffix.parse::<u64>().ok())
    }
    pub fn read_log_sequences(&self) -> io::Result<Vec<u64>> {
        let mut sequences = fs::read_dir(&self.base_path)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter_map(|entry| self.parse_log_filename(&entry))
            .collect::<Vec<u64>>();
        sequences.sort_unstable();
        sequences.reverse();
        Ok(sequences)
    }
    pub fn remove_log(&self, seq: u64) -> io::Result<()> {
        fs::remove_file(self.file_path(FileKind::Log(seq)))
    }
}

pub struct FileSystem {
    base_path: PathBuf,
}

impl FileSystem {
    pub fn series<S: AsRef<str>>(&self, name: S) -> io::Result<Arc<SeriesDir>> {
        let base_path = self.base_path.join("series").join(name.as_ref());
        fs::create_dir_all(&base_path)?;

        Ok(Arc::new(SeriesDir {
            base_path,
        }))
    }

    pub fn get_series(&self) -> io::Result<Vec<String>> {
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

pub fn open<P: AsRef<Path>>(base_path: P) -> io::Result<FileSystem> {
    fs::create_dir_all(base_path.as_ref().join("series"))?;
    Ok(FileSystem {
        base_path: base_path.as_ref().to_owned(),
    })
}
