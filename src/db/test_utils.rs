use std::fs::{create_dir_all, remove_dir_all};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TempDir {
    pub path: PathBuf,
}

impl Drop for TempDir {
    fn drop(&mut self) {
        remove_dir_all(&self.path).unwrap();
    }
}

fn gen_dir_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    return format!("temp-dir-{:?}", nanos);
}

pub fn create_temp_dir<P: AsRef<Path>>(base: P) -> io::Result<TempDir> {
    let path = base.as_ref().join(gen_dir_name());
    create_dir_all(path.clone())?;
    Ok(TempDir { path: path.clone() })
}
