use super::file_system::FileSystem;
use super::{SeriesReader, SeriesWriterGuard, SyncMode};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

struct SharedState {
    readers: HashMap<String, Arc<SeriesReader>>,
    writers: HashMap<String, Arc<SeriesWriterGuard>>,
}

pub struct SeriesTable {
    file_system: FileSystem,
}