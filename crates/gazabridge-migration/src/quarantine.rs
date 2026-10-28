//! Rejected records, written as MongoDB Extended JSON lines so the file can
//! be imported again once the problem is fixed.

use std::path::PathBuf;

use gazabridge_domain::Record;
use gazabridge_files::{FileError, FileFormat, FileWriter, JsonFlavor, WriteOptions};

pub(crate) struct Quarantine {
    dir: PathBuf,
    name: String,
    writer: Option<FileWriter>,
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, FileError> + Send + 'static,
) -> Result<T, FileError> {
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|e| Err(FileError::Invalid(format!("quarantine task failed: {e}"))))
}

impl Quarantine {
    pub(crate) fn new(dir: PathBuf, stream: &str) -> Self {
        let safe: String = stream
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        Self {
            dir,
            name: format!("{safe}-{ms}.rejected.jsonl"),
            writer: None,
        }
    }

    /// Appends records, creating the file on first use.
    pub(crate) async fn add(mut self, records: Vec<Record>) -> Result<Self, FileError> {
        let writer = self.writer.take();
        let path = self.dir.join(&self.name);
        let dir = self.dir.clone();
        let writer = blocking(move || {
            let mut w = match writer {
                Some(w) => w,
                None => {
                    std::fs::create_dir_all(&dir).map_err(|e| FileError::io(&dir, e))?;
                    let mut options = WriteOptions::new(FileFormat::Jsonl);
                    options.json_flavor = JsonFlavor::MongoRelaxed;
                    FileWriter::create(&path, options)?
                }
            };
            w.write(&records)?;
            Ok(w)
        })
        .await?;
        self.writer = Some(writer);
        Ok(self)
    }

    /// Completes the file; `None` when nothing was rejected.
    pub(crate) async fn finish(self) -> Result<Option<PathBuf>, FileError> {
        match self.writer {
            None => Ok(None),
            Some(w) => blocking(move || w.finish().map(|s| Some(s.path))).await,
        }
    }
}
