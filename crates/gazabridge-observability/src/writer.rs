use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use gazabridge_secrets::Redactor;

/// Scrubs secrets from every write before passing it on.
///
/// The tracing formatter writes one complete event per `write` call, so
/// secrets are never split across calls.
#[derive(Debug)]
pub struct RedactingWriter<W> {
    inner: W,
    redactor: Redactor,
}

impl<W> RedactingWriter<W> {
    pub fn new(inner: W, redactor: Redactor) -> Self {
        Self { inner, redactor }
    }
}

impl<W: Write> Write for RedactingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let text = String::from_utf8_lossy(buf);
        let scrubbed = self.redactor.scrub(&text);
        self.inner.write_all(scrubbed.as_bytes())?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// An append-only log file rotated by size: `name`, `name.1`, … `name.N`.
#[derive(Debug)]
pub struct SizeRotatingFile {
    path: PathBuf,
    file: File,
    size: u64,
    max_bytes: u64,
    max_files: usize,
}

impl SizeRotatingFile {
    pub fn open(path: PathBuf, max_bytes: u64, max_files: usize) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            path,
            file,
            size,
            max_bytes: max_bytes.max(1024),
            max_files,
        })
    }

    fn rotated(&self, n: usize) -> PathBuf {
        let mut s = self.path.clone().into_os_string();
        s.push(format!(".{n}"));
        PathBuf::from(s)
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.file.flush()?;
        if self.max_files == 0 {
            self.file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&self.path)?;
            self.size = 0;
            return Ok(());
        }
        let oldest = self.rotated(self.max_files);
        if oldest.exists() {
            fs::remove_file(&oldest)?;
        }
        for n in (1..self.max_files).rev() {
            let from = self.rotated(n);
            if from.exists() {
                fs::rename(&from, self.rotated(n + 1))?;
            }
        }
        fs::rename(&self.path, self.rotated(1))?;
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.size = 0;
        Ok(())
    }
}

impl Write for SizeRotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.size > 0 && self.size + buf.len() as u64 > self.max_bytes {
            self.rotate()?;
        }
        self.file.write_all(buf)?;
        self.size += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// Deletes rotated log files (`*.log.N`) older than `retention_days`.
pub(crate) fn apply_retention(dir: &Path, retention_days: u32) -> io::Result<()> {
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(u64::from(retention_days) * 86_400))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_rotated = name
            .rsplit_once(".log.")
            .is_some_and(|(_, n)| n.chars().all(|c| c.is_ascii_digit()));
        if !is_rotated {
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        if modified < cutoff {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotates_by_size_and_keeps_n_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        let mut f = SizeRotatingFile::open(path.clone(), 1024, 2).unwrap();
        let line = vec![b'x'; 600];
        for _ in 0..6 {
            f.write_all(&line).unwrap();
        }
        f.flush().unwrap();
        assert!(path.exists());
        assert!(dir.path().join("app.log.1").exists());
        assert!(dir.path().join("app.log.2").exists());
        assert!(!dir.path().join("app.log.3").exists());
        assert!(fs::metadata(&path).unwrap().len() <= 1024);
    }

    #[test]
    fn redacting_writer_scrubs_secrets() {
        let redactor = Redactor::new();
        redactor.register("hunter2-secret");
        let mut out = Vec::new();
        {
            let mut w = RedactingWriter::new(&mut out, redactor);
            w.write_all(b"auth failed for hunter2-secret at postgres://u:pw@h/db\n")
                .unwrap();
        }
        let s = String::from_utf8(out).unwrap();
        assert!(!s.contains("hunter2-secret"));
        assert!(!s.contains(":pw@"));
    }

    #[test]
    fn retention_only_removes_rotated_files() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("gazabridge.log"), b"active").unwrap();
        fs::write(dir.path().join("gazabridge.log.1"), b"old").unwrap();
        fs::write(dir.path().join("notes.txt"), b"keep").unwrap();
        // Zero-day retention removes every rotated file.
        apply_retention(dir.path(), 0).unwrap();
        assert!(dir.path().join("gazabridge.log").exists());
        assert!(!dir.path().join("gazabridge.log.1").exists());
        assert!(dir.path().join("notes.txt").exists());
    }
}
