use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mdb_common::{ErrorCode, MdbError, MdbResult};

#[derive(Debug, Clone)]
pub struct WalArchive {
    pub filename: String,
    pub size_bytes: u64,
    pub archived_at: i64,
    pub source_path: String,
}

#[derive(Debug)]
pub struct WalArchiver {
    archive_path: String,
    archive_interval: Duration,
}

impl WalArchiver {
    pub fn new(
        archive_path: impl AsRef<Path>,
        archive_interval: Duration,
    ) -> Self {
        Self {
            archive_path: archive_path.as_ref().to_string_lossy().into_owned(),
            archive_interval,
        }
    }

    pub fn archive_wal(&self, wal_file: &str) -> MdbResult<()> {
        let src = Path::new(wal_file);
        if !src.exists() {
            return Err(MdbError::new(
                ErrorCode::NamespaceNotFound,
                format!("WAL file '{wal_file}' not found"),
            ));
        }

        let archive_dir = Path::new(&self.archive_path);
        fs::create_dir_all(archive_dir)
            .map_err(|e| io_err(format!("create archive dir: {e}")))?;

        let filename = src
            .file_name()
            .ok_or_else(|| io_err("invalid WAL filename"))?
            .to_string_lossy()
            .to_string();

        let dst = archive_dir.join(&filename);

        if dst.exists() {
            return Ok(());
        }

        fs::copy(src, &dst)
            .map_err(|e| io_err(format!("copy WAL to archive: {e}")))?;

        let meta_path = archive_dir.join(format!("{filename}.meta"));
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let src_meta = fs::metadata(src)
            .map_err(|e| io_err(format!("stat source WAL: {e}")))?;

        let meta_content = format!(
            "filename: {}\nsize_bytes: {}\narchived_at: {}\nsource_path: {}\n",
            filename,
            src_meta.len(),
            now,
            wal_file
        );

        fs::write(&meta_path, meta_content)
            .map_err(|e| io_err(format!("write archive metadata: {e}")))?;

        Ok(())
    }

    pub fn list_archives(&self) -> MdbResult<Vec<WalArchive>> {
        let archive_dir = Path::new(&self.archive_path);
        if !archive_dir.exists() {
            return Ok(Vec::new());
        }

        let mut archives = Vec::new();
        let entries = fs::read_dir(archive_dir)
            .map_err(|e| io_err(format!("read archive dir: {e}")))?;

        for entry in entries {
            let entry = entry.map_err(|e| io_err(format!("read entry: {e}")))?;
            let path = entry.path();

            if path.to_string_lossy().ends_with(".meta") {
                if let Some(archive) = read_archive_meta(&path)? {
                    archives.push(archive);
                }
            }
        }

        archives.sort_by(|a, b| a.archived_at.cmp(&b.archived_at));
        Ok(archives)
    }

    pub fn cleanup(&self, retention_days: u32) -> MdbResult<u32> {
        let archive_dir = Path::new(&self.archive_path);
        if !archive_dir.exists() {
            return Ok(0);
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let cutoff = if retention_days == 0 {
            i64::MAX
        } else {
            now - (retention_days as i64 * 86400)
        };

        let archives = self.list_archives()?;
        let mut removed = 0u32;

        for archive in &archives {
            if archive.archived_at < cutoff {
                let wal_path = archive_dir.join(&archive.filename);
                let meta_path = archive_dir.join(format!("{}.meta", archive.filename));

                let _ = fs::remove_file(&wal_path);
                let _ = fs::remove_file(&meta_path);
                removed += 1;
            }
        }

        Ok(removed)
    }

    pub fn archive_interval(&self) -> Duration {
        self.archive_interval
    }
}

fn io_err(msg: impl std::fmt::Display) -> MdbError {
    MdbError::new(ErrorCode::HostUnreachable, msg.to_string())
}

fn read_archive_meta(meta_path: &Path) -> MdbResult<Option<WalArchive>> {
    let content = fs::read_to_string(meta_path)
        .map_err(|e| io_err(format!("read archive meta: {e}")))?;

    let mut filename = String::new();
    let mut size_bytes = 0u64;
    let mut archived_at = 0i64;
    let mut source_path = String::new();

    for line in content.lines() {
        if let Some(v) = line.strip_prefix("filename: ") {
            filename = v.trim().to_owned();
        } else if let Some(v) = line.strip_prefix("size_bytes: ") {
            size_bytes = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("archived_at: ") {
            archived_at = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("source_path: ") {
            source_path = v.trim().to_owned();
        }
    }

    if filename.is_empty() {
        return Ok(None);
    }

    Ok(Some(WalArchive {
        filename,
        size_bytes,
        archived_at,
        source_path,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn temp_dir(suffix: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = PathBuf::from(format!("test_walarch_{suffix}_{n}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn archive_wal_file() {
        let wal_dir = temp_dir("wal");
        let archive_dir = temp_dir("archive");

        let wal_file = wal_dir.join("wal-000000000000.log");
        fs::write(&wal_file, b"test wal content").unwrap();

        let archiver = WalArchiver::new(&archive_dir, Duration::from_secs(60));
        archiver.archive_wal(wal_file.to_str().unwrap()).unwrap();

        let archives = archiver.list_archives().unwrap();
        assert_eq!(archives.len(), 1);
        assert_eq!(archives[0].filename, "wal-000000000000.log");
    }

    #[test]
    fn archive_idempotent() {
        let wal_dir = temp_dir("wal2");
        let archive_dir = temp_dir("archive2");

        let wal_file = wal_dir.join("wal-000000000000.log");
        fs::write(&wal_file, b"test wal content").unwrap();

        let archiver = WalArchiver::new(&archive_dir, Duration::from_secs(60));
        archiver.archive_wal(wal_file.to_str().unwrap()).unwrap();
        archiver.archive_wal(wal_file.to_str().unwrap()).unwrap();

        let archives = archiver.list_archives().unwrap();
        assert_eq!(archives.len(), 1);
    }

    #[test]
    fn cleanup_removes_old_archives() {
        let archive_dir = temp_dir("archive3");

        let wal_content = "old wal content";
        let wal_path = archive_dir.join("old-wal.log");
        fs::write(&wal_path, wal_content).unwrap();

        let old_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 200000;

        let meta_content = format!(
            "filename: old-wal.log\nsize_bytes: {}\narchived_at: {}\nsource_path: /tmp/old-wal.log\n",
            wal_content.len(),
            old_time
        );
        fs::write(archive_dir.join("old-wal.log.meta"), meta_content).unwrap();

        let archiver = WalArchiver::new(&archive_dir, Duration::from_secs(60));
        let removed = archiver.cleanup(1).unwrap();

        assert_eq!(removed, 1);
        assert!(!wal_path.exists());
    }

    #[test]
    fn list_archives_empty() {
        let archive_dir = temp_dir("archive4");

        let archiver = WalArchiver::new(&archive_dir, Duration::from_secs(60));
        let archives = archiver.list_archives().unwrap();

        assert!(archives.is_empty());
    }
}
