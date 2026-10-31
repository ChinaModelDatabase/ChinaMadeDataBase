use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use mdb_common::{ErrorCode, MdbError, MdbResult};
use mdb_crypto::digest::Digest;
use mdb_crypto::sha256::Sha256;

use crate::backup::{BackupMetadata, BackupType};

#[derive(Debug, Clone)]
pub struct BackupSession {
    pub snapshot_seqno: u64,
    pub start_time: Instant,
    db_path: String,
    backup_path: String,
    backup_id: String,
}

impl BackupSession {
    pub fn copy_data_files(&self) -> MdbResult<()> {
        let db_path = Path::new(&self.db_path);
        let backup_dir = PathBuf::from(&self.backup_path).join(&self.backup_id);

        fs::create_dir_all(&backup_dir)
            .map_err(|e| io_err(format!("create backup dir: {e}")))?;

        copy_sst_files(db_path, &backup_dir)
    }

    pub fn copy_wal_files(&self) -> MdbResult<()> {
        let wal_dir = Path::new(&self.db_path).join("wal");
        if !wal_dir.exists() {
            return Ok(());
        }

        let backup_wal_dir = PathBuf::from(&self.backup_path)
            .join(&self.backup_id)
            .join("wal");

        fs::create_dir_all(&backup_wal_dir)
            .map_err(|e| io_err(format!("create wal backup dir: {e}")))?;

        let entries = fs::read_dir(&wal_dir)
            .map_err(|e| io_err(format!("read wal dir: {e}")))?;

        for entry in entries {
            let entry = entry.map_err(|e| io_err(format!("read entry: {e}")))?;
            let src = entry.path();
            if src.is_file() {
                let dst = backup_wal_dir.join(entry.file_name());
                fs::copy(&src, &dst)
                    .map_err(|e| io_err(format!("copy wal file: {e}")))?;
            }
        }

        Ok(())
    }

    pub fn finalize(self) -> MdbResult<BackupMetadata> {
        let duration_ms = self.start_time.elapsed().as_millis() as u64;
        let backup_dir = PathBuf::from(&self.backup_path).join(&self.backup_id);

        let size_bytes = dir_size(&backup_dir)?;
        let checksum = compute_quick_checksum(&backup_dir)?;

        let meta = BackupMetadata {
            id: self.backup_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            backup_type: BackupType::Full,
            size_bytes,
            checksum,
            lsn: self.snapshot_seqno,
            duration_ms,
        };

        write_session_metadata(&backup_dir, &meta)?;
        Ok(meta)
    }
}

#[derive(Debug)]
pub struct HotBackup {
    db_path: String,
    backup_path: String,
}

impl HotBackup {
    pub fn new(db_path: impl AsRef<Path>, backup_path: impl AsRef<Path>) -> Self {
        Self {
            db_path: db_path.as_ref().to_string_lossy().into_owned(),
            backup_path: backup_path.as_ref().to_string_lossy().into_owned(),
        }
    }

    pub fn start_backup(&self) -> MdbResult<BackupSession> {
        let seqno = take_snapshot(&self.db_path)?;
        let backup_id = format!(
            "hot_{}_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            std::process::id()
        );

        fs::create_dir_all(&self.backup_path)
            .map_err(|e| io_err(format!("create backup path: {e}")))?;

        Ok(BackupSession {
            snapshot_seqno: seqno,
            start_time: Instant::now(),
            db_path: self.db_path.clone(),
            backup_path: self.backup_path.clone(),
            backup_id,
        })
    }
}

fn take_snapshot(db_path: &str) -> MdbResult<u64> {
    let manifest_path = Path::new(db_path).join("manifest.json");
    if !manifest_path.exists() {
        return Ok(0);
    }

    let data = fs::read(&manifest_path)
        .map_err(|e| io_err(format!("read manifest: {e}")))?;
    let content = String::from_utf8(data)
        .map_err(|e| io_err(format!("invalid manifest utf8: {e}")))?;

    for line in content.lines() {
        if let Some(val) = line.strip_prefix("last_seqno:") {
            return val.trim().parse().map_err(|_| {
                io_err("invalid seqno in manifest")
            });
        }
    }

    Ok(0)
}

fn copy_sst_files(db_path: &Path, backup_dir: &Path) -> MdbResult<()> {
    let colcf_dir = db_path.join("colcf");
    if !colcf_dir.exists() {
        return Ok(());
    }

    let backup_colcf = backup_dir.join("colcf");
    fs::create_dir_all(&backup_colcf)
        .map_err(|e| io_err(format!("create colcf backup dir: {e}")))?;

    let entries = fs::read_dir(&colcf_dir)
        .map_err(|e| io_err(format!("read colcf dir: {e}")))?;

    for entry in entries {
        let entry = entry.map_err(|e| io_err(format!("read entry: {e}")))?;
        let cf_path = entry.path();

        if cf_path.is_dir() {
            let cf_name = entry.file_name();
            let backup_cf_dir = backup_colcf.join(&cf_name);
            fs::create_dir_all(&backup_cf_dir)
                .map_err(|e| io_err(format!("create cf backup dir: {e}")))?;

            let sst_entries = fs::read_dir(&cf_path)
                .map_err(|e| io_err(format!("read cf dir: {e}")))?;

            for sst_entry in sst_entries {
                let sst_entry = sst_entry
                    .map_err(|e| io_err(format!("read sst entry: {e}")))?;
                let src = sst_entry.path();
                if src.is_file() {
                    let dst = backup_cf_dir.join(sst_entry.file_name());
                    fs::copy(&src, &dst)
                        .map_err(|e| io_err(format!("copy sst file: {e}")))?;
                }
            }
        }
    }

    Ok(())
}

fn io_err(msg: impl std::fmt::Display) -> MdbError {
    MdbError::new(ErrorCode::HostUnreachable, msg.to_string())
}

fn dir_size(dir: &Path) -> MdbResult<u64> {
    let mut total = 0u64;
    dir_size_inner(dir, &mut total)?;
    Ok(total)
}

fn dir_size_inner(dir: &Path, total: &mut u64) -> MdbResult<()> {
    let entries = fs::read_dir(dir)
        .map_err(|e| io_err(format!("read dir: {e}")))?;

    for entry in entries {
        let entry = entry.map_err(|e| io_err(format!("read entry: {e}")))?;
        let path = entry.path();

        if path.is_dir() {
            dir_size_inner(&path, total)?;
        } else {
            let meta = fs::metadata(&path)
                .map_err(|e| io_err(format!("stat: {e}")))?;
            *total += meta.len();
        }
    }

    Ok(())
}

fn compute_quick_checksum(dir: &Path) -> MdbResult<String> {
    let mut data = Vec::new();
    collect_filenames(dir, &mut data)?;
    let hash = Sha256::digest(data.as_slice());
    Ok(hex_encode(hash.as_bytes()))
}

fn collect_filenames(dir: &Path, data: &mut Vec<u8>) -> MdbResult<()> {
    let entries = fs::read_dir(dir)
        .map_err(|e| io_err(format!("read dir: {e}")))?;

    let mut paths: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    paths.sort();

    for path in &paths {
        if path.is_file() {
            data.extend_from_slice(path.to_string_lossy().as_bytes());
            data.push(b'\n');
        } else if path.is_dir() {
            collect_filenames(path, data)?;
        }
    }

    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn write_session_metadata(dir: &Path, meta: &BackupMetadata) -> MdbResult<()> {
    let content = format!(
        "id: {}\ntimestamp: {}\nbackup_type: {:?}\nsize_bytes: {}\nchecksum: {}\nlsn: {}\nduration_ms: {}\n",
        meta.id, meta.timestamp, meta.backup_type, meta.size_bytes, meta.checksum, meta.lsn, meta.duration_ms
    );
    fs::write(dir.join("backup.meta"), content)
        .map_err(|e| io_err(format!("write metadata: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn temp_dir(suffix: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = PathBuf::from(format!("test_hot_{suffix}_{n}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn hot_backup_session_creates_files() {
        let db_dir = temp_dir("db");
        let backup_dir = temp_dir("bak");

        let colcf = db_dir.join("colcf").join("cf_0");
        fs::create_dir_all(&colcf).unwrap();
        fs::write(colcf.join("000001.sst"), b"sst data").unwrap();

        let wal_dir = db_dir.join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        fs::write(wal_dir.join("wal-000000000000.log"), b"wal data").unwrap();

        let hb = HotBackup::new(&db_dir, &backup_dir);
        let session = hb.start_backup().unwrap();
        session.copy_data_files().unwrap();
        session.copy_wal_files().unwrap();
        let meta = session.finalize().unwrap();

        assert!(!meta.id.is_empty());
        assert!(meta.size_bytes > 0);
    }
}
