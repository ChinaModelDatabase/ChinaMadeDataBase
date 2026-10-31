use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use mdb_common::{ErrorCode, MdbError, MdbResult};
use mdb_crypto::crc32c;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupType {
    Full,
    Incremental,
    Differential,
}

#[derive(Debug, Clone)]
pub struct BackupMetadata {
    pub id: String,
    pub timestamp: i64,
    pub backup_type: BackupType,
    pub size_bytes: u64,
    pub checksum: String,
    pub lsn: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone)]
pub struct VerificationResult {
    pub valid: bool,
    pub checksum_matches: bool,
    pub file_count: usize,
    pub total_size: u64,
    pub errors: Vec<String>,
}

#[derive(Debug)]
pub struct BackupManager {
    db_path: String,
    backup_path: String,
}

impl BackupManager {
    pub fn new(db_path: impl AsRef<Path>, backup_path: impl AsRef<Path>) -> Self {
        Self {
            db_path: db_path.as_ref().to_string_lossy().into_owned(),
            backup_path: backup_path.as_ref().to_string_lossy().into_owned(),
        }
    }

    pub fn full_backup(&self) -> MdbResult<BackupMetadata> {
        let start = Instant::now();
        let id = generate_backup_id();
        let backup_dir = PathBuf::from(&self.backup_path).join(&id);
        fs::create_dir_all(&backup_dir)
            .map_err(|e| io_err(format!("create backup dir: {e}")))?;

        let db_path = Path::new(&self.db_path);
        let mut total_size = 0u64;

        copy_dir_recursive(db_path, &backup_dir, &mut total_size)?;

        let duration_ms = start.elapsed().as_millis() as u64;
        let lsn = read_lsn_from_db(db_path).unwrap_or(0);
        let checksum = compute_dir_checksum(&backup_dir)?;

        let metadata = BackupMetadata {
            id,
            timestamp: current_timestamp(),
            backup_type: BackupType::Full,
            size_bytes: total_size,
            checksum,
            lsn,
            duration_ms,
        };

        write_metadata(&backup_dir, &metadata)?;
        Ok(metadata)
    }

    pub fn incremental_backup(&self) -> MdbResult<BackupMetadata> {
        let start = Instant::now();
        let id = generate_backup_id();
        let backup_dir = PathBuf::from(&self.backup_path).join(&id);
        fs::create_dir_all(&backup_dir)
            .map_err(|e| io_err(format!("create backup dir: {e}")))?;

        let base = self.find_latest_full_backup()?;
        let db_path = Path::new(&self.db_path);
        let mut total_size = 0u64;

        if let Some(base_dir) = &base {
            copy_changed_files(base_dir, db_path, &backup_dir, &mut total_size)?;
        } else {
            copy_dir_recursive(db_path, &backup_dir, &mut total_size)?;
        }

        let duration_ms = start.elapsed().as_millis() as u64;
        let lsn = read_lsn_from_db(db_path).unwrap_or(0);
        let checksum = compute_dir_checksum(&backup_dir)?;

        let metadata = BackupMetadata {
            id,
            timestamp: current_timestamp(),
            backup_type: BackupType::Incremental,
            size_bytes: total_size,
            checksum,
            lsn,
            duration_ms,
        };

        write_metadata(&backup_dir, &metadata)?;
        Ok(metadata)
    }

    pub fn list_backups(&self) -> MdbResult<Vec<BackupMetadata>> {
        let backup_path = Path::new(&self.backup_path);
        if !backup_path.exists() {
            return Ok(Vec::new());
        }

        let mut backups = Vec::new();
        let entries = fs::read_dir(backup_path)
            .map_err(|e| io_err(format!("read backup dir: {e}")))?;

        for entry in entries {
            let entry = entry.map_err(|e| io_err(format!("read dir entry: {e}")))?;
            let path = entry.path();
            if path.is_dir() {
                if let Some(meta) = read_metadata_from_dir(&path)? {
                    backups.push(meta);
                }
            }
        }

        backups.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(backups)
    }

    pub fn restore(backup_id: &str) -> MdbResult<()> {
        let backup_path = Path::new(backup_id);
        if !backup_path.exists() {
            return Err(MdbError::new(
                ErrorCode::NamespaceNotFound,
                format!("backup '{backup_id}' not found"),
            ));
        }

        let _meta = read_metadata_from_dir(backup_path)?
            .ok_or_else(|| MdbError::new(
                ErrorCode::DataCorruption,
                format!("backup '{backup_id}' has no metadata"),
            ))?;

        Ok(())
    }

    pub fn verify(&self, backup_id: &str) -> MdbResult<VerificationResult> {
        let backup_dir = PathBuf::from(&self.backup_path).join(backup_id);
        if !backup_dir.exists() {
            return Err(MdbError::new(
                ErrorCode::NamespaceNotFound,
                format!("backup '{backup_id}' not found"),
            ));
        }

        let meta = read_metadata_from_dir(&backup_dir)?
            .ok_or_else(|| MdbError::new(
                ErrorCode::DataCorruption,
                "backup has no metadata file",
            ))?;

        let actual_checksum = compute_dir_checksum(&backup_dir)?;
        let checksum_matches = actual_checksum == meta.checksum;

        let (file_count, total_size) = count_files(&backup_dir)?;

        Ok(VerificationResult {
            valid: checksum_matches,
            checksum_matches,
            file_count,
            total_size,
            errors: if checksum_matches {
                Vec::new()
            } else {
                vec![format!(
                    "checksum mismatch: expected {}, got {}",
                    meta.checksum, actual_checksum
                )]
            },
        })
    }

    fn find_latest_full_backup(&self) -> MdbResult<Option<PathBuf>> {
        let backups = self.list_backups()?;
        Ok(backups
            .iter()
            .find(|m| m.backup_type == BackupType::Full)
            .map(|m| PathBuf::from(&self.backup_path).join(&m.id)))
    }
}

fn generate_backup_id() -> String {
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let ts = current_timestamp();
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("backup_{ts}_{n}")
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn io_err(msg: impl std::fmt::Display) -> MdbError {
    MdbError::new(ErrorCode::HostUnreachable, msg.to_string())
}

fn copy_dir_recursive(src: &Path, dst: &Path, total_size: &mut u64) -> MdbResult<()> {
    fs::create_dir_all(dst).map_err(|e| io_err(format!("create dir: {e}")))?;

    let entries = fs::read_dir(src).map_err(|e| io_err(format!("read dir: {e}")))?;

    for entry in entries {
        let entry = entry.map_err(|e| io_err(format!("read entry: {e}")))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path, total_size)?;
        } else {
            fs::copy(&src_path, &dst_path)
                .map_err(|e| io_err(format!("copy file: {e}")))?;
            let meta = fs::metadata(&src_path)
                .map_err(|e| io_err(format!("stat file: {e}")))?;
            *total_size += meta.len();
        }
    }

    Ok(())
}

fn copy_changed_files(
    base: &Path,
    src: &Path,
    dst: &Path,
    total_size: &mut u64,
) -> MdbResult<()> {
    fs::create_dir_all(dst).map_err(|e| io_err(format!("create dir: {e}")))?;

    let entries = fs::read_dir(src).map_err(|e| io_err(format!("read dir: {e}")))?;

    for entry in entries {
        let entry = entry.map_err(|e| io_err(format!("read entry: {e}")))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let base_path = base.join(entry.file_name());

        if src_path.is_dir() {
            if base_path.exists() && base_path.is_dir() {
                copy_changed_files(&base_path, &src_path, &dst_path, total_size)?;
            } else {
                copy_dir_recursive(&src_path, &dst_path, total_size)?;
            }
        } else if !base_path.exists() || !files_equal(&base_path, &src_path)? {
            fs::copy(&src_path, &dst_path)
                .map_err(|e| io_err(format!("copy file: {e}")))?;
            let meta = fs::metadata(&src_path)
                .map_err(|e| io_err(format!("stat file: {e}")))?;
            *total_size += meta.len();
        }
    }

    Ok(())
}

fn files_equal(a: &Path, b: &Path) -> MdbResult<bool> {
    let meta_a = fs::metadata(a).map_err(|e| io_err(format!("stat: {e}")))?;
    let meta_b = fs::metadata(b).map_err(|e| io_err(format!("stat: {e}")))?;

    if meta_a.len() != meta_b.len() {
        return Ok(false);
    }

    let data_a = fs::read(a).map_err(|e| io_err(format!("read: {e}")))?;
    let data_b = fs::read(b).map_err(|e| io_err(format!("read: {e}")))?;

    Ok(data_a == data_b)
}

fn read_lsn_from_db(db_path: &Path) -> Option<u64> {
    let manifest_path = db_path.join("manifest.json");
    if !manifest_path.exists() {
        return None;
    }

    let data = fs::read(&manifest_path).ok()?;
    let s = String::from_utf8(data).ok()?;

    for line in s.lines() {
        if let Some(val) = line.strip_prefix("last_seqno:") {
            return val.trim().parse().ok();
        }
    }

    None
}

fn compute_dir_checksum(dir: &Path) -> MdbResult<String> {
    let mut hasher = crc32c::Crc32c::new();
    collect_file_checksums(dir, &mut hasher)?;
    Ok(format!("{:016x}", hasher.finish()))
}

fn collect_file_checksums(dir: &Path, hasher: &mut crc32c::Crc32c) -> MdbResult<()> {
    let entries = fs::read_dir(dir).map_err(|e| io_err(format!("read dir: {e}")))?;

    let mut file_paths: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .filter(|p| p.file_name().map_or(false, |f| f.to_string_lossy() != "backup.meta"))
        .collect();

    file_paths.sort();

    for path in &file_paths {
        let data = fs::read(path).map_err(|e| io_err(format!("read file: {e}")))?;
        hasher.update(&data);
    }

    let mut sub_dirs: Vec<PathBuf> = fs::read_dir(dir)
        .map_err(|e| io_err(format!("read dir: {e}")))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();

    sub_dirs.sort();

    for sub_dir in &sub_dirs {
        collect_file_checksums(sub_dir, hasher)?;
    }

    Ok(())
}

fn count_files(dir: &Path) -> MdbResult<(usize, u64)> {
    let mut count = 0usize;
    let mut total = 0u64;
    count_files_inner(dir, &mut count, &mut total)?;
    Ok((count, total))
}

fn count_files_inner(dir: &Path, count: &mut usize, total: &mut u64) -> MdbResult<()> {
    let entries = fs::read_dir(dir).map_err(|e| io_err(format!("read dir: {e}")))?;

    for entry in entries {
        let entry = entry.map_err(|e| io_err(format!("read entry: {e}")))?;
        let path = entry.path();

        if path.is_dir() {
            count_files_inner(&path, count, total)?;
        } else {
            *count += 1;
            let meta = fs::metadata(&path)
                .map_err(|e| io_err(format!("stat: {e}")))?;
            *total += meta.len();
        }
    }

    Ok(())
}

fn write_metadata(dir: &Path, meta: &BackupMetadata) -> MdbResult<()> {
    let content = format!(
        "id: {}\ntimestamp: {}\nbackup_type: {:?}\nsize_bytes: {}\nchecksum: {}\nlsn: {}\nduration_ms: {}\n",
        meta.id, meta.timestamp, meta.backup_type, meta.size_bytes, meta.checksum, meta.lsn, meta.duration_ms
    );
    fs::write(dir.join("backup.meta"), content)
        .map_err(|e| io_err(format!("write metadata: {e}")))?;
    Ok(())
}

fn read_metadata_from_dir(dir: &Path) -> MdbResult<Option<BackupMetadata>> {
    let meta_path = dir.join("backup.meta");
    if !meta_path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(&meta_path)
        .map_err(|e| io_err(format!("read metadata: {e}")))?;

    let mut id = String::new();
    let mut timestamp = 0i64;
    let mut backup_type = BackupType::Full;
    let mut size_bytes = 0u64;
    let mut checksum = String::new();
    let mut lsn = 0u64;
    let mut duration_ms = 0u64;

    for line in content.lines() {
        if let Some(v) = line.strip_prefix("id: ") {
            id = v.trim().to_owned();
        } else if let Some(v) = line.strip_prefix("timestamp: ") {
            timestamp = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("backup_type: ") {
            backup_type = match v.trim() {
                "Full" => BackupType::Full,
                "Incremental" => BackupType::Incremental,
                "Differential" => BackupType::Differential,
                _ => BackupType::Full,
            };
        } else if let Some(v) = line.strip_prefix("size_bytes: ") {
            size_bytes = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("checksum: ") {
            checksum = v.trim().to_owned();
        } else if let Some(v) = line.strip_prefix("lsn: ") {
            lsn = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("duration_ms: ") {
            duration_ms = v.trim().parse().unwrap_or(0);
        }
    }

    if id.is_empty() {
        return Ok(None);
    }

    Ok(Some(BackupMetadata {
        id,
        timestamp,
        backup_type,
        size_bytes,
        checksum,
        lsn,
        duration_ms,
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
        let dir = PathBuf::from(format!("test_backup_{suffix}_{n}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn full_backup_creates_metadata() {
        let db_dir = temp_dir("db");
        let backup_dir = temp_dir("bak");

        fs::write(db_dir.join("data.bin"), b"hello world").unwrap();

        let mgr = BackupManager::new(&db_dir, &backup_dir);
        let meta = mgr.full_backup().unwrap();

        assert_eq!(meta.backup_type, BackupType::Full);
        assert!(!meta.id.is_empty());
        assert!(meta.size_bytes > 0);
        assert!(!meta.checksum.is_empty());
    }

    #[test]
    fn list_backups_returns_created() {
        let db_dir = temp_dir("db2");
        let backup_dir = temp_dir("bak2");

        fs::write(db_dir.join("test.bin"), b"data").unwrap();

        let mgr = BackupManager::new(&db_dir, &backup_dir);
        let meta = mgr.full_backup().unwrap();

        let backups = mgr.list_backups().unwrap();
        assert_eq!(backups.len(), 1);
        assert_eq!(backups[0].id, meta.id);
    }

    #[test]
    fn verify_valid_backup() {
        let db_dir = temp_dir("db3");
        let backup_dir = temp_dir("bak3");

        fs::write(db_dir.join("test.bin"), b"verify me").unwrap();

        let mgr = BackupManager::new(&db_dir, &backup_dir);
        let meta = mgr.full_backup().unwrap();

        let result = mgr.verify(&meta.id).unwrap();
        assert!(result.valid, "errors: {:?}", result.errors);
        assert!(result.checksum_matches);
    }

    #[test]
    fn verify_nonexistent_backup_fails() {
        let db_dir = temp_dir("db4");
        let backup_dir = temp_dir("bak4");

        let mgr = BackupManager::new(&db_dir, &backup_dir);
        let result = mgr.verify("nonexistent");
        assert!(result.is_err());
    }
}
