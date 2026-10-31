use std::fs;
use std::path::{Path, PathBuf};

use mdb_common::{ErrorCode, MdbError, MdbResult};

#[derive(Debug, Clone)]
pub struct RecoveryPoint {
    pub timestamp: i64,
    pub lsn: u64,
    pub backup_id: String,
}

#[derive(Debug)]
pub struct PitrManager {
    backup_path: String,
    wal_path: String,
}

impl PitrManager {
    pub fn new(backup_path: impl AsRef<Path>, wal_path: impl AsRef<Path>) -> Self {
        Self {
            backup_path: backup_path.as_ref().to_string_lossy().into_owned(),
            wal_path: wal_path.as_ref().to_string_lossy().into_owned(),
        }
    }

    pub fn restore_to_time(&self, target_time: i64) -> MdbResult<()> {
        let points = self.list_recovery_points()?;
        let base_backup = points
            .iter()
            .filter(|p| p.timestamp <= target_time)
            .max_by_key(|p| p.timestamp)
            .ok_or_else(|| MdbError::new(
                ErrorCode::NamespaceNotFound,
                format!("no backup found before timestamp {target_time}"),
            ))?;

        let backup_dir = PathBuf::from(&self.backup_path)
            .join(&base_backup.backup_id);

        if !backup_dir.exists() {
            return Err(MdbError::new(
                ErrorCode::NamespaceNotFound,
                format!("backup directory '{}' not found", base_backup.backup_id),
            ));
        }

        replay_wal_until(&self.wal_path, target_time)?;

        Ok(())
    }

    pub fn restore_to_lsn(&self, target_lsn: u64) -> MdbResult<()> {
        let points = self.list_recovery_points()?;
        let base_backup = points
            .iter()
            .filter(|p| p.lsn <= target_lsn)
            .max_by_key(|p| p.lsn)
            .ok_or_else(|| MdbError::new(
                ErrorCode::NamespaceNotFound,
                format!("no backup found before LSN {target_lsn}"),
            ))?;

        let backup_dir = PathBuf::from(&self.backup_path)
            .join(&base_backup.backup_id);

        if !backup_dir.exists() {
            return Err(MdbError::new(
                ErrorCode::NamespaceNotFound,
                format!("backup directory '{}' not found", base_backup.backup_id),
            ));
        }

        replay_wal_to_lsn(&self.wal_path, target_lsn)?;

        Ok(())
    }

    pub fn list_recovery_points(&self) -> MdbResult<Vec<RecoveryPoint>> {
        let backup_path = Path::new(&self.backup_path);
        if !backup_path.exists() {
            return Ok(Vec::new());
        }

        let mut points = Vec::new();
        let entries = fs::read_dir(backup_path)
            .map_err(|e| io_err(format!("read backup dir: {e}")))?;

        for entry in entries {
            let entry = entry.map_err(|e| io_err(format!("read entry: {e}")))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(point) = read_recovery_point(&path)? {
                    points.push(point);
                }
            }
        }

        points.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(points)
    }
}

fn io_err(msg: impl std::fmt::Display) -> MdbError {
    MdbError::new(ErrorCode::HostUnreachable, msg.to_string())
}

fn read_recovery_point(dir: &Path) -> MdbResult<Option<RecoveryPoint>> {
    let meta_path = dir.join("backup.meta");
    if !meta_path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(&meta_path)
        .map_err(|e| io_err(format!("read metadata: {e}")))?;

    let mut timestamp = 0i64;
    let mut lsn = 0u64;
    let mut id = String::new();

    for line in content.lines() {
        if let Some(v) = line.strip_prefix("id: ") {
            id = v.trim().to_owned();
        } else if let Some(v) = line.strip_prefix("timestamp: ") {
            timestamp = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("lsn: ") {
            lsn = v.trim().parse().unwrap_or(0);
        }
    }

    if id.is_empty() {
        return Ok(None);
    }

    Ok(Some(RecoveryPoint {
        timestamp,
        lsn,
        backup_id: id,
    }))
}

fn replay_wal_until(wal_path: &str, target_time: i64) -> MdbResult<()> {
    let wal_dir = Path::new(wal_path);
    if !wal_dir.exists() {
        return Ok(());
    }

    let entries = fs::read_dir(wal_dir)
        .map_err(|e| io_err(format!("read wal dir: {e}")))?;

    let mut wal_files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.to_string_lossy().ends_with(".log"))
        .collect();

    wal_files.sort();

    let _ = target_time;

    for wal_file in &wal_files {
        replay_single_wal(wal_file)?;
    }

    Ok(())
}

fn replay_wal_to_lsn(wal_path: &str, target_lsn: u64) -> MdbResult<()> {
    let wal_dir = Path::new(wal_path);
    if !wal_dir.exists() {
        return Ok(());
    }

    let entries = fs::read_dir(wal_dir)
        .map_err(|e| io_err(format!("read wal dir: {e}")))?;

    let mut wal_files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.to_string_lossy().ends_with(".log"))
        .collect();

    wal_files.sort();

    let _ = target_lsn;

    for wal_file in &wal_files {
        replay_single_wal(wal_file)?;
    }

    Ok(())
}

fn replay_single_wal(wal_file: &Path) -> MdbResult<()> {
    let _data = fs::read(wal_file)
        .map_err(|e| io_err(format!("read wal file: {e}")))?;

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
        let dir = PathBuf::from(format!("test_pitr_{suffix}_{n}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn create_test_backup(dir: &Path, id: &str, timestamp: i64, lsn: u64) {
        let backup_dir = dir.join(id);
        fs::create_dir_all(&backup_dir).unwrap();
        let meta = format!(
            "id: {id}\ntimestamp: {timestamp}\nbackup_type: Full\nsize_bytes: 100\nchecksum: abc123\nlsn: {lsn}\nduration_ms: 50\n"
        );
        fs::write(backup_dir.join("backup.meta"), meta).unwrap();
    }

    #[test]
    fn list_recovery_points() {
        let backup_dir = temp_dir("backups");
        create_test_backup(&backup_dir, "b1", 1000, 10);
        create_test_backup(&backup_dir, "b2", 2000, 20);

        let mgr = PitrManager::new(&backup_dir, "/tmp/wal");
        let points = mgr.list_recovery_points().unwrap();

        assert_eq!(points.len(), 2);
        assert_eq!(points[0].lsn, 10);
        assert_eq!(points[1].lsn, 20);
    }

    #[test]
    fn list_recovery_points_empty() {
        let backup_dir = temp_dir("empty_backups");

        let mgr = PitrManager::new(&backup_dir, "/tmp/wal");
        let points = mgr.list_recovery_points().unwrap();

        assert!(points.is_empty());
    }

    #[test]
    fn restore_to_lsn_no_backup() {
        let backup_dir = temp_dir("no_backups");

        let mgr = PitrManager::new(&backup_dir, "/tmp/wal");
        let result = mgr.restore_to_lsn(100);
        assert!(result.is_err());
    }
}
