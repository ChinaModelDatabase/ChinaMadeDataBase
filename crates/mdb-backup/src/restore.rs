use std::fs;
use std::path::{Path, PathBuf};

use mdb_common::{ErrorCode, MdbError, MdbResult};
use mdb_crypto::crc32c;

use crate::backup::VerificationResult;

#[derive(Debug)]
pub struct RestoreManager {
    backup_path: String,
    target_path: String,
}

impl RestoreManager {
    pub fn new(backup_path: impl AsRef<Path>, target_path: impl AsRef<Path>) -> Self {
        Self {
            backup_path: backup_path.as_ref().to_string_lossy().into_owned(),
            target_path: target_path.as_ref().to_string_lossy().into_owned(),
        }
    }

    pub fn restore_full(&self, backup_id: &str) -> MdbResult<()> {
        let backup_dir = Path::new(&self.backup_path).join(backup_id);
        if !backup_dir.exists() {
            return Err(MdbError::new(
                ErrorCode::NamespaceNotFound,
                format!("backup '{backup_id}' not found"),
            ));
        }

        let target = Path::new(&self.target_path);
        if target.exists() {
            fs::remove_dir_all(target)
                .map_err(|e| io_err(format!("clean target dir: {e}")))?;
        }

        copy_dir_recursive(&backup_dir, target)
    }

    pub fn restore_with_wal(
        &self,
        backup_id: &str,
        wal_files: &[String],
    ) -> MdbResult<()> {
        self.restore_full(backup_id)?;

        let target = Path::new(&self.target_path);
        let wal_dir = target.join("wal");
        fs::create_dir_all(&wal_dir)
            .map_err(|e| io_err(format!("create wal dir: {e}")))?;

        for wal_file in wal_files {
            let src = Path::new(wal_file);
            if src.exists() {
                let filename = src
                    .file_name()
                    .ok_or_else(|| io_err("invalid WAL filename"))?;
                let dst = wal_dir.join(filename);
                fs::copy(src, &dst)
                    .map_err(|e| io_err(format!("copy WAL file: {e}")))?;
            }
        }

        Ok(())
    }

    pub fn verify_backup(&self, backup_id: &str) -> MdbResult<VerificationResult> {
        let backup_dir = Path::new(&self.backup_path).join(backup_id);
        if !backup_dir.exists() {
            return Err(MdbError::new(
                ErrorCode::NamespaceNotFound,
                format!("backup '{backup_id}' not found"),
            ));
        }

        let meta_path = backup_dir.join("backup.meta");
        if !meta_path.exists() {
            return Err(MdbError::new(
                ErrorCode::DataCorruption,
                "backup has no metadata file",
            ));
        }

        let meta_content = fs::read_to_string(&meta_path)
            .map_err(|e| io_err(format!("read metadata: {e}")))?;

        let expected_checksum = extract_checksum(&meta_content);

        let actual_checksum = compute_dir_checksum(&backup_dir)?;
        let checksum_matches = Some(&actual_checksum) == expected_checksum.as_ref();

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
                    "checksum mismatch: expected {:?}, got {}",
                    expected_checksum, actual_checksum
                )]
            },
        })
    }
}

fn io_err(msg: impl std::fmt::Display) -> MdbError {
    MdbError::new(ErrorCode::HostUnreachable, msg.to_string())
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> MdbResult<()> {
    fs::create_dir_all(dst)
        .map_err(|e| io_err(format!("create dir: {e}")))?;

    let entries = fs::read_dir(src)
        .map_err(|e| io_err(format!("read dir: {e}")))?;

    for entry in entries {
        let entry = entry.map_err(|e| io_err(format!("read entry: {e}")))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)
                .map_err(|e| io_err(format!("copy file: {e}")))?;
        }
    }

    Ok(())
}

fn extract_checksum(meta_content: &str) -> Option<String> {
    for line in meta_content.lines() {
        if let Some(v) = line.strip_prefix("checksum: ") {
            return Some(v.trim().to_owned());
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
    let entries = fs::read_dir(dir)
        .map_err(|e| io_err(format!("read dir: {e}")))?;

    let mut file_paths: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .filter(|p| p.file_name().map_or(false, |f| f.to_string_lossy() != "backup.meta"))
        .collect();

    file_paths.sort();

    for path in &file_paths {
        let data = fs::read(path)
            .map_err(|e| io_err(format!("read file: {e}")))?;
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
    let entries = fs::read_dir(dir)
        .map_err(|e| io_err(format!("read dir: {e}")))?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn temp_dir(suffix: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = PathBuf::from(format!("test_restore_{suffix}_{n}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn restore_full_copies_files() {
        let backup_dir = temp_dir("backup");
        let target_dir = temp_dir("target");

        fs::write(backup_dir.join("data.bin"), b"test data").unwrap();
        let sub = backup_dir.join("subdir");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("nested.bin"), b"nested data").unwrap();

        let mgr = RestoreManager::new(&backup_dir, &target_dir);
        mgr.restore_full("").unwrap();

        assert!(target_dir.join("data.bin").exists());
        assert!(target_dir.join("subdir").join("nested.bin").exists());

        let data = fs::read(target_dir.join("data.bin")).unwrap();
        assert_eq!(data, b"test data");
    }

    #[test]
    fn restore_nonexistent_backup_fails() {
        let backup_dir = temp_dir("backup2");
        let target_dir = temp_dir("target2");

        let mgr = RestoreManager::new(&backup_dir, &target_dir);
        let result = mgr.restore_full("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn verify_valid_backup() {
        let backup_dir = temp_dir("backup3");
        let target_dir = temp_dir("target3");

        let backup_subdir = backup_dir.join("test");
        fs::create_dir_all(&backup_subdir).unwrap();
        fs::write(backup_subdir.join("test.bin"), b"verify").unwrap();
        let checksum = compute_dir_checksum(&backup_subdir).unwrap();
        let meta = format!(
            "id: test\ntimestamp: 1234\nbackup_type: Full\nsize_bytes: 6\nchecksum: {checksum}\nlsn: 0\nduration_ms: 10\n"
        );
        fs::write(backup_subdir.join("backup.meta"), meta).unwrap();

        let mgr = RestoreManager::new(&backup_dir, &target_dir);
        let result = mgr.verify_backup("test").unwrap();

        assert!(result.valid, "errors: {:?}", result.errors);
        assert!(result.checksum_matches);
    }
}
