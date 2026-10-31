use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use mdb_backup::{
    BackupManager, BackupType, HotBackup, PitrManager, RestoreManager, WalArchiver,
};

fn temp_dir(suffix: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = PathBuf::from(format!("test_integration_{suffix}_{n}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn create_test_db(dir: &PathBuf) {
    fs::write(dir.join("manifest.json"), "last_seqno: 100\n").unwrap();

    let colcf = dir.join("colcf").join("cf_0");
    fs::create_dir_all(&colcf).unwrap();
    fs::write(colcf.join("000001.sst"), b"sst data content").unwrap();
    fs::write(colcf.join("000002.sst"), b"more sst data").unwrap();

    let wal_dir = dir.join("wal");
    fs::create_dir_all(&wal_dir).unwrap();
    fs::write(wal_dir.join("wal-000000000000.log"), b"wal record 1").unwrap();
}

#[test]
fn full_backup_and_verify() {
    let db_dir = temp_dir("db");
    let backup_dir = temp_dir("backup");

    create_test_db(&db_dir);

    let mgr = BackupManager::new(&db_dir, &backup_dir);
    let meta = mgr.full_backup().unwrap();

    assert_eq!(meta.backup_type, BackupType::Full);
    assert!(meta.size_bytes > 0);
    assert!(!meta.checksum.is_empty());

    let result = mgr.verify(&meta.id).unwrap();
    assert!(result.valid);
    assert!(result.checksum_matches);
    assert!(result.file_count > 0);
}

#[test]
fn list_backups_ordering() {
    let db_dir = temp_dir("db2");
    let backup_dir = temp_dir("backup2");

    create_test_db(&db_dir);

    let mgr = BackupManager::new(&db_dir, &backup_dir);

    let _meta1 = mgr.full_backup().unwrap();
    let _meta2 = mgr.full_backup().unwrap();

    let backups = mgr.list_backups().unwrap();
    assert_eq!(backups.len(), 2);
    assert!(backups[0].timestamp >= backups[1].timestamp);
}

#[test]
fn incremental_backup() {
    let db_dir = temp_dir("db3");
    let backup_dir = temp_dir("backup3");

    create_test_db(&db_dir);

    let mgr = BackupManager::new(&db_dir, &backup_dir);
    let _full = mgr.full_backup().unwrap();

    let colcf = db_dir.join("colcf").join("cf_0");
    fs::write(colcf.join("000003.sst"), b"new sst data").unwrap();

    let incr = mgr.incremental_backup().unwrap();
    assert_eq!(incr.backup_type, BackupType::Incremental);
}

#[test]
fn hot_backup_full_cycle() {
    let db_dir = temp_dir("db4");
    let backup_dir = temp_dir("backup4");

    create_test_db(&db_dir);

    let hb = HotBackup::new(&db_dir, &backup_dir);
    let session = hb.start_backup().unwrap();

    let seqno = session.snapshot_seqno;
    assert!(seqno > 0);

    session.copy_data_files().unwrap();
    session.copy_wal_files().unwrap();
    let meta = session.finalize().unwrap();

    assert!(!meta.id.is_empty());
    assert!(meta.size_bytes > 0);
    assert_eq!(meta.lsn, seqno);
}

#[test]
fn restore_manager_full_restore() {
    let backup_dir = temp_dir("backup5");
    let target_dir = temp_dir("target5");

    fs::write(backup_dir.join("data.bin"), b"restore me").unwrap();
    let sub = backup_dir.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("file.txt"), b"nested").unwrap();

    let mgr = RestoreManager::new(&backup_dir, &target_dir);
    mgr.restore_full("").unwrap();

    assert!(target_dir.join("data.bin").exists());
    assert!(target_dir.join("sub").join("file.txt").exists());

    let data = fs::read(target_dir.join("data.bin")).unwrap();
    assert_eq!(data, b"restore me");
}

#[test]
fn restore_with_wal_files() {
    let backup_dir = temp_dir("backup6");
    let target_dir = temp_dir("target6");
    let wal_src = temp_dir("wal_src");

    fs::write(backup_dir.join("data.bin"), b"data").unwrap();
    fs::write(wal_src.join("wal-000.log"), b"wal data").unwrap();
    fs::write(wal_src.join("wal-001.log"), b"wal data 2").unwrap();

    let wal_files: Vec<String> = vec![
        wal_src.join("wal-000.log").to_string_lossy().into(),
        wal_src.join("wal-001.log").to_string_lossy().into(),
    ];

    let mgr = RestoreManager::new(&backup_dir, &target_dir);
    mgr.restore_with_wal("", &wal_files).unwrap();

    assert!(target_dir.join("wal").join("wal-000.log").exists());
    assert!(target_dir.join("wal").join("wal-001.log").exists());
}

#[test]
fn wal_archiver_full_cycle() {
    let wal_dir = temp_dir("wal7");
    let archive_dir = temp_dir("archive7");

    fs::write(wal_dir.join("wal-000000000000.log"), b"wal content").unwrap();
    fs::write(wal_dir.join("wal-000000000001.log"), b"wal content 2").unwrap();

    let archiver = WalArchiver::new(&archive_dir, Duration::from_secs(60));

    archiver
        .archive_wal(wal_dir.join("wal-000000000000.log").to_str().unwrap())
        .unwrap();
    archiver
        .archive_wal(wal_dir.join("wal-000000000001.log").to_str().unwrap())
        .unwrap();

    let archives = archiver.list_archives().unwrap();
    assert_eq!(archives.len(), 2);

    let removed = archiver.cleanup(0).unwrap();
    assert_eq!(removed, 2);
}

#[test]
fn pitr_list_recovery_points() {
    let backup_dir = temp_dir("pitr_backups");

    for i in 0..3 {
        let backup_subdir = backup_dir.join(format!("backup_{i}"));
        fs::create_dir_all(&backup_subdir).unwrap();
        let meta = format!(
            "id: backup_{i}\ntimestamp: {}\nbackup_type: Full\nsize_bytes: 100\nchecksum: abc\nlsn: {}\nduration_ms: 10\n",
            1000 + i * 1000,
            (i + 1) * 100
        );
        fs::write(backup_subdir.join("backup.meta"), meta).unwrap();
    }

    let mgr = PitrManager::new(&backup_dir, "/tmp/wal");
    let points = mgr.list_recovery_points().unwrap();

    assert_eq!(points.len(), 3);
    assert!(points[0].timestamp < points[2].timestamp);
}

#[test]
fn verify_backup_integrity() {
    let db_dir = temp_dir("db_integrity");
    let backup_dir = temp_dir("backup_integrity");
    let target_dir = temp_dir("target_integrity");

    create_test_db(&db_dir);

    let mgr = BackupManager::new(&db_dir, &backup_dir);
    let meta = mgr.full_backup().unwrap();

    let restore_mgr = RestoreManager::new(&backup_dir, &target_dir);
    let result = restore_mgr.verify_backup(&meta.id).unwrap();

    assert!(result.valid);
    assert!(result.file_count > 0);
}

#[test]
fn backup_nonexistent_db() {
    let db_dir = temp_dir("nonexistent_db");
    let backup_dir = temp_dir("nonexistent_backup");

    let mgr = BackupManager::new(&db_dir, &backup_dir);
    let meta = mgr.full_backup().unwrap();

    assert_eq!(meta.backup_type, BackupType::Full);
}

#[test]
fn incremental_without_full_falls_back() {
    let db_dir = temp_dir("db_incr");
    let backup_dir = temp_dir("backup_incr");

    create_test_db(&db_dir);

    let mgr = BackupManager::new(&db_dir, &backup_dir);
    let meta = mgr.incremental_backup().unwrap();

    assert_eq!(meta.backup_type, BackupType::Incremental);
    assert!(meta.size_bytes > 0);
}
