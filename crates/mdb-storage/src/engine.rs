use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, AtomicU16, Ordering};

use mdb_common::MdbResult;
use mdb_common::config::StorageConfig;

use crate::column_family::{ColumnFamily, recover_cf_wal};
use crate::error::*;
use crate::manifest::Manifest;
use crate::types::*;

#[derive(Debug)]
pub struct Engine {
    dir: PathBuf,
    config: StorageConfig,
    cfs: Mutex<HashMap<String, Arc<ColumnFamily>>>,
    manifest: Mutex<Manifest>,
    next_seqno: AtomicU64,
    next_cf_id: AtomicU16,
    active_snapshots: Mutex<Vec<SequenceNumber>>,
}

impl Engine {
    pub fn open(dir: impl AsRef<Path>, config: StorageConfig) -> MdbResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|e| io_err(format!("create db dir: {e}")))?;

        Manifest::cleanup_tmp_files(&dir)?;

        let manifest = Manifest::open(&dir)?;
        let last_seqno = manifest.data().last_seqno;
        let max_cf_id = manifest.data().cfs.keys().copied().max().unwrap_or(0);

        let sst_base = dir.join("colcf");
        std::fs::create_dir_all(&sst_base).map_err(|e| io_err(format!("create colcf dir: {e}")))?;

        let wal_base = dir.join("wal");
        std::fs::create_dir_all(&wal_base).map_err(|e| io_err(format!("create wal dir: {e}")))?;

        let manifest_mutex = Mutex::new(manifest);
        let mut cfs = HashMap::new();

        let cf_ids: Vec<(ColumnFamilyId, String)> = {
            let m = manifest_mutex.lock().map_err(|e| io_err(format!("lock manifest: {e}")))?;
            m.data().cfs.iter().map(|(&id, cf)| (id, cf.name.clone())).collect()
        };

        for (cf_id, cf_name) in cf_ids {
            let cf_dir = sst_base.join(format!("cf_{}", cf_id));
            std::fs::create_dir_all(&cf_dir).map_err(|e| io_err(format!("create CF dir: {e}")))?;

            match ColumnFamily::open(
                cf_name.clone(),
                cf_id,
                &sst_base,
                config.clone(),
                &manifest_mutex,
                &wal_base,
            ) {
                Ok(cf) => {
                    let _ = recover_cf_wal(&cf, &manifest_mutex);
                    cfs.insert(cf_name, cf);
                }
                Err(e) => {
                    eprintln!("warning: failed to open CF {}: {e}", cf_id);
                }
            }
        }

        Ok(Self {
            dir,
            config,
            cfs: Mutex::new(cfs),
            manifest: manifest_mutex,
            next_seqno: AtomicU64::new(last_seqno + 1),
            next_cf_id: AtomicU16::new(max_cf_id + 1),
            active_snapshots: Mutex::new(Vec::new()),
        })
    }

    pub fn get(&self, cf_name: &str, key: &[u8]) -> MdbResult<Option<Vec<u8>>> {
        let snapshot_seqno = self.current_seqno();
        let cfs = self.cfs.lock().map_err(|e| io_err(format!("lock cfs: {e}")))?;
        let cf = cfs.get(cf_name)
            .ok_or_else(|| not_found_err(format!("column family '{cf_name}' not found")))?;
        cf.get(key, snapshot_seqno)
    }

    pub fn get_with_snapshot(&self, cf_name: &str, key: &[u8], snapshot_seqno: SequenceNumber) -> MdbResult<Option<Vec<u8>>> {
        let cfs = self.cfs.lock().map_err(|e| io_err(format!("lock cfs: {e}")))?;
        let cf = cfs.get(cf_name)
            .ok_or_else(|| not_found_err(format!("column family '{cf_name}' not found")))?;
        cf.get(key, snapshot_seqno)
    }

    pub fn put(&self, cf_name: &str, key: Vec<u8>, value: Vec<u8>) -> MdbResult<()> {
        let seqno = self.next_seqno.fetch_add(1, Ordering::SeqCst);

        let cf = {
            let cfs = self.cfs.lock().map_err(|e| io_err(format!("lock cfs: {e}")))?;
            cfs.get(cf_name)
                .ok_or_else(|| not_found_err(format!("column family '{cf_name}' not found")))?
                .clone()
        };

        cf.put(key, value, seqno)?;

        {
            let mut manifest = self.manifest.lock().map_err(|e| io_err(format!("lock manifest: {e}")))?;
            manifest.set_last_seqno(seqno);
        }

        if cf.maybe_freeze()? {
            let oldest = self.oldest_active_seqno();
            let _ = cf.maybe_compact(oldest);
        }

        Ok(())
    }

    pub fn delete(&self, cf_name: &str, key: &[u8]) -> MdbResult<()> {
        let seqno = self.next_seqno.fetch_add(1, Ordering::SeqCst);

        let cf = {
            let cfs = self.cfs.lock().map_err(|e| io_err(format!("lock cfs: {e}")))?;
            cfs.get(cf_name)
                .ok_or_else(|| not_found_err(format!("column family '{cf_name}' not found")))?
                .clone()
        };

        cf.delete(key.to_vec(), seqno)?;

        {
            let mut manifest = self.manifest.lock().map_err(|e| io_err(format!("lock manifest: {e}")))?;
            manifest.set_last_seqno(seqno);
        }

        if cf.maybe_freeze()? {
            let oldest = self.oldest_active_seqno();
            let _ = cf.maybe_compact(oldest);
        }

        Ok(())
    }

    pub fn range_scan(
        &self,
        cf_name: &str,
        start: &[u8],
        end: &[u8],
    ) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let snapshot_seqno = self.current_seqno();
        let cfs = self.cfs.lock().map_err(|e| io_err(format!("lock cfs: {e}")))?;
        let cf = cfs.get(cf_name)
            .ok_or_else(|| not_found_err(format!("column family '{cf_name}' not found")))?;
        cf.range_scan(start, end, snapshot_seqno)
    }

    /// 无上界全量扫描列族：返回 `[start, +inf)` 区间内的全部键值对。
    ///
    /// 键长度无上界，用有限 `\xff...` 哨兵作上界会漏键，
    /// 因此全量扫描必须使用本方法。
    pub fn scan_all(&self, cf_name: &str, start: &[u8]) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let snapshot_seqno = self.current_seqno();
        let cfs = self.cfs.lock().map_err(|e| io_err(format!("lock cfs: {e}")))?;
        let cf = cfs.get(cf_name)
            .ok_or_else(|| not_found_err(format!("column family '{cf_name}' not found")))?;
        cf.scan_all(start, snapshot_seqno)
    }

    pub fn range_scan_with_snapshot(
        &self,
        cf_name: &str,
        start: &[u8],
        end: &[u8],
        snapshot_seqno: SequenceNumber,
    ) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let cfs = self.cfs.lock().map_err(|e| io_err(format!("lock cfs: {e}")))?;
        let cf = cfs.get(cf_name)
            .ok_or_else(|| not_found_err(format!("column family '{cf_name}' not found")))?;
        cf.range_scan(start, end, snapshot_seqno)
    }

    pub fn create_cf(&self, name: &str) -> MdbResult<()> {
        let mut cfs = self.cfs.lock().map_err(|e| io_err(format!("lock cfs: {e}")))?;
        if cfs.contains_key(name) {
            return Err(invalid_err(format!("column family '{name}' already exists")));
        }

        let cf_id = self.next_cf_id.fetch_add(1, Ordering::SeqCst);
        let sst_base = self.dir.join("colcf");
        let wal_base = self.dir.join("wal");

        {
            let mut manifest = self.manifest.lock().map_err(|e| io_err(format!("lock manifest: {e}")))?;
            manifest.data_mut().cfs.insert(cf_id, crate::manifest::CfManifest {
                cf_id,
                name: name.to_string(),
                files: Vec::new(),
            });
            manifest.flush()?;
        }

        let cf = ColumnFamily::open(
            name.to_string(),
            cf_id,
            &sst_base,
            self.config.clone(),
            &self.manifest,
            &wal_base,
        )?;

        cfs.insert(name.to_string(), cf);
        Ok(())
    }

    pub fn drop_cf(&self, name: &str) -> MdbResult<()> {
        let mut cfs = self.cfs.lock().map_err(|e| io_err(format!("lock cfs: {e}")))?;
        let cf = cfs.remove(name)
            .ok_or_else(|| not_found_err(format!("column family '{name}' not found")))?;

        let cf_id = cf.id();
        let cf_dir = cf.dir().to_path_buf();

        {
            let mut manifest = self.manifest.lock().map_err(|e| io_err(format!("lock manifest: {e}")))?;
            manifest.data_mut().cfs.remove(&cf_id);
            manifest.flush()?;
        }

        let _ = std::fs::remove_dir_all(&cf_dir);
        Ok(())
    }

    pub fn create_snapshot(&self) -> MdbResult<SequenceNumber> {
        let seqno = self.next_seqno.load(Ordering::SeqCst).saturating_sub(1);
        let mut snapshots = self.active_snapshots.lock().map_err(|e| io_err(format!("lock snapshots: {e}")))?;
        snapshots.push(seqno);
        Ok(seqno)
    }

    pub fn drop_snapshot(&self, seqno: SequenceNumber) -> MdbResult<()> {
        let mut snapshots = self.active_snapshots.lock().map_err(|e| io_err(format!("lock snapshots: {e}")))?;
        snapshots.retain(|&s| s != seqno);
        Ok(())
    }

    pub fn flush_manifest(&self) -> MdbResult<()> {
        let mut manifest = self.manifest.lock().map_err(|e| io_err(format!("lock manifest: {e}")))?;
        manifest.flush()
    }

    fn current_seqno(&self) -> SequenceNumber {
        self.next_seqno.load(Ordering::SeqCst)
    }

    fn oldest_active_seqno(&self) -> SequenceNumber {
        let snapshots = self.active_snapshots.lock().unwrap_or_else(|e| e.into_inner());
        snapshots.iter().copied().min().unwrap_or(u64::MAX)
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.flush_manifest();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = PathBuf::from(format!("test_engine_{}_{:04}", std::process::id(), n));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn engine_open_and_put_get() {
        let dir = temp_dir();
        let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
        engine.create_cf("test").unwrap();

        engine.put("test", b"hello".to_vec(), b"world".to_vec()).unwrap();
        let result = engine.get("test", b"hello").unwrap();
        assert_eq!(result, Some(b"world".to_vec()));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_delete() {
        let dir = temp_dir();
        let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
        engine.create_cf("test").unwrap();

        engine.put("test", b"key".to_vec(), b"val".to_vec()).unwrap();
        engine.delete("test", b"key").unwrap();
        let result = engine.get("test", b"key").unwrap();
        assert_eq!(result, None);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_snapshot_isolation() {
        let dir = temp_dir();
        let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
        engine.create_cf("test").unwrap();

        engine.put("test", b"key".to_vec(), b"v1".to_vec()).unwrap();
        let snap = engine.create_snapshot().unwrap();

        engine.put("test", b"key".to_vec(), b"v2".to_vec()).unwrap();

        assert_eq!(engine.get_with_snapshot("test", b"key", snap).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get("test", b"key").unwrap(), Some(b"v2".to_vec()));

        engine.drop_snapshot(snap).unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_range_scan() {
        let dir = temp_dir();
        let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
        engine.create_cf("test").unwrap();

        for i in 0..10u64 {
            let key = format!("key_{:02}", i);
            engine.put("test", key.into_bytes(), b"v".to_vec()).unwrap();
        }

        let results = engine.range_scan("test", b"key_03", b"key_07").unwrap();
        assert!(results.len() >= 4);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_multiple_cfs() {
        let dir = temp_dir();
        let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
        engine.create_cf("col_a").unwrap();
        engine.create_cf("col_b").unwrap();

        engine.put("col_a", b"k".to_vec(), b"va".to_vec()).unwrap();
        engine.put("col_b", b"k".to_vec(), b"vb".to_vec()).unwrap();

        assert_eq!(engine.get("col_a", b"k").unwrap(), Some(b"va".to_vec()));
        assert_eq!(engine.get("col_b", b"k").unwrap(), Some(b"vb".to_vec()));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_persistence() {
        let dir = temp_dir();
        {
            let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
            engine.create_cf("test").unwrap();
            engine.put("test", b"persistent".to_vec(), b"data".to_vec()).unwrap();
        }

        {
            let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
            let result = engine.get("test", b"persistent").unwrap();
            assert_eq!(result, Some(b"data".to_vec()));
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_many_writes() {
        let dir = temp_dir();
        let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
        engine.create_cf("test").unwrap();

        for i in 0..500u64 {
            let key = format!("key_{:06}", i);
            let val = format!("val_{:06}", i);
            engine.put("test", key.into_bytes(), val.into_bytes()).unwrap();
        }

        for i in 0..500u64 {
            let key = format!("key_{:06}", i);
            let val = format!("val_{:06}", i);
            let result = engine.get("test", key.as_bytes()).unwrap();
            assert_eq!(result, Some(val.into_bytes()), "mismatch at {i}");
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_nul_keys_survive_reopen() {
        let dir = temp_dir();
        let key = |i: u32| {
            let mut k = vec![0x07u8];
            k.extend_from_slice(&i.to_be_bytes());
            k
        };
        {
            let mut cfg = StorageConfig::default();
            // 1MB memtable，强制多次 flush 到 SST。
            cfg.memtable_size_mb = 1;
            let engine = Engine::open(&dir, cfg).unwrap();
            engine.create_cf("test").unwrap();
            for i in 0..500u32 {
                engine
                    .put("test", key(i), format!("v{i}").into_bytes())
                    .unwrap();
            }
        }
        {
            let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
            for i in 0..500u32 {
                assert_eq!(
                    engine.get("test", &key(i)).unwrap(),
                    Some(format!("v{i}").into_bytes()),
                    "mismatch at {i}"
                );
            }
            let all = engine.scan_all("test", b"").unwrap();
            assert_eq!(all.len(), 500, "scan_all after reopen");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_wal_recovery_many_nul_keys() {
        // 默认 256MB memtable：不会flush，重启完全依赖 WAL 重放。
        let dir = temp_dir();
        let key = |i: u32| {
            let mut k = vec![0x07u8];
            k.extend_from_slice(&i.to_be_bytes());
            k
        };
        {
            let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
            engine.create_cf("test").unwrap();
            for i in 0..500u32 {
                engine
                    .put("test", key(i), format!("v{i}").into_bytes())
                    .unwrap();
            }
        }
        {
            let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
            let all = engine.scan_all("test", b"").unwrap();
            assert_eq!(all.len(), 500, "WAL recovery lost records");
            for i in 0..500u32 {
                assert_eq!(
                    engine.get("test", &key(i)).unwrap(),
                    Some(format!("v{i}").into_bytes()),
                    "mismatch at {i}"
                );
            }
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_nonexistent_cf() {
        let dir = temp_dir();
        let engine = Engine::open(&dir, StorageConfig::default()).unwrap();
        let result = engine.get("nope", b"key");
        assert!(result.is_err());

        let _ = fs::remove_dir_all(&dir);
    }
}
