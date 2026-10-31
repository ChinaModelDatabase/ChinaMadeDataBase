use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use mdb_common::MdbResult;
use mdb_common::config::StorageConfig;

use crate::error::*;
use crate::manifest::Manifest;
use crate::memtable::{MemTable, FrozenMemTable};
use crate::sstable::{SSTableMeta, SSTableReader, SSTableWriter};
use crate::types::*;
use crate::wal::WalManager;
use crate::compaction;

#[derive(Debug)]
pub struct ColumnFamily {
    name: String,
    id: ColumnFamilyId,
    dir: PathBuf,
    wal_dir: PathBuf,
    memtable: Mutex<MemTable>,
    frozen: Mutex<Vec<FrozenMemTable>>,
    l0: Mutex<Vec<SSTableMeta>>,
    levels: Mutex<Vec<Vec<SSTableMeta>>>,
    wal: WalManager,
    config: StorageConfig,
    next_sst_number: Mutex<u64>,
}

impl ColumnFamily {
    pub fn open(
        name: String,
        id: ColumnFamilyId,
        base_dir: &Path,
        config: StorageConfig,
        manifest: &Mutex<Manifest>,
        wal_dir: &Path,
    ) -> MdbResult<Arc<Self>> {
        let encoded = encode_collection_name(&name);
        let dir = base_dir.join(&encoded);
        fs::create_dir_all(&dir).map_err(|e| io_err(format!("create CF dir: {e}")))?;

        let wal_subdir = wal_dir.join(&encoded);
        fs::create_dir_all(&wal_subdir).map_err(|e| io_err(format!("create WAL dir: {e}")))?;

        let memtable = MemTable::new(config.memtable_size_mb as usize * 1024 * 1024);
        let wal = WalManager::open(&wal_subdir, config.wal_group_commit_us)?;

        let manifest_guard = manifest.lock().map_err(|e| io_err(format!("lock manifest: {e}")))?;
        let mut l0_files = Vec::new();
        let mut levels: Vec<Vec<SSTableMeta>> = vec![Vec::new(); 8];

        if let Some(cf_meta) = manifest_guard.get_cf(id) {
            for file in &cf_meta.files {
                let mut file_meta = file.clone();
                file_meta.path = dir.join(format!("{:012}.sst", file.file_number));
                if file.level == 0 {
                    l0_files.push(file_meta);
                } else if file.level < levels.len() {
                    levels[file.level].push(file_meta);
                }
            }
        }
        drop(manifest_guard);

        let max_num = l0_files.iter()
            .chain(levels.iter().flat_map(|l| l.iter()))
            .map(|f| f.file_number)
            .max()
            .unwrap_or(0);

        Ok(Arc::new(Self {
            name,
            id,
            dir,
            wal_dir: wal_subdir,
            memtable: Mutex::new(memtable),
            frozen: Mutex::new(Vec::new()),
            l0: Mutex::new(l0_files),
            levels: Mutex::new(levels),
            wal,
            config,
            next_sst_number: Mutex::new(max_num + 1),
        }))
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> ColumnFamilyId {
        self.id
    }

    pub fn get(&self, user_key: &[u8], snapshot_seqno: SequenceNumber) -> MdbResult<Option<Vec<u8>>> {
        {
            let mt = self.memtable.lock().map_err(|e| io_err(format!("lock memtable: {e}")))?;
            if let Some(result) = mt.get(user_key, snapshot_seqno) {
                return Ok(result);
            }
        }

        {
            let frozen = self.frozen.lock().map_err(|e| io_err(format!("lock frozen: {e}")))?;
            for fm in frozen.iter().rev() {
                let lo = make_internal_key(user_key, snapshot_seqno);
                let hi = make_internal_key(user_key, 0);
                for (ik, val) in fm.map.range(lo..=hi) {
                    if let Some(sn) = parse_internal_key(ik).map(|(_, sn)| sn) {
                        if ik_user_key_eq(ik, user_key) && sn <= snapshot_seqno {
                            if val.is_empty() {
                                return Ok(None);
                            }
                            return Ok(Some(val.clone()));
                        }
                    }
                    break;
                }
            }
        }

        {
            let l0 = self.l0.lock().map_err(|e| io_err(format!("lock l0: {e}")))?;
            for meta in l0.iter().rev() {
                if !meta.path.exists() {
                    continue;
                }
                let mut reader = SSTableReader::open(&meta.path, meta.clone())?;
                if !reader.might_contain(user_key) {
                    continue;
                }
                match reader.get(user_key, snapshot_seqno)? {
                    Some(result) => return Ok(result),
                    None => continue,
                }
            }
        }

        {
            let levels = self.levels.lock().map_err(|e| io_err(format!("lock levels: {e}")))?;
            for level in levels.iter().skip(1) {
                for meta in level {
                    if !meta.path.exists() {
                        continue;
                    }
                    if cmp_ik_user_key(&meta.first_key, user_key) == std::cmp::Ordering::Greater
                        || cmp_ik_user_key(&meta.last_key, user_key) == std::cmp::Ordering::Less
                    {
                        continue;
                    }
                    let mut reader = SSTableReader::open(&meta.path, meta.clone())?;
                    if !reader.might_contain(user_key) {
                        continue;
                    }
                    match reader.get(user_key, snapshot_seqno)? {
                        Some(result) => return Ok(result),
                        None => continue,
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn put(&self, user_key: Vec<u8>, value: Vec<u8>, seqno: SequenceNumber) -> MdbResult<()> {
        self.wal.append(self.id, OP_PUT, &user_key, &value, seqno)?;

        let mut mt = self.memtable.lock().map_err(|e| io_err(format!("lock memtable: {e}")))?;
        mt.put(user_key, seqno, value);
        Ok(())
    }

    pub fn delete(&self, user_key: Vec<u8>, seqno: SequenceNumber) -> MdbResult<()> {
        self.wal.append(self.id, OP_DELETE, &user_key, b"", seqno)?;

        let mut mt = self.memtable.lock().map_err(|e| io_err(format!("lock memtable: {e}")))?;
        mt.delete(user_key, seqno);
        Ok(())
    }

    pub fn range_scan(
        &self,
        start: &[u8],
        end: &[u8],
        snapshot_seqno: SequenceNumber,
    ) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        {
            let levels = self.levels.lock().map_err(|e| io_err(format!("lock levels: {e}")))?;
            for level in levels.iter().skip(1) {
                for meta in level {
                    if !meta.path.exists() {
                        continue;
                    }
                    if !meta_overlaps_range(meta, start, end) {
                        continue;
                    }
                    let mut reader = SSTableReader::open(&meta.path, meta.clone())?;
                    for (uk, val) in reader.range_scan(start, end, snapshot_seqno)? {
                        results.insert(uk, val);
                    }
                }
            }
        }

        {
            let l0 = self.l0.lock().map_err(|e| io_err(format!("lock l0: {e}")))?;
            for meta in l0.iter() {
                if !meta.path.exists() {
                    continue;
                }
                let mut reader = SSTableReader::open(&meta.path, meta.clone())?;
                for (uk, val) in reader.range_scan(start, end, snapshot_seqno)? {
                    results.insert(uk, val);
                }
            }
        }

        {
            let frozen = self.frozen.lock().map_err(|e| io_err(format!("lock frozen: {e}")))?;
            for fm in frozen.iter() {
                let lo = make_internal_key(start, snapshot_seqno);
                let hi = make_internal_key(end, 0);
                for (ik, val) in fm.map.range::<Vec<u8>, _>(lo..hi) {
                    if let Some((uk, sn)) = parse_internal_key_owned(ik) {
                        if sn <= snapshot_seqno {
                            if val.is_empty() {
                                results.remove(&uk);
                            } else {
                                results.insert(uk, val.clone());
                            }
                        }
                    }
                }
            }
        }

        {
            let mt = self.memtable.lock().map_err(|e| io_err(format!("lock memtable: {e}")))?;
            let mt_results = mt.range_scan(start, end, snapshot_seqno);
            for (uk, val) in mt_results {
                results.insert(uk, val);
            }
        }

        Ok(results.into_iter().collect())
    }

    /// 无上界全量扫描：返回 `[start, +inf)` 区间内的全部键值对。
    pub fn scan_all(&self, start: &[u8], snapshot_seqno: SequenceNumber) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        {
            let levels = self.levels.lock().map_err(|e| io_err(format!("lock levels: {e}")))?;
            for level in levels.iter().skip(1) {
                for meta in level {
                    if !meta.path.exists() {
                        continue;
                    }
                    let mut reader = SSTableReader::open(&meta.path, meta.clone())?;
                    for (uk, val) in reader.scan_all(start, snapshot_seqno)? {
                        results.insert(uk, val);
                    }
                }
            }
        }

        {
            let l0 = self.l0.lock().map_err(|e| io_err(format!("lock l0: {e}")))?;
            for meta in l0.iter() {
                if !meta.path.exists() {
                    continue;
                }
                let mut reader = SSTableReader::open(&meta.path, meta.clone())?;
                for (uk, val) in reader.scan_all(start, snapshot_seqno)? {
                    results.insert(uk, val);
                }
            }
        }

        {
            let frozen = self.frozen.lock().map_err(|e| io_err(format!("lock frozen: {e}")))?;
            let lo = make_internal_key(start, snapshot_seqno);
            for fm in frozen.iter() {
                for (ik, val) in fm.map.range::<Vec<u8>, _>(lo.clone()..) {
                    if let Some((uk, sn)) = parse_internal_key_owned(ik) {
                        if sn <= snapshot_seqno {
                            if val.is_empty() {
                                results.remove(&uk);
                            } else {
                                results.insert(uk, val.clone());
                            }
                        }
                    }
                }
            }
        }

        {
            let mt = self.memtable.lock().map_err(|e| io_err(format!("lock memtable: {e}")))?;
            for (uk, val) in mt.scan_all(start, snapshot_seqno) {
                results.insert(uk, val);
            }
        }

        Ok(results.into_iter().collect())
    }

    pub fn maybe_freeze(&self) -> MdbResult<bool> {
        let should_freeze = {
            let mt = self.memtable.lock().map_err(|e| io_err(format!("lock memtable: {e}")))?;
            mt.is_full() && !mt.is_empty()
        };

        if !should_freeze {
            return Ok(false);
        }

        let frozen_mt = {
            let mut mt = self.memtable.lock().map_err(|e| io_err(format!("lock memtable: {e}")))?;
            std::mem::replace(&mut *mt, MemTable::new(self.config.memtable_size_mb as usize * 1024 * 1024))
        };

        let frozen = frozen_mt.freeze();
        let frozen_max = frozen.max_seqno;

        {
            let mut frozen_list = self.frozen.lock().map_err(|e| io_err(format!("lock frozen: {e}")))?;
            frozen_list.push(frozen);
        }

        self.flush_frozen_to_l0(frozen_max)?;

        Ok(true)
    }

    fn flush_frozen_to_l0(&self, _max_seqno: SequenceNumber) -> MdbResult<()> {
        let frozen = {
            let mut frozen_list = self.frozen.lock().map_err(|e| io_err(format!("lock frozen: {e}")))?;
            if frozen_list.is_empty() {
                return Ok(());
            }
            frozen_list.remove(0)
        };

        if frozen.is_empty() {
            return Ok(());
        }

        let file_num = {
            let mut n = self.next_sst_number.lock().map_err(|e| io_err(format!("lock file num: {e}")))?;
            let num = *n;
            *n += 1;
            num
        };

        let path = self.dir.join(format!("{:012}.sst", file_num));
        let estimated = frozen.len();
        let mut writer = SSTableWriter::create(
            &path,
            file_num,
            self.config.block_size_kb,
            self.config.bloom_bits_per_key,
            estimated,
        )?;

        for (user_key, seqno, val) in frozen.iter() {
            let ik = make_internal_key(user_key, seqno);
            writer.add(&ik, val)?;
        }

        let meta = writer.finish()?;

        {
            let mut l0 = self.l0.lock().map_err(|e| io_err(format!("lock l0: {e}")))?;
            l0.push(meta.clone());
        }

        Ok(())
    }

    pub fn maybe_compact(&self, oldest_active_seqno: SequenceNumber) -> MdbResult<()> {
        let should_l0 = {
            let l0 = self.l0.lock().map_err(|e| io_err(format!("lock l0: {e}")))?;
            compaction::should_compact_l0(l0.len(), self.config.l0_compaction_trigger)
        };

        if should_l0 {
            self.compact_l0_to_l1(oldest_active_seqno)?;
        }

        {
            let levels = self.levels.lock().map_err(|e| io_err(format!("lock levels: {e}")))?;
            for level in 1..levels.len() {
                if compaction::should_compact_ln(levels[level].len(), self.config.max_files_per_level) {
                    drop(levels);
                    self.compact_ln(level, oldest_active_seqno)?;
                    break;
                }
            }
        }

        Ok(())
    }

    fn compact_l0_to_l1(&self, oldest_active_seqno: SequenceNumber) -> MdbResult<()> {
        let (l0_ssts, l1_ssts, file_num) = {
            let l0 = self.l0.lock().map_err(|e| io_err(format!("lock l0: {e}")))?;
            let levels = self.levels.lock().map_err(|e| io_err(format!("lock levels: {e}")))?;
            let n = self.next_sst_number.lock().map_err(|e| io_err(format!("lock file num: {e}")))?;
            (l0.clone(), levels[1].clone(), *n)
        };

        if l0_ssts.is_empty() {
            return Ok(());
        }

        let output = compaction::compact_l0_to_l1(
            &l0_ssts,
            &l1_ssts,
            &self.dir,
            file_num,
            self.config.block_size_kb,
            self.config.bloom_bits_per_key,
            oldest_active_seqno,
        )?;

        {
            let mut n = self.next_sst_number.lock().map_err(|e| io_err(format!("lock file num: {e}")))?;
            let max_new = output.new_ssts.iter().map(|f| f.file_number).max().unwrap_or(*n);
            *n = max_new + 1;
        }

        {
            let mut l0 = self.l0.lock().map_err(|e| io_err(format!("lock l0: {e}")))?;
            l0.clear();
            for meta in &output.new_ssts {
                if meta.level == 0 {
                    l0.push(meta.clone());
                }
            }
            let mut levels = self.levels.lock().map_err(|e| io_err(format!("lock levels: {e}")))?;
            levels[1].clear();
            for meta in &output.new_ssts {
                if meta.level == 1 {
                    levels[1].push(meta.clone());
                }
            }
        }

        for file_num in &output.removed_files {
            let path = self.dir.join(format!("{:012}.sst", file_num));
            let _ = fs::remove_file(&path);
        }

        Ok(())
    }

    fn compact_ln(&self, level: usize, oldest_active_seqno: SequenceNumber) -> MdbResult<()> {
        let (input_ssts, target_ssts, file_num) = {
            let levels = self.levels.lock().map_err(|e| io_err(format!("lock levels: {e}")))?;
            let n = self.next_sst_number.lock().map_err(|e| io_err(format!("lock file num: {e}")))?;
            let target_level = (level + 1).min(levels.len() - 1);
            (levels[level].clone(), levels[target_level].clone(), *n)
        };

        if input_ssts.is_empty() {
            return Ok(());
        }

        let output = compaction::compact_ln(
            &input_ssts,
            &target_ssts,
            &self.dir,
            file_num,
            self.config.block_size_kb,
            self.config.bloom_bits_per_key,
            oldest_active_seqno,
        )?;

        let target_level = (level + 1).min(7);

        {
            let mut n = self.next_sst_number.lock().map_err(|e| io_err(format!("lock file num: {e}")))?;
            let max_new = output.new_ssts.iter().map(|f| f.file_number).max().unwrap_or(*n);
            *n = max_new + 1;
        }

        {
            let mut levels = self.levels.lock().map_err(|e| io_err(format!("lock levels: {e}")))?;
            levels[level].clear();
            levels[target_level].clear();
            for meta in &output.new_ssts {
                let lvl = meta.level.min(levels.len() - 1);
                levels[lvl].push(meta.clone());
            }
        }

        for file_num in &output.removed_files {
            let path = self.dir.join(format!("{:012}.sst", file_num));
            let _ = fs::remove_file(&path);
        }

        Ok(())
    }

    pub fn max_seqno(&self) -> SequenceNumber {
        let mt = self.memtable.lock().unwrap_or_else(|e| e.into_inner());
        let frozen = self.frozen.lock().unwrap_or_else(|e| e.into_inner());
        let l0 = self.l0.lock().unwrap_or_else(|e| e.into_inner());

        let mt_max = mt.max_seqno();
        let frozen_max = frozen.iter().map(|f| f.max_seqno).max().unwrap_or(0);
        let l0_max = l0.iter().map(|f| f.seqno_max).max().unwrap_or(0);

        mt_max.max(frozen_max).max(l0_max)
    }

    pub fn sst_count(&self) -> usize {
        let l0 = self.l0.lock().unwrap_or_else(|e| e.into_inner());
        let levels = self.levels.lock().unwrap_or_else(|e| e.into_inner());
        l0.len() + levels.iter().map(|l| l.len()).sum::<usize>()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }
}

fn meta_overlaps_range(meta: &SSTableMeta, start: &[u8], end: &[u8]) -> bool {
    if meta.first_key.is_empty() || meta.last_key.is_empty() {
        return true;
    }
    let fs = escape_user_key(start);
    let fe = escape_user_key(end);
    user_key_of(&meta.first_key) < &fe && user_key_of(&meta.last_key) >= &fs
}

pub fn recover_cf_wal(
    cf: &ColumnFamily,
    manifest: &Mutex<Manifest>,
) -> MdbResult<()> {
    let wal_subdir = cf.wal_dir();

    let mut replayed = 0u64;
    let mut max_seqno = 0u64;

    WalManager::recover(wal_subdir, |cf_id, op, key, val, seqno| {
        if cf_id != cf.id() {
            return;
        }
        let mut mt = cf.memtable.lock().unwrap();
        match op {
            OP_PUT => mt.put(key.to_vec(), seqno, val.to_vec()),
            OP_DELETE => mt.delete(key.to_vec(), seqno),
            _ => {}
        }
        if seqno > max_seqno {
            max_seqno = seqno;
        }
        replayed += 1;
    })?;

    if max_seqno > 0 {
        let mut manifest_guard = manifest.lock().unwrap();
        manifest_guard.set_last_seqno(max_seqno);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = PathBuf::from(format!("test_cf_{}_{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn collection_name_encoding_roundtrip() {
        for name in &["test", "my.collection", "col$name", "a/b\\c.d"] {
            let enc = encode_collection_name(name);
            let dec = decode_collection_name(&enc).unwrap();
            assert_eq!(&dec, name);
            assert!(!enc.contains('.'));
        }
    }

    #[test]
    fn cf_put_and_get() {
        let dir = temp_dir();
        let manifest = Mutex::new(Manifest::open(&dir).unwrap());
        let cf = ColumnFamily::open(
            "test".into(), 1, &dir, StorageConfig::default(), &manifest, &dir,
        ).unwrap();

        cf.put(b"hello".to_vec(), b"world".to_vec(), 1).unwrap();
        let result = cf.get(b"hello", 10).unwrap();
        assert_eq!(result, Some(b"world".to_vec()));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cf_delete_tombstone() {
        let dir = temp_dir();
        let manifest = Mutex::new(Manifest::open(&dir).unwrap());
        let cf = ColumnFamily::open(
            "test".into(), 1, &dir, StorageConfig::default(), &manifest, &dir,
        ).unwrap();

        cf.put(b"key".to_vec(), b"val".to_vec(), 1).unwrap();
        cf.delete(b"key".to_vec(), 2).unwrap();
        let result = cf.get(b"key", 10).unwrap();
        assert_eq!(result, None);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cf_freeze_and_flush() {
        let dir = temp_dir();
        let manifest = Mutex::new(Manifest::open(&dir).unwrap());
        let mut config = StorageConfig::default();
        config.memtable_size_mb = 1;
        let cf = ColumnFamily::open(
            "test".into(), 1, &dir, config, &manifest, &dir,
        ).unwrap();

        for i in 0..1000u64 {
            let key = format!("key_{:06}", i);
            let val = format!("val_{:06}", i);
            cf.put(key.into_bytes(), val.into_bytes(), i + 1).unwrap();
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cf_range_scan() {
        let dir = temp_dir();
        let manifest = Mutex::new(Manifest::open(&dir).unwrap());
        let cf = ColumnFamily::open(
            "test".into(), 1, &dir, StorageConfig::default(), &manifest, &dir,
        ).unwrap();

        for i in 0..10u64 {
            let key = format!("key_{:02}", i);
            cf.put(key.into_bytes(), b"v".to_vec(), i + 1).unwrap();
        }

        let results = cf.range_scan(b"key_03", b"key_07", 100).unwrap();
        assert!(results.len() >= 4);

        let _ = fs::remove_dir_all(&dir);
    }
}
