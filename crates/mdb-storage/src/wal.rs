use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use mdb_common::MdbResult;
use mdb_crypto::crc32c;

use crate::error::*;
use crate::types::*;

const WAL_RECORD_HEADER: usize = 8 + 2 + 1 + 4 + 4 + 4;
const DEFAULT_MAX_FILE_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
struct WalRecord {
    seqno: SequenceNumber,
    cf: ColumnFamilyId,
    op: u8,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl WalRecord {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.total_size() + 4);
        buf.extend_from_slice(&self.seqno.to_be_bytes());
        buf.extend_from_slice(&self.cf.to_be_bytes());
        buf.push(self.op);
        buf.extend_from_slice(&(self.key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&(self.value.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.value);

        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_be_bytes());
        buf
    }

    fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < WAL_RECORD_HEADER {
            return None;
        }
        let seqno = u64::from_be_bytes(data[0..8].try_into().ok()?);
        let cf = u16::from_be_bytes(data[8..10].try_into().ok()?);
        let op = data[10];
        let key_len = u32::from_be_bytes(data[11..15].try_into().ok()?);
        if data.len() < 15 + key_len as usize + 4 {
            return None;
        }
        let key = data[15..15 + key_len as usize].to_vec();
        let val_start = 15 + key_len as usize;
        let val_len = u32::from_be_bytes(data[val_start..val_start + 4].try_into().ok()?);
        if data.len() < val_start + 4 + val_len as usize + 4 {
            return None;
        }
        let value = data[val_start + 4..val_start + 4 + val_len as usize].to_vec();

        let crc_start = val_start + 4 + val_len as usize;
        let stored_crc = u32::from_be_bytes(data[crc_start..crc_start + 4].try_into().ok()?);
        let computed_crc = crc32c::crc32c(&data[..crc_start]);
        if stored_crc != computed_crc {
            return None;
        }

        Some(WalRecord { seqno, cf, op, key, value })
    }

    fn total_size(&self) -> usize {
        WAL_RECORD_HEADER + self.key.len() + self.value.len()
    }

    fn total_encoded_size(data: &[u8]) -> Option<usize> {
        if data.len() < WAL_RECORD_HEADER {
            return None;
        }
        let key_len = u32::from_be_bytes(data[11..15].try_into().ok()?) as usize;
        let val_start = 15 + key_len;
        if data.len() < val_start + 4 {
            return None;
        }
        let val_len = u32::from_be_bytes(data[val_start..val_start + 4].try_into().ok()?) as usize;
        let total = WAL_RECORD_HEADER + key_len + val_len;
        if data.len() < total {
            return None;
        }
        Some(total)
    }
}

struct PendingBatch {
    records: Vec<WalRecord>,
    encoded_size: usize,
    senders: Vec<Arc<PendingWrite>>,
}

struct PendingWrite {
    result: Mutex<Option<MdbResult<()>>>,
}

struct WalState {
    file: Option<File>,
    dir: PathBuf,
    current_file_start: SequenceNumber,
    file_size: u64,
    committed_seqno: SequenceNumber,
    max_file_size: u64,
    pending: PendingBatch,
}

impl std::fmt::Debug for WalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalState")
            .field("has_file", &self.file.is_some())
            .field("dir", &self.dir)
            .field("current_file_start", &self.current_file_start)
            .field("file_size", &self.file_size)
            .field("committed_seqno", &self.committed_seqno)
            .field("max_file_size", &self.max_file_size)
            .field("pending_count", &self.pending.records.len())
            .finish()
    }
}

#[derive(Debug)]
pub struct WalManager {
    inner: Arc<WalManagerInner>,
}

#[derive(Debug)]
struct WalManagerInner {
    state: Mutex<WalState>,
    group_commit_us: u64,
    commit_cvar: Condvar,
}

impl WalManager {
    pub fn open(dir: impl Into<PathBuf>, group_commit_us: u64) -> MdbResult<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|e| io_err(format!("create WAL dir: {e}")))?;

        let state = WalState {
            file: None,
            dir,
            current_file_start: 0,
            file_size: 0,
            committed_seqno: 0,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            pending: PendingBatch {
                records: Vec::new(),
                encoded_size: 0,
                senders: Vec::new(),
            },
        };

        let inner = Arc::new(WalManagerInner {
            state: Mutex::new(state),
            group_commit_us,
            commit_cvar: Condvar::new(),
        });

        Ok(Self { inner })
    }

    fn wal_path(dir: &Path, start_seqno: SequenceNumber) -> PathBuf {
        dir.join(format!("wal-{:012}.log", start_seqno))
    }

    fn ensure_file(state: &mut WalState) -> MdbResult<()> {
        if state.file.is_some() {
            return Ok(());
        }
        let path = Self::wal_path(&state.dir, state.current_file_start);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| io_err(format!("open WAL: {e}")))?;
        state.file_size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        state.file = Some(file);
        Ok(())
    }

    fn rotate(state: &mut WalState, new_start: SequenceNumber) -> MdbResult<()> {
        if let Some(file) = state.file.take() {
            file.sync_all().map_err(|e| io_err(format!("fsync WAL rotate: {e}")))?;
        }
        state.current_file_start = new_start;
        state.file_size = 0;
        Self::ensure_file(state)?;
        Ok(())
    }

    pub fn append(
        &self,
        cf: ColumnFamilyId,
        op: u8,
        key: &[u8],
        value: &[u8],
        seqno: SequenceNumber,
    ) -> MdbResult<()> {
        let record = WalRecord { seqno, cf, op, key: key.to_vec(), value: value.to_vec() };
        let record_size = record.total_size();

        let pending_write = Arc::new(PendingWrite {
            result: Mutex::new(None),
        });

        let is_leader = {
            let mut state = self.inner.state.lock().map_err(|e| io_err(format!("lock: {e}")))?;

            Self::ensure_file(&mut state)?;

            if state.file_size + record_size as u64 > state.max_file_size {
                Self::rotate(&mut state, seqno)?;
            }

            let leader = state.pending.senders.is_empty();
            state.pending.records.push(record);
            state.pending.encoded_size += record_size;
            state.pending.senders.push(Arc::clone(&pending_write));
            leader
        };

        if is_leader {
            if self.inner.group_commit_us > 0 {
                let window = Duration::from_micros(self.inner.group_commit_us);
                std::thread::sleep(window);
            }

            let mut state = self.inner.state.lock().map_err(|e| io_err(format!("lock: {e}")))?;
            let batch = Self::take_pending_batch(&mut state);

            let result = self.commit_batch(&mut state, &batch);
            let committed = result.is_ok();

            if committed {
                if let Some(max_seqno) = batch.records.iter().map(|r| r.seqno).max() {
                    state.committed_seqno = max_seqno;
                }
            }

            Self::notify_waiters(&self.inner, &batch, result);

            if !committed {
                return Err(io_err("WAL commit failed"));
            }
            return Ok(());
        }

        let mut wait_result = pending_write
            .result
            .lock()
            .map_err(|e| io_err(format!("lock pending: {e}")))?;

        while wait_result.is_none() {
            wait_result = self
                .inner
                .commit_cvar
                .wait(wait_result)
                .map_err(|e| io_err(format!("wait: {e}")))?;
        }

        match wait_result.take() {
            Some(Ok(())) => Ok(()),
            Some(Err(e)) => Err(e),
            None => Err(io_err("WAL: unexpected empty result")),
        }
    }

    fn take_pending_batch(state: &mut WalState) -> PendingBatch {
        let empty = PendingBatch {
            records: Vec::new(),
            encoded_size: 0,
            senders: Vec::new(),
        };
        std::mem::replace(&mut state.pending, empty)
    }

    fn commit_batch(&self, state: &mut WalState, batch: &PendingBatch) -> MdbResult<()> {
        if batch.records.is_empty() {
            return Ok(());
        }

        let mut encoded = Vec::with_capacity(batch.encoded_size);
        for record in &batch.records {
            encoded.extend_from_slice(&record.encode());
        }

        if state.file_size + encoded.len() as u64 > state.max_file_size {
            Self::rotate(state, batch.records.first().map(|r| r.seqno).unwrap_or(0))?;
        }

        let file = state.file.as_mut().ok_or_else(|| io_err("WAL: no file"))?;
        file.write_all(&encoded).map_err(|e| io_err(format!("write WAL: {e}")))?;
        file.sync_all().map_err(|e| io_err(format!("fsync WAL: {e}")))?;

        state.file_size += encoded.len() as u64;

        Ok(())
    }

    fn notify_waiters(inner: &WalManagerInner, batch: &PendingBatch, result: MdbResult<()>) {
        for pw in &batch.senders {
            if let Ok(mut guard) = pw.result.lock() {
                *guard = Some(result.clone());
            }
        }
        inner.commit_cvar.notify_all();
    }

    pub fn recover<F>(
        dir: &Path,
        mut replay: F,
    ) -> MdbResult<(SequenceNumber, Vec<(PathBuf, SequenceNumber)>)>
    where
        F: FnMut(ColumnFamilyId, u8, &[u8], &[u8], SequenceNumber),
    {
        let mut wal_files: Vec<(PathBuf, SequenceNumber)> = Vec::new();
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries {
                let entry = entry.map_err(|e| io_err(format!("read dir entry: {e}")))?;
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(seq_str) = name_str.strip_prefix("wal-").and_then(|s| s.strip_suffix(".log")) {
                    if let Ok(seqno) = seq_str.parse::<SequenceNumber>() {
                        wal_files.push((entry.path(), seqno));
                    }
                }
            }
        }
        wal_files.sort_by_key(|(_, seq)| *seq);

        let mut max_seqno: SequenceNumber = 0;

        for (path, _start_seqno) in &wal_files {
            let mut file = File::open(path).map_err(|e| io_err(format!("open WAL: {e}")))?;
            let mut contents = Vec::new();
            file.read_to_end(&mut contents).map_err(|e| io_err(format!("read WAL: {e}")))?;

            let mut pos = 0;
            while pos < contents.len() {
                let total = match WalRecord::total_encoded_size(&contents[pos..]) {
                    Some(t) => t,
                    None => break,
                };

                match WalRecord::decode(&contents[pos..pos + total]) {
                    Some(record) => {
                        replay(record.cf, record.op, &record.key, &record.value, record.seqno);
                        if record.seqno > max_seqno {
                            max_seqno = record.seqno;
                        }
                        pos += total;
                    }
                    None => break,
                }
            }
        }

        Ok((max_seqno, wal_files))
    }

    pub fn delete_wal_files(files: &[(PathBuf, SequenceNumber)]) -> MdbResult<()> {
        for (path, _) in files {
            let _ = fs::remove_file(path);
        }
        Ok(())
    }

    pub fn committed_seqno(&self) -> MdbResult<SequenceNumber> {
        let state = self.inner.state.lock().map_err(|e| io_err(format!("lock: {e}")))?;
        Ok(state.committed_seqno)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    fn temp_dir() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = PathBuf::from(format!("test_wal_{}_{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn wal_write_and_read() {
        let dir = temp_dir();
        let wal = WalManager::open(&dir, 0).unwrap();

        wal.append(1, OP_PUT, b"key1", b"val1", 1).unwrap();
        wal.append(1, OP_PUT, b"key2", b"val2", 2).unwrap();
        wal.append(1, OP_DELETE, b"key1", b"", 3).unwrap();

        let mut records = Vec::new();
        WalManager::recover(&dir, |cf, op, key, val, seqno| {
            records.push((cf, op, key.to_vec(), val.to_vec(), seqno));
        })
        .unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].1, OP_PUT);
        assert_eq!(records[0].2, b"key1");
        assert_eq!(records[0].3, b"val1");
        assert_eq!(records[2].1, OP_DELETE);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_group_commit() {
        let dir = temp_dir();
        let wal = Arc::new(WalManager::open(&dir, 0).unwrap());
        let mut handles = Vec::new();

        for i in 0..10u64 {
            let wal = wal.clone();
            let key = format!("key_{i}");
            let val = format!("val_{i}");
            handles.push(std::thread::spawn(move || {
                wal.append(1, OP_PUT, key.as_bytes(), val.as_bytes(), i + 1).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let mut records = Vec::new();
        WalManager::recover(&dir, |_, op, key, val, seqno| {
            records.push((op, key.to_vec(), val.to_vec(), seqno));
        })
        .unwrap();

        assert_eq!(records.len(), 10);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_record_encode_decode() {
        let record = WalRecord {
            seqno: 42,
            cf: 1,
            op: OP_PUT,
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
        };
        let encoded = record.encode();
        let decoded = WalRecord::decode(&encoded).unwrap();
        assert_eq!(decoded.seqno, 42);
        assert_eq!(decoded.cf, 1);
        assert_eq!(decoded.op, OP_PUT);
        assert_eq!(decoded.key, b"hello");
        assert_eq!(decoded.value, b"world");
    }

    #[test]
    fn wal_corrupted_record_stops_recovery() {
        let dir = temp_dir();
        let wal = WalManager::open(&dir, 0).unwrap();
        wal.append(1, OP_PUT, b"k", b"v", 1).unwrap();

        let wal_path = WalManager::wal_path(&dir, 0);
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&[0xFF; 20]).unwrap();
        drop(file);

        let mut count = 0;
        WalManager::recover(&dir, |_, _, _, _, _| {
            count += 1;
        })
        .unwrap();
        assert_eq!(count, 1);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_empty_recovery() {
        let dir = temp_dir();
        let mut count = 0;
        WalManager::recover(&dir, |_, _, _, _, _| {
            count += 1;
        })
        .unwrap();
        assert_eq!(count, 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_group_commit_actual_batching() {
        let dir = temp_dir();
        let wal = Arc::new(WalManager::open(&dir, 0).unwrap());
        let mut handles = Vec::new();
        let start = Instant::now();

        for i in 0..20u64 {
            let wal = wal.clone();
            handles.push(thread::spawn(move || {
                wal.append(1, OP_PUT, format!("k{i}").as_bytes(), format!("v{i}").as_bytes(), i + 1).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_secs(10), "took too long: {elapsed:?}");

        let mut records = Vec::new();
        WalManager::recover(&dir, |_, _, _, _, _| {
            records.push(());
        })
        .unwrap();
        assert_eq!(records.len(), 20);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_committed_seqno() {
        let dir = temp_dir();
        let wal = WalManager::open(&dir, 0).unwrap();

        assert_eq!(wal.committed_seqno().unwrap(), 0);
        wal.append(1, OP_PUT, b"k", b"v", 42).unwrap();
        assert_eq!(wal.committed_seqno().unwrap(), 42);
        wal.append(1, OP_PUT, b"k2", b"v2", 100).unwrap();
        assert_eq!(wal.committed_seqno().unwrap(), 100);

        let _ = fs::remove_dir_all(&dir);
    }
}
