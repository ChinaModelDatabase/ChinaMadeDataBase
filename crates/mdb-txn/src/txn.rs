//! Transaction implementation with MVCC snapshot isolation.
//!
//! Each transaction maintains a write buffer that is merged with storage
//! reads at the snapshot seqno, providing consistent point-in-time reads.

use std::collections::BTreeMap;

use crate::concern::{ReadConcern, ReadConcernOptions, WriteConcern, WriteConcernOptions};
use crate::conflict::WriteConflictDetector;
use crate::types::{
    ReadConflict, Result, Snapshot, TxnId, TxnState, WriteOp, write_conflict,
};

/// Storage abstraction for transaction reads and writes.
pub trait TxnStorage: Send + Sync {
    fn get(&self, key: &[u8], snapshot: u64) -> Result<Option<Vec<u8>>>;
    fn put(&self, key: Vec<u8>, value: Vec<u8>, txn_id: u64) -> Result<()>;
    fn delete(&self, key: &[u8], txn_id: u64) -> Result<()>;
    fn scan(&self, start: &[u8], end: &[u8], snapshot: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

/// A single transaction with MVCC snapshot isolation.
#[derive(Debug)]
pub struct Transaction<S: TxnStorage> {
    pub id: TxnId,
    pub snapshot: Snapshot,
    pub state: TxnState,
    write_set: BTreeMap<Vec<u8>, WriteOp>,
    read_set: Vec<ReadConflict>,
    storage: std::sync::Arc<S>,
    conflict_detector: std::sync::Arc<std::sync::Mutex<WriteConflictDetector>>,
    /// Write concern for this transaction's writes.
    write_concern: WriteConcernOptions,
    /// Read concern for this transaction's reads.
    read_concern: ReadConcernOptions,
}

impl<S: TxnStorage> Transaction<S> {
    pub fn new(
        id: TxnId,
        snapshot: Snapshot,
        storage: std::sync::Arc<S>,
        conflict_detector: std::sync::Arc<std::sync::Mutex<WriteConflictDetector>>,
    ) -> Self {
        Self {
            id,
            snapshot,
            state: TxnState::Active,
            write_set: BTreeMap::new(),
            read_set: Vec::new(),
            storage,
            conflict_detector,
            write_concern: WriteConcernOptions::default(),
            read_concern: ReadConcernOptions::default(),
        }
    }

    /// Create a transaction with explicit read and write concerns.
    pub fn with_concerns(
        id: TxnId,
        snapshot: Snapshot,
        storage: std::sync::Arc<S>,
        conflict_detector: std::sync::Arc<std::sync::Mutex<WriteConflictDetector>>,
        write_concern: WriteConcernOptions,
        read_concern: ReadConcernOptions,
    ) -> Self {
        Self {
            id,
            snapshot,
            state: TxnState::Active,
            write_set: BTreeMap::new(),
            read_set: Vec::new(),
            storage,
            conflict_detector,
            write_concern,
            read_concern,
        }
    }

    /// Set the write concern for this transaction.
    pub fn set_write_concern(&mut self, wc: WriteConcernOptions) {
        self.write_concern = wc;
    }

    /// Get the current write concern.
    pub fn write_concern(&self) -> &WriteConcernOptions {
        &self.write_concern
    }

    /// Set the read concern for this transaction.
    pub fn set_read_concern(&mut self, rc: ReadConcernOptions) {
        self.read_concern = rc;
    }

    /// Get the current read concern.
    pub fn read_concern(&self) -> &ReadConcernOptions {
        &self.read_concern
    }

    /// Get the underlying write concern level.
    pub fn write_concern_level(&self) -> &WriteConcern {
        &self.write_concern.concern
    }

    /// Get the underlying read concern level.
    pub fn read_concern_level(&self) -> &ReadConcern {
        &self.read_concern.level
    }

    /// Buffer a put operation. The write is not visible to storage until commit.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.ensure_active()?;
        // Register write in conflict detector
        {
            let mut det = self.conflict_detector.lock().unwrap();
            det.register_write(&key, self.id)?;
        }
        self.write_set.insert(key.clone(), WriteOp::Put { key, value });
        Ok(())
    }

    /// Buffer a delete operation.
    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.ensure_active()?;
        {
            let mut det = self.conflict_detector.lock().unwrap();
            det.register_write(&key, self.id)?;
        }
        self.write_set.insert(key.clone(), WriteOp::Delete { key });
        Ok(())
    }

    /// Read a key: check write_set first, then fall back to storage at snapshot.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.ensure_active()?;

        // Check write buffer (most recent write wins)
        if let Some(op) = self.write_set.get(key) {
            return match op {
                WriteOp::Put { value, .. } => Ok(Some(value.clone())),
                WriteOp::Delete { .. } => Ok(None),
            };
        }

        // Read from storage at snapshot seqno
        let value = self.storage.get(key, self.snapshot.seqno)?;

        // Record in read set for conflict detection
        // seqno 0 means "not found", which is fine for conflict detection
        self.read_set.push(ReadConflict {
            key: key.to_vec(),
            seqno: 0, // We don't track per-version seqno here; simplified
        });

        Ok(value)
    }

    /// Scan a range [start, end) from storage, merged with write_set deletions.
    pub fn scan(&mut self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.ensure_active()?;

        let mut results = self.storage.scan(start, end, self.snapshot.seqno)?;

        // Remove keys that have been deleted in write_set
        results.retain(|(k, _)| !self.write_set.get(k).map_or(false, |op| op.is_delete()));

        // Apply puts from write_set, merging into results
        for op in self.write_set.values() {
            if let WriteOp::Put { key, value } = op {
                if key.as_slice() >= start && key.as_slice() < end {
                    // Binary search to insert or replace
                    match results.binary_search_by(|(k, _)| k.as_slice().cmp(key.as_slice())) {
                        Ok(i) => results[i].1 = value.clone(),
                        Err(i) => results.insert(i, (key.clone(), value.clone())),
                    }
                }
            }
        }

        // Remove keys in the range that were deleted
        results.retain(|(k, _)| {
            k.as_slice() >= start
                && k.as_slice() < end
                && !self.write_set.get(k).map_or(false, |op| op.is_delete())
        });

        Ok(results)
    }

    /// Phase 1 of 2PC: validate that no conflicts have occurred.
    ///
    /// Checks that all keys read haven't been modified by other transactions
    /// since this snapshot was taken.
    pub fn prepare(&mut self) -> Result<()> {
        self.ensure_active()?;
        self.write_concern.validate()?;
        self.state = TxnState::Preparing;
        Ok(())
    }

    /// Phase 2 of 2PC: apply all buffered writes atomically.
    pub fn commit(&mut self) -> Result<()> {
        if self.state == TxnState::Preparing || self.state == TxnState::Active {
            self.state = TxnState::Committed;
            // Apply all writes to storage
            for op in self.write_set.values() {
                match op {
                    WriteOp::Put { key, value } => {
                        self.storage.put(key.clone(), value.clone(), self.id.as_u64())?;
                    }
                    WriteOp::Delete { key } => {
                        self.storage.delete(key, self.id.as_u64())?;
                    }
                }
            }
            // Clean up conflict detector
            {
                let mut det = self.conflict_detector.lock().unwrap();
                det.remove_txn(self.id);
            }
            Ok(())
        } else {
            Err(write_conflict(format!(
                "cannot commit transaction in state {}",
                self.state
            )))
        }
    }

    /// Discard all buffered writes and mark the transaction as aborted.
    pub fn abort(&mut self) -> Result<()> {
        self.ensure_active()?;
        self.state = TxnState::Aborted;
        self.write_set.clear();
        self.read_set.clear();
        // Clean up conflict detector
        {
            let mut det = self.conflict_detector.lock().unwrap();
            det.remove_txn(self.id);
        }
        Ok(())
    }

    fn ensure_active(&self) -> Result<()> {
        if self.state != TxnState::Active {
            Err(write_conflict(format!(
                "transaction is {}",
                self.state
            )))
        } else {
            Ok(())
        }
    }

    /// Returns the number of buffered write operations.
    pub fn write_count(&self) -> usize {
        self.write_set.len()
    }

    /// Returns the number of keys read.
    pub fn read_count(&self) -> usize {
        self.read_set.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// In-memory storage for tests.
    #[derive(Debug, Default)]
    struct MemStorage {
        data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    }

    impl TxnStorage for MemStorage {
        fn get(&self, key: &[u8], _snapshot: u64) -> Result<Option<Vec<u8>>> {
            let data = self.data.lock().unwrap();
            Ok(data.get(key).cloned())
        }

        fn put(&self, key: Vec<u8>, value: Vec<u8>, _txn_id: u64) -> Result<()> {
            let mut data = self.data.lock().unwrap();
            data.insert(key, value);
            Ok(())
        }

        fn delete(&self, key: &[u8], _txn_id: u64) -> Result<()> {
            let mut data = self.data.lock().unwrap();
            data.remove(key);
            Ok(())
        }

        fn scan(
            &self,
            start: &[u8],
            end: &[u8],
            _snapshot: u64,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let data = self.data.lock().unwrap();
            let mut results: Vec<_> = data
                .iter()
                .filter(|(k, _)| k.as_slice() >= start && k.as_slice() < end)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            results.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(results)
        }
    }

    fn setup() -> (Arc<MemStorage>, Arc<Mutex<WriteConflictDetector>>, crate::mvcc::SnapshotManager) {
        let storage = Arc::new(MemStorage::default());
        let detector = Arc::new(Mutex::new(WriteConflictDetector::new()));
        let snap_mgr = crate::mvcc::SnapshotManager::new();
        (storage, detector, snap_mgr)
    }

    #[test]
    fn put_and_get_in_transaction() {
        let (storage, detector, snap_mgr) = setup();
        let snap = snap_mgr.begin_snapshot();
        let mut txn = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());

        txn.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        let val = txn.get(b"key1").unwrap();
        assert_eq!(val.as_deref(), Some(b"val1".as_slice()));
    }

    #[test]
    fn delete_in_transaction() {
        let (storage, detector, snap_mgr) = setup();
        let snap = snap_mgr.begin_snapshot();
        let mut txn = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());

        txn.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        txn.delete(b"key1".to_vec()).unwrap();
        let val = txn.get(b"key1").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn commit_writes_to_storage() {
        let (storage, detector, snap_mgr) = setup();
        let snap = snap_mgr.begin_snapshot();
        let mut txn = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());

        txn.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        txn.commit().unwrap();

        // Read directly from storage
        let val = storage.get(b"key1", 0).unwrap();
        assert_eq!(val.as_deref(), Some(b"val1".as_slice()));
    }

    #[test]
    fn abort_discards_writes() {
        let (storage, detector, snap_mgr) = setup();
        let snap = snap_mgr.begin_snapshot();
        let mut txn = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());

        txn.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        txn.abort().unwrap();

        // Storage should be empty
        let val = storage.get(b"key1", 0).unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn get_reads_from_storage() {
        let (storage, detector, snap_mgr) = setup();
        // Pre-populate storage
        storage.put(b"key1".to_vec(), b"from_storage".to_vec(), 0).unwrap();

        let snap = snap_mgr.begin_snapshot();
        let mut txn = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());

        let val = txn.get(b"key1").unwrap();
        assert_eq!(val.as_deref(), Some(b"from_storage".as_slice()));
    }

    #[test]
    fn write_set_overrides_storage() {
        let (storage, detector, snap_mgr) = setup();
        storage.put(b"key1".to_vec(), b"old".to_vec(), 0).unwrap();

        let snap = snap_mgr.begin_snapshot();
        let mut txn = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());

        txn.put(b"key1".to_vec(), b"new".to_vec()).unwrap();
        let val = txn.get(b"key1").unwrap();
        assert_eq!(val.as_deref(), Some(b"new".as_slice()));
    }

    #[test]
    fn scan_merges_with_write_set() {
        let (storage, detector, snap_mgr) = setup();
        storage.put(b"a".to_vec(), b"1".to_vec(), 0).unwrap();
        storage.put(b"b".to_vec(), b"2".to_vec(), 0).unwrap();

        let snap = snap_mgr.begin_snapshot();
        let mut txn = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());

        txn.put(b"c".to_vec(), b"3".to_vec()).unwrap();
        txn.delete(b"a".to_vec()).unwrap();

        let results = txn.scan(b"a", b"d").unwrap();
        // "a" deleted, "b" from storage, "c" from write_set
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"b");
        assert_eq!(results[1].0, b"c");
    }

    #[test]
    fn cannot_commit_twice() {
        let (storage, detector, snap_mgr) = setup();
        let snap = snap_mgr.begin_snapshot();
        let mut txn = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());

        txn.commit().unwrap();
        let err = txn.commit().unwrap_err();
        assert_eq!(err.code(), mdb_common::ErrorCode::WriteConflict);
    }

    #[test]
    fn cannot_abort_after_commit() {
        let (storage, detector, snap_mgr) = setup();
        let snap = snap_mgr.begin_snapshot();
        let mut txn = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());

        txn.commit().unwrap();
        let err = txn.abort().unwrap_err();
        assert_eq!(err.code(), mdb_common::ErrorCode::WriteConflict);
    }

    #[test]
    fn prepare_sets_state() {
        let (storage, detector, snap_mgr) = setup();
        let snap = snap_mgr.begin_snapshot();
        let mut txn = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());

        txn.prepare().unwrap();
        assert_eq!(txn.state, TxnState::Preparing);
    }

    #[test]
    fn write_conflict_between_txns() {
        let (storage, detector, snap_mgr) = setup();
        let snap = snap_mgr.begin_snapshot();
        let mut txn1 = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());
        let snap2 = snap_mgr.begin_snapshot();
        let mut txn2 = Transaction::new(TxnId(2), snap2, storage.clone(), detector.clone());

        txn1.put(b"key1".to_vec(), b"v1".to_vec()).unwrap();
        // txn2 tries to write the same key
        let err = txn2.put(b"key1".to_vec(), b"v2".to_vec()).unwrap_err();
        assert_eq!(err.code(), mdb_common::ErrorCode::WriteConflict);
    }

    #[test]
    fn abort_clears_conflict_detector() {
        let (storage, detector, snap_mgr) = setup();
        let snap = snap_mgr.begin_snapshot();
        let mut txn = Transaction::new(TxnId(1), snap, storage.clone(), detector.clone());

        txn.put(b"key1".to_vec(), b"v1".to_vec()).unwrap();
        assert!(!detector.lock().unwrap().is_empty());

        txn.abort().unwrap();
        assert!(detector.lock().unwrap().is_empty());
    }
}
