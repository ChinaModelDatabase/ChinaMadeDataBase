//! Transaction Manager: creates, commits, and aborts transactions.
//!
//! Central coordinator for transaction lifecycle management. Provides
//! conflict detection at the key level, session-to-transaction binding,
//! causal consistency tracking, and retryable write support.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::causal::SharedCausalSession;
use crate::concern::{ReadConcernOptions, WriteConcernOptions};
use crate::conflict::WriteConflictDetector;
use crate::mvcc::SnapshotManager;
use crate::preference::{ReadPreference, WritePreference};
use crate::retry::{DefaultRetryableWrite, RetryableWrite, WriteResult, WriteResultLog};
use crate::txn::{Transaction, TxnStorage};
use crate::types::{Result, SessionId, Snapshot, TxnId};

/// Transaction Manager.
///
/// Coordinates the lifecycle of all transactions, provides snapshot
/// management, and enforces write conflict detection.
#[derive(Debug)]
pub struct TransactionManager<S: TxnStorage> {
    next_txn_id: AtomicU64,
    snapshot_manager: SnapshotManager,
    storage: Arc<S>,
    conflict_detector: Arc<Mutex<WriteConflictDetector>>,
    /// Mapping from TxnId to session id for session tracking.
    txn_sessions: Mutex<HashMap<TxnId, SessionId>>,
    /// Per-session write result logs for retryable write deduplication.
    write_logs: Mutex<HashMap<SessionId, WriteResultLog>>,
    /// Per-session causal sessions.
    causal_sessions: Mutex<HashMap<SessionId, SharedCausalSession>>,
    /// Default read preference for new transactions.
    read_preference: Mutex<ReadPreference>,
    /// Default write preference for new transactions.
    write_preference: Mutex<WritePreference>,
}

impl<S: TxnStorage> TransactionManager<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            snapshot_manager: SnapshotManager::new(),
            storage,
            conflict_detector: Arc::new(Mutex::new(WriteConflictDetector::new())),
            txn_sessions: Mutex::new(HashMap::new()),
            write_logs: Mutex::new(HashMap::new()),
            causal_sessions: Mutex::new(HashMap::new()),
            read_preference: Mutex::new(ReadPreference::default()),
            write_preference: Mutex::new(WritePreference::default()),
        }
    }

    /// Begin a new transaction.
    ///
    /// Allocates a new TxnId, takes an MVCC snapshot, and returns a
    /// transaction ready for read/write operations.
    pub fn begin(&self) -> Transaction<S> {
        self.begin_with_session(None)
    }

    /// Begin a transaction associated with a session.
    pub fn begin_with_session(&self, session_id: Option<SessionId>) -> Transaction<S> {
        let id = TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));
        let snapshot = self.snapshot_manager.begin_snapshot();

        if let Some(sid) = session_id {
            let mut sessions = self.txn_sessions.lock().unwrap();
            sessions.insert(id, sid);
        }

        Transaction::new(
            id,
            snapshot,
            self.storage.clone(),
            self.conflict_detector.clone(),
        )
    }

    /// Begin a transaction with explicit read and write concerns.
    pub fn begin_with_concerns(
        &self,
        session_id: Option<SessionId>,
        write_concern: WriteConcernOptions,
        read_concern: ReadConcernOptions,
    ) -> Transaction<S> {
        let id = TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));
        let snapshot = self.snapshot_manager.begin_snapshot();

        if let Some(sid) = session_id {
            let mut sessions = self.txn_sessions.lock().unwrap();
            sessions.insert(id, sid);
        }

        Transaction::with_concerns(
            id,
            snapshot,
            self.storage.clone(),
            self.conflict_detector.clone(),
            write_concern,
            read_concern,
        )
    }

    /// Commit a transaction by its id.
    ///
    /// This finalizes Phase 2 of 2PC: all buffered writes are applied
    /// atomically to storage.
    pub fn commit(&self, txn: &mut Transaction<S>) -> Result<()> {
        txn.commit()
    }

    /// Abort a transaction by its id.
    ///
    /// Discards all buffered writes and cleans up conflict tracking.
    pub fn abort(&self, txn: &mut Transaction<S>) -> Result<()> {
        txn.abort()
    }

    /// Release the snapshot held by a transaction.
    ///
    /// Should be called after commit or abort to free snapshot resources.
    pub fn release_snapshot(&self, snapshot: Snapshot) {
        self.snapshot_manager.end_snapshot(snapshot.seqno);
    }

    /// Returns the oldest active snapshot seqno, used for compaction pruning.
    pub fn oldest_active_snapshot(&self) -> Option<u64> {
        self.snapshot_manager.oldest_active_snapshot()
    }

    /// Get the session associated with a transaction.
    pub fn session_for_txn(&self, txn_id: TxnId) -> Option<SessionId> {
        let sessions = self.txn_sessions.lock().unwrap();
        sessions.get(&txn_id).copied()
    }

    /// Remove session association for a committed/aborted transaction.
    pub fn remove_txn_session(&self, txn_id: TxnId) {
        let mut sessions = self.txn_sessions.lock().unwrap();
        sessions.remove(&txn_id);
    }

    /// Get or create a causal session for the given session id.
    pub fn causal_session(&self, session_id: SessionId) -> SharedCausalSession {
        let mut sessions = self.causal_sessions.lock().unwrap();
        sessions
            .entry(session_id)
            .or_insert_with(SharedCausalSession::new)
            .clone()
    }

    /// Get the causal session for a session id, if it exists.
    pub fn get_causal_session(&self, session_id: SessionId) -> Option<SharedCausalSession> {
        let sessions = self.causal_sessions.lock().unwrap();
        sessions.get(&session_id).cloned()
    }

    /// Remove the causal session for a session id.
    pub fn remove_causal_session(&self, session_id: SessionId) {
        let mut sessions = self.causal_sessions.lock().unwrap();
        sessions.remove(&session_id);
    }

    /// Get the write result log for a session.
    pub fn write_log(&self, _session_id: SessionId) -> std::sync::MutexGuard<'_, HashMap<SessionId, WriteResultLog>> {
        self.write_logs.lock().unwrap()
    }

    /// Record a write result for retryable write deduplication.
    pub fn record_write_result(&self, session_id: SessionId, result: WriteResult) {
        let mut logs = self.write_logs.lock().unwrap();
        logs.entry(session_id)
            .or_insert_with(WriteResultLog::with_default_capacity)
            .record(result);
    }

    /// Check if a write is a duplicate (already succeeded with same txnNumber).
    pub fn is_duplicate_write(&self, session_id: SessionId, txn_number: u64) -> bool {
        let logs = self.write_logs.lock().unwrap();
        logs.get(&session_id)
            .map(|log| log.is_duplicate(txn_number))
            .unwrap_or(false)
    }

    /// Set the default read preference.
    pub fn set_read_preference(&self, pref: ReadPreference) {
        let mut current = self.read_preference.lock().unwrap();
        *current = pref;
    }

    /// Get the current default read preference.
    pub fn read_preference(&self) -> ReadPreference {
        *self.read_preference.lock().unwrap()
    }

    /// Set the default write preference.
    pub fn set_write_preference(&self, pref: WritePreference) {
        let mut current = self.write_preference.lock().unwrap();
        *current = pref;
    }

    /// Get the current default write preference.
    pub fn write_preference(&self) -> WritePreference {
        *self.write_preference.lock().unwrap()
    }

    /// Execute a write operation with retryable write semantics.
    ///
    /// Retries the operation on transient errors, using the session's
    /// txnNumber for deduplication.
    pub fn execute_retryable_write<F>(
        &self,
        session_id: SessionId,
        max_retries: u32,
        mut f: F,
    ) -> Result<WriteResult>
    where
        F: FnMut(u64) -> Result<()>,
    {
        let mut txn_number = {
            let logs = self.write_logs.lock().unwrap();
            logs.get(&session_id)
                .map(|log| {
                    log.get(log.len() as u64)
                        .map(|r| r.txn_number)
                        .unwrap_or(0)
                })
                .unwrap_or(0)
        };

        let result = DefaultRetryableWrite.execute_with_retry(max_retries, &mut txn_number, &mut f)?;
        self.record_write_result(session_id, result.clone());
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txn::TxnStorage;
    use std::collections::HashMap;
    use std::sync::Mutex;

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

    #[test]
    fn begin_assigns_unique_ids() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let txn1 = mgr.begin();
        let txn2 = mgr.begin();
        assert!(txn1.id != txn2.id);
    }

    #[test]
    fn commit_and_read() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let mut txn = mgr.begin();
        txn.put(b"hello".to_vec(), b"world".to_vec()).unwrap();
        mgr.commit(&mut txn).unwrap();

        let mut txn2 = mgr.begin();
        let val = txn2.get(b"hello").unwrap();
        assert_eq!(val.as_deref(), Some(b"world".as_slice()));
    }

    #[test]
    fn abort_discards() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let mut txn = mgr.begin();
        txn.put(b"key".to_vec(), b"val".to_vec()).unwrap();
        mgr.abort(&mut txn).unwrap();

        let mut txn2 = mgr.begin();
        let val = txn2.get(b"key").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn concurrent_write_conflict_detection() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let mut txn1 = mgr.begin();
        let mut txn2 = mgr.begin();

        txn1.put(b"shared".to_vec(), b"v1".to_vec()).unwrap();
        let err = txn2.put(b"shared".to_vec(), b"v2".to_vec()).unwrap_err();
        assert_eq!(err.code(), mdb_common::ErrorCode::WriteConflict);
    }

    #[test]
    fn session_tracking() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let sid = SessionId(42);
        let mut txn = mgr.begin_with_session(Some(sid));

        assert_eq!(mgr.session_for_txn(txn.id), Some(sid));

        mgr.commit(&mut txn).unwrap();
        mgr.release_snapshot(txn.snapshot);
        mgr.remove_txn_session(txn.id);

        assert_eq!(mgr.session_for_txn(txn.id), None);
    }

    #[test]
    fn oldest_active_snapshot() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        assert_eq!(mgr.oldest_active_snapshot(), None);

        let mut txn1 = mgr.begin();
        let snap1 = txn1.snapshot;
        assert_eq!(mgr.oldest_active_snapshot(), Some(snap1.seqno));

        let mut txn2 = mgr.begin();
        assert_eq!(mgr.oldest_active_snapshot(), Some(snap1.seqno));

        mgr.commit(&mut txn1).unwrap();
        mgr.release_snapshot(snap1);
        assert_eq!(mgr.oldest_active_snapshot(), Some(txn2.snapshot.seqno));

        mgr.abort(&mut txn2).unwrap();
        mgr.release_snapshot(txn2.snapshot);
        assert_eq!(mgr.oldest_active_snapshot(), None);
    }

    #[test]
    fn multiple_transactions_on_different_keys() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let mut txn1 = mgr.begin();
        let mut txn2 = mgr.begin();

        txn1.put(b"key1".to_vec(), b"v1".to_vec()).unwrap();
        txn2.put(b"key2".to_vec(), b"v2".to_vec()).unwrap();

        mgr.commit(&mut txn1).unwrap();
        mgr.commit(&mut txn2).unwrap();

        let mut txn3 = mgr.begin();
        assert_eq!(txn3.get(b"key1").unwrap().as_deref(), Some(b"v1".as_slice()));
        assert_eq!(txn3.get(b"key2").unwrap().as_deref(), Some(b"v2".as_slice()));
    }

    // --- Write Concern tests ---

    #[test]
    fn begin_with_write_concern() {
        use crate::concern::WriteConcernBuilder;
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let wc = WriteConcernBuilder::majority().journal(true).build();
        let txn = mgr.begin_with_concerns(
            None,
            wc.clone(),
            ReadConcernOptions::default(),
        );
        assert_eq!(txn.write_concern().concern, crate::concern::WriteConcern::Majority);
        assert!(txn.write_concern().journal);
    }

    #[test]
    fn set_write_concern_on_transaction() {
        use crate::concern::WriteConcernBuilder;
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let mut txn = mgr.begin();
        let wc = WriteConcernBuilder::num_ack(3).timeout(std::time::Duration::from_secs(2)).build();
        txn.set_write_concern(wc);
        assert_eq!(txn.write_concern_level(), &crate::concern::WriteConcern::NumAck(3));
    }

    #[test]
    fn write_concern_validates_on_prepare() {
        use crate::concern::WriteConcernBuilder;
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let mut txn = mgr.begin_with_concerns(
            None,
            WriteConcernBuilder::num_ack(-1).build(),
            ReadConcernOptions::default(),
        );
        let err = txn.prepare().unwrap_err();
        assert_eq!(err.code(), mdb_common::ErrorCode::WriteConcernFailed);
    }

    // --- Read Concern tests ---

    #[test]
    fn begin_with_read_concern() {
        use crate::concern::ReadConcernOptions;
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let rc = ReadConcernOptions::new(crate::concern::ReadConcern::Snapshot)
            .with_after_cluster_time(42);
        let txn = mgr.begin_with_concerns(
            None,
            WriteConcernOptions::default(),
            rc,
        );
        assert_eq!(txn.read_concern_level(), &crate::concern::ReadConcern::Snapshot);
        assert_eq!(txn.read_concern().after_cluster_time, Some(42));
    }

    #[test]
    fn set_read_concern_on_transaction() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let mut txn = mgr.begin();
        let rc = ReadConcernOptions::new(crate::concern::ReadConcern::Majority);
        txn.set_read_concern(rc);
        assert_eq!(txn.read_concern_level(), &crate::concern::ReadConcern::Majority);
    }

    // --- Causal Consistency tests ---

    #[test]
    fn causal_session_created_on_demand() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let sid = SessionId(1);
        let cs = mgr.causal_session(sid);
        assert_eq!(cs.cluster_time(), 0);
        assert_eq!(cs.after_cluster_time(), None);
    }

    #[test]
    fn causal_session_shares_state() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let sid = SessionId(1);
        let cs1 = mgr.causal_session(sid);
        let cs2 = mgr.causal_session(sid);
        cs1.advance_cluster_time(50);
        assert_eq!(cs2.cluster_time(), 50);
    }

    #[test]
    fn causal_session_advances_across_operations() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let sid = SessionId(1);
        let cs = mgr.causal_session(sid);

        // Simulate a write that returns operationTime=10
        cs.update_operation_time(10);
        assert_eq!(cs.after_cluster_time(), Some(10));

        // Simulate a read that returns clusterTime=15
        cs.advance_cluster_time(15);
        assert_eq!(cs.after_cluster_time(), Some(15));
    }

    #[test]
    fn remove_causal_session() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let sid = SessionId(1);
        let _cs = mgr.causal_session(sid);
        assert!(mgr.get_causal_session(sid).is_some());

        mgr.remove_causal_session(sid);
        assert!(mgr.get_causal_session(sid).is_none());
    }

    // --- Read/Write Preference tests ---

    #[test]
    fn default_preferences() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        assert_eq!(mgr.read_preference(), ReadPreference::Primary);
        assert_eq!(mgr.write_preference(), WritePreference::Primary);
    }

    #[test]
    fn set_preferences() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        mgr.set_read_preference(ReadPreference::SecondaryPreferred);
        mgr.set_write_preference(WritePreference::PrimaryPreferred);
        assert_eq!(mgr.read_preference(), ReadPreference::SecondaryPreferred);
        assert_eq!(mgr.write_preference(), WritePreference::PrimaryPreferred);
    }

    // --- Retryable Writes tests ---

    #[test]
    fn retryable_write_succeeds() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let sid = SessionId(1);
        let result = mgr.execute_retryable_write(sid, 3, |txn| {
            assert!(txn > 0);
            Ok(())
        }).unwrap();
        assert!(result.ok);
    }

    #[test]
    fn retryable_write_retries_on_transient_error() {
        use std::cell::Cell;
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let sid = SessionId(1);
        let counter = Cell::new(0);

        let result = mgr.execute_retryable_write(sid, 3, |_txn| {
            counter.set(counter.get() + 1);
            if counter.get() < 3 {
                Err(mdb_common::MdbError::new(
                    mdb_common::ErrorCode::WriteConflict,
                    "conflict",
                ))
            } else {
                Ok(())
            }
        }).unwrap();

        assert!(result.ok);
        assert_eq!(counter.get(), 3);
    }

    #[test]
    fn retryable_write_records_result() {
        let mgr = TransactionManager::new(Arc::new(MemStorage::default()));
        let sid = SessionId(1);
        let _result = mgr.execute_retryable_write(sid, 3, |_| Ok(())).unwrap();
        assert!(mgr.is_duplicate_write(sid, 1));
    }
}
