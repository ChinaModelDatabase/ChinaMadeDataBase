//! Logical session management.
//!
//! Sessions track client state across multiple transactions, providing
//! transaction numbering and timeout-based cleanup (matching MongoDB's
//! 30-minute default session timeout).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::mvcc::SnapshotManager;
use crate::txn::{Transaction, TxnStorage};
use crate::types::{Result, Session, SessionId, SESSION_TIMEOUT, TxnId};

/// Session Manager: creates, tracks, and expires logical sessions.
#[derive(Debug)]
pub struct SessionManager<S: TxnStorage> {
    next_session_id: std::sync::atomic::AtomicU64,
    sessions: Mutex<HashMap<SessionId, Session>>,
    storage: Arc<S>,
    snapshot_manager: Arc<SnapshotManager>,
    timeout: Duration,
}

impl<S: TxnStorage> SessionManager<S> {
    pub fn new(storage: Arc<S>, snapshot_manager: Arc<SnapshotManager>) -> Self {
        Self::with_timeout(storage, snapshot_manager, SESSION_TIMEOUT)
    }

    pub fn with_timeout(
        storage: Arc<S>,
        snapshot_manager: Arc<SnapshotManager>,
        timeout: Duration,
    ) -> Self {
        Self {
            next_session_id: std::sync::atomic::AtomicU64::new(1),
            sessions: Mutex::new(HashMap::new()),
            storage,
            snapshot_manager,
            timeout,
        }
    }

    /// Create a new logical session.
    pub fn start_session(&self) -> SessionId {
        let id = SessionId(
            self.next_session_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );
        let session = Session::new(id);
        let mut sessions = self.sessions.lock().unwrap();
        sessions.insert(id, session);
        id
    }

    /// Start a new transaction within a session.
    ///
    /// Increments the session's transaction number and creates a new
    /// transaction bound to this session.
    pub fn start_transaction(&self, session_id: SessionId) -> Result<Transaction<S>> {
        let txn_number = {
            let mut sessions = self.sessions.lock().unwrap();
            let session = sessions
                .get_mut(&session_id)
                .ok_or_else(|| {
                    mdb_common::MdbError::new(
                        mdb_common::ErrorCode::IllegalOperation,
                        format!("session {} not found", session_id),
                    )
                })?;
            if session.is_expired(self.timeout) {
                return Err(mdb_common::MdbError::new(
                    mdb_common::ErrorCode::ExceededTimeLimit,
                    format!("session {} has expired", session_id),
                ));
            }
            session.next_txn_number()
        };

        let id = TxnId(txn_number);
        let snapshot = self.snapshot_manager.begin_snapshot();
        Ok(Transaction::new(
            id,
            snapshot,
            self.storage.clone(),
            Arc::new(std::sync::Mutex::new(
                crate::conflict::WriteConflictDetector::new(),
            )),
        ))
    }

    /// End (abandon) a session. Removes all tracking state.
    pub fn end_session(&self, session_id: SessionId) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.remove(&session_id);
    }

    /// Clean up expired sessions. Returns the number of sessions removed.
    pub fn cleanup_expired(&self) -> usize {
        let mut sessions = self.sessions.lock().unwrap();
        let before = sessions.len();
        sessions.retain(|_, session| !session.is_expired(self.timeout));
        before - sessions.len()
    }

    /// Returns the number of active sessions.
    pub fn active_session_count(&self) -> usize {
        let sessions = self.sessions.lock().unwrap();
        sessions.len()
    }

    /// Check if a session exists and is still active.
    pub fn is_session_active(&self, session_id: SessionId) -> bool {
        let sessions = self.sessions.lock().unwrap();
        sessions
            .get(&session_id)
            .map(|s| !s.is_expired(self.timeout))
            .unwrap_or(false)
    }

    /// Touch a session to extend its lifetime.
    pub fn touch_session(&self, session_id: SessionId) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(&session_id) {
            session.touch();
        }
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

    fn setup() -> SessionManager<MemStorage> {
        let storage = Arc::new(MemStorage::default());
        let snap_mgr = Arc::new(SnapshotManager::new());
        SessionManager::new(storage, snap_mgr)
    }

    #[test]
    fn start_session_creates_new_session() {
        let mgr = setup();
        let id1 = mgr.start_session();
        let id2 = mgr.start_session();
        assert!(id1 != id2);
        assert_eq!(mgr.active_session_count(), 2);
    }

    #[test]
    fn end_session_removes_session() {
        let mgr = setup();
        let id = mgr.start_session();
        assert!(mgr.is_session_active(id));

        mgr.end_session(id);
        assert!(!mgr.is_session_active(id));
        assert_eq!(mgr.active_session_count(), 0);
    }

    #[test]
    fn start_transaction_increments_number() {
        let mgr = setup();
        let id = mgr.start_session();

        let txn1 = mgr.start_transaction(id).unwrap();
        assert_eq!(txn1.id, TxnId(1));

        let txn2 = mgr.start_transaction(id).unwrap();
        assert_eq!(txn2.id, TxnId(2));
    }

    #[test]
    fn expired_session_rejects_transactions() {
        let storage = Arc::new(MemStorage::default());
        let snap_mgr = Arc::new(SnapshotManager::new());
        let mgr = SessionManager::with_timeout(storage, snap_mgr, Duration::from_millis(1));

        let id = mgr.start_session();
        // Wait for timeout
        std::thread::sleep(Duration::from_millis(5));

        let err = mgr.start_transaction(id).unwrap_err();
        assert_eq!(
            err.code(),
            mdb_common::ErrorCode::ExceededTimeLimit
        );
    }

    #[test]
    fn cleanup_removes_expired_sessions() {
        let storage = Arc::new(MemStorage::default());
        let snap_mgr = Arc::new(SnapshotManager::new());
        let mgr = SessionManager::with_timeout(storage, snap_mgr, Duration::from_millis(1));

        let _id1 = mgr.start_session();
        let _id2 = mgr.start_session();
        assert_eq!(mgr.active_session_count(), 2);

        std::thread::sleep(Duration::from_millis(5));
        let removed = mgr.cleanup_expired();
        assert_eq!(removed, 2);
        assert_eq!(mgr.active_session_count(), 0);
    }

    #[test]
    fn touch_session_extends_lifetime() {
        let storage = Arc::new(MemStorage::default());
        let snap_mgr = Arc::new(SnapshotManager::new());
        let mgr = SessionManager::with_timeout(storage, snap_mgr, Duration::from_millis(50));

        let id = mgr.start_session();
        std::thread::sleep(Duration::from_millis(20));
        mgr.touch_session(id);
        std::thread::sleep(Duration::from_millis(20));

        // Session should still be active because we touched it
        assert!(mgr.is_session_active(id));
    }

    #[test]
    fn transaction_works_in_session() {
        let mgr = setup();
        let id = mgr.start_session();

        let mut txn = mgr.start_transaction(id).unwrap();
        txn.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        txn.commit().unwrap();

        let mut txn2 = mgr.start_transaction(id).unwrap();
        let val = txn2.get(b"key").unwrap();
        assert_eq!(val.as_deref(), Some(b"value".as_slice()));
    }
}
