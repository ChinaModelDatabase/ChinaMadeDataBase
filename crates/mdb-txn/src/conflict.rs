//! Write conflict detection for optimistic concurrency control.
//!
//! Implements key-level optimistic locking: when two concurrent transactions
//! write to the same key, one of them must abort with a `WriteConflict` error.

use std::collections::HashMap;

use crate::types::{Result, TxnId, write_conflict};

/// Tracks writes per key across concurrent transactions.
///
/// When transaction A writes to key K, and transaction B also writes to K,
/// this detector identifies the conflict and aborts one of them.
#[derive(Debug)]
pub struct WriteConflictDetector {
    /// Map from key -> set of txn_ids that have written this key.
    writes: HashMap<Vec<u8>, Vec<TxnId>>,
}

impl Default for WriteConflictDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteConflictDetector {
    pub fn new() -> Self {
        Self {
            writes: HashMap::new(),
        }
    }

    /// Register a write to a key by a transaction.
    ///
    /// Returns `Err(WriteConflict)` if another active transaction has already
    /// written to the same key.
    pub fn register_write(&mut self, key: &[u8], txn_id: TxnId) -> Result<()> {
        let entry = self.writes.entry(key.to_vec()).or_default();
        // Check if another txn already wrote this key
        if let Some(&conflict_id) = entry.iter().find(|&&id| id != txn_id) {
            return Err(write_conflict(format!(
                "key {:?} already written by {}",
                String::from_utf8_lossy(key),
                conflict_id
            )));
        }
        if !entry.contains(&txn_id) {
            entry.push(txn_id);
        }
        Ok(())
    }

    /// Check if a key has been written by another transaction.
    pub fn has_conflict(&self, key: &[u8], txn_id: TxnId) -> bool {
        self.writes
            .get(key)
            .map(|txns| txns.iter().any(|&id| id != txn_id))
            .unwrap_or(false)
    }

    /// Remove all writes registered by a transaction.
    /// Called after commit or abort to clean up.
    pub fn remove_txn(&mut self, txn_id: TxnId) {
        for txns in self.writes.values_mut() {
            txns.retain(|&id| id != txn_id);
        }
        self.writes.retain(|_, txns| !txns.is_empty());
    }

    /// Returns true if the detector has no tracked writes.
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    /// Number of keys with tracked writes.
    pub fn key_count(&self) -> usize {
        self.writes.len()
    }
}

/// Attempt a transaction operation with automatic retry on WriteConflict.
///
/// Retries the closure up to `max_retries` times. Returns the first
/// successful result or the last error if all retries are exhausted.
pub fn retry_on_write_conflict<T>(
    max_retries: u32,
    mut f: impl FnMut() -> Result<T>,
) -> Result<T> {
    let mut last_err = None;
    for _ in 0..=max_retries {
        match f() {
            Ok(v) => return Ok(v),
            Err(e) if e.code() == mdb_common::ErrorCode::WriteConflict => {
                last_err = Some(e);
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        write_conflict("exhausted retries without conflict resolution")
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_conflict_on_single_writer() {
        let mut det = WriteConflictDetector::new();
        det.register_write(b"key1", TxnId(1)).unwrap();
        assert!(!det.has_conflict(b"key1", TxnId(1)));
        assert!(det.has_conflict(b"key1", TxnId(2)));
    }

    #[test]
    fn detects_write_write_conflict() {
        let mut det = WriteConflictDetector::new();
        det.register_write(b"key1", TxnId(1)).unwrap();
        let err = det.register_write(b"key1", TxnId(2)).unwrap_err();
        assert_eq!(err.code(), mdb_common::ErrorCode::WriteConflict);
    }

    #[test]
    fn same_txn_can_write_same_key_multiple_times() {
        let mut det = WriteConflictDetector::new();
        det.register_write(b"key1", TxnId(1)).unwrap();
        det.register_write(b"key1", TxnId(1)).unwrap();
    }

    #[test]
    fn remove_txn_cleans_up() {
        let mut det = WriteConflictDetector::new();
        det.register_write(b"key1", TxnId(1)).unwrap();
        assert!(!det.is_empty());

        det.remove_txn(TxnId(1));
        assert!(det.is_empty());
        // No conflict after removal
        assert!(!det.has_conflict(b"key1", TxnId(2)));
    }

    #[test]
    fn different_keys_no_conflict() {
        let mut det = WriteConflictDetector::new();
        det.register_write(b"key1", TxnId(1)).unwrap();
        det.register_write(b"key2", TxnId(2)).unwrap();
        // key1 written by TxnId(1) does not conflict with TxnId(1)
        assert!(!det.has_conflict(b"key1", TxnId(1)));
        // key2 written by TxnId(2) does not conflict with TxnId(2)
        assert!(!det.has_conflict(b"key2", TxnId(2)));
        // key1 written by TxnId(1) conflicts with TxnId(3)
        assert!(det.has_conflict(b"key1", TxnId(3)));
        // key2 written by TxnId(2) conflicts with TxnId(3)
        assert!(det.has_conflict(b"key2", TxnId(3)));
    }

    #[test]
    fn retry_on_write_conflict_succeeds_after_retries() {
        use std::cell::Cell;
        let counter = Cell::new(0);
        let result = retry_on_write_conflict(3, || {
            counter.set(counter.get() + 1);
            if counter.get() < 3 {
                Err(write_conflict("conflict"))
            } else {
                Ok(42)
            }
        });
        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.get(), 3);
    }

    #[test]
    fn retry_on_write_conflict_exhausts_retries() {
        let result: Result<()> = retry_on_write_conflict(2, || Err(write_conflict("conflict")));
        assert_eq!(result.unwrap_err().code(), mdb_common::ErrorCode::WriteConflict);
    }

    #[test]
    fn retry_non_conflict_error_returns_immediately() {
        let result: Result<()> = retry_on_write_conflict(3, || {
            Err(mdb_common::MdbError::bare(mdb_common::ErrorCode::BadValue))
        });
        assert_eq!(result.unwrap_err().code(), mdb_common::ErrorCode::BadValue);
    }
}
