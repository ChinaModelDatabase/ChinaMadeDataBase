//! Retryable writes for transient error recovery.
//!
//! When a write operation fails with a transient error (`WriteConflict`,
//! `NotMaster`, `PrimarySteppedDown`, etc.), the write is automatically
//! retried up to a configurable limit. Each retry increments the session's
//! `txnNumber` to ensure idempotency tracking on the server side.

use std::collections::VecDeque;

use crate::types::Result;

/// Maximum number of retry attempts for transient errors.
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// Error codes that are eligible for retryable write behavior.
///
/// These correspond to MongoDB's `TransientTransactionError` category for
/// write operations. The server may return these during elections, replica
/// set failovers, or temporary write conflicts.
pub fn is_retryable_write_error(code: mdb_common::ErrorCode) -> bool {
    use mdb_common::ErrorCode::*;
    matches!(
        code,
        WriteConflict
            | NotMaster
            | NotMasterNoSlaveOk
            | PrimarySteppedDown
            | ReplicaSetNoPrimary
            | ShutdownInProgress
            | HostUnreachable
            | HostNotFound
            | LockBusy
    )
}

/// Result of a single write attempt, used for deduplication tracking.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// The txnNumber used for this write.
    pub txn_number: u64,
    /// Whether the write succeeded.
    pub ok: bool,
    /// Optional error code if the write failed.
    pub error_code: Option<mdb_common::ErrorCode>,
}

/// Per-session write history for deduplication.
///
/// Stores the last N write results so the server can detect whether a
/// retried operation has already been applied. This prevents duplicate
/// writes when a client retries after a timeout even though the original
/// write succeeded on the server.
#[derive(Debug)]
pub struct WriteResultLog {
    /// Maximum number of results to retain per session.
    max_entries: usize,
    /// Ring buffer of recent write results, newest at the back.
    results: VecDeque<WriteResult>,
}

impl WriteResultLog {
    /// Create a new log with the given maximum entry count.
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            results: VecDeque::with_capacity(max_entries),
        }
    }

    /// Create a log with the default capacity (256 entries).
    pub fn with_default_capacity() -> Self {
        Self::new(256)
    }

    /// Record a write result.
    pub fn record(&mut self, result: WriteResult) {
        if self.results.len() >= self.max_entries {
            self.results.pop_front();
        }
        self.results.push_back(result);
    }

    /// Check if a write with the given txnNumber has already been recorded
    /// as successful.
    pub fn is_duplicate(&self, txn_number: u64) -> bool {
        self.results
            .iter()
            .any(|r| r.txn_number == txn_number && r.ok)
    }

    /// Get the result of a previous write by txnNumber.
    pub fn get(&self, txn_number: u64) -> Option<&WriteResult> {
        self.results.iter().rfind(|r| r.txn_number == txn_number)
    }

    /// Number of stored results.
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }
}

/// Trait for operations that can be retried on transient errors.
///
/// Implementors provide a closure that may fail with transient errors;
/// the `RetryableWrite` infrastructure handles automatic retries with
/// proper backoff and txnNumber management.
pub trait RetryableWrite {
    /// Execute the write operation, retrying on transient errors.
    ///
    /// `current_txn_number` is the session's current txnNumber; each retry
    /// increments it. Returns the final result.
    fn execute_with_retry<F>(
        &self,
        max_retries: u32,
        txn_number: &mut u64,
        f: F,
    ) -> Result<WriteResult>
    where
        F: FnMut(u64) -> Result<()>;

    /// Execute with the default retry count.
    fn execute<F>(&self, txn_number: &mut u64, f: F) -> Result<WriteResult>
    where
        F: FnMut(u64) -> Result<()>,
    {
        self.execute_with_retry(DEFAULT_MAX_RETRIES, txn_number, f)
    }
}

/// Default implementation of `RetryableWrite`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefaultRetryableWrite;

impl RetryableWrite for DefaultRetryableWrite {
    fn execute_with_retry<F>(
        &self,
        max_retries: u32,
        txn_number: &mut u64,
        mut f: F,
    ) -> Result<WriteResult>
    where
        F: FnMut(u64) -> Result<()>,
    {
        let mut last_err: Option<mdb_common::MdbError> = None;

        for attempt in 0..=max_retries {
            *txn_number += 1;
            let current = *txn_number;

            match f(current) {
                Ok(()) => {
                    return Ok(WriteResult {
                        txn_number: current,
                        ok: true,
                        error_code: None,
                    });
                }
                Err(e) if is_retryable_write_error(e.code()) => {
                    last_err = Some(e);
                    // Continue to retry
                    if attempt == max_retries {
                        break;
                    }
                }
                Err(e) => {
                    // Non-retryable error: return immediately
                    return Ok(WriteResult {
                        txn_number: current,
                        ok: false,
                        error_code: Some(e.code()),
                    });
                }
            }
        }

        // All retries exhausted
        let err = last_err.unwrap_or_else(|| {
            mdb_common::MdbError::new(
                mdb_common::ErrorCode::WriteConflict,
                "exhausted retries without resolution",
            )
        });
        let _code = err.code();
        Err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_error_codes() {
        assert!(is_retryable_write_error(mdb_common::ErrorCode::WriteConflict));
        assert!(is_retryable_write_error(mdb_common::ErrorCode::NotMaster));
        assert!(is_retryable_write_error(mdb_common::ErrorCode::PrimarySteppedDown));
        assert!(is_retryable_write_error(mdb_common::ErrorCode::ShutdownInProgress));
        assert!(is_retryable_write_error(mdb_common::ErrorCode::LockBusy));
        assert!(!is_retryable_write_error(mdb_common::ErrorCode::DuplicateKey));
        assert!(!is_retryable_write_error(mdb_common::ErrorCode::TypeMismatch));
        assert!(!is_retryable_write_error(mdb_common::ErrorCode::BadValue));
    }

    #[test]
    fn write_result_log_record_and_get() {
        let mut log = WriteResultLog::new(10);
        log.record(WriteResult {
            txn_number: 1,
            ok: true,
            error_code: None,
        });
        log.record(WriteResult {
            txn_number: 2,
            ok: false,
            error_code: Some(mdb_common::ErrorCode::WriteConflict),
        });

        assert_eq!(log.len(), 2);
        let r = log.get(1).unwrap();
        assert!(r.ok);
        let r = log.get(2).unwrap();
        assert!(!r.ok);
    }

    #[test]
    fn write_result_log_is_duplicate() {
        let mut log = WriteResultLog::new(10);
        log.record(WriteResult {
            txn_number: 5,
            ok: true,
            error_code: None,
        });
        assert!(log.is_duplicate(5));
        assert!(!log.is_duplicate(6));

        // Failed writes are not considered duplicates
        log.record(WriteResult {
            txn_number: 7,
            ok: false,
            error_code: Some(mdb_common::ErrorCode::WriteConflict),
        });
        assert!(!log.is_duplicate(7));
    }

    #[test]
    fn write_result_log_evicts_oldest() {
        let mut log = WriteResultLog::new(3);
        for i in 1..=5 {
            log.record(WriteResult {
                txn_number: i,
                ok: true,
                error_code: None,
            });
        }
        assert_eq!(log.len(), 3);
        assert!(!log.is_duplicate(1));
        assert!(!log.is_duplicate(2));
        assert!(log.is_duplicate(3));
        assert!(log.is_duplicate(4));
        assert!(log.is_duplicate(5));
    }

    #[test]
    fn retryable_write_succeeds_first_try() {
        let rw = DefaultRetryableWrite;
        let mut txn_num = 0;
        let result = rw.execute(&mut txn_num, |txn| {
            assert_eq!(txn, 1);
            Ok(())
        });
        assert!(result.unwrap().ok);
        assert_eq!(txn_num, 1);
    }

    #[test]
    fn retryable_write_succeeds_after_retries() {
        use std::cell::Cell;
        let counter = Cell::new(0);
        let rw = DefaultRetryableWrite;
        let mut txn_num = 0;

        let result = rw.execute_with_retry(3, &mut txn_num, |_txn| {
            counter.set(counter.get() + 1);
            if counter.get() < 3 {
                Err(mdb_common::MdbError::new(
                    mdb_common::ErrorCode::WriteConflict,
                    "conflict",
                ))
            } else {
                Ok(())
            }
        });
        assert!(result.unwrap().ok);
        assert_eq!(counter.get(), 3);
        assert_eq!(txn_num, 3);
    }

    #[test]
    fn retryable_write_exhausts_retries() {
        let rw = DefaultRetryableWrite;
        let mut txn_num = 0;

        let result = rw.execute_with_retry(2, &mut txn_num, |_txn| {
            Err(mdb_common::MdbError::new(
                mdb_common::ErrorCode::WriteConflict,
                "conflict",
            ))
        });
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code(),
            mdb_common::ErrorCode::WriteConflict
        );
        assert_eq!(txn_num, 3); // 1 initial + 2 retries
    }

    #[test]
    fn retryable_write_non_retryable_error_returns_immediately() {
        let rw = DefaultRetryableWrite;
        let mut txn_num = 0;

        let result = rw.execute(&mut txn_num, |_txn| {
            Err(mdb_common::MdbError::bare(mdb_common::ErrorCode::DuplicateKey))
        });
        let wr = result.unwrap();
        assert!(!wr.ok);
        assert_eq!(wr.error_code, Some(mdb_common::ErrorCode::DuplicateKey));
        assert_eq!(txn_num, 1); // No retry
    }

    #[test]
    fn retryable_write_not_master_is_retryable() {
        use std::cell::Cell;
        let rw = DefaultRetryableWrite;
        let mut txn_num = 0;
        let counter = Cell::new(0);

        let result = rw.execute_with_retry(3, &mut txn_num, |_txn| {
            counter.set(counter.get() + 1);
            if counter.get() < 2 {
                Err(mdb_common::MdbError::bare(mdb_common::ErrorCode::NotMaster))
            } else {
                Ok(())
            }
        });
        assert!(result.unwrap().ok);
    }

    #[test]
    fn txn_number_increments_across_retries() {
        let rw = DefaultRetryableWrite;
        let mut txn_num = 0;
        let seen = std::cell::Cell::new(Vec::new());

        let _result = rw.execute_with_retry(3, &mut txn_num, |txn| {
            let mut v = seen.take();
            v.push(txn);
            seen.set(v);
            Err(mdb_common::MdbError::new(
                mdb_common::ErrorCode::WriteConflict,
                "conflict",
            ))
        });

        let seen = seen.take();
        assert_eq!(seen, vec![1, 2, 3, 4]);
    }
}
