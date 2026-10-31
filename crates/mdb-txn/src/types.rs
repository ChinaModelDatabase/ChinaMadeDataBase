//! Transaction core types.

use std::fmt;
use std::time::{Duration, Instant};

use mdb_common::{ErrorCode, MdbError};

/// Unique transaction identifier (monotonically increasing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId(pub u64);

impl TxnId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Txn({})", self.0)
    }
}

/// Transaction lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxnState {
    /// Transaction is running, accepting reads and writes.
    Active,
    /// Two-phase commit Phase 1: validation in progress.
    Preparing,
    /// Transaction committed successfully.
    Committed,
    /// Transaction aborted (either explicit or due to conflict).
    Aborted,
}

impl TxnState {
    pub const fn as_str(self) -> &'static str {
        match self {
            TxnState::Active => "active",
            TxnState::Preparing => "preparing",
            TxnState::Committed => "committed",
            TxnState::Aborted => "aborted",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, TxnState::Committed | TxnState::Aborted)
    }
}

impl fmt::Display for TxnState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single write operation buffered in a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOp {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
}

impl WriteOp {
    pub fn key(&self) -> &[u8] {
        match self {
            WriteOp::Put { key, .. } => key,
            WriteOp::Delete { key } => key,
        }
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, WriteOp::Delete { .. })
    }
}

/// A read conflict record for optimistic validation.
#[derive(Debug, Clone)]
pub struct ReadConflict {
    /// The key that was read.
    pub key: Vec<u8>,
    /// The seqno of the value read from storage.
    pub seqno: u64,
}

/// MVCC snapshot reference.
#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    /// Sequence number at which this snapshot was taken.
    /// Only data with seqno <= this value is visible.
    pub seqno: u64,
}

impl Snapshot {
    pub const fn new(seqno: u64) -> Self {
        Self { seqno }
    }
}

/// Default session timeout: 30 minutes (matching MongoDB).
pub const SESSION_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Session ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Session({})", self.0)
    }
}

/// A logical client session.
#[derive(Debug)]
pub struct Session {
    pub id: SessionId,
    pub txn_number: u64,
    pub last_used: Instant,
}

impl Session {
    pub fn new(id: SessionId) -> Self {
        Self {
            id,
            txn_number: 0,
            last_used: Instant::now(),
        }
    }

    pub fn next_txn_number(&mut self) -> u64 {
        self.txn_number += 1;
        self.last_used = Instant::now();
        self.txn_number
    }

    pub fn touch(&mut self) {
        self.last_used = Instant::now();
    }

    pub fn is_expired(&self, timeout: Duration) -> bool {
        self.last_used.elapsed() >= timeout
    }
}

/// Result type alias for this crate.
pub type Result<T> = std::result::Result<T, MdbError>;

/// Create a WriteConflict error.
pub fn write_conflict(msg: impl Into<Box<str>>) -> MdbError {
    MdbError::new(ErrorCode::WriteConflict, msg)
}

/// Create a WriteConcernFailed error.
pub fn write_concern_failed(msg: impl Into<Box<str>>) -> MdbError {
    MdbError::new(ErrorCode::WriteConcernFailed, msg)
}

/// Create a NotMaster error.
pub fn not_master(msg: impl Into<Box<str>>) -> MdbError {
    MdbError::new(ErrorCode::NotMaster, msg)
}

/// Create an ExceededTimeLimit error.
pub fn exceeded_time_limit(msg: impl Into<Box<str>>) -> MdbError {
    MdbError::new(ErrorCode::ExceededTimeLimit, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txn_id_ordering() {
        assert!(TxnId(1) < TxnId(2));
        assert!(TxnId(100) > TxnId(99));
    }

    #[test]
    fn txn_id_display() {
        assert_eq!(TxnId(42).to_string(), "Txn(42)");
    }

    #[test]
    fn txn_state_transitions() {
        let s = TxnState::Active;
        assert!(!s.is_terminal());
        assert_eq!(s.as_str(), "active");

        let c = TxnState::Committed;
        assert!(c.is_terminal());

        let a = TxnState::Aborted;
        assert!(a.is_terminal());
    }

    #[test]
    fn write_op_key_and_is_delete() {
        let put = WriteOp::Put { key: b"k".to_vec(), value: b"v".to_vec() };
        assert_eq!(put.key(), b"k");
        assert!(!put.is_delete());

        let del = WriteOp::Delete { key: b"k".to_vec() };
        assert_eq!(del.key(), b"k");
        assert!(del.is_delete());
    }

    #[test]
    fn snapshot_creation() {
        let s = Snapshot::new(100);
        assert_eq!(s.seqno, 100);
    }

    #[test]
    fn session_txn_number_increments() {
        let mut session = Session::new(SessionId(1));
        assert_eq!(session.next_txn_number(), 1);
        assert_eq!(session.next_txn_number(), 2);
    }

    #[test]
    fn session_timeout() {
        let mut session = Session::new(SessionId(1));
        assert!(!session.is_expired(Duration::from_secs(1)));
        // Simulate expiry: last_used was 2000 seconds ago, timeout is 1800s
        session.last_used = Instant::now()
            .checked_sub(Duration::from_secs(2000))
            .unwrap();
        assert!(session.is_expired(Duration::from_secs(30 * 60)));
    }
}
