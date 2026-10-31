//! 事务与 MVCC：快照隔离、写冲突检测、2PC 协调、TSO 分配
//!
//! This crate provides the transaction subsystem for MDB, implementing
//! Multi-Version Concurrency Control (MVCC) with snapshot isolation,
//! optimistic write conflict detection, two-phase commit (2PC),
//! and logical session management.
//!
//! # Architecture
//!
//! ```text
//! TransactionManager
//!   ├── SnapshotManager    (MVCC snapshot allocation)
//!   ├── WriteConflictDetector (key-level optimistic locking)
//!   ├── Transaction        (read/write buffer + 2PC)
//!   ├── SessionManager     (logical session lifecycle)
//!   ├── CausalSession      (causal consistency tracking)
//!   ├── RetryableWrite     (transient error retry)
//!   └── Concerns           (read/write concern levels)
//! ```
//!
//! # Dependencies
//!
//! Only `mdb-common` is used for error types (`MdbError`, `ErrorCode`).

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod causal;
pub mod concern;
pub mod conflict;
pub mod manager;
pub mod mvcc;
pub mod preference;
pub mod retry;
pub mod session;
pub mod txn;
pub mod types;

// Re-exports for convenience
pub use causal::{CausalSession, SharedCausalSession};
pub use concern::{ReadConcern, WriteConcern};
pub use conflict::{WriteConflictDetector, retry_on_write_conflict};
pub use manager::TransactionManager;
pub use mvcc::SnapshotManager;
pub use preference::{ReadPreference, ReadPreferenceOptions, WritePreference, WritePreferenceOptions};
pub use retry::{DefaultRetryableWrite, RetryableWrite, WriteResult, WriteResultLog};
pub use session::SessionManager;
pub use txn::{Transaction, TxnStorage};
pub use types::{
    ReadConflict, Session, SessionId, Snapshot, TxnId, TxnState, WriteOp, SESSION_TIMEOUT,
};
