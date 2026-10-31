//! Causal consistency for session-based operations.
//!
//! A `CausalSession` tracks `clusterTime` and `operationTime` to ensure
//! that reads within a session see a causally consistent view of data:
//! if operation A happens-before operation B, then B observes the results
//! of A.
//!
//! # Protocol
//!
//! 1. Each write operation updates the session's `operationTime` to the
//!    cluster timestamp at which the write was applied.
//! 2. Each read operation includes `afterClusterTime` set to the session's
//!    current `clusterTime`, ensuring the read waits until the server has
//!    advanced past that timestamp.
//! 3. When a read returns, its `operationTime` is merged into the session.

use std::sync::{Arc, Mutex};

/// Tracks causal consistency state for a logical session.
///
/// Thread-safe: internal state is protected by a `Mutex`.
#[derive(Debug)]
pub struct CausalSession {
    inner: Mutex<CausalInner>,
}

#[derive(Debug)]
struct CausalInner {
    /// The cluster time observed by this session (maximum timestamp seen
    /// from any operation).
    cluster_time: u64,
    /// The timestamp of the last write operation performed by this session.
    operation_time: Option<u64>,
}

impl CausalSession {
    /// Create a new causal session starting at cluster time 0.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(CausalInner {
                cluster_time: 0,
                operation_time: None,
            }),
        }
    }

    /// Create a new causal session starting at a given cluster time.
    pub fn with_cluster_time(initial: u64) -> Self {
        Self {
            inner: Mutex::new(CausalInner {
                cluster_time: initial,
                operation_time: None,
            }),
        }
    }

    /// Get the current cluster time.
    pub fn cluster_time(&self) -> u64 {
        self.inner.lock().unwrap().cluster_time
    }

    /// Get the operation time of the last write (if any).
    pub fn operation_time(&self) -> Option<u64> {
        self.inner.lock().unwrap().operation_time
    }

    /// Get the `afterClusterTime` value to include in a read operation.
    ///
    /// Returns `None` if no causal consistency is needed (session has not
    /// observed any operations yet).
    pub fn after_cluster_time(&self) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        if inner.cluster_time > 0 {
            Some(inner.cluster_time)
        } else {
            None
        }
    }

    /// Advance the session's cluster time after observing a server response.
    ///
    /// The new cluster time is the maximum of the current and provided values.
    pub fn advance_cluster_time(&self, new_time: u64) {
        let mut inner = self.inner.lock().unwrap();
        if new_time > inner.cluster_time {
            inner.cluster_time = new_time;
        }
    }

    /// Record the operation time from a write operation.
    ///
    /// Also advances cluster time if the operation time is greater.
    pub fn update_operation_time(&self, op_time: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.operation_time = Some(op_time);
        if op_time > inner.cluster_time {
            inner.cluster_time = op_time;
        }
    }

    /// Merge cluster time and operation time from a server response.
    ///
    /// Called after any operation to keep the session's view of the cluster
    /// timeline up to date.
    pub fn merge_response(&self, cluster_time: u64, operation_time: Option<u64>) {
        let mut inner = self.inner.lock().unwrap();
        if cluster_time > inner.cluster_time {
            inner.cluster_time = cluster_time;
        }
        if let Some(op_time) = operation_time {
            inner.operation_time = Some(op_time);
            if op_time > inner.cluster_time {
                inner.cluster_time = op_time;
            }
        }
    }
}

impl Default for CausalSession {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared causal session wrapper for use across concurrent operations.
#[derive(Debug, Clone)]
pub struct SharedCausalSession {
    session: Arc<CausalSession>,
}

impl SharedCausalSession {
    /// Create a new shared causal session.
    pub fn new() -> Self {
        Self {
            session: Arc::new(CausalSession::new()),
        }
    }

    /// Create from an existing causal session.
    pub fn from_session(session: CausalSession) -> Self {
        Self {
            session: Arc::new(session),
        }
    }

    /// Get a reference to the inner session.
    pub fn inner(&self) -> &CausalSession {
        &self.session
    }

    /// Get the current cluster time.
    pub fn cluster_time(&self) -> u64 {
        self.session.cluster_time()
    }

    /// Get the operation time.
    pub fn operation_time(&self) -> Option<u64> {
        self.session.operation_time()
    }

    /// Get `afterClusterTime` for a read.
    pub fn after_cluster_time(&self) -> Option<u64> {
        self.session.after_cluster_time()
    }

    /// Advance cluster time.
    pub fn advance_cluster_time(&self, new_time: u64) {
        self.session.advance_cluster_time(new_time);
    }

    /// Update operation time from a write.
    pub fn update_operation_time(&self, op_time: u64) {
        self.session.update_operation_time(op_time);
    }

    /// Merge a server response.
    pub fn merge_response(&self, cluster_time: u64, operation_time: Option<u64>) {
        self.session.merge_response(cluster_time, operation_time);
    }
}

impl Default for SharedCausalSession {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_session_starts_at_zero() {
        let session = CausalSession::new();
        assert_eq!(session.cluster_time(), 0);
        assert_eq!(session.operation_time(), None);
        assert_eq!(session.after_cluster_time(), None);
    }

    #[test]
    fn with_cluster_time_initial() {
        let session = CausalSession::with_cluster_time(100);
        assert_eq!(session.cluster_time(), 100);
        assert_eq!(session.after_cluster_time(), Some(100));
    }

    #[test]
    fn advance_cluster_time() {
        let session = CausalSession::new();
        session.advance_cluster_time(50);
        assert_eq!(session.cluster_time(), 50);
        assert_eq!(session.after_cluster_time(), Some(50));

        // Does not go backwards
        session.advance_cluster_time(30);
        assert_eq!(session.cluster_time(), 50);

        // Advances forward
        session.advance_cluster_time(60);
        assert_eq!(session.cluster_time(), 60);
    }

    #[test]
    fn update_operation_time() {
        let session = CausalSession::new();
        session.update_operation_time(42);
        assert_eq!(session.operation_time(), Some(42));
        assert_eq!(session.cluster_time(), 42);
    }

    #[test]
    fn merge_response_advances_both() {
        let session = CausalSession::new();
        session.advance_cluster_time(10);
        session.merge_response(20, Some(25));
        assert_eq!(session.cluster_time(), 25);
        assert_eq!(session.operation_time(), Some(25));
    }

    #[test]
    fn merge_response_without_operation_time() {
        let session = CausalSession::new();
        session.advance_cluster_time(10);
        session.merge_response(20, None);
        assert_eq!(session.cluster_time(), 20);
        assert_eq!(session.operation_time(), None);
    }

    #[test]
    fn shared_causal_session_clones_share_state() {
        let shared = SharedCausalSession::new();
        let shared2 = shared.clone();

        shared.advance_cluster_time(100);
        assert_eq!(shared2.cluster_time(), 100);
    }

    #[test]
    fn causal_session_default_is_zero() {
        let session = CausalSession::default();
        assert_eq!(session.cluster_time(), 0);
    }

    #[test]
    fn multiple_advances_converge_to_max() {
        let session = CausalSession::new();
        session.advance_cluster_time(5);
        session.advance_cluster_time(3);
        session.advance_cluster_time(7);
        assert_eq!(session.cluster_time(), 7);
    }

    #[test]
    fn merge_with_lower_time_is_noop() {
        let session = CausalSession::new();
        session.advance_cluster_time(50);
        session.merge_response(10, Some(20));
        assert_eq!(session.cluster_time(), 50);
        assert_eq!(session.operation_time(), Some(20));
    }
}
