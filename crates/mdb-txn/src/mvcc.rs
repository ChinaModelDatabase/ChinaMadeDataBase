//! Multi-Version Concurrency Control (MVCC) snapshot management.
//!
//! Tracks active snapshots to determine which versions of data are visible
//! to concurrent transactions, and which versions can be pruned during compaction.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::Snapshot;

/// Manages MVCC snapshots. Thread-safe via internal locking.
///
/// The snapshot manager maintains a globally monotonically increasing sequence
/// number (seqno) and tracks which snapshots are currently active. The oldest
/// active snapshot determines the compaction floor: data older than this
/// can be safely discarded.
#[derive(Debug)]
pub struct SnapshotManager {
    /// Next seqno to be assigned.
    next_seqno: AtomicU64,
    /// Set of currently active snapshot seqnos.
    /// Protected by RwLock for concurrent reads / exclusive writes.
    active: std::sync::RwLock<Vec<u64>>,
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self {
            next_seqno: AtomicU64::new(1),
            active: std::sync::RwLock::new(Vec::new()),
        }
    }

    /// Allocate a new snapshot seqno.
    ///
    /// Returns a `Snapshot` with a seqno that reflects the state of all
    /// committed writes up to (but not including) any concurrent writes
    /// that haven't been committed yet.
    pub fn begin_snapshot(&self) -> Snapshot {
        let seqno = self.next_seqno.fetch_add(1, Ordering::SeqCst);
        {
            let mut active = self.write_lock();
            active.push(seqno);
        }
        Snapshot::new(seqno)
    }

    /// Release a previously acquired snapshot.
    ///
    /// After this call, the seqno may become the oldest active snapshot,
    /// enabling compaction to prune data older than it.
    pub fn end_snapshot(&self, seqno: u64) {
        let mut active = self.write_lock();
        active.retain(|&s| s != seqno);
    }

    /// Returns the oldest active snapshot seqno, or `None` if no snapshots
    /// are active. Used by the compaction engine to determine the pruning floor.
    pub fn oldest_active_snapshot(&self) -> Option<u64> {
        let active = self.read_lock();
        active.iter().copied().min()
    }

    /// Returns the current seqno counter value (next seqno to be assigned).
    pub fn current_seqno(&self) -> u64 {
        self.next_seqno.load(Ordering::SeqCst)
    }

    /// Returns the number of currently active snapshots.
    pub fn active_count(&self) -> usize {
        let active = self.read_lock();
        active.len()
    }

    fn read_lock(&self) -> std::sync::RwLockReadGuard<'_, Vec<u64>> {
        self.active.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_lock(&self) -> std::sync::RwLockWriteGuard<'_, Vec<u64>> {
        self.active.write().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn begin_returns_increasing_seqnos() {
        let mgr = SnapshotManager::new();
        let s1 = mgr.begin_snapshot();
        let s2 = mgr.begin_snapshot();
        assert!(s1.seqno < s2.seqno);
        assert_eq!(s1.seqno, 1);
        assert_eq!(s2.seqno, 2);
    }

    #[test]
    fn end_snapshot_removes_from_active() {
        let mgr = SnapshotManager::new();
        let s1 = mgr.begin_snapshot();
        let s2 = mgr.begin_snapshot();
        assert_eq!(mgr.active_count(), 2);

        mgr.end_snapshot(s1.seqno);
        assert_eq!(mgr.active_count(), 1);
        assert_eq!(mgr.oldest_active_snapshot(), Some(s2.seqno));
    }

    #[test]
    fn oldest_active_snapshot() {
        let mgr = SnapshotManager::new();
        assert_eq!(mgr.oldest_active_snapshot(), None);

        let s1 = mgr.begin_snapshot();
        assert_eq!(mgr.oldest_active_snapshot(), Some(s1.seqno));

        let s2 = mgr.begin_snapshot();
        assert_eq!(mgr.oldest_active_snapshot(), Some(s1.seqno));

        mgr.end_snapshot(s1.seqno);
        assert_eq!(mgr.oldest_active_snapshot(), Some(s2.seqno));

        mgr.end_snapshot(s2.seqno);
        assert_eq!(mgr.oldest_active_snapshot(), None);
    }

    #[test]
    fn concurrent_begin_snapshot() {
        let mgr = std::sync::Arc::new(SnapshotManager::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let mgr = mgr.clone();
            handles.push(thread::spawn(move || {
                let _snap = mgr.begin_snapshot();
                thread::yield_now();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All 10 snapshots still active
        assert_eq!(mgr.active_count(), 10);
    }

    #[test]
    fn current_seqno_monotonic() {
        let mgr = SnapshotManager::new();
        let s1 = mgr.begin_snapshot();
        let s2 = mgr.begin_snapshot();
        assert_eq!(mgr.current_seqno(), s2.seqno + 1);
        mgr.end_snapshot(s1.seqno);
        // current_seqno doesn't decrease
        assert!(mgr.current_seqno() >= s2.seqno + 1);
    }
}
