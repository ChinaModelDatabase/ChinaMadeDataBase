//! Read concern determines the consistency and isolation level of read
//! operations against a replica set or sharded cluster.
//!
//! Mirrors MongoDB read concern levels:
//! - `Local`: reads from the current node (may see uncommitted data)
//! - `Majority`: reads only data confirmed by a majority of nodes
//! - `Linearizable`: reads with strict linearizability guarantee
//! - `Snapshot`: reads from a consistent point-in-time snapshot

use std::fmt;

use crate::types::Result;

/// The read concern level for a read operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadConcern {
    /// Read from the current node. May return data that has not been
    /// replicated to a majority (default for non-transactional reads).
    Local,
    /// Read only data that has been acknowledged by a majority of replica
    /// set members. Guarantees the read reflects committed data.
    Majority,
    /// Read with linearizability guarantees. The result reflects the most
    /// recent write that completed before the read started. Requires
    /// majority read concern internally.
    Linearizable,
    /// Read from a consistent snapshot. Used within multi-statement
    /// transactions to provide snapshot isolation.
    Snapshot,
}

impl Default for ReadConcern {
    fn default() -> Self {
        Self::Local
    }
}

impl ReadConcern {
    /// The level name as a string for the wire protocol.
    pub fn level_name(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Majority => "majority",
            Self::Linearizable => "linearizable",
            Self::Snapshot => "snapshot",
        }
    }

    /// Whether this concern level requires majority replication.
    pub fn requires_majority(&self) -> bool {
        matches!(
            self,
            Self::Majority | Self::Linearizable | Self::Snapshot
        )
    }

    /// Whether this concern is suitable for use within a transaction.
    ///
    /// Transactions always use `Snapshot` internally; `Local` and
    /// `Majority` can be used for non-transactional reads within a session.
    pub fn is_valid_in_transaction(&self) -> bool {
        matches!(self, Self::Snapshot | Self::Majority)
    }

    /// Validate the read concern level.
    pub fn validate(&self) -> Result<()> {
        // All variants are valid; this exists for forward-compatibility
        // when new levels are added.
        Ok(())
    }
}

impl fmt::Display for ReadConcern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "level:{}", self.level_name())
    }
}

/// Read concern options that can be attached to a read operation.
#[derive(Debug, Clone)]
pub struct ReadConcernOptions {
    /// The read concern level.
    pub level: ReadConcern,
    /// Optional `afterClusterTime` for causal consistency (see `CausalSession`).
    pub after_cluster_time: Option<u64>,
}

impl Default for ReadConcernOptions {
    fn default() -> Self {
        Self {
            level: ReadConcern::default(),
            after_cluster_time: None,
        }
    }
}

impl ReadConcernOptions {
    /// Create options with just a level.
    pub fn new(level: ReadConcern) -> Self {
        Self {
            level,
            after_cluster_time: None,
        }
    }

    /// Set `afterClusterTime` for causal consistency.
    pub fn with_after_cluster_time(mut self, cluster_time: u64) -> Self {
        self.after_cluster_time = Some(cluster_time);
        self
    }

    /// Whether this is a causal read (has afterClusterTime).
    pub fn is_causal(&self) -> bool {
        self.after_cluster_time.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_read_concern_is_local() {
        assert_eq!(ReadConcern::default(), ReadConcern::Local);
    }

    #[test]
    fn level_names() {
        assert_eq!(ReadConcern::Local.level_name(), "local");
        assert_eq!(ReadConcern::Majority.level_name(), "majority");
        assert_eq!(ReadConcern::Linearizable.level_name(), "linearizable");
        assert_eq!(ReadConcern::Snapshot.level_name(), "snapshot");
    }

    #[test]
    fn requires_majority() {
        assert!(!ReadConcern::Local.requires_majority());
        assert!(ReadConcern::Majority.requires_majority());
        assert!(ReadConcern::Linearizable.requires_majority());
        assert!(ReadConcern::Snapshot.requires_majority());
    }

    #[test]
    fn valid_in_transaction() {
        assert!(ReadConcern::Snapshot.is_valid_in_transaction());
        assert!(ReadConcern::Majority.is_valid_in_transaction());
        assert!(!ReadConcern::Local.is_valid_in_transaction());
        assert!(!ReadConcern::Linearizable.is_valid_in_transaction());
    }

    #[test]
    fn validate_all_levels() {
        assert!(ReadConcern::Local.validate().is_ok());
        assert!(ReadConcern::Majority.validate().is_ok());
        assert!(ReadConcern::Linearizable.validate().is_ok());
        assert!(ReadConcern::Snapshot.validate().is_ok());
    }

    #[test]
    fn display_read_concern() {
        assert_eq!(ReadConcern::Local.to_string(), "level:local");
        assert_eq!(ReadConcern::Majority.to_string(), "level:majority");
        assert_eq!(ReadConcern::Linearizable.to_string(), "level:linearizable");
        assert_eq!(ReadConcern::Snapshot.to_string(), "level:snapshot");
    }

    #[test]
    fn read_concern_options_defaults() {
        let opts = ReadConcernOptions::default();
        assert_eq!(opts.level, ReadConcern::Local);
        assert!(!opts.is_causal());
        assert_eq!(opts.after_cluster_time, None);
    }

    #[test]
    fn read_concern_options_with_cluster_time() {
        let opts = ReadConcernOptions::new(ReadConcern::Majority)
            .with_after_cluster_time(42);
        assert_eq!(opts.level, ReadConcern::Majority);
        assert!(opts.is_causal());
        assert_eq!(opts.after_cluster_time, Some(42));
    }
}
