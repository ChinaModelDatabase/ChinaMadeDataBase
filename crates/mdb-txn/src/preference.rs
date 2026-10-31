//! Read and write preference routing.
//!
//! Determines which replica set member should receive a read or write
//! operation, and how to handle failover when the preferred node is
//! unavailable.
//!
//! # Read Preferences
//!
//! - `Primary`: all reads go to the primary (default, strongest consistency)
//! - `PrimaryPreferred`: prefer primary, fall back to secondary
//! - `Secondary`: all reads go to secondaries (may be stale)
//! - `SecondaryPreferred`: prefer secondaries, fall back to primary
//! - `Nearest`: read from the node with the lowest network latency
//!
//! # Write Preferences
//!
//! - `Primary`: all writes go to the primary (default, only option for
//!   non-sharded replica sets)
//! - `PrimaryPreferred`: prefer primary, allow secondary in sharded clusters

use std::fmt;

/// Read preference for routing read operations to replica set members.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadPreference {
    /// All reads go to the primary. Provides strongest consistency.
    Primary,
    /// Prefer primary, but accept reads from secondaries if the primary
    /// is unavailable.
    PrimaryPreferred,
    /// All reads go to secondaries. Provides eventual consistency with
    /// reduced load on the primary.
    Secondary,
    /// Prefer secondaries, but fall back to primary if no secondaries
    /// are available.
    SecondaryPreferred,
    /// Read from the node with the lowest network latency, regardless
    /// of its role.
    Nearest,
}

impl Default for ReadPreference {
    fn default() -> Self {
        Self::Primary
    }
}

impl ReadPreference {
    /// The wire protocol name for this preference.
    pub fn mode_name(&self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::PrimaryPreferred => "primaryPreferred",
            Self::Secondary => "secondary",
            Self::SecondaryPreferred => "secondaryPreferred",
            Self::Nearest => "nearest",
        }
    }

    /// Whether this preference allows reading from secondaries.
    pub fn allows_secondary(&self) -> bool {
        matches!(
            self,
            Self::Secondary | Self::SecondaryPreferred | Self::Nearest
        )
    }

    /// Whether this preference requires reading from the primary.
    pub fn requires_primary(&self) -> bool {
        matches!(self, Self::Primary)
    }
}

impl fmt::Display for ReadPreference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mode:{}", self.mode_name())
    }
}

/// Write preference for routing write operations.
///
/// In MongoDB, all writes ultimately go to the primary for a replica set.
/// `PrimaryPreferred` is relevant for sharded clusters where the config
/// server or a secondary might accept certain writes during failover.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WritePreference {
    /// All writes go to the primary (default).
    Primary,
    /// Prefer primary, but allow writes to a designated secondary in
    /// specific sharded topologies.
    PrimaryPreferred,
}

impl Default for WritePreference {
    fn default() -> Self {
        Self::Primary
    }
}

impl WritePreference {
    /// The wire protocol name for this preference.
    pub fn mode_name(&self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::PrimaryPreferred => "primaryPreferred",
        }
    }

    /// Whether this preference allows writing to secondaries.
    pub fn allows_secondary(&self) -> bool {
        matches!(self, Self::PrimaryPreferred)
    }
}

impl fmt::Display for WritePreference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mode:{}", self.mode_name())
    }
}

/// Tag set for fine-grained replica set member selection.
///
/// A tag set is a set of key-value pairs that must all match for a
/// replica set member to be considered a candidate for reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagSet {
    /// Tag patterns to match. Each pattern is a set of key-value pairs.
    /// A member must match at least one pattern to be considered.
    pub tags: Vec<std::collections::HashMap<String, String>>,
}

impl TagSet {
    /// Create an empty tag set (matches all members).
    pub fn empty() -> Self {
        Self { tags: Vec::new() }
    }

    /// Create a tag set with a single pattern.
    pub fn single(pattern: std::collections::HashMap<String, String>) -> Self {
        Self {
            tags: vec![pattern],
        }
    }

    /// Create a tag set from a list of patterns.
    pub fn new(patterns: Vec<std::collections::HashMap<String, String>>) -> Self {
        Self { tags: patterns }
    }

    /// Whether this tag set matches all members (empty = match all).
    pub fn matches_all(&self) -> bool {
        self.tags.is_empty()
    }
}

/// Read preference options including tag sets and max staleness.
#[derive(Debug, Clone)]
pub struct ReadPreferenceOptions {
    /// The read preference mode.
    pub mode: ReadPreference,
    /// Optional tag sets for fine-grained member selection.
    pub tag_sets: Option<TagSet>,
    /// Optional maximum staleness in seconds.
    ///
    /// Limits how far behind a secondary can be from the primary before
    /// it is excluded as a read target. A value of 0 or `None` means
    /// no staleness limit.
    pub max_staleness_secs: Option<u32>,
}

impl ReadPreferenceOptions {
    /// Create options with just a mode.
    pub fn new(mode: ReadPreference) -> Self {
        Self {
            mode,
            tag_sets: None,
            max_staleness_secs: None,
        }
    }

    /// Set tag sets.
    pub fn with_tag_sets(mut self, tag_sets: TagSet) -> Self {
        self.tag_sets = Some(tag_sets);
        self
    }

    /// Set max staleness.
    pub fn with_max_staleness(mut self, secs: u32) -> Self {
        self.max_staleness_secs = Some(secs);
        self
    }
}

/// Write preference options.
#[derive(Debug, Clone)]
pub struct WritePreferenceOptions {
    /// The write preference mode.
    pub mode: WritePreference,
}

impl WritePreferenceOptions {
    /// Create options with a mode.
    pub fn new(mode: WritePreference) -> Self {
        Self { mode }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_preference_default_is_primary() {
        assert_eq!(ReadPreference::default(), ReadPreference::Primary);
    }

    #[test]
    fn read_preference_mode_names() {
        assert_eq!(ReadPreference::Primary.mode_name(), "primary");
        assert_eq!(ReadPreference::PrimaryPreferred.mode_name(), "primaryPreferred");
        assert_eq!(ReadPreference::Secondary.mode_name(), "secondary");
        assert_eq!(ReadPreference::SecondaryPreferred.mode_name(), "secondaryPreferred");
        assert_eq!(ReadPreference::Nearest.mode_name(), "nearest");
    }

    #[test]
    fn read_preference_allows_secondary() {
        assert!(!ReadPreference::Primary.allows_secondary());
        assert!(!ReadPreference::PrimaryPreferred.allows_secondary());
        assert!(ReadPreference::Secondary.allows_secondary());
        assert!(ReadPreference::SecondaryPreferred.allows_secondary());
        assert!(ReadPreference::Nearest.allows_secondary());
    }

    #[test]
    fn read_preference_requires_primary() {
        assert!(ReadPreference::Primary.requires_primary());
        assert!(!ReadPreference::PrimaryPreferred.requires_primary());
        assert!(!ReadPreference::Secondary.requires_primary());
        assert!(!ReadPreference::SecondaryPreferred.requires_primary());
        assert!(!ReadPreference::Nearest.requires_primary());
    }

    #[test]
    fn read_preference_display() {
        assert_eq!(ReadPreference::Primary.to_string(), "mode:primary");
        assert_eq!(
            ReadPreference::SecondaryPreferred.to_string(),
            "mode:secondaryPreferred"
        );
    }

    #[test]
    fn write_preference_default_is_primary() {
        assert_eq!(WritePreference::default(), WritePreference::Primary);
    }

    #[test]
    fn write_preference_mode_names() {
        assert_eq!(WritePreference::Primary.mode_name(), "primary");
        assert_eq!(
            WritePreference::PrimaryPreferred.mode_name(),
            "primaryPreferred"
        );
    }

    #[test]
    fn write_preference_allows_secondary() {
        assert!(!WritePreference::Primary.allows_secondary());
        assert!(WritePreference::PrimaryPreferred.allows_secondary());
    }

    #[test]
    fn write_preference_display() {
        assert_eq!(WritePreference::Primary.to_string(), "mode:primary");
        assert_eq!(
            WritePreference::PrimaryPreferred.to_string(),
            "mode:primaryPreferred"
        );
    }

    #[test]
    fn tag_set_empty_matches_all() {
        let ts = TagSet::empty();
        assert!(ts.matches_all());
    }

    #[test]
    fn tag_set_with_patterns() {
        let mut pattern = std::collections::HashMap::new();
        pattern.insert("dc".to_string(), "us-east".to_string());
        let ts = TagSet::single(pattern);
        assert!(!ts.matches_all());
        assert_eq!(ts.tags.len(), 1);
    }

    #[test]
    fn read_preference_options_builder() {
        let mut pattern = std::collections::HashMap::new();
        pattern.insert("dc".to_string(), "eu".to_string());

        let opts = ReadPreferenceOptions::new(ReadPreference::SecondaryPreferred)
            .with_tag_sets(TagSet::single(pattern))
            .with_max_staleness(120);

        assert_eq!(opts.mode, ReadPreference::SecondaryPreferred);
        assert!(opts.tag_sets.is_some());
        assert_eq!(opts.max_staleness_secs, Some(120));
    }

    #[test]
    fn write_preference_options_builder() {
        let opts = WritePreferenceOptions::new(WritePreference::PrimaryPreferred);
        assert_eq!(opts.mode, WritePreference::PrimaryPreferred);
    }
}
