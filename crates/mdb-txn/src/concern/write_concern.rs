//! Write concern determines the level of acknowledgment required from the
//! replica set before a write operation is considered successful.
//!
//! Mirrors MongoDB's `w` field semantics:
//! - `Acknowledged` (w=1): wait for primary only
//! - `Majority` (w="majority"): wait for majority of voting nodes
//! - `NumAck(n)`: wait for n specific nodes
//! - `Tag(String)`: wait for a named replica set tag set

use std::fmt;
use std::time::Duration;

use crate::types::Result;

/// The write concern for a write operation.
///
/// Controls how many replica set members must acknowledge a write before
/// the operation returns success. Optionally supports journal sync (`j`)
/// and a timeout (`wtimeout_ms`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WriteConcern {
    /// Wait for the primary to acknowledge (w=1). Default.
    Acknowledged,
    /// Wait for a majority of voting replica set members (w="majority").
    Majority,
    /// Wait for exactly `n` nodes to acknowledge (w=n).
    NumAck(i32),
    /// Wait for nodes matching a named tag set (w="tagName").
    Tag(String),
}

impl Default for WriteConcern {
    fn default() -> Self {
        Self::Acknowledged
    }
}

impl WriteConcern {
    /// The `w` value as a string suitable for the wire protocol `w` field.
    pub fn w_value(&self) -> &str {
        match self {
            Self::Acknowledged => "1",
            Self::Majority => "majority",
            Self::NumAck(_) => "",
            Self::Tag(t) => t,
        }
    }

    /// The numeric `w` value when it is an integer (None for majority/tag).
    pub fn w_int(&self) -> Option<i32> {
        match self {
            Self::Acknowledged => Some(1),
            Self::Majority => None,
            Self::NumAck(n) => Some(*n),
            Self::Tag(_) => None,
        }
    }
}

/// Builder for constructing a [`WriteConcern`] with journal sync and timeout.
#[derive(Debug, Clone)]
pub struct WriteConcernBuilder {
    wc: WriteConcern,
    journal: bool,
    wtimeout: Option<Duration>,
}

impl WriteConcernBuilder {
    /// Start building a `WriteConcern` from the given concern level.
    pub fn new(wc: WriteConcern) -> Self {
        Self {
            wc,
            journal: false,
            wtimeout: None,
        }
    }

    /// Start building from an acknowledged (w=1) concern.
    pub fn acknowledged() -> Self {
        Self::new(WriteConcern::Acknowledged)
    }

    /// Start building from a majority concern.
    pub fn majority() -> Self {
        Self::new(WriteConcern::Majority)
    }

    /// Start building from a numeric ack count.
    pub fn num_ack(n: i32) -> Self {
        Self::new(WriteConcern::NumAck(n))
    }

    /// Start building from a tag name.
    pub fn tag(name: impl Into<String>) -> Self {
        Self::new(WriteConcern::Tag(name.into()))
    }

    /// Require journal sync before acknowledging (j=true).
    pub fn journal(mut self, sync: bool) -> Self {
        self.journal = sync;
        self
    }

    /// Set the timeout for write concern fulfillment.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.wtimeout = Some(timeout);
        self
    }

    /// Build the final `WriteConcernOptions`.
    pub fn build(self) -> WriteConcernOptions {
        WriteConcernOptions {
            concern: self.wc,
            journal: self.journal,
            wtimeout_ms: self.wtimeout.map(|d| d.as_millis() as u32),
        }
    }
}

/// A fully-specified write concern with journal and timeout options.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriteConcernOptions {
    /// The concern level.
    pub concern: WriteConcern,
    /// Whether to sync to journal before acknowledging.
    pub journal: bool,
    /// Timeout in milliseconds for write concern fulfillment.
    /// `None` means no timeout (wait indefinitely).
    pub wtimeout_ms: Option<u32>,
}

impl Default for WriteConcernOptions {
    fn default() -> Self {
        Self {
            concern: WriteConcern::Acknowledged,
            journal: false,
            wtimeout_ms: None,
        }
    }
}

impl WriteConcernOptions {
    /// Whether this concern requires acknowledgment from more than just the primary.
    pub fn requires_majority(&self) -> bool {
        matches!(self.concern, WriteConcern::Majority)
    }

    /// Whether journal sync is required.
    pub fn requires_journal_sync(&self) -> bool {
        self.journal
    }

    /// Validate the write concern options.
    pub fn validate(&self) -> Result<()> {
        if let WriteConcern::NumAck(n) = &self.concern {
            if *n < 0 {
                return Err(mdb_common::MdbError::new(
                    mdb_common::ErrorCode::WriteConcernFailed,
                    "write concern 'w' must be non-negative",
                ));
            }
        }
        Ok(())
    }

    /// Convert to a wire-protocol-compatible document (key-value pairs).
    pub fn to_doc(&self) -> Vec<(&str, String)> {
        let mut doc = Vec::new();
        match &self.concern {
            WriteConcern::Acknowledged => {
                doc.push(("w", "1".to_string()));
            }
            WriteConcern::Majority => {
                doc.push(("w", "majority".to_string()));
            }
            WriteConcern::NumAck(n) => {
                doc.push(("w", n.to_string()));
            }
            WriteConcern::Tag(t) => {
                doc.push(("w", t.clone()));
            }
        }
        if self.journal {
            doc.push(("j", "true".to_string()));
        }
        if let Some(ms) = self.wtimeout_ms {
            doc.push(("wtimeoutMS", ms.to_string()));
        }
        doc
    }
}

impl fmt::Display for WriteConcern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acknowledged => write!(f, "w:1"),
            Self::Majority => write!(f, "w:majority"),
            Self::NumAck(n) => write!(f, "w:{n}"),
            Self::Tag(t) => write!(f, "w:{t}"),
        }
    }
}

impl fmt::Display for WriteConcernOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.concern)?;
        if self.journal {
            write!(f, ", j:true")?;
        }
        if let Some(ms) = self.wtimeout_ms {
            write!(f, ", wtimeout:{ms}ms")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_write_concern_is_acknowledged() {
        let wc = WriteConcern::default();
        assert_eq!(wc, WriteConcern::Acknowledged);
    }

    #[test]
    fn w_values() {
        assert_eq!(WriteConcern::Acknowledged.w_value(), "1");
        assert_eq!(WriteConcern::Majority.w_value(), "majority");
        assert_eq!(WriteConcern::NumAck(3).w_value(), "");
        assert_eq!(WriteConcern::Tag("us-east".into()).w_value(), "us-east");
    }

    #[test]
    fn w_int_values() {
        assert_eq!(WriteConcern::Acknowledged.w_int(), Some(1));
        assert_eq!(WriteConcern::Majority.w_int(), None);
        assert_eq!(WriteConcern::NumAck(5).w_int(), Some(5));
        assert_eq!(WriteConcern::Tag("x".into()).w_int(), None);
    }

    #[test]
    fn builder_defaults() {
        let opts = WriteConcernBuilder::acknowledged().build();
        assert_eq!(opts.concern, WriteConcern::Acknowledged);
        assert!(!opts.journal);
        assert_eq!(opts.wtimeout_ms, None);
    }

    #[test]
    fn builder_with_journal_and_timeout() {
        let opts = WriteConcernBuilder::majority()
            .journal(true)
            .timeout(Duration::from_secs(5))
            .build();
        assert_eq!(opts.concern, WriteConcern::Majority);
        assert!(opts.journal);
        assert_eq!(opts.wtimeout_ms, Some(5000));
    }

    #[test]
    fn builder_tag() {
        let opts = WriteConcernBuilder::tag("ny").journal(true).build();
        assert_eq!(opts.concern, WriteConcern::Tag("ny".into()));
        assert!(opts.journal);
    }

    #[test]
    fn validate_negative_w() {
        let opts = WriteConcernBuilder::num_ack(-1).build();
        assert!(opts.validate().is_err());
    }

    #[test]
    fn validate_positive_w() {
        let opts = WriteConcernBuilder::num_ack(3).build();
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn requires_majority() {
        assert!(WriteConcernBuilder::majority().build().requires_majority());
        assert!(!WriteConcernBuilder::acknowledged().build().requires_majority());
    }

    #[test]
    fn to_doc_acknowledged() {
        let opts = WriteConcernBuilder::acknowledged().build();
        let doc = opts.to_doc();
        assert_eq!(doc, vec![("w", "1".to_string())]);
    }

    #[test]
    fn to_doc_majority_with_journal_and_timeout() {
        let opts = WriteConcernBuilder::majority()
            .journal(true)
            .timeout(Duration::from_millis(2500))
            .build();
        let doc = opts.to_doc();
        assert!(doc.contains(&("w", "majority".to_string())));
        assert!(doc.contains(&("j", "true".to_string())));
        assert!(doc.contains(&("wtimeoutMS", "2500".to_string())));
    }

    #[test]
    fn to_doc_num_ack() {
        let opts = WriteConcernBuilder::num_ack(5).build();
        let doc = opts.to_doc();
        assert_eq!(doc, vec![("w", "5".to_string())]);
    }

    #[test]
    fn display_write_concern() {
        assert_eq!(WriteConcern::Acknowledged.to_string(), "w:1");
        assert_eq!(WriteConcern::Majority.to_string(), "w:majority");
        assert_eq!(WriteConcern::NumAck(3).to_string(), "w:3");
        assert_eq!(
            WriteConcern::Tag("dc1".into()).to_string(),
            "w:dc1"
        );
    }

    #[test]
    fn display_with_options() {
        let opts = WriteConcernBuilder::majority()
            .journal(true)
            .timeout(Duration::from_millis(1000))
            .build();
        assert_eq!(opts.to_string(), "w:majority, j:true, wtimeout:1000ms");
    }
}
