//! Operation Log（Oplog）实现。
//!
//! 基于内存环形缓冲区的 oplog，支持自动轮转、按时间戳扫描和游标复制。

use crate::types::OplogEntry;
use mdb_doc::Timestamp;

/// Oplog 配置。
#[derive(Debug, Clone)]
pub struct OplogConfig {
    /// 最大条目数。
    pub max_entries: usize,
}

impl Default for OplogConfig {
    fn default() -> Self {
        Self {
            max_entries: 10_000,
        }
    }
}

/// Oplog：内存环形缓冲区 + 持久化接口。
#[derive(Debug)]
pub struct Oplog {
    entries: std::collections::VecDeque<OplogEntry>,
    config: OplogConfig,
}

impl Oplog {
    /// 创建新的 oplog。
    pub fn new(config: OplogConfig) -> Self {
        Self {
            entries: std::collections::VecDeque::with_capacity(config.max_entries),
            config,
        }
    }

    /// 追加一条 oplog 条目。
    pub fn append(&mut self, entry: OplogEntry) {
        if self.entries.len() >= self.config.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    /// 按时间戳扫描：返回 ts >= from_ts 的所有条目。
    pub fn scan(&self, from_ts: Timestamp) -> Vec<OplogEntry> {
        self.entries
            .iter()
            .filter(|e| e.ts >= from_ts)
            .cloned()
            .collect()
    }

    /// 按逻辑索引扫描（游标复制用）：返回 ts.as_u64() > after_cursor 的条目。
    pub fn scan_after_cursor(&self, after_cursor: u64, limit: usize) -> Vec<OplogEntry> {
        self.entries
            .iter()
            .filter(|e| e.ts.as_u64() > after_cursor)
            .take(limit)
            .cloned()
            .collect()
    }

    /// 返回最后一条 oplog 条目。
    pub fn last_entry(&self) -> Option<OplogEntry> {
        self.entries.back().cloned()
    }

    /// 返回当前条目数。
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 返回指定索引范围的条目（用于批量同步）。
    pub fn get_range(&self, start_index: u64, end_index: u64) -> Vec<OplogEntry> {
        self.entries
            .iter()
            .filter(|e| e.index() >= start_index && e.index() < end_index)
            .cloned()
            .collect()
    }

    /// 清空 oplog。
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// OplogEntry 辅助 trait，提供 index() 方法。
trait OplogEntryExt {
    fn index(&self) -> u64;
}

impl OplogEntryExt for OplogEntry {
    /// 用 ts 的 inc 字段作为逻辑索引（简化实现）。
    fn index(&self) -> u64 {
        self.ts.as_u64()
    }
}

impl Default for Oplog {
    fn default() -> Self {
        Self::new(OplogConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::OpType;
    use mdb_doc::{Document, ObjectId, Timestamp};

    fn make_entry(ts_secs: u32, ts_inc: u32, op: OpType, ns: &str) -> OplogEntry {
        let mut doc = Document::new();
        doc.insert("x", mdb_doc::Bson::Int32(1));
        OplogEntry {
            ts: Timestamp::new(ts_secs, ts_inc),
            op,
            ns: ns.to_owned(),
            o: doc,
            o2: None,
            ui: ObjectId::from_bytes([ts_secs as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        }
    }

    #[test]
    fn append_and_last_entry() {
        let mut oplog = Oplog::new(OplogConfig { max_entries: 100 });
        assert!(oplog.is_empty());

        let entry1 = make_entry(100, 1, OpType::Insert, "test.users");
        let entry2 = make_entry(100, 2, OpType::Update, "test.users");

        oplog.append(entry1);
        assert_eq!(oplog.len(), 1);

        oplog.append(entry2);
        assert_eq!(oplog.len(), 2);

        let last = oplog.last_entry().unwrap();
        assert_eq!(last.ts.inc, 2);
    }

    #[test]
    fn scan_filters_by_timestamp() {
        let mut oplog = Oplog::new(OplogConfig { max_entries: 100 });
        oplog.append(make_entry(10, 1, OpType::Insert, "test.a"));
        oplog.append(make_entry(20, 1, OpType::Insert, "test.b"));
        oplog.append(make_entry(30, 1, OpType::Insert, "test.c"));

        let results = oplog.scan(Timestamp::new(20, 0));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].ts.secs, 20);
        assert_eq!(results[1].ts.secs, 30);
    }

    #[test]
    fn scan_empty() {
        let oplog = Oplog::new(OplogConfig::default());
        let results = oplog.scan(Timestamp::new(0, 0));
        assert!(results.is_empty());
    }

    #[test]
    fn auto_rotation() {
        let mut oplog = Oplog::new(OplogConfig { max_entries: 3 });
        oplog.append(make_entry(1, 1, OpType::Insert, "t.a"));
        oplog.append(make_entry(2, 1, OpType::Insert, "t.b"));
        oplog.append(make_entry(3, 1, OpType::Insert, "t.c"));
        assert_eq!(oplog.len(), 3);

        oplog.append(make_entry(4, 1, OpType::Insert, "t.d"));
        assert_eq!(oplog.len(), 3);
        assert_eq!(oplog.entries.front().unwrap().ts.secs, 2);
    }

    #[test]
    fn get_range() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(1, 1, OpType::Insert, "t.a"));
        oplog.append(make_entry(2, 1, OpType::Insert, "t.b"));
        oplog.append(make_entry(3, 1, OpType::Insert, "t.c"));
        oplog.append(make_entry(4, 1, OpType::Insert, "t.d"));

        let results = oplog.get_range(
            Timestamp::new(2, 0).as_u64(),
            Timestamp::new(4, 0).as_u64(),
        );
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn clear() {
        let mut oplog = Oplog::new(OplogConfig { max_entries: 10 });
        oplog.append(make_entry(1, 1, OpType::Insert, "t.a"));
        oplog.append(make_entry(2, 1, OpType::Insert, "t.b"));
        assert_eq!(oplog.len(), 2);
        oplog.clear();
        assert!(oplog.is_empty());
    }

    #[test]
    fn default_config() {
        let config = OplogConfig::default();
        assert_eq!(config.max_entries, 10_000);
    }

    #[test]
    fn oplog_default() {
        let oplog = Oplog::default();
        assert!(oplog.is_empty());
        assert_eq!(oplog.config.max_entries, 10_000);
    }

    // --- Cursor-based replication tests ---

    #[test]
    fn scan_after_cursor_returns_new_entries() {
        let mut oplog = Oplog::new(OplogConfig { max_entries: 100 });
        oplog.append(make_entry(10, 1, OpType::Insert, "t.a"));
        oplog.append(make_entry(20, 1, OpType::Insert, "t.b"));
        oplog.append(make_entry(30, 1, OpType::Insert, "t.c"));

        let cursor = Timestamp::new(10, 1).as_u64();
        let results = oplog.scan_after_cursor(cursor, 10);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].ts.secs, 20);
        assert_eq!(results[1].ts.secs, 30);
    }

    #[test]
    fn scan_after_cursor_respects_limit() {
        let mut oplog = Oplog::new(OplogConfig { max_entries: 100 });
        for i in 1..=10 {
            oplog.append(make_entry(100, i, OpType::Insert, "t.a"));
        }

        let results = oplog.scan_after_cursor(0, 3);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn scan_after_cursor_empty_when_caught_up() {
        let mut oplog = Oplog::new(OplogConfig { max_entries: 100 });
        oplog.append(make_entry(10, 1, OpType::Insert, "t.a"));

        let cursor = Timestamp::new(10, 1).as_u64();
        let results = oplog.scan_after_cursor(cursor, 10);
        assert!(results.is_empty());
    }

    #[test]
    fn scan_after_cursor_from_zero() {
        let mut oplog = Oplog::new(OplogConfig { max_entries: 100 });
        oplog.append(make_entry(10, 1, OpType::Insert, "t.a"));
        oplog.append(make_entry(20, 1, OpType::Insert, "t.b"));

        let results = oplog.scan_after_cursor(0, 10);
        assert_eq!(results.len(), 2);
    }
}
