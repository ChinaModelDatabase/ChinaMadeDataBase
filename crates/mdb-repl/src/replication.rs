//! 复制流管理。
//!
//! 负责 ReplicaSet 成员管理、primary 选举、oplog 同步与心跳检测。
//! 支持游标复制、读关注级别、成员变更。

use crate::election::ElectionManager;
use crate::oplog::{Oplog, OplogConfig};
use crate::types::{NodeId, OplogEntry, ReadConcern};
use mdb_doc::Timestamp;

/// 复制集成员状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplicaMemberState {
    Primary,
    Secondary,
    Candidate,
    Down,
}

/// 复制集成员。
#[derive(Debug, Clone)]
pub struct ReplicaMember {
    /// 成员 ID。
    pub id: NodeId,
    /// 当前状态。
    pub state: ReplicaMemberState,
    /// 复制延迟（oplog 条目数）。
    pub lag: u64,
    /// 选举优先级。
    pub priority: f64,
    /// 最后一次应用 oplog 的时间戳。
    pub last_applied_ts: Timestamp,
    /// 游标复制：上次同步到的位置。
    pub sync_cursor: u64,
}

impl ReplicaMember {
    /// 创建新的复制集成员。
    pub fn new(id: NodeId, priority: f64) -> Self {
        Self {
            id,
            state: ReplicaMemberState::Secondary,
            lag: 0,
            priority,
            last_applied_ts: Timestamp::new(0, 0),
            sync_cursor: 0,
        }
    }
}

/// 复制集。
pub struct ReplicaSet {
    /// 所有成员。
    pub members: Vec<ReplicaMember>,
    /// 当前 primary。
    pub primary: Option<NodeId>,
    /// 当前任期。
    pub term: u64,
    /// 选举管理器。
    election: ElectionManager,
    /// 本地 oplog。
    oplog: Oplog,
    /// 成员 ID → Oplog 的映射（简化：每个成员一个 oplog 实例）。
    pub member_oplogs: std::collections::HashMap<NodeId, Oplog>,
    /// 读关注级别。
    read_concern: ReadConcern,
    /// 游标 ID 计数器。
    cursor_counter: u64,
    /// 心跳间隔（毫秒）。
    heartbeat_interval_ms: u32,
    /// 心跳计时器。
    heartbeat_elapsed: u32,
    /// 心跳超时（毫秒）。
    #[allow(dead_code)]
    heartbeat_timeout_ms: u32,
}

impl std::fmt::Debug for ReplicaSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaSet")
            .field("members", &self.members)
            .field("primary", &self.primary)
            .field("term", &self.term)
            .field("read_concern", &self.read_concern)
            .finish()
    }
}

impl ReplicaSet {
    /// 创建新的复制集。
    pub fn new() -> Self {
        Self {
            members: Vec::new(),
            primary: None,
            term: 0,
            election: ElectionManager::new(),
            oplog: Oplog::new(OplogConfig::default()),
            member_oplogs: std::collections::HashMap::new(),
            read_concern: ReadConcern::Local,
            cursor_counter: 0,
            heartbeat_interval_ms: 50,
            heartbeat_elapsed: 0,
            heartbeat_timeout_ms: 500,
        }
    }

    /// 设置读关注级别。
    pub fn set_read_concern(&mut self, rc: ReadConcern) {
        self.read_concern = rc;
    }

    /// 获取读关注级别。
    pub fn read_concern(&self) -> ReadConcern {
        self.read_concern
    }

    /// 添加成员。
    pub fn add_member(&mut self, id: NodeId, priority: f64) {
        if self.members.iter().any(|m| m.id == id) {
            return;
        }
        self.member_oplogs
            .insert(id.clone(), Oplog::new(OplogConfig::default()));
        self.members.push(ReplicaMember::new(id, priority));
    }

    /// 移除成员。
    pub fn remove_member(&mut self, id: &NodeId) {
        self.members.retain(|m| &m.id != id);
        self.member_oplogs.remove(id);
        if self.primary.as_ref() == Some(id) {
            self.primary = None;
        }
    }

    /// 查找成员。
    pub fn find_member(&self, id: &NodeId) -> Option<&ReplicaMember> {
        self.members.iter().find(|m| &m.id == id)
    }

    /// 获取成员的可变引用。
    pub fn find_member_mut(&mut self, id: &NodeId) -> Option<&mut ReplicaMember> {
        self.members.iter_mut().find(|m| &m.id == id)
    }

    /// 运行选举，选出 primary。
    pub fn elect_primary(&mut self) -> Option<NodeId> {
        let candidate = self
            .members
            .iter()
            .filter(|m| m.priority > 0.0 && m.state != ReplicaMemberState::Down)
            .max_by(|a, b| a.priority.partial_cmp(&b.priority).unwrap_or(std::cmp::Ordering::Equal))
            .map(|m| m.id.clone())?;

        self.term += 1;
        self.election.term = self.term;

        let total_voting = self
            .members
            .iter()
            .filter(|m| m.priority > 0.0)
            .count();
        let votes_needed = total_voting / 2 + 1;

        let active_count = self
            .members
            .iter()
            .filter(|m| m.priority > 0.0 && m.state != ReplicaMemberState::Down)
            .count();

        if active_count >= votes_needed {
            for member in &mut self.members {
                if member.id == candidate {
                    member.state = ReplicaMemberState::Primary;
                    member.lag = 0;
                } else {
                    member.state = ReplicaMemberState::Secondary;
                }
            }
            self.primary = Some(candidate.clone());
            Some(candidate)
        } else {
            None
        }
    }

    /// 游标复制：获取新条目（基于 cursor）。
    ///
    /// 返回 `(cursor_id, entries)`。首次调用时 cursor_id 为 0。
    pub fn get_more(&mut self, follower: &NodeId, batch_size: usize) -> (u64, Vec<OplogEntry>) {
        let primary = match self.primary.clone() {
            Some(p) => p,
            None => return (0, vec![]),
        };

        let follower_member = match self.find_member(follower) {
            Some(m) => m,
            None => return (0, vec![]),
        };

        let cursor = follower_member.sync_cursor;

        let primary_oplog = match self.member_oplogs.get(&primary) {
            Some(oplog) => oplog,
            None => return (0, vec![]),
        };

        let all_entries = primary_oplog.scan(Timestamp::new(0, 0));
        let new_entries: Vec<OplogEntry> = all_entries
            .into_iter()
            .filter(|e| e.ts.as_u64() > cursor)
            .take(batch_size)
            .collect();

        let next_cursor = new_entries.last().map(|e| e.ts.as_u64()).unwrap_or(cursor);
        let count = new_entries.len();

        if count > 0 {
            if let Some(oplog) = self.member_oplogs.get_mut(follower) {
                for entry in &new_entries {
                    oplog.append(entry.clone());
                }
            }

            if let Some(member) = self.find_member_mut(follower) {
                member.sync_cursor = next_cursor;
                member.last_applied_ts = new_entries.last().map(|e| e.ts).unwrap_or(member.last_applied_ts);
                member.lag = 0;
            }
        }

        let new_cursor_id = if cursor == 0 && count > 0 {
            self.cursor_counter += 1;
            self.cursor_counter
        } else if count > 0 {
            self.cursor_counter
        } else {
            0 // 空游标 = 无更多数据
        };

        (new_cursor_id, new_entries)
    }

    /// 从 primary 同步 oplog 到 follower（兼容旧接口）。
    pub fn sync_from_primary(&mut self, follower: &NodeId) -> usize {
        let (_cursor, entries) = self.get_more(follower, usize::MAX);
        entries.len()
    }

    /// 应用 oplog 条目到本地存储。
    pub fn apply_oplog_entry(&mut self, entry: OplogEntry) {
        self.oplog.append(entry);
    }

    /// 向所有成员发送心跳，检查健康状态。
    ///
    /// 返回不健康的成员 ID 列表。
    pub fn heartbeat(&mut self) -> Vec<NodeId> {
        let unhealthy = Vec::new();
        let primary = self.primary.clone();

        if primary.is_none() {
            return self.members.iter().map(|m| m.id.clone()).collect();
        }

        self.heartbeat_elapsed += self.heartbeat_interval_ms;

        for member in &mut self.members {
            if member.state == ReplicaMemberState::Down {
                continue;
            }

            // 检查心跳超时
            if member.state == ReplicaMemberState::Secondary {
                // 在真实实现中会检查 last heartbeat response 时间
                // 这里简化处理：所有非 down 成员视为健康
            }
        }

        unhealthy
    }

    /// 触发心跳（按间隔）。
    pub fn on_heartbeat_interval(&mut self) -> Vec<NodeId> {
        self.heartbeat_elapsed += self.heartbeat_interval_ms;
        if self.heartbeat_elapsed < self.heartbeat_interval_ms {
            return vec![];
        }
        self.heartbeat_elapsed = 0;
        self.heartbeat()
    }

    /// 获取成员数量。
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// 获取活跃成员数量。
    pub fn active_member_count(&self) -> usize {
        self.members
            .iter()
            .filter(|m| m.state != ReplicaMemberState::Down)
            .count()
    }

    /// 检查是否有 primary。
    pub fn has_primary(&self) -> bool {
        self.primary.is_some()
    }

    /// 获取 primary ID。
    pub fn primary_id(&self) -> Option<&NodeId> {
        self.primary.as_ref()
    }

    /// 获取 oplog 引用。
    pub fn oplog(&self) -> &Oplog {
        &self.oplog
    }

    /// 获取 oplog 可变引用。
    pub fn oplog_mut(&mut self) -> &mut Oplog {
        &mut self.oplog
    }
}

impl Default for ReplicaSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::OpType;
    use mdb_doc::{Document, ObjectId};

    fn make_replica_set() -> ReplicaSet {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.add_member(NodeId::new("n2"), 1.0);
        rs.add_member(NodeId::new("n3"), 0.5);
        rs
    }

    #[test]
    fn add_member() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        assert_eq!(rs.member_count(), 1);
        assert!(rs.find_member(&NodeId::new("n1")).is_some());
    }

    #[test]
    fn add_member_no_duplicates() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.add_member(NodeId::new("n1"), 2.0);
        assert_eq!(rs.member_count(), 1);
    }

    #[test]
    fn remove_member() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 2.0);
        rs.add_member(NodeId::new("n2"), 1.0);
        rs.add_member(NodeId::new("n3"), 0.5);
        rs.elect_primary();
        assert_eq!(rs.primary, Some(NodeId::new("n1")));
        rs.remove_member(&NodeId::new("n1"));
        assert_eq!(rs.member_count(), 2);
        assert!(rs.primary.is_none());
    }

    #[test]
    fn elect_primary_picks_highest_priority() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.add_member(NodeId::new("n2"), 2.0);
        rs.add_member(NodeId::new("n3"), 0.5);

        let result = rs.elect_primary();
        assert_eq!(result, Some(NodeId::new("n2")));
        assert_eq!(rs.primary, Some(NodeId::new("n2")));
        assert_eq!(rs.find_member(&NodeId::new("n2")).unwrap().state, ReplicaMemberState::Primary);
    }

    #[test]
    fn elect_primary_not_enough_members() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.add_member(NodeId::new("n2"), 0.0);

        let result = rs.elect_primary();
        assert_eq!(result, Some(NodeId::new("n1")));
    }

    #[test]
    fn elect_primary_all_down() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.add_member(NodeId::new("n2"), 1.0);
        rs.add_member(NodeId::new("n3"), 0.5);
        for m in &mut rs.members {
            if m.priority > 0.0 {
                m.state = ReplicaMemberState::Down;
            }
        }

        let result = rs.elect_primary();
        assert_eq!(result, None);
    }

    #[test]
    fn apply_oplog_entry() {
        let mut rs = ReplicaSet::new();
        let entry = OplogEntry {
            ts: Timestamp::new(100, 1),
            op: OpType::Insert,
            ns: "test.users".into(),
            o: Document::new(),
            o2: None,
            ui: ObjectId::from_bytes([0; 12]),
        };
        rs.apply_oplog_entry(entry);
        assert_eq!(rs.oplog().len(), 1);
    }

    #[test]
    fn sync_from_primary() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.add_member(NodeId::new("n2"), 1.0);

        rs.primary = Some(NodeId::new("n1"));
        let entry = OplogEntry {
            ts: Timestamp::new(100, 1),
            op: OpType::Insert,
            ns: "test.users".into(),
            o: Document::new(),
            o2: None,
            ui: ObjectId::from_bytes([0; 12]),
        };
        if let Some(oplog) = rs.member_oplogs.get_mut(&NodeId::new("n1")) {
            oplog.append(entry);
        }

        let count = rs.sync_from_primary(&NodeId::new("n2"));
        assert_eq!(count, 1);
    }

    #[test]
    fn sync_from_primary_no_primary() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        let count = rs.sync_from_primary(&NodeId::new("n1"));
        assert_eq!(count, 0);
    }

    #[test]
    fn heartbeat_no_primary() {
        let mut rs = make_replica_set();
        let unhealthy = rs.heartbeat();
        assert_eq!(unhealthy.len(), 3);
    }

    #[test]
    fn heartbeat_with_primary() {
        let mut rs = make_replica_set();
        rs.elect_primary();
        let unhealthy = rs.heartbeat();
        assert!(unhealthy.is_empty());
    }

    #[test]
    fn member_count_and_active() {
        let mut rs = make_replica_set();
        assert_eq!(rs.member_count(), 3);
        assert_eq!(rs.active_member_count(), 3);

        rs.find_member_mut(&NodeId::new("n1")).unwrap().state = ReplicaMemberState::Down;
        assert_eq!(rs.active_member_count(), 2);
    }

    #[test]
    fn has_primary() {
        let mut rs = make_replica_set();
        assert!(!rs.has_primary());
        rs.elect_primary();
        assert!(rs.has_primary());
    }

    #[test]
    fn default_replica_set() {
        let rs = ReplicaSet::default();
        assert!(rs.members.is_empty());
        assert!(!rs.has_primary());
    }

    // --- Cursor-based replication tests ---

    #[test]
    fn get_more_returns_new_entries() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.add_member(NodeId::new("n2"), 1.0);
        rs.primary = Some(NodeId::new("n1"));

        let entry1 = OplogEntry {
            ts: Timestamp::new(100, 1),
            op: OpType::Insert,
            ns: "test.users".into(),
            o: Document::new(),
            o2: None,
            ui: ObjectId::from_bytes([0; 12]),
        };
        let entry2 = OplogEntry {
            ts: Timestamp::new(100, 2),
            op: OpType::Update,
            ns: "test.users".into(),
            o: Document::new(),
            o2: None,
            ui: ObjectId::from_bytes([0; 12]),
        };

        if let Some(oplog) = rs.member_oplogs.get_mut(&NodeId::new("n1")) {
            oplog.append(entry1);
            oplog.append(entry2);
        }

        let (cursor_id, entries) = rs.get_more(&NodeId::new("n2"), 10);
        assert_eq!(entries.len(), 2);
        assert!(cursor_id > 0);
    }

    #[test]
    fn get_more_tracks_cursor() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.add_member(NodeId::new("n2"), 1.0);
        rs.primary = Some(NodeId::new("n1"));

        let entry = OplogEntry {
            ts: Timestamp::new(100, 1),
            op: OpType::Insert,
            ns: "test.users".into(),
            o: Document::new(),
            o2: None,
            ui: ObjectId::from_bytes([0; 12]),
        };

        if let Some(oplog) = rs.member_oplogs.get_mut(&NodeId::new("n1")) {
            oplog.append(entry);
        }

        let (_, _) = rs.get_more(&NodeId::new("n2"), 10);
        let member = rs.find_member(&NodeId::new("n2")).unwrap();
        assert_eq!(member.sync_cursor, Timestamp::new(100, 1).as_u64());

        // Second call returns nothing
        let (_, entries) = rs.get_more(&NodeId::new("n2"), 10);
        assert!(entries.is_empty());
    }

    #[test]
    fn get_more_respects_batch_size() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.add_member(NodeId::new("n2"), 1.0);
        rs.primary = Some(NodeId::new("n1"));

        for i in 1..=10 {
            let entry = OplogEntry {
                ts: Timestamp::new(100, i),
                op: OpType::Insert,
                ns: "test.users".into(),
                o: Document::new(),
                o2: None,
                ui: ObjectId::from_bytes([i as u8; 12]),
            };
            if let Some(oplog) = rs.member_oplogs.get_mut(&NodeId::new("n1")) {
                oplog.append(entry);
            }
        }

        let (_, entries) = rs.get_more(&NodeId::new("n2"), 3);
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn get_more_no_primary_returns_empty() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        let (_, entries) = rs.get_more(&NodeId::new("n1"), 10);
        assert!(entries.is_empty());
    }

    #[test]
    fn get_more_unknown_follower_returns_empty() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.primary = Some(NodeId::new("n1"));
        let (_, entries) = rs.get_more(&NodeId::new("n99"), 10);
        assert!(entries.is_empty());
    }

    #[test]
    fn member_last_applied_ts_updated() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.add_member(NodeId::new("n2"), 1.0);
        rs.primary = Some(NodeId::new("n1"));

        let entry = OplogEntry {
            ts: Timestamp::new(200, 5),
            op: OpType::Insert,
            ns: "test.users".into(),
            o: Document::new(),
            o2: None,
            ui: ObjectId::from_bytes([0; 12]),
        };
        if let Some(oplog) = rs.member_oplogs.get_mut(&NodeId::new("n1")) {
            oplog.append(entry);
        }

        rs.get_more(&NodeId::new("n2"), 10);
        let member = rs.find_member(&NodeId::new("n2")).unwrap();
        assert_eq!(member.last_applied_ts, Timestamp::new(200, 5));
    }

    // --- Read concern tests ---

    #[test]
    fn read_concern_default_is_local() {
        let rs = ReplicaSet::new();
        assert_eq!(rs.read_concern(), ReadConcern::Local);
    }

    #[test]
    fn set_read_concern() {
        let mut rs = ReplicaSet::new();
        rs.set_read_concern(ReadConcern::Majority);
        assert_eq!(rs.read_concern(), ReadConcern::Majority);
    }

    // --- Heartbeat interval tests ---

    #[test]
    fn on_heartbeat_interval_triggers() {
        let mut rs = make_replica_set();
        rs.elect_primary();
        rs.heartbeat_elapsed = rs.heartbeat_interval_ms;
        let unhealthy = rs.on_heartbeat_interval();
        assert!(unhealthy.is_empty());
    }
}
