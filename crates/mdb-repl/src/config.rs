//! 副本集配置。

use crate::types::NodeId;

/// 副本集配置。
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaConfig {
    /// 副本集名称。
    pub name: String,
    /// 成员配置列表。
    pub members: Vec<MemberConfig>,
    /// 全局设置。
    pub settings: RsSettings,
}

impl ReplicaConfig {
    /// 创建新的副本集配置。
    pub fn new(name: impl Into<String>, members: Vec<MemberConfig>) -> Self {
        Self {
            name: name.into(),
            members,
            settings: RsSettings::default(),
        }
    }

    /// 查找指定成员的配置。
    pub fn find_member(&self, id: &NodeId) -> Option<&MemberConfig> {
        self.members.iter().find(|m| &m.id == id)
    }

    /// 活跃投票成员数（非仲裁节点）。
    pub fn voting_members_count(&self) -> usize {
        self.members
            .iter()
            .filter(|m| m.votes > 0 && !m.arbiter_only)
            .count()
    }
}

/// 单个副本集成员配置。
#[derive(Debug, Clone, PartialEq)]
pub struct MemberConfig {
    /// 成员 ID（如 `"node1:27017"`）。
    pub id: NodeId,
    /// 主机地址。
    pub host: String,
    /// 选举优先级（0 表示不能成为 primary）。
    pub priority: f64,
    /// 投票权重（0 或 1）。
    pub votes: i32,
    /// 是否为仲裁节点（不存储数据）。
    pub arbiter_only: bool,
    /// 是否对客户端隐藏。
    pub hidden: bool,
    /// 从节点复制延迟（秒），0 表示无延迟。
    pub secondary_delay_secs: i64,
}

impl MemberConfig {
    /// 创建标准数据成员。
    pub fn new(id: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            id: NodeId::new(id),
            host: host.into(),
            priority: 1.0,
            votes: 1,
            arbiter_only: false,
            hidden: false,
            secondary_delay_secs: 0,
        }
    }

    /// 创建仲裁节点。
    pub fn arbiter(id: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            id: NodeId::new(id),
            host: host.into(),
            priority: 0.0,
            votes: 1,
            arbiter_only: true,
            hidden: false,
            secondary_delay_secs: 0,
        }
    }

    /// 设置优先级。
    pub fn with_priority(mut self, priority: f64) -> Self {
        self.priority = priority;
        self
    }

    /// 设置为隐藏节点。
    pub fn hidden(mut self) -> Self {
        self.hidden = true;
        self
    }

    /// 设置延迟复制（秒）。
    pub fn with_delay(mut self, delay_secs: i64) -> Self {
        self.secondary_delay_secs = delay_secs;
        self
    }
}

/// 副本集全局设置。
#[derive(Debug, Clone, PartialEq)]
pub struct RsSettings {
    /// 心跳超时时间（秒）。
    pub heartbeat_timeout_secs: u32,
    /// 选举超时时间（毫秒）。
    pub election_timeout_millis: u32,
    /// Raft 日志最大条目数（超过后触发快照）。
    pub max_log_entries: usize,
    /// WAL 批量刷盘大小。
    pub wal_batch_size: usize,
    /// 最大快照大小（字节）。
    pub max_snapshot_bytes: usize,
    /// Follower 心跳超时（毫秒）。
    pub follower_heartbeat_timeout_ms: u32,
}

impl Default for RsSettings {
    fn default() -> Self {
        Self {
            heartbeat_timeout_secs: 10,
            election_timeout_millis: 10000,
            max_log_entries: 10000,
            wal_batch_size: 64,
            max_snapshot_bytes: 4 * 1024 * 1024,
            follower_heartbeat_timeout_ms: 500,
        }
    }
}

impl RsSettings {
    /// 创建自定义设置。
    pub fn new(heartbeat_timeout_secs: u32, election_timeout_millis: u32) -> Self {
        Self {
            heartbeat_timeout_secs,
            election_timeout_millis,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_config_new() {
        let members = vec![
            MemberConfig::new("n1", "node1:27017"),
            MemberConfig::new("n2", "node2:27017"),
        ];
        let config = ReplicaConfig::new("rs0", members);
        assert_eq!(config.name, "rs0");
        assert_eq!(config.members.len(), 2);
    }

    #[test]
    fn find_member() {
        let members = vec![
            MemberConfig::new("n1", "node1:27017"),
            MemberConfig::new("n2", "node2:27017"),
        ];
        let config = ReplicaConfig::new("rs0", members);
        let n1 = NodeId::new("n1");
        assert!(config.find_member(&n1).is_some());
        let missing = NodeId::new("n3");
        assert!(config.find_member(&missing).is_none());
    }

    #[test]
    fn voting_members_count() {
        let members = vec![
            MemberConfig::new("n1", "node1:27017"),
            MemberConfig::new("n2", "node2:27017"),
            MemberConfig::arbiter("a1", "arbiter:27017"),
        ];
        let config = ReplicaConfig::new("rs0", members);
        assert_eq!(config.voting_members_count(), 2);
    }

    #[test]
    fn member_config_new() {
        let m = MemberConfig::new("n1", "node1:27017");
        assert_eq!(m.id, NodeId::new("n1"));
        assert_eq!(m.host, "node1:27017");
        assert_eq!(m.priority, 1.0);
        assert_eq!(m.votes, 1);
        assert!(!m.arbiter_only);
        assert!(!m.hidden);
        assert_eq!(m.secondary_delay_secs, 0);
    }

    #[test]
    fn member_config_arbiter() {
        let m = MemberConfig::arbiter("a1", "arbiter:27017");
        assert!(m.arbiter_only);
        assert_eq!(m.priority, 0.0);
        assert_eq!(m.votes, 1);
    }

    #[test]
    fn member_config_builder() {
        let m = MemberConfig::new("n1", "node1:27017")
            .with_priority(2.5)
            .hidden()
            .with_delay(3600);
        assert_eq!(m.priority, 2.5);
        assert!(m.hidden);
        assert_eq!(m.secondary_delay_secs, 3600);
    }

    #[test]
    fn rs_settings_default() {
        let s = RsSettings::default();
        assert_eq!(s.heartbeat_timeout_secs, 10);
        assert_eq!(s.election_timeout_millis, 10000);
        assert_eq!(s.max_log_entries, 10000);
        assert_eq!(s.wal_batch_size, 64);
        assert_eq!(s.max_snapshot_bytes, 4 * 1024 * 1024);
        assert_eq!(s.follower_heartbeat_timeout_ms, 500);
    }

    #[test]
    fn rs_settings_custom() {
        let s = RsSettings::new(5, 5000);
        assert_eq!(s.heartbeat_timeout_secs, 5);
        assert_eq!(s.election_timeout_millis, 5000);
        assert_eq!(s.max_log_entries, 10000); // default
    }
}
