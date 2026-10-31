//! 复制与高可用：Raft 选主、复制流、Oplog、Change Stream
//!
//! ## 依赖方向（单向，禁止反向依赖）
//!
//! `  ext
//! mdb-common <- mdb-crypto <- mdb-doc <- mdb-wire <- mdb-server
//!                     \                     \             \
//!                      \                     \             +- mdb-cli
//!                       \- mdb-storage <- mdb-index <- mdb-query
//!                                            \            \        /
//!                                             \- mdb-txn --+- mdb-repl
//!                                                          \- mdb-router
//! `
//!
//! ## 模块概览
//!
//! - [`types`]：复制基础类型（NodeId、RaftState、LogEntry、Command、Snapshot、ReadConcern 等）
//! - [`raft`]：Raft 共识协议实现（含快照、预投票、WAL、成员变更、心跳检测）
//! - [`config`]：副本集配置（ReplicaConfig、MemberConfig、RsSettings）
//! - [`election`]：Leader 选举管理
//! - [`oplog`]：操作日志（Oplog）环形缓冲区，支持游标复制
//! - [`replication`]：复制流管理（ReplicaSet、游标复制、心跳检测、读关注）
//! - [`change_stream`]：基于 Oplog 的 Change Stream

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod change_stream;
pub mod config;
pub mod election;
pub mod oplog;
pub mod raft;
pub mod replication;
pub mod transport;
pub mod types;

pub use change_stream::{
    ChangeStream, ChangeStreamConfig, ChangeStreamError, FullDocumentBeforeChangeMode,
    FullDocumentMode, PipelineStage, ResumeStartPoint, StreamLevel,
};
pub use config::{MemberConfig, ReplicaConfig, RsSettings};
pub use election::ElectionManager;
pub use oplog::{Oplog, OplogConfig};
pub use raft::RaftNode;
pub use replication::{ReplicaMember, ReplicaMemberState, ReplicaSet};
pub use transport::{RaftMessage, RaftTransport, TcpRaftTransport};
pub use types::{
    ChangeStreamEvent, ClusterMembership, Command, LogEntry, NodeId, OperationType, OplogEntry,
    OpType, PreVoteRequest, PreVoteResponse, RaftState, ReadConcern, ResumeToken, Snapshot,
    WalEntry,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn re_exports_are_accessible() {
        let _node_id = NodeId::new("test:27017");
        let _state = RaftState::Follower;
        let _command = Command::Noop;
        let _op_type = OpType::Insert;
        let _op_type_str = OperationType::Insert.as_str();
        let _read_concern = ReadConcern::default();
        let _snapshot = Snapshot {
            last_included_index: 0,
            last_included_term: 0,
            state: mdb_doc::Document::new(),
        };
    }

    #[test]
    fn full_replica_set_lifecycle() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("node1:27017"), 1.0);
        rs.add_member(NodeId::new("node2:27017"), 1.0);
        rs.add_member(NodeId::new("node3:27017"), 0.5);

        let primary = rs.elect_primary();
        assert!(primary.is_some());

        let entry = OplogEntry {
            ts: mdb_doc::Timestamp::new(1000, 1),
            op: OpType::Insert,
            ns: "test.users".into(),
            o: mdb_doc::Document::new(),
            o2: None,
            ui: mdb_doc::ObjectId::from_bytes([0; 12]),
        };
        rs.apply_oplog_entry(entry);
        assert_eq!(rs.oplog().len(), 1);

        let unhealthy = rs.heartbeat();
        assert!(unhealthy.is_empty());
    }

    #[test]
    fn raft_node_basic_workflow() {
        let mut node = RaftNode::new(NodeId::new("node1:27017"), RsSettings::default());

        assert_eq!(node.state, RaftState::Follower);

        let entry = node.append_entry(Command::Noop);
        assert_eq!(entry.index, 1);

        node.commit_index = 1;
        let applied = node.apply_committed();
        assert_eq!(applied.len(), 1);
    }

    #[test]
    fn raft_node_snapshot_and_wal() {
        let mut node = RaftNode::new(NodeId::new("node1:27017"), RsSettings::default());
        node.state = RaftState::Leader;

        for _ in 0..5 {
            node.append_entry(Command::Noop);
        }
        node.last_applied = 5;

        let snap = node.take_snapshot(mdb_doc::Document::new());
        assert_eq!(snap.last_included_index, 5);
        assert!(node.needs_snapshot() == false || node.log.len() <= 2);
    }

    #[test]
    fn raft_node_prevote_and_election() {
        let mut node = RaftNode::new(NodeId::new("node1:27017"), RsSettings::default());
        node.election_elapsed = RaftNode::ELECTION_TIMEOUT_MAX_MS + 100;

        let action = node.on_election_timeout();
        assert!(matches!(action, Some(raft::ElectionAction::PreVote(_))));

        if let Some(raft::ElectionAction::PreVote(pv)) = action {
            assert_eq!(pv.len(), 1);
            assert_eq!(pv[0].candidate_id, NodeId::new("node1:27017"));
        }
    }

    #[test]
    fn election_manager_workflow() {
        let mut mgr = ElectionManager::new();
        let candidate = NodeId::new("node1:27017");

        let requests = mgr.start_election(1, &candidate, 0, 0);
        assert_eq!(requests.len(), 1);

        let responses = vec![
            (NodeId::new("node2:27017"), true, 1),
            (NodeId::new("node3:27017"), true, 1),
        ];
        let winner = mgr.count_votes(&responses, 3, &candidate);
        assert_eq!(winner, Some(candidate));
    }

    #[test]
    fn change_stream_workflow() {
        let mut oplog = Oplog::new(OplogConfig::default());
        let entry = OplogEntry {
            ts: mdb_doc::Timestamp::new(1000, 1),
            op: OpType::Insert,
            ns: "test.users".into(),
            o: mdb_doc::Document::new(),
            o2: None,
            ui: mdb_doc::ObjectId::from_bytes([0; 12]),
        };
        oplog.append(entry);

        let mut cs = ChangeStream::new("test.users");
        let event = cs.next_event(&oplog);
        assert!(event.is_some());
        assert_eq!(event.unwrap().operation_type, OperationType::Insert);
    }

    #[test]
    fn membership_change_via_raft() {
        let mut node = RaftNode::new(NodeId::new("n1"), RsSettings::default());
        node.state = RaftState::Leader;
        node.membership.members = vec![NodeId::new("n1")];

        node.append_entry(Command::AddNode(NodeId::new("n2")));
        node.commit_index = 1;
        let applied = node.apply_committed();
        assert_eq!(applied.len(), 1);
        assert!(node.membership.contains(&NodeId::new("n2")));

        node.append_entry(Command::RemoveNode(NodeId::new("n2")));
        node.commit_index = 2;
        node.apply_committed();
        assert!(!node.membership.contains(&NodeId::new("n2")));
    }

    #[test]
    fn read_concern_levels() {
        let mut node = RaftNode::new(NodeId::new("n1"), RsSettings::default());
        node.state = RaftState::Leader;
        node.committed_index = 1;

        assert!(node.can_read(ReadConcern::Local));
        assert!(node.can_read(ReadConcern::Majority));
    }

    #[test]
    fn cursor_based_replication() {
        let mut rs = ReplicaSet::new();
        rs.add_member(NodeId::new("n1"), 1.0);
        rs.add_member(NodeId::new("n2"), 1.0);
        rs.primary = Some(NodeId::new("n1"));

        let entry = OplogEntry {
            ts: mdb_doc::Timestamp::new(100, 1),
            op: OpType::Insert,
            ns: "test.users".into(),
            o: mdb_doc::Document::new(),
            o2: None,
            ui: mdb_doc::ObjectId::from_bytes([0; 12]),
        };
        if let Some(oplog) = rs.member_oplogs.get_mut(&NodeId::new("n1")) {
            oplog.append(entry);
        }

        let (cursor, entries) = rs.get_more(&NodeId::new("n2"), 10);
        assert_eq!(entries.len(), 1);
        assert!(cursor > 0);

        let (_, entries2) = rs.get_more(&NodeId::new("n2"), 10);
        assert!(entries2.is_empty());
    }
}
