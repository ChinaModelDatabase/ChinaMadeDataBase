//! 复制基础类型定义。

use mdb_doc::{Document, ObjectId, Timestamp};

/// 节点标识，例如 `"node1:27017"`。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Raft 节点状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

/// Raft 日志条目。
#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
}

/// 复制命令。
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Insert {
        ns: String,
        doc: Document,
    },
    Update {
        ns: String,
        query: Document,
        update: Document,
    },
    Delete {
        ns: String,
        query: Document,
    },
    /// 添加副本集成员。
    AddNode(NodeId),
    /// 移除副本集成员。
    RemoveNode(NodeId),
    Noop,
}

/// Oplog 操作类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpType {
    Insert,
    Update,
    Delete,
    Command,
}

impl OpType {
    /// MongoDB 单字符 op 编码。
    pub fn as_op_char(self) -> char {
        match self {
            OpType::Insert => 'i',
            OpType::Update => 'u',
            OpType::Delete => 'd',
            OpType::Command => 'c',
        }
    }

    /// 从 MongoDB op 字符解析。
    pub fn from_op_char(c: char) -> Option<Self> {
        match c {
            'i' => Some(OpType::Insert),
            'u' => Some(OpType::Update),
            'd' => Some(OpType::Delete),
            'c' => Some(OpType::Command),
            _ => None,
        }
    }
}

/// Change Stream 事件操作类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    Insert,
    Update,
    Replace,
    Delete,
    Invalidate,
    Drop,
    Rename,
    DropDatabase,
}

impl OperationType {
    /// 从字符串解析。
    pub fn from_op_str(s: &str) -> Option<Self> {
        match s {
            "insert" => Some(OperationType::Insert),
            "update" => Some(OperationType::Update),
            "replace" => Some(OperationType::Replace),
            "delete" => Some(OperationType::Delete),
            "invalidate" => Some(OperationType::Invalidate),
            "drop" => Some(OperationType::Drop),
            "rename" => Some(OperationType::Rename),
            "dropDatabase" => Some(OperationType::DropDatabase),
            _ => None,
        }
    }

    /// 转为字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            OperationType::Insert => "insert",
            OperationType::Update => "update",
            OperationType::Replace => "replace",
            OperationType::Delete => "delete",
            OperationType::Invalidate => "invalidate",
            OperationType::Drop => "drop",
            OperationType::Rename => "rename",
            OperationType::DropDatabase => "dropDatabase",
        }
    }
}

/// Oplog 条目。
#[derive(Debug, Clone, PartialEq)]
pub struct OplogEntry {
    /// 时间戳。
    pub ts: Timestamp,
    /// 操作类型。
    pub op: OpType,
    /// 命名空间，格式 `db.collection`。
    pub ns: String,
    /// 操作文档：insert 时为完整文档，update 时为 `$set` 表达式，delete 时为查询条件。
    pub o: Document,
    /// update 的查询条件（仅 update 操作）。
    pub o2: Option<Document>,
    /// 唯一标识。
    pub ui: ObjectId,
}

/// Change Stream 恢复令牌。
#[derive(Debug, Clone, PartialEq)]
pub struct ResumeToken {
    data: Document,
}

impl ResumeToken {
    /// 从 Document 构造。
    pub fn new(data: Document) -> Self {
        Self { data }
    }

    /// 获取内部数据引用。
    pub fn data(&self) -> &Document {
        &self.data
    }

    /// 序列化为 Bson。
    pub fn to_bson(&self) -> mdb_doc::Bson {
        mdb_doc::Bson::Document(self.data.clone())
    }

    /// 从 Bson 反序列化。
    pub fn from_bson(b: &mdb_doc::Bson) -> Option<Self> {
        match b {
            mdb_doc::Bson::Document(d) => Some(Self { data: d.clone() }),
            _ => None,
        }
    }

    /// 从 Change Stream 事件构造恢复令牌。
    pub fn from_event(event: &ChangeStreamEvent) -> Self {
        Self {
            data: event._id.clone(),
        }
    }
}

/// Change Stream 事件。
#[derive(Debug, Clone)]
pub struct ChangeStreamEvent {
    /// 恢复令牌（嵌入在事件中）。
    pub _id: Document,
    /// 操作类型。
    pub operation_type: OperationType,
    /// 命名空间。
    pub ns: String,
    /// 完整文档（insert/replace 时提供，update 时按 fullDocument 选项返回）。
    pub full_document: Option<Document>,
    /// 修改前的文档快照（fullDocumentBeforeChange 选项启用时返回）。
    pub full_document_before_change: Option<Document>,
    /// 文档键。
    pub document_key: Document,
    /// 集群时间戳。
    pub cluster_time: Timestamp,
    /// 事务编号（多文档事务中使用）。
    pub txn_number: Option<i64>,
    /// 更新描述（update/replace 事件时提供）。
    pub update_description: Option<Document>,
}

/// Raft 快照：包含某个索引/任期处的状态机状态。
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// 快照对应的最后日志索引。
    pub last_included_index: u64,
    /// 快照对应的最后日志任期。
    pub last_included_term: u64,
    /// 状态机状态。
    pub state: Document,
}

/// 读关注级别。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadConcern {
    /// 从 leader 当前状态读取（可能读到未提交的数据）。
    Local,
    /// 只读已提交到多数节点的数据。
    Majority,
    /// 线性一致性读：保证读到最新的已提交数据。
    Linearizable,
}

impl Default for ReadConcern {
    fn default() -> Self {
        Self::Local
    }
}

impl ReadConcern {
    /// 从字符串解析。
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "local" => Some(Self::Local),
            "majority" => Some(Self::Majority),
            "linearizable" => Some(Self::Linearizable),
            _ => None,
        }
    }

    /// 转为字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Majority => "majority",
            Self::Linearizable => "linearizable",
        }
    }
}

/// 预投票请求（Pre-Vote）。
#[derive(Debug, Clone)]
pub struct PreVoteRequest {
    /// 候选人当前任期（不递增）。
    pub term: u64,
    /// 候选人 ID。
    pub candidate_id: NodeId,
    /// 候选人最后日志索引。
    pub last_log_index: u64,
    /// 候选人最后日志任期。
    pub last_log_term: u64,
}

/// 预投票响应。
#[derive(Debug, Clone)]
pub struct PreVoteResponse {
    /// 响应者当前任期。
    pub term: u64,
    /// 是否同意预投票。
    pub vote_granted: bool,
}

/// 集群成员配置。
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterMembership {
    /// 当前成员列表。
    pub members: Vec<NodeId>,
    /// 是否处于联合共识阶段。
    pub joint: bool,
    /// 联合共识的目标配置（新成员列表）。
    pub joint_members: Option<Vec<NodeId>>,
}

impl Default for ClusterMembership {
    fn default() -> Self {
        Self {
            members: Vec::new(),
            joint: false,
            joint_members: None,
        }
    }
}

impl ClusterMembership {
    /// 创建新的集群成员配置。
    pub fn new(members: Vec<NodeId>) -> Self {
        Self {
            members,
            joint: false,
            joint_members: None,
        }
    }

    /// 开始联合共识：过渡到新配置。
    pub fn begin_joint(&mut self, new_members: Vec<NodeId>) {
        self.joint = true;
        self.joint_members = Some(new_members);
    }

    /// 完成联合共识：切换到最终配置。
    pub fn commit_joint(&mut self) {
        if let Some(new) = self.joint_members.take() {
            self.members = new;
        }
        self.joint = false;
    }

    /// 获取当前投票所需多数（联合阶段需要两个配置各自多数同意）。
    pub fn majority(&self) -> usize {
        let base = self.members.len();
        base / 2 + 1
    }

    /// 检查节点是否为当前成员。
    pub fn contains(&self, id: &NodeId) -> bool {
        self.members.contains(id)
            || self
                .joint_members
                .as_ref()
                .map_or(false, |jm| jm.contains(id))
    }
}

/// WAL 持久化条目（用于写前日志）。
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// 日志索引。
    pub index: u64,
    /// 日志任期。
    pub term: u64,
    /// 命令。
    pub command: Command,
}

/// Oplog 复制游标。
#[derive(Debug, Clone)]
pub struct OplogCursor {
    /// 游标 ID。
    pub cursor_id: u64,
    /// 起始时间戳。
    pub start_ts: Timestamp,
    /// 批大小。
    pub batch_size: usize,
}

impl Default for OplogCursor {
    fn default() -> Self {
        Self {
            cursor_id: 0,
            start_ts: Timestamp::new(0, 0),
            batch_size: 100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_display() {
        let id = NodeId::new("node1:27017");
        assert_eq!(id.as_str(), "node1:27017");
        assert_eq!(format!("{id}"), "node1:27017");
    }

    #[test]
    fn node_id_from_str() {
        let id: NodeId = "node2:27017".into();
        assert_eq!(id.0, "node2:27017");
    }

    #[test]
    fn node_id_eq_hash() {
        let a = NodeId::new("n1:27017");
        let b = NodeId::new("n1:27017");
        assert_eq!(a, b);
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(a.clone(), 1);
        assert_eq!(map.get(&b), Some(&1));
    }

    #[test]
    fn raft_state_variants() {
        assert_ne!(RaftState::Follower, RaftState::Candidate);
        assert_ne!(RaftState::Candidate, RaftState::Leader);
        assert_ne!(RaftState::Follower, RaftState::Leader);
    }

    #[test]
    fn log_entry_clone() {
        let entry = LogEntry {
            term: 1,
            index: 0,
            command: Command::Noop,
        };
        let cloned = entry.clone();
        assert_eq!(entry, cloned);
    }

    #[test]
    fn command_variants() {
        let insert = Command::Insert {
            ns: "test.col".into(),
            doc: Document::new(),
        };
        let update = Command::Update {
            ns: "test.col".into(),
            query: Document::new(),
            update: Document::new(),
        };
        let delete = Command::Delete {
            ns: "test.col".into(),
            query: Document::new(),
        };
        let add_node = Command::AddNode(NodeId::new("n1"));
        let remove_node = Command::RemoveNode(NodeId::new("n1"));
        let noop = Command::Noop;
        assert_ne!(insert, update);
        assert_ne!(delete, noop);
        assert_ne!(add_node, remove_node);
    }

    #[test]
    fn op_type_roundtrip() {
        for op in [OpType::Insert, OpType::Update, OpType::Delete, OpType::Command] {
            let c = op.as_op_char();
            assert_eq!(OpType::from_op_char(c), Some(op));
        }
        assert_eq!(OpType::from_op_char('x'), None);
    }

    #[test]
    fn operation_type_str_roundtrip() {
        for (s, expected) in [
            ("insert", OperationType::Insert),
            ("update", OperationType::Update),
            ("replace", OperationType::Replace),
            ("delete", OperationType::Delete),
            ("invalidate", OperationType::Invalidate),
            ("drop", OperationType::Drop),
            ("rename", OperationType::Rename),
            ("dropDatabase", OperationType::DropDatabase),
        ] {
            let ot = OperationType::from_op_str(s).unwrap();
            assert_eq!(ot, expected);
            assert_eq!(ot.as_str(), s);
        }
        assert!(OperationType::from_op_str("unknown").is_none());
    }

    #[test]
    fn oplog_entry_fields() {
        let entry = OplogEntry {
            ts: Timestamp::new(1000, 1),
            op: OpType::Insert,
            ns: "test.users".into(),
            o: Document::new(),
            o2: None,
            ui: ObjectId::from_bytes([0; 12]),
        };
        assert_eq!(entry.ts.secs, 1000);
        assert_eq!(entry.op, OpType::Insert);
        assert_eq!(entry.ns, "test.users");
        assert!(entry.o2.is_none());
    }

    #[test]
    fn resume_token_roundtrip() {
        let mut data = Document::new();
        data.insert("_data", "token123");
        let token = ResumeToken::new(data.clone());
        let bson = token.to_bson();
        let restored = ResumeToken::from_bson(&bson).unwrap();
        assert_eq!(token, restored);
    }

    #[test]
    fn resume_token_from_bson_non_document() {
        let bson = mdb_doc::Bson::Int32(42);
        assert!(ResumeToken::from_bson(&bson).is_none());
    }

    #[test]
    fn change_stream_event_fields() {
        let event = ChangeStreamEvent {
            _id: {
                let mut d = Document::new();
                d.insert("_data", "abc");
                d
            },
            operation_type: OperationType::Insert,
            ns: "test.col".into(),
            full_document: Some(Document::new()),
            full_document_before_change: None,
            document_key: {
                let mut dk = Document::new();
                dk.insert("_id", mdb_doc::Bson::Int32(1));
                dk
            },
            cluster_time: Timestamp::new(100, 0),
            txn_number: Some(1),
            update_description: None,
        };
        assert_eq!(event.operation_type, OperationType::Insert);
        assert!(event.full_document.is_some());
        assert!(event.full_document_before_change.is_none());
        assert_eq!(event.txn_number, Some(1));
        assert!(event.update_description.is_none());
    }

    #[test]
    fn snapshot_fields() {
        let snap = Snapshot {
            last_included_index: 10,
            last_included_term: 2,
            state: Document::new(),
        };
        assert_eq!(snap.last_included_index, 10);
        assert_eq!(snap.last_included_term, 2);
    }

    #[test]
    fn read_concern_roundtrip() {
        for s in ["local", "majority", "linearizable"] {
            let rc = ReadConcern::from_str(s).unwrap();
            assert_eq!(rc.as_str(), s);
        }
        assert!(ReadConcern::from_str("invalid").is_none());
    }

    #[test]
    fn read_concern_default() {
        assert_eq!(ReadConcern::default(), ReadConcern::Local);
    }

    #[test]
    fn prevote_request_fields() {
        let req = PreVoteRequest {
            term: 5,
            candidate_id: NodeId::new("n1"),
            last_log_index: 10,
            last_log_term: 3,
        };
        assert_eq!(req.term, 5);
        assert_eq!(req.candidate_id, NodeId::new("n1"));
    }

    #[test]
    fn prevote_response_fields() {
        let resp = PreVoteResponse {
            term: 5,
            vote_granted: true,
        };
        assert!(resp.vote_granted);
    }

    #[test]
    fn cluster_membership_basic() {
        let cm = ClusterMembership::new(vec![
            NodeId::new("n1"),
            NodeId::new("n2"),
            NodeId::new("n3"),
        ]);
        assert!(!cm.joint);
        assert!(cm.contains(&NodeId::new("n1")));
        assert!(!cm.contains(&NodeId::new("n4")));
        assert_eq!(cm.majority(), 2);
    }

    #[test]
    fn cluster_membership_joint_consensus() {
        let mut cm = ClusterMembership::new(vec![
            NodeId::new("n1"),
            NodeId::new("n2"),
            NodeId::new("n3"),
        ]);
        cm.begin_joint(vec![
            NodeId::new("n1"),
            NodeId::new("n2"),
            NodeId::new("n3"),
            NodeId::new("n4"),
            NodeId::new("n5"),
        ]);
        assert!(cm.joint);
        assert!(cm.contains(&NodeId::new("n4")));
        cm.commit_joint();
        assert!(!cm.joint);
        assert_eq!(cm.members.len(), 5);
    }

    #[test]
    fn oplog_cursor_default() {
        let c = OplogCursor::default();
        assert_eq!(c.cursor_id, 0);
        assert_eq!(c.batch_size, 100);
    }

    #[test]
    fn wal_entry_fields() {
        let e = WalEntry {
            index: 1,
            term: 2,
            command: Command::Noop,
        };
        assert_eq!(e.index, 1);
        assert_eq!(e.term, 2);
    }
}
