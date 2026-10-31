//! Raft 网络传输层：基于 TCP 的节点间 RPC。
//!
//! # 设计
//!
//! - 使用长度前缀 + BSON 编码的 RPC 消息
//! - 每个节点监听一个 TCP 端口
//! - 支持 RequestVote、AppendEntries、InstallSnapshot 三种 RPC
//!
//! # 协议格式
//!
//! ```text
//! [4 字节消息长度][BSON 编码的 RPC 消息]
//! ```
//!
//! BSON 消息格式：
//! ```json
//! {
//!   "type": "requestVote" | "requestVoteResponse" | "appendEntries" | ...,
//!   "from": "node_id",
//!   "payload": { ... }
//! }
//! ```

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use mdb_common::{MdbError, MdbResult};
use mdb_doc::{Bson, Document};

use crate::raft::{AppendEntries, AppendEntriesResponse, InstallSnapshot, InstallSnapshotResponse, RequestVote, RequestVoteResponse};
use crate::types::{LogEntry, NodeId};

/// Raft RPC 类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRpcType {
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse,
    InstallSnapshot,
    InstallSnapshotResponse,
}

impl RaftRpcType {
    fn as_str(&self) -> &'static str {
        match self {
            RaftRpcType::RequestVote => "requestVote",
            RaftRpcType::RequestVoteResponse => "requestVoteResponse",
            RaftRpcType::AppendEntries => "appendEntries",
            RaftRpcType::AppendEntriesResponse => "appendEntriesResponse",
            RaftRpcType::InstallSnapshot => "installSnapshot",
            RaftRpcType::InstallSnapshotResponse => "installSnapshotResponse",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "requestVote" => Some(RaftRpcType::RequestVote),
            "requestVoteResponse" => Some(RaftRpcType::RequestVoteResponse),
            "appendEntries" => Some(RaftRpcType::AppendEntries),
            "appendEntriesResponse" => Some(RaftRpcType::AppendEntriesResponse),
            "installSnapshot" => Some(RaftRpcType::InstallSnapshot),
            "installSnapshotResponse" => Some(RaftRpcType::InstallSnapshotResponse),
            _ => None,
        }
    }
}

/// Raft RPC 消息。
#[derive(Debug, Clone)]
pub enum RaftMessage {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(Box<AppendEntries>),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshot(Box<InstallSnapshot>),
    InstallSnapshotResponse(InstallSnapshotResponse),
}

impl RaftMessage {
    pub fn rpc_type(&self) -> RaftRpcType {
        match self {
            RaftMessage::RequestVote(_) => RaftRpcType::RequestVote,
            RaftMessage::RequestVoteResponse(_) => RaftRpcType::RequestVoteResponse,
            RaftMessage::AppendEntries(_) => RaftRpcType::AppendEntries,
            RaftMessage::AppendEntriesResponse(_) => RaftRpcType::AppendEntriesResponse,
            RaftMessage::InstallSnapshot(_) => RaftRpcType::InstallSnapshot,
            RaftMessage::InstallSnapshotResponse(_) => RaftRpcType::InstallSnapshotResponse,
        }
    }

    /// 序列化为 BSON 文档。
    pub fn to_bson(&self, from: &NodeId) -> Document {
        let mut doc = Document::new();
        doc.insert("type", Bson::String(self.rpc_type().as_str().to_string()));
        doc.insert("from", Bson::String(from.to_string()));

        let payload = match self {
            RaftMessage::RequestVote(rv) => {
                let mut p = Document::new();
                p.insert("term", Bson::Int64(rv.term as i64));
                p.insert("candidateId", Bson::String(rv.candidate_id.to_string()));
                p.insert("lastLogIndex", Bson::Int64(rv.last_log_index as i64));
                p.insert("lastLogTerm", Bson::Int64(rv.last_log_term as i64));
                Bson::Document(p)
            }
            RaftMessage::RequestVoteResponse(rv) => {
                let mut p = Document::new();
                p.insert("term", Bson::Int64(rv.term as i64));
                p.insert("voteGranted", Bson::Boolean(rv.vote_granted));
                Bson::Document(p)
            }
            RaftMessage::AppendEntries(ae) => {
                let mut p = Document::new();
                p.insert("term", Bson::Int64(ae.term as i64));
                p.insert("leaderId", Bson::String(ae.leader_id.to_string()));
                p.insert("prevLogIndex", Bson::Int64(ae.prev_log_index as i64));
                p.insert("prevLogTerm", Bson::Int64(ae.prev_log_term as i64));
                p.insert("leaderCommit", Bson::Int64(ae.leader_commit as i64));
                let entries: Vec<Bson> = ae.entries.iter().map(|e| {
                    Bson::Document({
                        let mut d = Document::new();
                        d.insert("index", Bson::Int64(e.index as i64));
                        d.insert("term", Bson::Int64(e.term as i64));
                        d.insert("command", Bson::String(format!("{:?}", e.command)));
                        d
                    })
                }).collect();
                p.insert("entries", Bson::Array(entries));
                Bson::Document(p)
            }
            RaftMessage::AppendEntriesResponse(ae) => {
                let mut p = Document::new();
                p.insert("term", Bson::Int64(ae.term as i64));
                p.insert("success", Bson::Boolean(ae.success));
                p.insert("matchIndex", Bson::Int64(ae.match_index as i64));
                Bson::Document(p)
            }
            RaftMessage::InstallSnapshot(is) => {
                let mut p = Document::new();
                p.insert("term", Bson::Int64(is.term as i64));
                p.insert("leaderId", Bson::String(is.leader_id.to_string()));
                p.insert("lastIncludedIndex", Bson::Int64(is.last_included_index as i64));
                p.insert("lastIncludedTerm", Bson::Int64(is.last_included_term as i64));
                p.insert("done", Bson::Boolean(is.done));
                p.insert("offset", Bson::Int64(is.offset as i64));
                p.insert("dataSize", Bson::Int64(is.data_size as i64));
                Bson::Document(p)
            }
            RaftMessage::InstallSnapshotResponse(is) => {
                let mut p = Document::new();
                p.insert("term", Bson::Int64(is.term as i64));
                Bson::Document(p)
            }
        };
        doc.insert("payload", payload);
        doc
    }

    /// 从 BSON 文档反序列化。
    pub fn from_bson(doc: &Document) -> Option<Self> {
        let rpc_type_str = doc.get("type")?.as_str()?;
        let rpc_type = RaftRpcType::from_str(rpc_type_str)?;
        let payload = doc.get("payload")?.as_document()?;

        // 辅助函数
        let get_str = |d: &Document, k: &str| d.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
        let get_i64 = |d: &Document, k: &str| d.get(k).and_then(|v| v.as_i64()).unwrap_or(0);
        let get_bool = |d: &Document, k: &str| d.get(k).and_then(|v| v.as_bool()).unwrap_or(false);

        Some(match rpc_type {
            RaftRpcType::RequestVote => RaftMessage::RequestVote(RequestVote {
                term: get_i64(payload, "term") as u64,
                candidate_id: NodeId::new(&get_str(payload, "candidateId")),
                last_log_index: get_i64(payload, "lastLogIndex") as u64,
                last_log_term: get_i64(payload, "lastLogTerm") as u64,
            }),
            RaftRpcType::RequestVoteResponse => RaftMessage::RequestVoteResponse(RequestVoteResponse {
                term: get_i64(payload, "term") as u64,
                vote_granted: get_bool(payload, "voteGranted"),
            }),
            RaftRpcType::AppendEntries => {
                let entries = payload.get("entries").and_then(|v| v.as_array()).map(|arr| {
                    arr.iter().filter_map(|e| e.as_document()).map(|d| LogEntry {
                        index: get_i64(d, "index") as u64,
                        term: get_i64(d, "term") as u64,
                        command: crate::types::Command::Noop,
                    }).collect()
                }).unwrap_or_default();
                RaftMessage::AppendEntries(Box::new(AppendEntries {
                    term: get_i64(payload, "term") as u64,
                    leader_id: NodeId::new(&get_str(payload, "leaderId")),
                    prev_log_index: get_i64(payload, "prevLogIndex") as u64,
                    prev_log_term: get_i64(payload, "prevLogTerm") as u64,
                    entries,
                    leader_commit: get_i64(payload, "leaderCommit") as u64,
                }))
            }
            RaftRpcType::AppendEntriesResponse => RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: get_i64(payload, "term") as u64,
                success: get_bool(payload, "success"),
                match_index: get_i64(payload, "matchIndex") as u64,
            }),
            RaftRpcType::InstallSnapshot => RaftMessage::InstallSnapshot(Box::new(InstallSnapshot {
                term: get_i64(payload, "term") as u64,
                leader_id: NodeId::new(&get_str(payload, "leaderId")),
                last_included_index: get_i64(payload, "lastIncludedIndex") as u64,
                last_included_term: get_i64(payload, "lastIncludedTerm") as u64,
                data: vec![],
                done: get_bool(payload, "done"),
                offset: get_i64(payload, "offset") as u64,
                data_size: get_i64(payload, "dataSize") as u64,
            })),
            RaftRpcType::InstallSnapshotResponse => RaftMessage::InstallSnapshotResponse(InstallSnapshotResponse {
                term: get_i64(payload, "term") as u64,
            }),
        })
    }
}

/// Raft 传输层 trait。
pub trait RaftTransport: Send + Sync {
    /// 发送 RPC 消息到指定节点，等待响应。
    fn send_rpc(&self, to: &NodeId, msg: &RaftMessage) -> MdbResult<RaftMessage>;

    /// 本节点 ID。
    fn local_id(&self) -> &NodeId;
}

/// TCP Raft 传输层。
pub struct TcpRaftTransport {
    local_id: NodeId,
    local_addr: String,
    /// 节点 ID → 地址映射。
    peers: Arc<Mutex<HashMap<String, String>>>,
    /// 接收线程句柄。
    _listener_thread: Option<thread::JoinHandle<()>>,
    /// 接收队列。
    rx: Arc<Mutex<mpsc::Receiver<(NodeId, RaftMessage)>>>,
    /// 发送队列（用于接收线程通知）。
    _tx: Sender<(NodeId, RaftMessage)>,
}

impl std::fmt::Debug for TcpRaftTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpRaftTransport")
            .field("local_id", &self.local_id)
            .field("local_addr", &self.local_addr)
            .field("peers", &self.peers.lock().unwrap().len())
            .finish()
    }
}

impl TcpRaftTransport {
    /// 创建 TCP 传输层并开始监听。
    pub fn new(local_id: NodeId, listen_addr: &str) -> MdbResult<Self> {
        let listener = TcpListener::bind(listen_addr)
            .map_err(|e| MdbError::new(mdb_common::ErrorCode::IllegalOperation, format!("bind {listen_addr}: {e}")))?;

        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));

        let peers: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

        // 接收线程
        let peers_clone = peers.clone();
        let local_id_clone = local_id.clone();
        let tx_clone = tx.clone();
        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let peers = peers_clone.clone();
                let local_id = local_id_clone.clone();
                let tx = tx_clone.clone();
                thread::spawn(move || {
                    let _ = handle_connection(stream, &peers, &local_id, &tx);
                });
            }
        });

        Ok(Self {
            local_id,
            local_addr: listen_addr.to_string(),
            peers,
            _listener_thread: Some(handle),
            rx,
            _tx: tx,
        })
    }

    /// 添加对等节点。
    pub fn add_peer(&self, node_id: &NodeId, addr: &str) {
        self.peers.lock().unwrap().insert(node_id.to_string(), addr.to_string());
    }

    /// 接收一个 RPC 消息（非阻塞）。
    pub fn try_recv(&self) -> Option<(NodeId, RaftMessage)> {
        self.rx.lock().unwrap().try_recv().ok()
    }
}

impl RaftTransport for TcpRaftTransport {
    fn send_rpc(&self, to: &NodeId, msg: &RaftMessage) -> MdbResult<RaftMessage> {
        let peers = self.peers.lock().unwrap();
        let addr = peers.get(to.as_str())
            .ok_or_else(|| MdbError::new(mdb_common::ErrorCode::HostUnreachable, format!("no address for {to}")))?;

        let mut stream = TcpStream::connect(addr)
            .map_err(|e| MdbError::new(mdb_common::ErrorCode::HostUnreachable, format!("connect {addr}: {e}")))?;

        // 发送消息
        let doc = msg.to_bson(&self.local_id);
        let data = mdb_doc::codec::to_bytes(&doc)
            .map_err(|e| MdbError::new(mdb_common::ErrorCode::BadValue, format!("encode: {e}")))?;
        let len = (data.len() as u32).to_be_bytes();
        stream.write_all(&len)
            .map_err(|e| MdbError::new(mdb_common::ErrorCode::HostUnreachable, format!("write: {e}")))?;
        stream.write_all(&data)
            .map_err(|e| MdbError::new(mdb_common::ErrorCode::HostUnreachable, format!("write: {e}")))?;
        stream.flush()
            .map_err(|e| MdbError::new(mdb_common::ErrorCode::HostUnreachable, format!("flush: {e}")))?;

        // 读取响应
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)
            .map_err(|e| MdbError::new(mdb_common::ErrorCode::HostUnreachable, format!("read len: {e}")))?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf)
            .map_err(|e| MdbError::new(mdb_common::ErrorCode::HostUnreachable, format!("read resp: {e}")))?;

        let resp_doc = mdb_doc::codec::from_bytes(&resp_buf)
            .map_err(|e| MdbError::new(mdb_common::ErrorCode::BadValue, format!("decode: {e}")))?;

        RaftMessage::from_bson(&resp_doc)
            .ok_or_else(|| MdbError::new(mdb_common::ErrorCode::BadValue, "invalid response"))
    }

    fn local_id(&self) -> &NodeId {
        &self.local_id
    }
}

/// 处理单个 TCP 连接。
fn handle_connection(
    mut stream: TcpStream,
    peers: &Arc<Mutex<HashMap<String, String>>>,
    local_id: &NodeId,
    tx: &Sender<(NodeId, RaftMessage)>,
) -> MdbResult<()> {
    loop {
        // 读取消息长度
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).is_err() {
            break;
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len == 0 || msg_len > 10 * 1024 * 1024 {
            break;
        }

        // 读取消息体
        let mut msg_buf = vec![0u8; msg_len];
        if stream.read_exact(&mut msg_buf).is_err() {
            break;
        }

        // 解析消息
        let doc = match mdb_doc::codec::from_bytes(&msg_buf) {
            Ok(d) => d,
            Err(_) => break,
        };

        let Some(msg) = RaftMessage::from_bson(&doc) else { break };
        let from = NodeId::new(doc.get("from").and_then(|v| v.as_str()).unwrap_or(""));

        // 发送到接收队列
        if tx.send((from.clone(), msg.clone())).is_err() {
            break;
        }

        // 处理 RPC 并发送响应
        let response = process_rpc(&msg, peers, local_id);
        if let Some(resp) = response {
            let resp_doc = resp.to_bson(local_id);
            let resp_data = match mdb_doc::codec::to_bytes(&resp_doc) {
                Ok(d) => d,
                Err(_) => break,
            };
            let resp_len = (resp_data.len() as u32).to_be_bytes();
            if stream.write_all(&resp_len).is_err() { break; }
            if stream.write_all(&resp_data).is_err() { break; }
            if stream.flush().is_err() { break; }
        }
    }
    Ok(())
}

/// 处理 RPC 消息，生成响应。
fn process_rpc(msg: &RaftMessage, _peers: &Arc<Mutex<HashMap<String, String>>>, _local_id: &NodeId) -> Option<RaftMessage> {
    match msg {
        RaftMessage::RequestVote(rv) => {
            // 简化实现：总是授予投票
            Some(RaftMessage::RequestVoteResponse(RequestVoteResponse {
                term: rv.term,
                vote_granted: true,
            }))
        }
        RaftMessage::AppendEntries(ae) => {
            // 简化实现：总是成功
            Some(RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: ae.term,
                success: true,
                match_index: ae.prev_log_index + ae.entries.len() as u64,
            }))
        }
        RaftMessage::InstallSnapshot(is) => {
            Some(RaftMessage::InstallSnapshotResponse(InstallSnapshotResponse {
                term: is.term,
            }))
        }
        _ => None, // 响应消息不需要处理
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_serialization() {
        let msg = RaftMessage::RequestVote(RequestVote {
            term: 5,
            candidate_id: NodeId::new("node1"),
            last_log_index: 10,
            last_log_term: 4,
        });

        let from = NodeId::new("node2");
        let doc = msg.to_bson(&from);
        let restored = RaftMessage::from_bson(&doc).expect("deserialize");

        match restored {
            RaftMessage::RequestVote(rv) => {
                assert_eq!(rv.term, 5);
                assert_eq!(rv.candidate_id.to_string(), "node1");
                assert_eq!(rv.last_log_index, 10);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_append_entries_serialization() {
        let msg = RaftMessage::AppendEntries(Box::new(AppendEntries {
            term: 3,
            leader_id: NodeId::new("leader"),
            prev_log_index: 5,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 4,
        }));

        let from = NodeId::new("follower");
        let doc = msg.to_bson(&from);
        let restored = RaftMessage::from_bson(&doc).expect("deserialize");

        match restored {
            RaftMessage::AppendEntries(ae) => {
                assert_eq!(ae.term, 3);
                assert_eq!(ae.leader_commit, 4);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
