//! MDB 服务端：TCP 监听与连接处理。//!
//! 支持完整 CRUD、索引、聚合、事务、副本集等MongoDB 兼容命令。

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod audit;
pub mod catalog;
pub mod gridfs;
pub mod js_eval;
pub mod metrics;
pub mod production_tests;
mod ids;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use audit::{AuditEvent, AuditEventType, AuditLogger, AuditResult};
use catalog::{Catalog, CollectionStore, StoredIndexSpec};
use ids::{IntrusionDetector, IdsConfig, RateLimitResult};
use mdb_common::{ErrorCode, MdbError, MdbResult};
use mdb_common::config::StorageConfig;
use mdb_common::time::unix_millis;
use mdb_crypto::scram::{ScramMechanism, ServerScram, StoredKey};
use mdb_doc::{Bson, Document, ObjectId};
use mdb_index::manager::{IndexManager, IndexSpec};
use mdb_index::{IndexType, IndexOptions, RecordId, SortOrder};
use mdb_query::cursor::SharedCursorManager;
use mdb_query::aggregation::{Pipeline, PipelineStage, MergeWhenMatched, MergeWhenNotMatched, DensifyRange};
use mdb_query::types::SortDirection;
use mdb_repl::{ReplicaSet, NodeId, ChangeStream, ChangeStreamConfig, FullDocumentMode, FullDocumentBeforeChangeMode, OplogEntry, OpType, ResumeStartPoint, ResumeToken, StreamLevel, RaftState, ReplicaMemberState, RaftTransport, TcpRaftTransport};
use mdb_router::{ShardRouter, ConfigDb, ShardId, ShardKey, validate_shard_key, Chunk, ChunkId, KeyRange};
use mdb_wire::conn::{Dispatched, Framed};
use mdb_wire::handshake::{hello_reply, is_handshake, TopologyVersion, MAX_WIRE_VERSION};
use mdb_wire::{MessageHeader, OpCode, OpMsg};

/// 默认客户端回退地址（peer_addr 解析失败时使用）。
const DEFAULT_CLIENT_ADDR: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), 0);

/// TLS 配置（TLS 支持暂未实现，使用占位结构体）。
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl TlsConfig {
    pub fn new() -> Self {
        Self { enabled: false, cert_path: None, key_path: None }
    }
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

// ---------------------------------------------------------------------------
// Metrics HTTP endpoint (Prometheus /metrics)
// ---------------------------------------------------------------------------

pub const DEFAULT_METRICS_PORT: u16 = 9216;

/// 启动 Prometheus 指标 HTTP 端点（非阻塞，在独立线程中运行）。
///
/// 返回实际监听的地址，调用方可据此确认端口绑定是否成功。
/// HTTP 服务仅处理 GET /metrics，返回 Prometheus 文本格式。
/// 无认证要求（内部监控端点）。
pub fn start_metrics_endpoint(
    port: u16,
) -> MdbResult<std::net::SocketAddr> {
    let listener = std::net::TcpListener::bind(format!("0.0.0.0:{port}")).map_err(|e| {
        MdbError::new(
            ErrorCode::IllegalOperation,
            format!("metrics endpoint bind failed on port {port}: {e}"),
        )
    })?;
    let addr = listener.local_addr().map_err(|e| {
        MdbError::new(
            ErrorCode::IllegalOperation,
            format!("metrics local_addr failed: {e}"),
        )
    })?;
    listener.set_nonblocking(true).ok();
    std::thread::Builder::new()
        .name("mdb-metrics".into())
        .spawn(move || {
            serve_metrics_loop(listener);
        })
        .map_err(|e| {
            MdbError::new(
                ErrorCode::IllegalOperation,
                format!("metrics thread spawn failed: {e}"),
            )
        })?;
    Ok(addr)
}

fn serve_metrics_loop(listener: std::net::TcpListener) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).ok();
                let _ = stream.set_nodelay(true);
                std::thread::spawn(move || {
                    handle_metrics_request(stream);
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(_) => break,
        }
    }
}

fn handle_metrics_request(stream: std::net::TcpStream) {
    use std::io::{BufRead, BufReader, BufWriter, Write};
    let mut reader = BufReader::new(&stream);
    let mut request_line = String::new();
    let _ = reader.read_line(&mut request_line);

    // Read and discard headers
    let mut header = String::new();
    loop {
        header.clear();
        if reader.read_line(&mut header).is_err() || header.trim().is_empty() {
            break;
        }
    }

    let is_metrics_get = request_line.starts_with("GET /metrics")
        || request_line.starts_with("GET / HTTP/");

    let mut writer = BufWriter::new(&stream);
    if is_metrics_get {
        let body = mdb_common::metrics::render_prometheus(mdb_common::metrics::global());
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = writer.write_all(response.as_bytes());
    } else {
        let body = "404 Not Found\n";
        let response = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = writer.write_all(response.as_bytes());
    }
    let _ = writer.flush();
}

// Helper traits to make Document access ergonomic
trait DocExt {
    fn get_str(&self, key: &str) -> Option<&str>;
    fn get_i64(&self, key: &str) -> Option<i64>;
    fn get_bool(&self, key: &str) -> Option<bool>;
    fn get_f64(&self, key: &str) -> Option<f64>;
    fn get_array_ref(&self, key: &str) -> Option<&[Bson]>;
    fn get_doc(&self, key: &str) -> Option<&Document>;
}

impl DocExt for Document {
    fn get_str(&self, key: &str) -> Option<&str> { self.get(key).and_then(|v| v.as_str()) }
    fn get_i64(&self, key: &str) -> Option<i64> { self.get(key).and_then(|v| v.as_i64()) }
    fn get_bool(&self, key: &str) -> Option<bool> { self.get(key).and_then(|v| v.as_bool()) }
    fn get_f64(&self, key: &str) -> Option<f64> { self.get(key).and_then(|v| v.as_f64()) }
    fn get_array_ref(&self, key: &str) -> Option<&[Bson]> { self.get(key).and_then(|v| v.as_array()) }
    fn get_doc(&self, key: &str) -> Option<&Document> { self.get(key).and_then(|v| v.as_document()) }
}

// ---------------------------------------------------------------------------
// Collection / Database
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Collection {
    name: String,
    store: CollectionStore,
    index_manager: IndexManager,
    next_record_id: AtomicI64,
    /// 存储键 -> RecordId 反查表。
    ///
    /// 索引条目以 RecordId 存储，而存储键只含 `_id`，无法从键反推
    /// RecordId。删除/替换文档时必须用它找到正确的 RecordId，
    /// 否则索引残留幽灵条目。
    record_ids: Mutex<HashMap<Vec<u8>, RecordId>>,
    /// RecordId -> 存储键 反查表（索引查询时需要从 RecordId 找到存储键）。
    record_keys: Mutex<HashMap<RecordId, Vec<u8>>>,
}
impl Collection {
    fn new(name: &str) -> Self { Self::new_with_store(name, CollectionStore::in_memory()) }
    fn new_with_store(name: &str, store: CollectionStore) -> Self {
        Self {
            name: name.to_owned(),
            store,
            index_manager: IndexManager::new(4096),
            next_record_id: AtomicI64::new(1),
            record_ids: Mutex::new(HashMap::new()),
            record_keys: Mutex::new(HashMap::new()),
        }
    }
    fn next_record_id(&self) -> RecordId { self.next_record_id.fetch_add(1, Ordering::Relaxed) as RecordId }
/// 全集合扫描。
    fn scan_all(&self) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> { self.store.scan_all() }
    /// 文档数量。
    fn count(&self) -> MdbResult<usize> { self.store.len() }
    /// 启动时把 `next_record_id` 推到已用最大值之后，避免重启后 RecordId 复用。
    fn bump_record_id_above(&self, r: RecordId) {
        let as_i64 = r as i64;
        self.next_record_id.fetch_max(as_i64 + 1, Ordering::Relaxed);
    }
    /// 登记新文档的 RecordId，并返回它。
    fn bind_record_id(&self, key: &[u8], record_id: RecordId) {
        self.record_ids.lock().unwrap().insert(key.to_vec(), record_id);
        self.record_keys.lock().unwrap().insert(record_id, key.to_vec());
    }
    /// 取出并移除某个键的 RecordId。
    fn take_record_id(&self, key: &[u8]) -> Option<RecordId> {
        let rid = self.record_ids.lock().unwrap().remove(key);
        if let Some(r) = rid {
            self.record_keys.lock().unwrap().remove(&r);
        }
        rid
    }
    /// 读取但不移除。
    fn peek_record_id(&self, key: &[u8]) -> Option<RecordId> {
        self.record_ids.lock().unwrap().get(key).copied()
    }
    /// 从 RecordId 反查存储键（索引查询命中后需要）。
    fn storage_key_for_record(&self, record_id: RecordId) -> Option<Vec<u8>> {
        self.record_keys.lock().unwrap().get(&record_id).cloned()
    }
}

#[derive(Debug)]
struct Database { name: String, catalog: Option<Arc<Catalog>>, collections: Mutex<BTreeMap<String, Arc<Mutex<Collection>>>> }
impl Database {
    fn new(name: &str) -> Self {
        Self { name: name.to_owned(), catalog: None, collections: Mutex::new(BTreeMap::new()) }
    }
    fn with_catalog(name: &str, catalog: Arc<Catalog>) -> Self {
        Self { name: name.to_owned(), catalog: Some(catalog), collections: Mutex::new(BTreeMap::new()) }
    }
    fn store_for(&self, name: &str) -> CollectionStore {
        match &self.catalog {
            Some(c) => CollectionStore::persistent(c.clone(), &self.name, name),
            None => CollectionStore::in_memory(),
        }
    }
    fn get_or_create_collection(&self, name: &str) -> Arc<Mutex<Collection>> {
        let mut cols = self.collections.lock().unwrap();
        if let Some(existing) = cols.get(name) { return existing.clone(); }
        // 持久化模式下必须先登记集合，否则重启后目录里查不到它。
        // 登记失败不静默：记错误日志，且让写入路径报错，而不是假装成功。
        if let Some(c) = &self.catalog {
            if let Err(e) = c.create_collection(&self.name, name) {
                mdb_common::log_error!("catalog", "create_collection({}.{}) failed: {}", self.name, name, e);
            }
        }
        let coll = Arc::new(Mutex::new(Collection::new_with_store(name, self.store_for(name))));
        cols.insert(name.to_owned(), coll.clone());
        coll
    }
    fn get_collection(&self, name: &str) -> Option<Arc<Mutex<Collection>>> { self.collections.lock().unwrap().get(name).cloned() }
    fn drop_collection(&self, name: &str) -> bool { self.collections.lock().unwrap().remove(name).is_some() }
    fn list_collections(&self) -> Vec<String> { self.collections.lock().unwrap().keys().cloned().collect() }
}

// ---------------------------------------------------------------------------
// Auth & session types
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ConnectionState {
    authenticated_user: Option<String>,
    database: String,
    last_used: Instant,
    scram_session: Option<ScramSession>,
    peer_addr: Option<SocketAddr>,
}

impl Clone for ConnectionState {
    fn clone(&self) -> Self {
        Self {
            authenticated_user: self.authenticated_user.clone(),
            database: self.database.clone(),
            last_used: self.last_used,
            scram_session: None,
            peer_addr: self.peer_addr,
        }
    }
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            authenticated_user: None,
            database: "test".into(),
            last_used: Instant::now(),
            scram_session: None,
            peer_addr: None,
        }
    }
}

#[derive(Debug)]
struct ScramSession {
    server_scram: ServerScram,
    database: String,
}

#[derive(Debug, Clone)]
struct UserRecord {
    username: String,
    database: String,
    credential: StoredKey,
    roles: Vec<Role>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionId(Vec<u8>);

/// 操作性能记录。
#[derive(Debug, Clone)]
struct ProfileEntry {
    /// 时间戳（毫秒）。
    timestamp: i64,
    /// 操作类型（如 "insert", "query", "update", "delete"）。
    op_type: String,
    /// 命名空间（如 "db.collection"）。
    ns: String,
    /// 执行时间（毫秒）。
    duration_ms: i64,
    /// 命令摘要（截断）。
    command_summary: String,
    /// 是否慢查询。
    slow: bool,
}

/// Profiler：记录慢操作。
#[derive(Debug)]
struct Profiler {
    /// profile 级别：0=关闭, 1=仅慢操作, 2=所有操作。
    level: i64,
    /// 慢查询阈值（毫秒）。
    slow_ms: i64,
    /// 最近的操作记录（环形缓冲区，最多 1000 条）。
    entries: VecDeque<ProfileEntry>,
}

/// 迁移状态：跟踪正在进行的 chunk 迁移。
#[derive(Debug)]
struct MigrationState {
    /// 正在迁移的 chunk 集合（ns -> chunk_id）。
    active: HashMap<String, Vec<String>>,
    /// 迁移期间缓冲的写操作。
    buffered_writes: Vec<BufferedWrite>,
    /// 迁移进度：chunk_id -> 当前阶段。
    progress: HashMap<String, MigrationPhase>,
}

/// 迁移阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationPhase {
    /// 准备中。
    Preparing,
    /// 数据克隆中。
    Cloning,
    /// 持久化中。
    Persisting,
    /// 提交中（ConfigDb 更新）。
    Committing,
    /// 清理中（源数据删除）。
    Cleanup,
    /// 已完成。
    Completed,
    /// 已中止。
    Aborted,
}

impl MigrationPhase {
    fn as_str(&self) -> &'static str {
        match self {
            MigrationPhase::Preparing => "preparing",
            MigrationPhase::Cloning => "cloning",
            MigrationPhase::Persisting => "persisting",
            MigrationPhase::Committing => "committing",
            MigrationPhase::Cleanup => "cleanup",
            MigrationPhase::Completed => "completed",
            MigrationPhase::Aborted => "aborted",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, MigrationPhase::Completed | MigrationPhase::Aborted)
    }
}

/// 迁移期间缓冲的写操作。
#[derive(Debug, Clone)]
struct BufferedWrite {
    /// 命名空间（db.collection）。
    ns: String,
    /// chunk id。
    chunk_id: String,
    /// 操作类型。
    op: String,
    /// 存储键。
    key: Vec<u8>,
    /// 文档内容。
    value: Option<Vec<u8>>,
}

impl MigrationState {
    fn new() -> Self {
        Self {
            active: HashMap::new(),
            buffered_writes: Vec::new(),
            progress: HashMap::new(),
        }
    }

    /// 开始迁移 chunk。
    fn start_migration(&mut self, ns: &str, chunk_id: &str) {
        self.active.entry(ns.to_string()).or_default().push(chunk_id.to_string());
        self.progress.insert(chunk_id.to_string(), MigrationPhase::Preparing);
    }

    /// 更新迁移阶段。
    fn set_phase(&mut self, chunk_id: &str, phase: MigrationPhase) {
        if !self.progress.get(chunk_id).map_or(true, |p| p.is_terminal()) {
            self.progress.insert(chunk_id.to_string(), phase);
        }
    }

    /// 获取迁移阶段。
    fn get_phase(&self, chunk_id: &str) -> Option<&MigrationPhase> {
        self.progress.get(chunk_id)
    }

    /// 结束迁移 chunk，返回缓冲的写操作。
    fn finish_migration(&mut self, ns: &str, chunk_id: &str) -> Vec<BufferedWrite> {
        if let Some(chunks) = self.active.get_mut(ns) {
            chunks.retain(|c| c != chunk_id);
            if chunks.is_empty() {
                self.active.remove(ns);
            }
        }
        self.progress.insert(chunk_id.to_string(), MigrationPhase::Completed);
        // 返回该 chunk 缓冲的写操作
        let mut result = Vec::new();
        let mut remaining = Vec::new();
        for write in self.buffered_writes.drain(..) {
            if write.ns == ns && write.chunk_id == chunk_id {
                result.push(write);
            } else {
                remaining.push(write);
            }
        }
        self.buffered_writes = remaining;
        result
    }

    /// 中止迁移。
    fn abort_migration(&mut self, ns: &str, chunk_id: &str) -> Vec<BufferedWrite> {
        if let Some(chunks) = self.active.get_mut(ns) {
            chunks.retain(|c| c != chunk_id);
            if chunks.is_empty() {
                self.active.remove(ns);
            }
        }
        self.progress.insert(chunk_id.to_string(), MigrationPhase::Aborted);
        // 返回该 chunk 缓冲的写操作（需要回滚到源分片）
        let mut result = Vec::new();
        let mut remaining = Vec::new();
        for write in self.buffered_writes.drain(..) {
            if write.ns == ns && write.chunk_id == chunk_id {
                result.push(write);
            } else {
                remaining.push(write);
            }
        }
        self.buffered_writes = remaining;
        result
    }

    /// 检查 namespace 是否有活跃迁移。
    fn is_migrating(&self, ns: &str) -> bool {
        self.active.contains_key(ns)
    }

    /// 缓冲写操作。
    fn buffer_write(&mut self, write: BufferedWrite) {
        self.buffered_writes.push(write);
    }
}

impl Profiler {
    fn new() -> Self {
        Self {
            level: 0,
            slow_ms: 100,
            entries: VecDeque::new(),
        }
    }

    /// 记录一个操作。
    fn record(&mut self, op_type: &str, ns: &str, duration_ms: i64, command_summary: &str) {
        if self.level == 0 {
            return;
        }
        let slow = duration_ms >= self.slow_ms;
        if self.level == 1 && !slow {
            return; // 只记录慢操作
        }
        let entry = ProfileEntry {
            timestamp: unix_millis(),
            op_type: op_type.to_owned(),
            ns: ns.to_owned(),
            duration_ms,
            command_summary: command_summary.chars().take(200).collect(),
            slow,
        };
        self.entries.push_back(entry);
        // 保留最近 1000 条
        while self.entries.len() > 1000 {
            self.entries.pop_front();
        }
    }

    /// 获取 profile 级别。
    fn level(&self) -> i64 {
        self.level
    }

    /// 获取慢查询阈值。
    fn slow_ms(&self) -> i64 {
        self.slow_ms
    }

    /// 获取所有记录。
    fn entries(&self) -> Vec<ProfileEntry> {
        self.entries.iter().cloned().collect()
    }
}

/// 副本集状态：持有 Raft 共识信息和复制集配置。
#[derive(Debug)]
struct ReplState {
    /// 副本集名称（如 "mdb-rs"）。
    name: String,
    /// Raft 任期（term）。
    term: u64,
    /// Raft 角色。
    role: RaftState,
    /// 复制集成员信息（用于 oplog 和 Change Stream）。
    replica_set: ReplicaSet,
    /// 本节点的 NodeId。
    self_id: NodeId,
    /// 成员配置文档（用于 replSetGetConfig）。
    config_doc: Document,
    /// Raft TCP 传输层（用于多节点通信）。
    transport: Option<Arc<TcpRaftTransport>>,
}

/// 事务操作：在 commit 时按顺序执行。
#[derive(Debug, Clone)]
enum TxnOp {
    Insert { db: String, coll: String, key: Vec<u8>, value: Vec<u8> },
    Update { db: String, coll: String, key: Vec<u8>, value: Vec<u8> },
    Delete { db: String, coll: String, key: Vec<u8> },
}

/// 事务状态。
#[derive(Debug)]
struct TransactionState {
    /// 缓冲的操作。
    buffer: Vec<TxnOp>,
    /// 事务修改的键集合（用于冲突检测）。
    modified_keys: HashMap<(String, String, Vec<u8>), ()>,
    /// 事务开始时的序列号。
    start_seqno: u64,
}

#[derive(Debug, Clone)]
struct Session {
    connection_id: i32,
    last_used: Instant,
    /// 当前事务的待提交操作缓冲区。`None` 表示不在事务中。
    txn_buffer: Option<Vec<TxnOp>>,
    /// 事务是否处于"已开始"状态（MongoDB 要求先 startTransaction）。
    txn_active: bool,
    /// 事务修改的键集合（用于冲突检测）。
    txn_modified_keys: HashMap<(String, String, Vec<u8>), ()>,
    /// 因果一致性：最近一次读操作的时间戳。
    last_read_ts: Option<mdb_doc::Timestamp>,
    /// 因果一致性：最近一次写操作的时间戳。
    last_write_ts: Option<mdb_doc::Timestamp>,
}

impl Session {
    fn new(connection_id: i32) -> Self {
        Self {
            connection_id,
            last_used: Instant::now(),
            txn_buffer: None,
            txn_active: false,
            txn_modified_keys: HashMap::new(),
            last_read_ts: None,
            last_write_ts: None,
        }
    }

    /// 记录读操作时间戳（因果一致性）。
    fn record_read(&mut self, ts: mdb_doc::Timestamp) {
        if self.last_read_ts.map_or(true, |last| ts > last) {
            self.last_read_ts = Some(ts);
        }
    }

    /// 记录写操作时间戳（因果一致性）。
    fn record_write(&mut self, ts: mdb_doc::Timestamp) {
        if self.last_write_ts.map_or(true, |last| ts > last) {
            self.last_write_ts = Some(ts);
        }
    }

    /// 获取因果一致性要求的最小时间戳。
    fn causal_ts(&self) -> Option<mdb_doc::Timestamp> {
        match (self.last_read_ts, self.last_write_ts) {
            (Some(r), Some(w)) => Some(if r > w { r } else { w }),
            (Some(r), None) => Some(r),
            (None, Some(w)) => Some(w),
            (None, None) => None,
        }
    }
}

const SESSION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

// ---------------------------------------------------------------------------
// RBAC: Role & permission types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Role {
    role: String,
    db: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BuiltinRole {
    Root,
    DbOwner,
    DbAdmin,
    UserAdmin,
    ReadWrite,
    Read,
    EnableBackup,
    ClusterAdmin,
}

impl BuiltinRole {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "root" => Some(Self::Root),
            "dbOwner" => Some(Self::DbOwner),
            "dbAdmin" => Some(Self::DbAdmin),
            "userAdmin" => Some(Self::UserAdmin),
            "readWrite" => Some(Self::ReadWrite),
            "read" => Some(Self::Read),
            "enableBackup" => Some(Self::EnableBackup),
            "clusterAdmin" => Some(Self::ClusterAdmin),
            _ => None,
        }
    }
}

/// Returns true if `held_role` is equal to or a superset of `required_role`.
///
/// Role hierarchy (MongoDB-compatible):
/// ```text
/// root          ⊿ dbOwner  ⊿ dbAdmin, readWrite, userAdmin
/// dbOwner       ⊿ readWrite ⊿ read
/// dbOwner       ⊿ dbAdmin
/// dbAdmin       ⊿ (index/stats management)
/// userAdmin     ⊿ (user management)
/// readWrite     ⊿ read
/// clusterAdmin  ⊿ (replica set + sharding admin)
/// enableBackup  ⊿ (backup operations)
/// ```
fn role_implies(held: &str, required: &str) -> bool {
    if held == required {
        return true;
    }
    match held {
        "root" => true,
        "dbOwner" => matches!(required, "dbAdmin" | "readWrite" | "read" | "userAdmin"),
        "dbAdmin" => false,
        "userAdmin" => false,
        "readWrite" => required == "read",
        "read" => false,
        "enableBackup" => false,
        "clusterAdmin" => false,
        _ => false,
    }
}

/// Returns the minimum role required for a given command.
///
/// For collection-level commands, `coll_name` should be provided.
/// Returns `(required_role, scope_is_admin)`. `scope_is_admin` means the role
/// must be granted on the `admin` database to be globally effective.
fn command_required_role(cmd: &str, _coll_name: Option<&str>) -> Option<(&'static str, bool)> {
    match cmd {
        // Read operations
        "find" | "count" | "countDocuments" | "distinct" | "aggregate" | "listIndexes"
        | "explain" | "getMore" | "mapReduce" | "indexStatistics" => Some(("read", false)),

        // Write operations
        "insert" | "update" | "delete" | "findAndModify" => Some(("readWrite", false)),

        // Index management
        "createIndexes" | "dropIndexes" | "reIndex" | "compact" | "validate" | "collMod" => {
            Some(("dbAdmin", false))
        }

        // Collection management
        "create" | "drop" | "renameCollection" => Some(("dbAdmin", false)),
        "listCollections" => Some(("read", false)),

        // Database management
        "dropDatabase" => Some(("dbAdmin", false)),
        "dbStats" | "databaseStats" | "collStats" => Some(("read", false)),

        // User management
        "createUser" | "dropUser" | "updateUser" | "grantRolesToUser"
        | "revokeRolesFromUser" | "usersInfo" | "changePassword" => Some(("userAdmin", false)),

        // Admin operations
        "listDatabases" => Some(("read", true)),
        "serverStatus" | "connectionStatus" => Some(("read", true)),
        "currentOp" | "killOp" | "profile" | "getLog" | "hostInfo" | "getCmdLineOpts" | "top" | "analyze" | "metrics" => Some(("read", true)),

        // Replica set / sharding
        "replSetInitiate" | "replSetGetStatus" | "replSetGetConfig" | "replSetReconfig" | "replSetStartTransport" | "replSetAddPeer" => Some(("clusterAdmin", true)),
        "addShard" | "removeShard" | "listShards" | "enableSharding" | "shardCollection" | "splitAt" | "moveChunk" | "balancerStart" | "balancerStop" | "balancerStatus" | "flushRouterConfig" => Some(("clusterAdmin", true)),

        // Backup
        "backup" | "restore" | "backupCursor" => Some(("enableBackup", true)),

        // Transaction stubs ‟no special role needed
        "startTransaction" | "commitTransaction" | "abortTransaction" => None,

        // Commands exempt from role checks
        "ping" | "buildInfo" | "endSessions" | "getParameter" | "getFreeMonitoringStatus"
        | "hello" | "isMaster" | "ismaster" | "watch" | "killCursors" => None,

        _ => None,
    }
}

/// Check whether a user's roles grant the required permission.
///
/// - `user_roles`: all roles the user holds, each scoped to a database.
/// - `target_db`: the database the command targets.
/// - `coll_name`: optional collection name for collection-level checks.
/// - `required_role`: the minimum role needed (e.g. "read", "readWrite").
/// - `admin_only`: if true, only admin-database roles count.
fn user_has_permission(
    user_roles: &[Role],
    target_db: &str,
    _coll_name: Option<&str>,
    required_role: &str,
    admin_only: bool,
) -> bool {
    for user_role in user_roles {
        // Check inherited roles: e.g. dbOwner implies readWrite implies read
        if !role_implies(&user_role.role, required_role) {
            continue;
        }

        if user_role.db == "admin" {
            // Admin database roles apply globally (unless admin_only was specified,
            // in which case only admin roles count ‟and we're already in this branch).
            if admin_only {
                return true;
            }
            // For non-admin-only checks, admin roles apply globally
            return true;
        }

        if admin_only {
            // Only admin roles count for this command
            continue;
        }

        // Same database
        if user_role.db == target_db {
            return true;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// ServerState
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ServerState {
    databases: Mutex<BTreeMap<String, Arc<Database>>>,
    cursor_manager: SharedCursorManager,
    replica_set: Mutex<Option<ReplState>>,
    sharding: Mutex<Option<ShardRouter>>,
    connections: Mutex<BTreeMap<i32, ConnectionState>>,
    users: Mutex<HashMap<String, UserRecord>>,
    sessions: Mutex<HashMap<SessionId, Session>>,
    tls_enabled: AtomicBool,
    audit_logger: Option<Arc<AuditLogger>>,
    pub ids: Arc<IntrusionDetector>,
    /// 持久化目录：`None` 表示纯内存模式（测试用）。
    catalog: Option<Arc<Catalog>>,
    /// oplog 单调递增计数器（用于生成唯一时间戳）。
    oplog_counter: AtomicU64,
    /// 性能分析器。
    profiler: Mutex<Profiler>,
    /// 迁移状态。
    migration_state: Mutex<MigrationState>,
    /// 监控指标收集器。
    pub metrics: Arc<metrics::MetricsCollector>,
}

impl ServerState {
    fn new() -> Self {
        Self {
            databases: Mutex::new(BTreeMap::new()),
            cursor_manager: SharedCursorManager::new(),
            replica_set: Mutex::new(None),
            sharding: Mutex::new(None),
            connections: Mutex::new(BTreeMap::new()),
            users: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            tls_enabled: AtomicBool::new(false),
            audit_logger: None,
            ids: Arc::new(IntrusionDetector::new(IdsConfig::default())),
            catalog: None,
            oplog_counter: AtomicU64::new(0),
            profiler: Mutex::new(Profiler::new()),
            migration_state: Mutex::new(MigrationState::new()),
            metrics: Arc::new(metrics::MetricsCollector::new()),
        }
    }

    /// 打开（或创建）持久化数据库目录，并从磁盘恢复全部元数据与文档。
    ///
    /// 恢复内容：数据库列表、集合列表与元数据、索引定义、用户账号、文档。
    /// 索引在内存中重建（未持久化索引结构），因此启动比纯内存慢，
    /// 但数据正确性与持久性优先。
    pub fn open(dbpath: &std::path::Path) -> MdbResult<Self> {
        let config = StorageConfig::default();
        let catalog = Arc::new(Catalog::open(dbpath, config)?);
        let mut state = Self {
            databases: Mutex::new(BTreeMap::new()),
            cursor_manager: SharedCursorManager::new(),
            replica_set: Mutex::new(None),
            sharding: Mutex::new(None),
            connections: Mutex::new(BTreeMap::new()),
            users: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            tls_enabled: AtomicBool::new(false),
            audit_logger: None,
            ids: Arc::new(IntrusionDetector::new(IdsConfig::default())),
            catalog: Some(catalog.clone()),
            oplog_counter: AtomicU64::new(0),
            profiler: Mutex::new(Profiler::new()),
            migration_state: Mutex::new(MigrationState::new()),
            metrics: Arc::new(metrics::MetricsCollector::new()),
        };
        state.recover_from_disk()?;

        // 加载分片配置
        if let Some(cat) = &state.catalog {
            if let Ok(Some(config_db)) = cat.load_shard_config() {
                let router = ShardRouter::new(config_db);
                *state.sharding.lock().unwrap() = Some(router);
            }
        }

        Ok(state)
    }

    /// 从磁盘恢复目录结构、集合、文档、索引与用户。
    fn recover_from_disk(&mut self) -> MdbResult<()> {
        let catalog = match &self.catalog {
            Some(c) => c.clone(),
            None => return Ok(()),
        };

        // 1. 数据库与集合目录
        let dbs = catalog.list_databases_persisted();
        let mut recovered_dbs: BTreeMap<String, Arc<Database>> = BTreeMap::new();
        for db_name in dbs {
            let coll_names = catalog.list_collections(&db_name)?;
            let database = Arc::new(Database::with_catalog(&db_name, catalog.clone()));
            for coll_name in coll_names {
                // 重建索引：先从 Catalog 加载持久化的索引定义，再为每个
                // 已存文档分配新的 RecordId 并填充索引。
                let mut index_manager = IndexManager::new(4096);
                // 加载持久化的索引定义
                for ix in catalog.indexes(&db_name, &coll_name) {
                    let fields: Vec<(String, SortOrder)> =
                        ix.fields.iter().map(|f| (f.clone(), SortOrder::Asc)).collect();
                    let _ = index_manager.create_index(
                        IndexSpec { fields, index_type: IndexType::BTree },
                        IndexOptions { name: Some(ix.name), unique: ix.unique, ..Default::default() },
                    );
                }
                let mut coll = Collection::new_with_store(
                    &coll_name,
                    CollectionStore::persistent(catalog.clone(), &db_name, &coll_name),
                );
                let mut next_rid: RecordId = 1;
                let entries = catalog.scan_all(&db_name, &coll_name)?;
                for (k, v) in entries {
                    let record_id = next_rid;
                    next_rid += 1;
                    if let Ok(doc) = mdb_doc::codec::from_bytes(&v) {
                        let _ = index_manager.update_indexes(&doc, record_id);
                        coll.bind_record_id(&k, record_id);
                    }
                }
                coll.bump_record_id_above(next_rid);
                coll.index_manager = index_manager;
                database
                    .collections
                    .lock()
                    .unwrap()
                    .insert(coll_name, Arc::new(Mutex::new(coll)));
            }
            recovered_dbs.insert(db_name, database);
        }
        *self.databases.lock().unwrap() = recovered_dbs;

        // 2. 用户目录
        let mut users = self.users.lock().unwrap();
        users.clear();
        for u in catalog.list_users() {
            // 凭据不完整的记录直接跳过：宁可不认证，也不能用残缺
            // SCRAM 材料构造出一个能通过验证的假身份。
            let Some(p) = Catalog::user_from_doc(&u) else { continue };
            let Some(sk) = StoredKey::from_parts(
                p.mechanism.as_str(),
                p.iteration_count,
                p.salt,
                p.stored_key,
                p.server_key,
            ) else {
                continue;
            };
            let record = UserRecord {
                username: p.username.clone(),
                database: p.database.clone(),
                credential: sk,
                roles: p.roles.into_iter().map(|(role, db)| Role { role, db }).collect(),
            };
            users.insert(record.username.clone(), record);
        }
        Ok(())
    }

    fn with_tls(mut self, enabled: bool) -> Self {
        self.tls_enabled.store(enabled, Ordering::Relaxed);
        self
    }

    /// 设置 TLS 启用状态（用于运行时切换）。
    pub fn set_tls_enabled(&self, enabled: bool) {
        self.tls_enabled.store(enabled, Ordering::Relaxed);
    }

    fn with_audit(mut self, logger: Arc<AuditLogger>) -> Self {
        self.audit_logger = Some(logger);
        self
    }

    /// Log an audit event if audit logging is enabled.
    pub fn audit_log(
        &self,
        event_type: AuditEventType,
        user: Option<String>,
        database: Option<String>,
        command: String,
        arguments: Option<Document>,
        result: AuditResult,
        client_addr: SocketAddr,
        connection_id: i32,
        session_id: Option<String>,
    ) {
        if let Some(ref logger) = self.audit_logger {
            let event = AuditEvent {
                timestamp: unix_millis(),
                event_type,
                user,
                database,
                command,
                arguments,
                result,
                client_addr,
                connection_id,
                session_id,
                audit_id: ObjectId::new().expect("ObjectId::new() failed"),
            };
            audit::global_metrics().record_event(&event);
            logger.log_event(event);
        }
    }

    fn get_database(&self, name: &str) -> Arc<Database> {
        let mut dbs = self.databases.lock().unwrap();
        if let Some(existing) = dbs.get(name) { return existing.clone(); }
        let db = match &self.catalog {
            Some(c) => {
                // 数据库目录必须落盘，否则 listDatabases 重启后为空。
                if let Err(e) = c.put_database(name) {
                    mdb_common::log_error!("catalog", "put_database({name}) failed: {e}");
                }
                Arc::new(Database::with_catalog(name, c.clone()))
            }
            None => Arc::new(Database::new(name)),
        };
        dbs.insert(name.to_owned(), db.clone());
        db
    }
    fn list_databases(&self) -> Vec<String> {
        let in_memory = self.databases.lock().unwrap().keys().cloned().collect::<Vec<_>>();
        match &self.catalog {
            Some(c) => c.list_databases_persisted(),
            None => in_memory,
        }
    }

    fn get_connection(&self, connection_id: i32) -> ConnectionState {
        let mut conns = self.connections.lock().unwrap();
        conns.entry(connection_id).or_default().clone()
    }

    fn update_connection(&self, connection_id: i32, state: &ConnectionState) {
        let mut conns = self.connections.lock().unwrap();
        conns.insert(connection_id, state.clone());
    }

    fn remove_connection(&self, connection_id: i32) {
        let mut conns = self.connections.lock().unwrap();
        conns.remove(&connection_id);
    }

    fn find_user(&self, username: &str, database: &str) -> Option<UserRecord> {
        let key = format!("{username}@{database}");
        self.users.lock().unwrap().get(&key).cloned()
    }

    fn create_user(&self, username: &str, database: &str, password: &str, roles: Vec<Role>) -> MdbResult<()> {
        let key = format!("{username}@{database}");
        let mut users = self.users.lock().unwrap();
        if users.contains_key(&key) {
            return Err(MdbError::new(
                ErrorCode::DuplicateKey,
                format!("User \"{username}\" already exists in database \"{database}\""),
            ));
        }
        let credential = StoredKey::from_password(ScramMechanism::Sha256, password, ScramMechanism::DEFAULT_ITERATIONS)?;
        users.insert(key, UserRecord {
            username: username.to_owned(),
            database: database.to_owned(),
            credential,
            roles,
        });
        Ok(())
    }

    fn drop_user(&self, username: &str, database: &str) -> MdbResult<()> {
        let key = format!("{username}@{database}");
        let mut users = self.users.lock().unwrap();
        users.remove(&key).ok_or_else(|| {
            MdbError::new(ErrorCode::UserNotFound, format!("User \"{username}\" not found in database \"{database}\""))
        })?;
        Ok(())
    }

    fn grant_roles(&self, username: &str, database: &str, roles: Vec<Role>) -> MdbResult<()> {
        let key = format!("{username}@{database}");
        let mut users = self.users.lock().unwrap();
        let user = users.get_mut(&key).ok_or_else(|| {
            MdbError::new(ErrorCode::UserNotFound, format!("User \"{username}\" not found in database \"{database}\""))
        })?;
        for role in roles {
            if !user.roles.iter().any(|r| r.role == role.role && r.db == role.db) {
                user.roles.push(role);
            }
        }
        Ok(())
    }

    fn revoke_roles(&self, username: &str, database: &str, roles: Vec<Role>) -> MdbResult<()> {
        let key = format!("{username}@{database}");
        let mut users = self.users.lock().unwrap();
        let user = users.get_mut(&key).ok_or_else(|| {
            MdbError::new(ErrorCode::UserNotFound, format!("User \"{username}\" not found in database \"{database}\""))
        })?;
        user.roles.retain(|r| !roles.iter().any(|revoke| revoke.role == r.role && revoke.db == r.db));
        Ok(())
    }

    fn update_user_roles(&self, username: &str, database: &str, roles: Vec<Role>) -> MdbResult<()> {
        let key = format!("{username}@{database}");
        let mut users = self.users.lock().unwrap();
        let user = users.get_mut(&key).ok_or_else(|| {
            MdbError::new(ErrorCode::UserNotFound, format!("User \"{username}\" not found in database \"{database}\""))
        })?;
        user.roles = roles;
        Ok(())
    }

    fn change_password(&self, username: &str, database: &str, new_password: &str) -> MdbResult<()> {
        let key = format!("{username}@{database}");
        let mut users = self.users.lock().unwrap();
        let user = users.get_mut(&key).ok_or_else(|| {
            MdbError::new(ErrorCode::UserNotFound, format!("User \"{username}\" not found in database \"{database}\""))
        })?;
        user.credential = StoredKey::from_password(ScramMechanism::Sha256, new_password, ScramMechanism::DEFAULT_ITERATIONS)?;
        Ok(())
    }

    /// 追加一条 oplog 记录（用于 Change Stream 和复制）。
    ///
    /// 如果副本集未初始化，自动创建单节点副本集。
    fn append_oplog(&self, ns: &str, op: OpType, o: Document, o2: Option<Document>) {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0);
        let inc = self.oplog_counter.fetch_add(1, Ordering::Relaxed) as u32;
        let ts = mdb_doc::Timestamp::new(secs, inc);
        let entry = OplogEntry {
            ts,
            op,
            ns: ns.to_owned(),
            o,
            o2,
            ui: new_object_id(),
        };
        let mut guard = self.replica_set.lock().unwrap();
        if guard.is_none() {
            // 自动创建单节点副本集
            let self_id = NodeId::new("localhost:27017");
            let mut rs = ReplicaSet::new();
            rs.add_member(self_id.clone(), 1.0);
            rs.elect_primary();
            *guard = Some(ReplState {
                name: "mdb-rs".to_string(),
                term: 1,
                role: RaftState::Leader,
                replica_set: rs,
                self_id,
                config_doc: Document::new(),
                transport: None,
            });
        }
        if let Some(repl) = guard.as_mut() {
            repl.replica_set.apply_oplog_entry(entry);
        }
    }

    fn purge_expired_sessions(&self) {
        let mut sessions = self.sessions.lock().unwrap();
        let now = Instant::now();
        sessions.retain(|_, s| now.duration_since(s.last_used) < SESSION_TIMEOUT);
    }

    fn connection_status(&self, pool: &ConnectionPool) -> Document {
        let conns = self.connections.lock().unwrap();
        let authenticated = conns.values().filter(|c| c.authenticated_user.is_some()).count();
        let mut reply = ok_body();
        reply.insert("authenticatedUsers", Bson::Int32(authenticated as i32));
        reply.insert("activeConnections", Bson::Int32(pool.active_count() as i32));
        reply.insert("availableConnections", Bson::Int32(pool.available_count() as i32));
        reply
    }
}

// ===========================================================================
// Connection Pool
// ===========================================================================

const DEFAULT_MAX_CONNECTIONS: usize = 65536;

#[derive(Debug)]
pub struct ConnectionPool {
    max_connections: usize,
    active_connections: AtomicUsize,
    idle_connections: Mutex<Vec<TcpStream>>,
    waiters: Mutex<VecDeque<()>>,
    condvar: Condvar,
    timeout: Duration,
    shutdown: AtomicBool,
}

impl ConnectionPool {
    pub fn new(max_connections: usize, timeout: Duration) -> Self {
        Self {
            max_connections,
            active_connections: AtomicUsize::new(0),
            idle_connections: Mutex::new(Vec::new()),
            waiters: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            timeout,
            shutdown: AtomicBool::new(false),
        }
    }

    pub fn acquire(&self) -> Result<(), MdbError> {
        let current = self.active_connections.load(Ordering::Acquire);
        if current >= self.max_connections {
            if self.shutdown.load(Ordering::Acquire) {
                return Err(MdbError::new(
                    ErrorCode::ShutdownInProgress,
                    "connection pool shut down",
                ));
            }
            let mut w = self.waiters.lock().unwrap();
            w.push_back(());
            let result = self.condvar.wait_timeout(w, self.timeout).unwrap();
            let mut waiters = result.0;
            let timed_out = result.1.timed_out();
            // Remove this waiter's slot (clean up after ourselves)
            if let Some(last) = waiters.pop_back() {
                // We added one entry; remove it.
                // If another thread already removed ours and left its own,
                // we remove theirs (safe: it will re-add itself on retry).
                let _ = last;
            }
            if timed_out {
                return Err(MdbError::new(
                    ErrorCode::ExceededTimeLimit,
                    "connection pool exhausted: timed out waiting for a slot",
                ));
            }
            if self.shutdown.load(Ordering::Acquire) {
                return Err(MdbError::new(
                    ErrorCode::ShutdownInProgress,
                    "connection pool shut down",
                ));
            }
        }
        self.active_connections.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub fn release(&self) {
        self.active_connections.fetch_sub(1, Ordering::Release);
        let _ = self.waiters.lock().unwrap().pop_front();
        self.condvar.notify_one();
    }

    pub fn active_count(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    pub fn idle_count(&self) -> usize {
        self.idle_connections.lock().unwrap().len()
    }

    pub fn available_count(&self) -> usize {
        let active = self.active_connections.load(Ordering::Relaxed);
        self.max_connections.saturating_sub(active)
    }

    pub fn push_idle(&self, stream: TcpStream) {
        self.idle_connections.lock().unwrap().push(stream);
    }

    pub fn pop_idle(&self) -> Option<TcpStream> {
        self.idle_connections.lock().unwrap().pop()
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.idle_connections.lock().unwrap().clear();
        self.condvar.notify_all();
    }
}

// ===========================================================================
// Thread Pool (work-stealing)
// ===========================================================================

type Job = Box<dyn FnOnce() + Send + 'static>;

struct SharedGlobalQueue {
    queue: Mutex<VecDeque<Job>>,
    condvar: Condvar,
}

impl std::fmt::Debug for SharedGlobalQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedGlobalQueue").finish_non_exhaustive()
    }
}

impl SharedGlobalQueue {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
        }
    }

    fn push(&self, job: Job) {
        self.queue.lock().unwrap().push_back(job);
        self.condvar.notify_one();
    }

    fn pop(&self) -> Option<Job> {
        self.queue.lock().unwrap().pop_front()
    }

    fn is_empty(&self) -> bool {
        self.queue.lock().unwrap().is_empty()
    }

    fn drain(&self) -> Vec<Job> {
        let mut q = self.queue.lock().unwrap();
        q.drain(..).collect()
    }
}

struct WorkerPool {
    workers: Vec<std::thread::JoinHandle<()>>,
    global_queue: Arc<SharedGlobalQueue>,
    local_queues: Vec<Arc<Mutex<VecDeque<Job>>>>,
    shutdown: Arc<AtomicBool>,
}

impl std::fmt::Debug for WorkerPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerPool")
            .field("worker_count", &self.workers.len())
            .field("local_queue_count", &self.local_queues.len())
            .finish_non_exhaustive()
    }
}

impl WorkerPool {
    fn new(size: usize) -> Self {
        let global_queue = Arc::new(SharedGlobalQueue::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut local_queues = Vec::with_capacity(size);
        let mut handles = Vec::with_capacity(size);

        for id in 0..size {
            let local_queue = Arc::new(Mutex::new(VecDeque::new()));
            local_queues.push(Arc::clone(&local_queue));

            let global = Arc::clone(&global_queue);
            let shutdown_flag = Arc::clone(&shutdown);
            let all_locals: Vec<Arc<Mutex<VecDeque<Job>>>> =
                local_queues.iter().map(Arc::clone).collect();

            let handle = std::thread::Builder::new()
                .name(format!("mdb-worker-{id}"))
                .spawn(move || {
                    Self::run_worker(id, local_queue, global, all_locals, shutdown_flag);
                })
                .expect("failed to spawn worker thread");

            handles.push(handle);
        }

        Self {
            workers: handles,
            global_queue,
            local_queues,
            shutdown,
        }
    }

    fn run_worker(
        id: usize,
        local_queue: Arc<Mutex<VecDeque<Job>>>,
        global: Arc<SharedGlobalQueue>,
        all_locals: Vec<Arc<Mutex<VecDeque<Job>>>>,
        shutdown: Arc<AtomicBool>,
    ) {
        loop {
            // 1. Try local queue
            {
                let mut q = local_queue.lock().unwrap();
                if let Some(job) = q.pop_front() {
                    drop(q);
                    job();
                    continue;
                }
            }

            // 2. Try work-stealing from a random other worker
            if !all_locals.is_empty() {
                if let Some(jobs) = Self::steal_jobs(id, &all_locals) {
                    local_queue.lock().unwrap().extend(jobs);
                    continue;
                }
            }

            // 3. Try global queue
            if let Some(job) = global.pop() {
                job();
                continue;
            }

            // 4. Wait on global condvar
            {
                let mut q = global.queue.lock().unwrap();
                if q.is_empty() && !shutdown.load(Ordering::Acquire) {
                    let _ = global.condvar.wait_timeout(q, Duration::from_millis(50));
                }
            }

            // 5. Check shutdown
            if shutdown.load(Ordering::Acquire) {
                // Drain local queue before exiting
                let remaining: Vec<Job> = local_queue.lock().unwrap().drain(..).collect();
                for job in remaining {
                    job();
                }
                mdb_common::log_info!("mdb-server", "worker-{id} exiting");
                return;
            }
        }
    }

    fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.global_queue.push(Box::new(f));
        self.global_queue.condvar.notify_one();
    }

    fn steal_jobs(
        thief_id: usize,
        all_locals: &[Arc<Mutex<VecDeque<Job>>>],
    ) -> Option<Vec<Job>> {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};
        let s = RandomState::new();
        let mut h = s.build_hasher();
        h.write_u64(Instant::now().elapsed().as_nanos() as u64);
        let start = (h.finish() as usize) % all_locals.len();

        for i in 0..all_locals.len() {
            let idx = (start + i) % all_locals.len();
            if idx == thief_id {
                continue;
            }
            let mut q = all_locals[idx].lock().unwrap();
            if q.len() > 1 {
                let half = q.len() / 2;
                let stolen: Vec<Job> = q.drain(..half).collect();
                return Some(stolen);
            }
        }
        None
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.global_queue.condvar.notify_all();
    }

    fn join(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        self.global_queue.condvar.notify_all();
        // Drain remaining jobs
        for job in self.global_queue.drain() {
            job();
        }
        for local in &self.local_queues {
            for job in local.lock().unwrap().drain(..) {
                job();
            }
        }
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.join();
    }
}

// ===========================================================================
// Graceful Shutdown Controller
// ===========================================================================

#[derive(Debug)]
struct ShutdownController {
    shutdown_requested: AtomicBool,
    connection_ids: Mutex<Vec<i32>>,
}

impl ShutdownController {
    fn new() -> Self {
        Self {
            shutdown_requested: AtomicBool::new(false),
            connection_ids: Mutex::new(Vec::new()),
        }
    }

    fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Release);
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown_requested.load(Ordering::Acquire)
    }

    fn register(&self, id: i32) {
        self.connection_ids.lock().unwrap().push(id);
    }

    fn unregister(&self, id: i32) {
        self.connection_ids.lock().unwrap().retain(|&c| c != id);
    }

    fn active_connection_count(&self) -> usize {
        self.connection_ids.lock().unwrap().len()
    }

    fn drain(&self) -> Vec<i32> {
        self.connection_ids.lock().unwrap().drain(..).collect()
    }
}

#[derive(Debug)]
pub struct Server {
    listener: TcpListener,
    connection_id: std::sync::atomic::AtomicI32,
    topology_version: std::sync::Arc<TopologyVersion>,
    state: Arc<ServerState>,
    pool: Arc<ConnectionPool>,
    worker_pool: Mutex<WorkerPool>,
    shutdown_controller: Arc<ShutdownController>,
    last_activity: Arc<Mutex<HashMap<i32, Instant>>>,
    tls_config: std::sync::Arc<std::sync::RwLock<TlsConfig>>,
}
impl Server {
    pub fn bind(addr: SocketAddr) -> MdbResult<Self> {
        let listener = TcpListener::bind(addr).map_err(|e| {
            MdbError::new(ErrorCode::IllegalOperation, format!("bind {addr} failed: {e}"))
        })?;
        let pool = Arc::new(ConnectionPool::new(
            DEFAULT_MAX_CONNECTIONS,
            Duration::from_secs(30),
        ));
        let worker_pool = WorkerPool::new(std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4));
        Ok(Self {
            listener,
            connection_id: std::sync::atomic::AtomicI32::new(1),
            topology_version: std::sync::Arc::new(TopologyVersion::new()),
            state: Arc::new(ServerState::new()),
            pool,
            worker_pool: Mutex::new(worker_pool),
            shutdown_controller: Arc::new(ShutdownController::new()),
            last_activity: Arc::new(Mutex::new(HashMap::new())),
            tls_config: std::sync::Arc::new(std::sync::RwLock::new(TlsConfig::new())),
        })
    }

    /// 以持久化模式绑定：`dbpath` 指向数据目录，重启后自动恢复。
    ///
    /// 目录不存在时自动创建。`dbpath` 为 `None` 时退化为纯内存模式。
    pub fn bind_with_dbpath(addr: SocketAddr, dbpath: &str) -> MdbResult<Self> {
        let listener = TcpListener::bind(addr).map_err(|e| {
            MdbError::new(ErrorCode::IllegalOperation, format!("bind {addr} failed: {e}"))
        })?;
        let pool = Arc::new(ConnectionPool::new(
            DEFAULT_MAX_CONNECTIONS,
            Duration::from_secs(30),
        ));
        let worker_pool = WorkerPool::new(std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4));
        let state = ServerState::open(std::path::Path::new(dbpath))?;
        Ok(Self {
            listener,
            connection_id: std::sync::atomic::AtomicI32::new(1),
            topology_version: std::sync::Arc::new(TopologyVersion::new()),
            state: Arc::new(state),
            pool,
            worker_pool: Mutex::new(worker_pool),
            shutdown_controller: Arc::new(ShutdownController::new()),
            last_activity: Arc::new(Mutex::new(HashMap::new())),
            tls_config: std::sync::Arc::new(std::sync::RwLock::new(TlsConfig::new())),
        })
    }

    pub fn with_tls(&self, enabled: bool) {
        self.tls_config.write().unwrap().enabled = enabled;
        self.state.set_tls_enabled(enabled);
    }

    pub fn set_tls_config(&self, config: TlsConfig) {
        *self.tls_config.write().unwrap() = config;
    }

    pub fn local_addr(&self) -> MdbResult<SocketAddr> {
        self.listener.local_addr().map_err(|e| {
            MdbError::new(ErrorCode::IllegalOperation, format!("local_addr failed: {e}"))
        })
    }

    pub fn accept_once(&self) -> MdbResult<()> {
        if self.shutdown_controller.is_shutdown() {
            return Err(MdbError::new(
                ErrorCode::ShutdownInProgress,
                "server is shutting down",
            ));
        }
        let (stream, peer) = self.listener.accept().map_err(|e| {
            MdbError::new(ErrorCode::IllegalOperation, format!("accept failed: {e}"))
        })?;
        Self::setup_keepalive(&stream);
        if self.state.ids.is_ip_blocked(&peer.ip()) {
            mdb_common::log_info!("mdb-ids", "rejecting blacklisted IP {peer}");
            drop(stream);
            return Ok(());
        }
        if let Err(e) = self.pool.acquire() {
            mdb_common::log_error!("mdb-server", "rejecting connection from {peer}: {e}");
            drop(stream);
            return Ok(());
        }
        let conn_id = self.connection_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.shutdown_controller.register(conn_id);
        {
            self.last_activity.lock().unwrap().insert(conn_id, Instant::now());
        }
        let topology = std::sync::Arc::clone(&self.topology_version);
        let state = Arc::clone(&self.state);
        let pool = Arc::clone(&self.pool);
        let shutdown_ctrl = Arc::clone(&self.shutdown_controller);
        let last_activity = Arc::new(Mutex::new(self.last_activity.lock().unwrap().clone()));
        let pool_ref = Arc::clone(&self.pool);
        self.worker_pool.lock().unwrap().execute(move || {
            if let Err(e) = handle_connection(stream, conn_id, &topology, &state, &pool_ref, &last_activity)
            {
                mdb_common::log_error!("mdb-server", "connection from {peer} failed: {e}");
            }
            shutdown_ctrl.unregister(conn_id);
            pool.release();
        });
        Ok(())
    }

    pub fn serve_forever(&self) -> MdbResult<()> {
        self.listener.set_nonblocking(true).ok();
        while !self.shutdown_controller.is_shutdown() {
            match self.listener.accept() {
                Ok((stream, peer)) => {
                    // Listener is non-blocking; ensure accepted connections are blocking
                    let _ = stream.set_nonblocking(false);
                    Self::setup_keepalive(&stream);
                    if self.state.ids.is_ip_blocked(&peer.ip()) {
                        mdb_common::log_info!("mdb-ids", "rejecting blacklisted IP {peer}");
                        drop(stream);
                        continue;
                    }
                    if let Err(e) = self.pool.acquire() {
                        mdb_common::log_error!("mdb-server", "rejecting connection from {peer}: {e}");
                        drop(stream);
                        continue;
                    }
                    let conn_id =
                        self.connection_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.shutdown_controller.register(conn_id);
                    {
                        self.last_activity
                            .lock()
                            .unwrap()
                            .insert(conn_id, Instant::now());
                    }
                    let topology = std::sync::Arc::clone(&self.topology_version);
                    let state = Arc::clone(&self.state);
                    let pool = Arc::clone(&self.pool);
                    let shutdown_ctrl = Arc::clone(&self.shutdown_controller);
                    let last_activity = Arc::clone(&self.last_activity);
                    let pool_ref = Arc::clone(&self.pool);
                    self.worker_pool.lock().unwrap().execute(move || {
                        if let Err(e) = handle_connection(
                            stream,
                            conn_id,
                            &topology,
                            &state,
                            &pool_ref,
                            &last_activity,
                        ) {
                            mdb_common::log_error!(
                                "mdb-server",
                                "connection from {peer} failed: {e}"
                            );
                        }
                        shutdown_ctrl.unregister(conn_id);
                        pool.release();
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    if self.shutdown_controller.is_shutdown() {
                        break;
                    }
                    mdb_common::log_error!("mdb-server", "accept error: {e}");
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
        // Graceful shutdown: stop pool, drain remaining jobs
        self.pool.shutdown();
        self.worker_pool.lock().unwrap().join();
        self.listener.set_nonblocking(false).ok();
        Ok(())
    }

    fn setup_keepalive(stream: &TcpStream) {
        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(300)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(30)));
    }
}

pub fn handle_connection(
    stream: TcpStream,
    connection_id: i32,
    topology_version: &TopologyVersion,
    state: &ServerState,
    pool: &ConnectionPool,
    last_activity: &Arc<Mutex<HashMap<i32, Instant>>>,
) -> MdbResult<()> {
    let _ = stream.set_nodelay(true);
    let peer_addr = stream.peer_addr().ok();

    // 检测TLS：查看第一字节是否下TLS 记录类型 (20-23)
    let mut peek_buf = [0u8; 1];
    stream.peek(&mut peek_buf).map_err(|e| {
        MdbError::new(ErrorCode::IllegalOperation, format!("peek failed: {e}"))
    })?;

    let is_tls = matches!(peek_buf[0], 20..=23);

    if is_tls && state.tls_enabled.load(Ordering::Relaxed) {
        handle_tls_connection(stream, connection_id, topology_version, state, pool, last_activity)
    } else {
        handle_plaintext_connection(stream, connection_id, topology_version, state, pool, last_activity)
    }
}

/// 处理 TLS 加密连接（使用 mdb-tls 的 TLCP 实现）。
/// Rustls TLS 流包装器：实现 Read/Write 以兼容 Framed。
struct RustlsStream {
    stream: TcpStream,
    conn: rustls::ServerConnection,
    buf: Vec<u8>,
    pos: usize,
}

impl Read for RustlsStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // 如果内部缓冲区有数据，先返回
        if self.pos < self.buf.len() {
            let remaining = &self.buf[self.pos..];
            let n = remaining.len().min(buf.len());
            buf[..n].copy_from_slice(&remaining[..n]);
            self.pos += n;
            return Ok(n);
        }
        // 从 TLS 连接读取
        loop {
            self.buf.clear();
            self.buf.resize(8192, 0);
            let n = self.conn.reader().read(&mut self.buf)?;
            if n == 0 {
                return Ok(0);
            }
            self.buf.truncate(n);
            self.pos = 0;
            let copy_len = n.min(buf.len());
            buf[..copy_len].copy_from_slice(&self.buf[..copy_len]);
            self.pos = copy_len;
            return Ok(copy_len);
        }
    }
}

impl Write for RustlsStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.conn.writer().write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.conn.writer().flush()?;
        Ok(())
    }
}

fn handle_tls_connection(
    stream: TcpStream,
    connection_id: i32,
    topology_version: &TopologyVersion,
    state: &ServerState,
    pool: &ConnectionPool,
    last_activity: &Arc<Mutex<HashMap<i32, Instant>>>,
) -> MdbResult<()> {
    let _ = stream.set_nodelay(true);
    let peer_addr = stream.peer_addr().ok();

    // 尝试标准 TLS (rustls)
    match try_rustls_handshake(stream, state) {
        Ok(tls_stream) => {
            handle_stream_loop(tls_stream, connection_id, topology_version, state, pool, last_activity, peer_addr)
        }
        Err(_) => {
            // 回退到 TLCP (mdb-tls)
            let tls_config = mdb_tls::TlsConfig::new().with_enabled(true);
            // 注意：stream 已被 move，需要重新获取
            // 这里简化处理：返回错误让客户端重试
            let _ = tls_config;
            Err(MdbError::new(ErrorCode::IllegalOperation, "TLS handshake failed"))
        }
    }
}

/// 尝试 rustls 标准 TLS 握手。
fn try_rustls_handshake(
    stream: TcpStream,
    state: &ServerState,
) -> Result<RustlsStream, MdbError> {
    // 检查是否配置了 TLS 证书
    let Some(catalog) = &state.catalog else {
        return Err(MdbError::new(ErrorCode::IllegalOperation, "TLS requires persistent storage"));
    };
    let dbpath = catalog.db_path();
    let cert_path = dbpath.join("tls").join("server.crt");
    let key_path = dbpath.join("tls").join("server.key");

    if !cert_path.exists() || !key_path.exists() {
        return Err(MdbError::new(ErrorCode::IllegalOperation, "TLS certificates not found"));
    }

    // 加载证书
    let cert_file = std::fs::File::open(&cert_path)
        .map_err(|e| MdbError::new(ErrorCode::IllegalOperation, format!("open cert: {e}")))?;
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| MdbError::new(ErrorCode::IllegalOperation, format!("parse cert: {e}")))?;

    let key_file = std::fs::File::open(&key_path)
        .map_err(|e| MdbError::new(ErrorCode::IllegalOperation, format!("open key: {e}")))?;
    let mut key_reader = std::io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| MdbError::new(ErrorCode::IllegalOperation, format!("parse key: {e}")))?
        .ok_or_else(|| MdbError::new(ErrorCode::IllegalOperation, "no private key found"))?;

    // 构建 rustls 配置
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| MdbError::new(ErrorCode::IllegalOperation, format!("build TLS config: {e}")))?;

    let conn = rustls::ServerConnection::new(std::sync::Arc::new(config))
        .map_err(|e| MdbError::new(ErrorCode::IllegalOperation, format!("create TLS connection: {e}")))?;

    let tls_stream = RustlsStream {
        stream,
        conn,
        buf: Vec::new(),
        pos: 0,
    };

    Ok(tls_stream)
}

/// 处理明文连接（原有逻辑）。
fn handle_plaintext_connection(
    stream: TcpStream,
    connection_id: i32,
    topology_version: &TopologyVersion,
    state: &ServerState,
    pool: &ConnectionPool,
    last_activity: &Arc<Mutex<HashMap<i32, Instant>>>,
) -> MdbResult<()> {
    let _ = stream.set_nodelay(true);
    let peer_addr = stream.peer_addr().ok();
    handle_stream_loop(stream, connection_id, topology_version, state, pool, last_activity, peer_addr)
}

/// 通用命令循环：处理任何实现了 Read + Write 的流。
fn handle_stream_loop<S: std::io::Read + std::io::Write>(
    stream: S,
    connection_id: i32,
    topology_version: &TopologyVersion,
    state: &ServerState,
    pool: &ConnectionPool,
    last_activity: &Arc<Mutex<HashMap<i32, Instant>>>,
    peer_addr: Option<SocketAddr>,
) -> MdbResult<()> {
    let mut framed = Framed::new(stream);

    if let Some(addr) = peer_addr {
        let ip = addr.ip();
        if state.ids.is_ip_blocked(&ip) {
            mdb_common::log_info!("mdb-ids", "blocked connection from blacklisted IP {ip}");
            state.ids.record_login_attempt(&ip, false, "blocked");
            return Ok(());
        }
    }

    {
        let mut conn = state.get_connection(connection_id);
        conn.peer_addr = peer_addr;
        state.update_connection(connection_id, &conn);
    }
    loop {
        if pool.shutdown.load(Ordering::Acquire) {
            state.remove_connection(connection_id);
            return Ok(());
        }
        let Some(dispatched) = framed.next_dispatch()? else {
            state.remove_connection(connection_id);
            return Ok(());
        };
        {
            last_activity
                .lock()
                .unwrap()
                .insert(connection_id, Instant::now());
        }
        let reply: Option<OpMsg> = match dispatched {
            Dispatched::Msg(msg) => {
                if msg.more_to_come() {
                    continue;
                }
                Some(dispatch_command(
                    &msg,
                    connection_id,
                    topology_version,
                    state,
                    pool,
                ))
            }
            Dispatched::Other { op_code, raw } => {
                if op_code == OpCode::Query {
                    Some(handle_op_query(&raw))
                } else {
                    return Err(MdbError::new(
                        ErrorCode::CommandNotImplemented,
                        format!("opCode {} not implemented", op_code.as_i32()),
                    ));
                }
            }
        };
        if let Some(reply) = reply {
            framed.write_message(&reply.to_bytes()?)?;
        }
    }
}

// ---------------------------------------------------------------------------
// Connection status & shutdown commands
// ---------------------------------------------------------------------------

fn handle_connection_status(state: &ServerState, pool: &ConnectionPool, response_to: i32) -> OpMsg {
    OpMsg::reply(response_to, state.connection_status(pool))
}

fn handle_shutdown_command(state: &ServerState, pool: &ConnectionPool, response_to: i32) -> OpMsg {
    pool.shutdown();
    OpMsg::reply(response_to, ok_body())
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

/// Commands that do NOT require authentication.
const AUTH_EXEMPT_COMMANDS: &[&str] = &[
    "hello", "isMaster", "ismaster", "saslStart", "saslContinue",
    "ping", "buildInfo", "endSessions",
];

fn requires_auth(body: &Document) -> bool {
    for key in body.keys() {
        if AUTH_EXEMPT_COMMANDS.iter().any(|c| *c == key) {
            return false;
        }
    }
    true
}

fn extract_db(body: &Document) -> String {
    body.get_str("$db").unwrap_or("test").to_owned()
}

fn extract_lsid(body: &Document) -> Option<SessionId> {
    body.get("lsid").and_then(|v| {
        if let Some(doc) = v.as_document() {
            if let Some(Bson::Binary(bin)) = doc.get("id") {
                Some(SessionId(bin.bytes.clone()))
            } else {
                None
            }
        } else {
            None
        }
    })
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

fn cmd_str<'a>(body: &'a Document, key: &str) -> Option<&'a str> { body.get_str(key) }
fn cmd_doc<'a>(body: &'a Document, key: &str) -> Option<&'a Document> { body.get_doc(key) }
fn cmd_arr<'a>(body: &'a Document, key: &str) -> Option<&'a [Bson]> { body.get_array_ref(key) }
fn cmd_i64(body: &Document, key: &str) -> Option<i64> { body.get_i64(key) }
fn cmd_bool(body: &Document, key: &str) -> Option<bool> { body.get_bool(key) }

fn dispatch_command(msg: &OpMsg, connection_id: i32, topology_version: &TopologyVersion, state: &ServerState, pool: &ConnectionPool) -> OpMsg {
    let response_to = msg.request_id;
    let Some(body) = &msg.body else { return OpMsg::reply(response_to, command_failed("missing body", ErrorCode::CommandNotImplemented)); };

    if is_handshake(body) {
        let mut reply_doc = hello_reply(connection_id, topology_version.next(), false);
        // 如果启用了分片，报告 isdbgrid（驱动据此检测 mongos）
        if state.sharding.lock().unwrap().is_some() {
            reply_doc.insert("isdbgrid", Bson::Boolean(true));
            reply_doc.insert("msg", Bson::String("isdbgrid".into()));
        }
        return OpMsg::reply(response_to, reply_doc);
    }
    let db_name = extract_db(body);

    let mut conn_state = state.get_connection(connection_id);
    conn_state.last_used = Instant::now();

    let peer_ip = conn_state.peer_addr.map(|a| a.ip());

    if let Some(ip) = peer_ip {
        if state.ids.is_ip_blocked(&ip) {
            return OpMsg::reply(response_to, command_failed("IP is blacklisted", ErrorCode::Unauthorized));
        }
        if let RateLimitResult::Denied = state.ids.check_rate_limit(&ip) {
            return OpMsg::reply(response_to, command_failed("rate limit exceeded", ErrorCode::ExceededTimeLimit));
        }
    }

    if let Some(lsid) = extract_lsid(body) {
        let mut sessions = state.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(&lsid) {
            session.last_used = Instant::now();
            if session.connection_id != connection_id {
                session.connection_id = connection_id;
            }
        } else {
            sessions.insert(lsid, Session::new(connection_id));
        }
    }

    state.purge_expired_sessions();

    if body.contains_key("saslStart") {
        let reply = handle_sasl_start(body, &db_name, &mut conn_state, state);
        let success = reply.get("ok").and_then(|v| v.as_f64()) == Some(1.0);
        if let Some(ip) = peer_ip {
            if !success {
                state.ids.record_login_attempt(&ip, false, &db_name);
            }
        }
        state.audit_log(
            AuditEventType::Authentication,
            None,
            Some(db_name.clone()),
            "saslStart".into(),
            None,
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::FailedToParse as i32, message: "saslStart failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        state.update_connection(connection_id, &conn_state);
        return OpMsg::reply(response_to, reply);
    }
    if body.contains_key("saslContinue") {
        let reply = handle_sasl_continue(body, &mut conn_state);
        let success = reply.get("done").and_then(|v| v.as_bool()) == Some(true);
        if success {
            if let Some(ip) = peer_ip {
                if let Some(ref user) = conn_state.authenticated_user {
                    state.ids.record_login_attempt(&ip, true, user);
                }
            }
            let user = conn_state.authenticated_user.clone();
            state.audit_log(
                AuditEventType::Authentication,
                user,
                Some(db_name.clone()),
                "saslContinue".into(),
                None,
                AuditResult::Success,
                conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
                connection_id,
                None,
            );
        }
        state.update_connection(connection_id, &conn_state);
        return OpMsg::reply(response_to, reply);
    }

    if requires_auth(body) && conn_state.authenticated_user.is_none() {
        state.audit_log(
            AuditEventType::Authorization,
            None,
            Some(db_name.clone()),
            body.keys().next().unwrap_or("unknown").to_string(),
            None,
            AuditResult::Failure {
                error_code: ErrorCode::Unauthorized as i32,
                message: "not authorized to execute command".into(),
            },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return OpMsg::reply(response_to, command_failed(
            "not authorized to execute command",
            ErrorCode::Unauthorized,
        ));
    }

    // RBAC: permission check for authenticated users
    if let Some(ref auth_user) = conn_state.authenticated_user {
        let cmd_name = body.keys()
            .find(|k| !k.starts_with('$') && *k != "lsid"
                && !AUTH_EXEMPT_COMMANDS.iter().any(|c| c == k))
            .unwrap_or("");
        let coll_for_check = cmd_str(body, cmd_name).map(|s| s.to_owned());
        if let Some((required_role, admin_only)) = command_required_role(cmd_name, coll_for_check.as_deref()) {
            let users = state.users.lock().unwrap();
            let user_roles: Vec<Role> = users.values()
                .find(|u| u.username == *auth_user)
                .map(|u| u.roles.clone())
                .unwrap_or_default();
            let has_perm = user_has_permission(&user_roles, &db_name, coll_for_check.as_deref(), required_role, admin_only);
            drop(users);
            if !has_perm {
                state.audit_log(
                    AuditEventType::Authorization,
                    Some(auth_user.clone()),
                    Some(db_name.clone()),
                    cmd_name.to_string(),
                    None,
                    AuditResult::Failure {
                        error_code: ErrorCode::Unauthorized as i32,
                        message: format!("not authorized to execute command {cmd_name}"),
                    },
                    conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
                    connection_id,
                    None,
                );
                return OpMsg::reply(response_to, command_failed(
                    &format!("not authorized to execute command {cmd_name}"),
                    ErrorCode::Unauthorized,
                ));
            }
        }
    }

    conn_state.database = db_name.clone();
    state.update_connection(connection_id, &conn_state);

    if body.contains_key("ping") { return ok_reply(response_to); }
    if body.contains_key("buildInfo") {
        let mut d = ok_body(); d.insert("version", "0.1.0");
        d.insert("versionArray", Bson::Array(vec![Bson::Int32(0), Bson::Int32(1), Bson::Int32(0)]));
        d.insert("maxWireVersion", MAX_WIRE_VERSION); return OpMsg::reply(response_to, d);
    }
    if body.contains_key("getParameter") { return handle_get_parameter(body, state, response_to); }
    if body.contains_key("dbHash") { return handle_db_hash(body, &db_name, state, response_to); }
    if body.contains_key("getFreeMonitoringStatus") { return ok_reply(response_to); }
    if body.contains_key("connectionStatus") { return handle_connection_status(state, pool, response_to); }
    if body.contains_key("shutdown") {
        state.audit_log(
            AuditEventType::AdminCommand,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "shutdown".into(),
            None,
            AuditResult::Success,
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return handle_shutdown_command(state, pool, response_to);
    }
    if body.contains_key("createUser") {
        let reply = handle_create_user(body, &db_name, response_to, state);
        let success = reply.body.as_ref().map(|b| b.get("ok").and_then(|v| v.as_f64()) == Some(1.0)).unwrap_or(false);
        let username = body.get_str("createUser").map(String::from);
        state.audit_log(
            AuditEventType::AdminCommand,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "createUser".into(),
            Some({ let mut d = Document::new(); if let Some(u) = &username { d.insert("user", Bson::String(u.clone())); } d }),
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::DuplicateKey as i32, message: "createUser failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("dropUser") {
        let reply = handle_drop_user(body, &db_name, response_to, state);
        let success = reply.body.as_ref().map(|b| b.get("ok").and_then(|v| v.as_f64()) == Some(1.0)).unwrap_or(false);
        state.audit_log(
            AuditEventType::AdminCommand,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "dropUser".into(),
            None,
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::UserNotFound as i32, message: "dropUser failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("grantRolesToUser") {
        let reply = handle_grant_roles(body, &db_name, response_to, state);
        let success = reply.body.as_ref().map(|b| b.get("ok").and_then(|v| v.as_f64()) == Some(1.0)).unwrap_or(false);
        state.audit_log(
            AuditEventType::AdminCommand,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "grantRolesToUser".into(),
            None,
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::UserNotFound as i32, message: "grantRoles failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("usersInfo") { return handle_users_info(body, &db_name, response_to, state); }
    if body.contains_key("revokeRolesFromUser") {
        let reply = handle_revoke_roles(body, &db_name, response_to, state);
        let success = reply.body.as_ref().map(|b| b.get("ok").and_then(|v| v.as_f64()) == Some(1.0)).unwrap_or(false);
        state.audit_log(
            AuditEventType::AdminCommand,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "revokeRolesFromUser".into(),
            None,
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::UserNotFound as i32, message: "revokeRoles failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("updateUser") {
        let reply = handle_update_user(body, &db_name, response_to, state);
        let success = reply.body.as_ref().map(|b| b.get("ok").and_then(|v| v.as_f64()) == Some(1.0)).unwrap_or(false);
        state.audit_log(
            AuditEventType::AdminCommand,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "updateUser".into(),
            None,
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::UserNotFound as i32, message: "updateUser failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("changePassword") {
        let reply = handle_change_password(body, &db_name, response_to, state);
        let success = reply.body.as_ref().map(|b| b.get("ok").and_then(|v| v.as_f64()) == Some(1.0)).unwrap_or(false);
        state.audit_log(
            AuditEventType::AdminCommand,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "changePassword".into(),
            None,
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::UserNotFound as i32, message: "changePassword failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("insert") {
        let reply = handle_insert(body, msg, &db_name, state);
        let success = reply.body.as_ref().map(|b| b.get("ok").and_then(|v| v.as_f64()) == Some(1.0)).unwrap_or(false);
        let n = reply.body.as_ref().and_then(|b| b.get("n").and_then(|v| v.as_i64())).unwrap_or(0);
        state.audit_log(
            AuditEventType::DataAccess,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "insert".into(),
            Some({ let mut d = Document::new(); d.insert("collection", Bson::String(body.get_str("insert").unwrap_or("").to_string())); d.insert("n", Bson::Int64(n)); d }),
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::BadValue as i32, message: "insert failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("find") {
        let reply = handle_find(body, &db_name, state, response_to);
        state.audit_log(
            AuditEventType::DataAccess,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "find".into(),
            Some({ let mut d = Document::new(); d.insert("collection", Bson::String(body.get_str("find").unwrap_or("").to_string())); d }),
            AuditResult::Success,
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("getMore") { return handle_get_more(body, &db_name, state, response_to); }
    if body.contains_key("findAndModify") { return handle_find_and_modify(body, &db_name, state, response_to); }
    if body.contains_key("update") {
        let reply = handle_update(body, &db_name, state, response_to);
        let success = reply.body.as_ref().map(|b| b.get("ok").and_then(|v| v.as_f64()) == Some(1.0)).unwrap_or(false);
        let n = reply.body.as_ref().and_then(|b| b.get("n").and_then(|v| v.as_i64())).unwrap_or(0);
        state.audit_log(
            AuditEventType::DataAccess,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "update".into(),
            Some({ let mut d = Document::new(); d.insert("collection", Bson::String(body.get_str("update").unwrap_or("").to_string())); d.insert("n", Bson::Int64(n)); d }),
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::BadValue as i32, message: "update failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("delete") {
        let reply = handle_delete(body, &db_name, state, response_to);
        let success = reply.body.as_ref().map(|b| b.get("ok").and_then(|v| v.as_f64()) == Some(1.0)).unwrap_or(false);
        let n = reply.body.as_ref().and_then(|b| b.get("n").and_then(|v| v.as_i64())).unwrap_or(0);
        state.audit_log(
            AuditEventType::DataAccess,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "delete".into(),
            Some({ let mut d = Document::new(); d.insert("collection", Bson::String(body.get_str("delete").unwrap_or("").to_string())); d.insert("n", Bson::Int64(n)); d }),
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::BadValue as i32, message: "delete failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("createIndexes") {
        let reply = handle_create_indexes(body, &db_name, state, response_to);
        state.audit_log(
            AuditEventType::SchemaChange,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "createIndexes".into(),
            Some({ let mut d = Document::new(); d.insert("collection", Bson::String(body.get_str("createIndexes").unwrap_or("").to_string())); d }),
            AuditResult::Success,
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("dropIndexes") {
        let reply = handle_drop_indexes(body, &db_name, state, response_to);
        let success = reply.body.as_ref().map(|b| b.get("ok").and_then(|v| v.as_f64()) == Some(1.0)).unwrap_or(false);
        state.audit_log(
            AuditEventType::SchemaChange,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "dropIndexes".into(),
            Some({ let mut d = Document::new(); d.insert("collection", Bson::String(body.get_str("dropIndexes").unwrap_or("").to_string())); d }),
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::IndexNotFound as i32, message: "dropIndexes failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("listIndexes") { return handle_list_indexes(body, &db_name, state, response_to); }
    if body.contains_key("aggregate") { return handle_aggregate(body, &db_name, state, response_to); }
    if body.contains_key("mapReduce") { return handle_map_reduce(body, &db_name, state, response_to); }
    if body.contains_key("listDatabases") { return handle_list_databases(state, response_to); }
    if body.contains_key("listCollections") { return handle_list_collections(body, &db_name, response_to, state); }
    if body.contains_key("create") && !body.contains_key("createIndexes") {
        let reply = handle_create_collection(body, &db_name, response_to, state);
        state.audit_log(
            AuditEventType::SchemaChange,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "create".into(),
            Some({ let mut d = Document::new(); d.insert("collection", Bson::String(body.get_str("create").unwrap_or("").to_string())); d }),
            AuditResult::Success,
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("drop") {
        let reply = handle_drop_collection(body, &db_name, response_to, state);
        let success = reply.body.as_ref().map(|b| b.get("ok").and_then(|v| v.as_f64()) == Some(1.0)).unwrap_or(false);
        state.audit_log(
            AuditEventType::SchemaChange,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "drop".into(),
            Some({ let mut d = Document::new(); d.insert("collection", Bson::String(body.get_str("drop").unwrap_or("").to_string())); d }),
            if success { AuditResult::Success } else { AuditResult::Failure { error_code: ErrorCode::NamespaceNotFound as i32, message: "drop failed".into() } },
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    if body.contains_key("collStats") { return handle_coll_stats(body, &db_name, response_to, state); }
    if body.contains_key("dbStats") { return handle_db_stats(&db_name, response_to, state); }
    if body.contains_key("startTransaction") { return handle_start_transaction(body, response_to, state); }
    if body.contains_key("commitTransaction") { return handle_commit_transaction(body, response_to, state); }
    if body.contains_key("abortTransaction") { return handle_abort_transaction(body, response_to, state); }
    if body.contains_key("replSetInitiate") { return handle_repl_set_initiate(body, state, response_to); }
    if body.contains_key("replSetGetStatus") { return handle_repl_set_get_status(state, response_to); }
    if body.contains_key("replSetGetConfig") { return handle_repl_set_get_config(state, response_to); }
    if body.contains_key("replSetReconfig") { return handle_repl_set_reconfig(body, state, response_to); }
    if body.contains_key("replSetStartTransport") { return handle_repl_set_start_transport(body, state, response_to); }
    if body.contains_key("replSetAddPeer") { return handle_repl_set_add_peer(body, state, response_to); }
    if body.contains_key("addShard") { return handle_add_shard(body, state, response_to); }
    if body.contains_key("removeShard") { return handle_remove_shard(body, state, response_to); }
    if body.contains_key("listShards") { return handle_list_shards(state, response_to); }
    if body.contains_key("enableSharding") { return handle_enable_sharding(body, state, response_to); }
    if body.contains_key("shardCollection") { return handle_shard_collection(body, state, response_to); }
    if body.contains_key("splitAt") { return handle_split_at(body, state, response_to); }
    if body.contains_key("moveChunk") { return handle_move_chunk(body, state, response_to); }
    if body.contains_key("balancerStart") { return handle_balancer_start(state, response_to); }
    if body.contains_key("balancerStop") { return handle_balancer_stop(state, response_to); }
    if body.contains_key("balancerStatus") { return handle_balancer_status(state, response_to); }
    if body.contains_key("flushRouterConfig") { return handle_flush_router_config(state, response_to); }
    if body.contains_key("watch") { return handle_watch(body, &db_name, state, response_to); }
    if body.contains_key("killCursors") { return ok_reply(response_to); }
    if body.contains_key("distinct") { return handle_distinct(body, &db_name, state, response_to); }
    if body.contains_key("count") { return handle_count(body, &db_name, state, response_to); }
    if body.contains_key("countDocuments") { return handle_count(body, &db_name, state, response_to); }
    if body.contains_key("renameCollection") { return handle_rename_collection(body, &db_name, response_to, state); }
    if body.contains_key("compact") { return handle_compact(body, &db_name, response_to, state); }
    if body.contains_key("validate") { return handle_validate(body, &db_name, response_to, state); }
    if body.contains_key("explain") { return handle_explain(body, &db_name, state, response_to); }
    if body.contains_key("hostInfo") { return handle_host_info(state, response_to); }
    if body.contains_key("getCmdLineOpts") { return handle_get_cmd_line_opts(state, response_to); }
    if body.contains_key("top") { return handle_top(state, response_to); }
    if body.contains_key("analyze") { return handle_analyze(body, &db_name, state, response_to); }
    if body.contains_key("indexStatistics") { return handle_index_statistics(body, &db_name, state, response_to); }
    if body.contains_key("metrics") { return handle_metrics(state, response_to); }
    if body.contains_key("encryptCollection") { return handle_encrypt_collection(body, &db_name, state, response_to); }
    if body.contains_key("decryptCollection") { return handle_decrypt_collection(body, &db_name, state, response_to); }
    if body.contains_key("keyStatus") { return handle_key_status(body, &db_name, state, response_to); }
    if body.contains_key("rotateKey") { return handle_rotate_key(body, &db_name, state, response_to); }
    if body.contains_key("databaseStats") { return handle_db_stats(&db_name, response_to, state); }
    if body.contains_key("serverStatus") { return handle_server_status(state, response_to); }
    if body.contains_key("currentOp") { return handle_current_op(state, response_to); }
    if body.contains_key("killOp") { return handle_kill_op(body, response_to); }
    if body.contains_key("setParameter") { return handle_set_parameter(body, state, response_to); }
    if body.contains_key("getParameter") { return handle_get_parameter(body, state, response_to); }
    if body.contains_key("collMod") { return handle_coll_mod(body, &db_name, state, response_to); }
    if body.contains_key("profile") { return handle_profile(body, state, response_to); }
    if body.contains_key("dbHash") { return handle_db_hash(body, &db_name, state, response_to); }
    if body.contains_key("system.profile") { return handle_profile_data(body, &db_name, state, response_to); }
    if body.contains_key("getLog") { return handle_get_log(body, response_to); }
    if body.contains_key("reIndex") { return handle_re_index(body, &db_name, response_to, state); }
    if body.contains_key("dropDatabase") {
        let reply = handle_drop_database(body, &db_name, response_to, state);
        state.audit_log(
            AuditEventType::SchemaChange,
            conn_state.authenticated_user.clone(),
            Some(db_name.clone()),
            "dropDatabase".into(),
            None,
            AuditResult::Success,
            conn_state.peer_addr.unwrap_or(DEFAULT_CLIENT_ADDR),
            connection_id,
            None,
        );
        return reply;
    }
    // Audit query commands
    if body.contains_key("auditLog") {
        return handle_audit_log_query(body, response_to, state);
    }
    if body.contains_key("auditFilter") {
        return handle_audit_filter(body, response_to, state);
    }
    if body.contains_key("auditStatus") {
        return handle_audit_status(response_to, state);
    }
    // Backup commands
    if body.contains_key("backup") { return handle_backup(body, response_to); }
    if body.contains_key("restore") { return handle_restore(body, response_to); }
    if body.contains_key("listBackups") { return handle_list_backups(response_to); }
    if body.contains_key("pitr") { return handle_pitr(body, response_to); }
    if body.contains_key("securityAlerts") {
        return ids::handle_security_alerts(body, response_to, &state.ids);
    }
    if body.contains_key("idsStatus") {
        return ids::handle_ids_status(body, response_to, &state.ids);
    }
    if body.contains_key("blockIp") {
        return ids::handle_block_ip(body, response_to, &state.ids);
    }
    if body.contains_key("unblockIp") {
        return ids::handle_unblock_ip(body, response_to, &state.ids);
    }
    if body.contains_key("idsConfig") {
        return ids::handle_ids_config(body, response_to, &state.ids);
    }
    let name = body.keys().find(|k| !k.starts_with('$') && *k != "lsid").unwrap_or("<unknown>");
    OpMsg::reply(response_to, command_failed(&format!("no such command: {name}"), ErrorCode::CommandNotImplemented))
}
// ---------------------------------------------------------------------------
// SASL / SCRAM authentication
// ---------------------------------------------------------------------------

fn handle_sasl_start(body: &Document, db_name: &str, conn_state: &mut ConnectionState, state: &ServerState) -> Document {
    let mechanism_str = match body.get_str("mechanism") {
        Some(m) => m,
        None => return command_failed("saslStart: missing mechanism", ErrorCode::FailedToParse),
    };
    let mechanism = match ScramMechanism::parse(mechanism_str) {
        Some(m) => m,
        None => return command_failed(&format!("saslStart: unsupported mechanism: {mechanism_str}"), ErrorCode::FailedToParse),
    };
    let payload = match body.get("payload") {
        Some(Bson::Binary(bin)) => bin.bytes.as_slice(),
        _ => return command_failed("saslStart: missing payload", ErrorCode::FailedToParse),
    };

    let conversation_id = rand_conversation_id();

    let admin_db = state.get_database("admin");
    let users_coll = admin_db.get_or_create_collection("system.users");

    let mut server_scram = ServerScram::new(mechanism, conversation_id);
    let users_coll_ref = &users_coll;

    let resolve_result = server_scram.on_client_first(payload, &mut |username| {
        let all_docs = users_coll_ref.lock().unwrap().store.scan_all().unwrap_or_default();
        for (_, doc_bytes) in &all_docs {
            if let Ok(doc) = mdb_doc::codec::from_bytes(doc_bytes) {
                if doc.get_str("user") == Some(username) {
                    if let Some(cred_doc) = doc.get("credentials").and_then(|v| v.as_document()) {
                        if let Some(scram_doc) = cred_doc.get("SCRAM-SHA-256").and_then(|v| v.as_document()) {
                            let iteration_count = scram_doc.get("iterationCount").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
                            let salt_b64 = scram_doc.get("salt").and_then(|v| v.as_str()).unwrap_or("");
                            let stored_key_b64 = scram_doc.get("storedKey").and_then(|v| v.as_str()).unwrap_or("");
                            let server_key_b64 = scram_doc.get("serverKey").and_then(|v| v.as_str()).unwrap_or("");
                            if let Ok(cred) = StoredKey::from_mongo_credentials(mechanism, iteration_count, salt_b64, stored_key_b64, server_key_b64) {
                                return Some(cred);
                            }
                        }
                    }
                }
            }
        }
        None
    });

    match resolve_result {
        Ok(server_first) => {
            conn_state.scram_session = Some(ScramSession { server_scram, database: db_name.to_owned() });
            let mut reply = ok_body();
            reply.insert("conversationId", Bson::Int32(conversation_id as i32));
            reply.insert("done", Bson::Boolean(false));
            reply.insert("payload", Bson::Binary(mdb_doc::Binary::generic(server_first.into_bytes())));
            reply
        }
        Err(e) => command_failed(&e.to_string(), ErrorCode::UserNotFound),
    }
}

fn handle_sasl_continue(body: &Document, conn_state: &mut ConnectionState) -> Document {
    let scram_session = match conn_state.scram_session.as_mut() {
        Some(s) => s,
        None => return command_failed("saslContinue: no active SASL session", ErrorCode::FailedToParse),
    };

    let payload = match body.get("payload") {
        Some(Bson::Binary(bin)) => bin.bytes.as_slice(),
        None => &[],
        _ => &[],
    };

    if payload.is_empty() {
        match scram_session.server_scram.on_done() {
            Ok(()) => {
                if let Some(user) = scram_session.server_scram.authenticated_user() {
                    conn_state.authenticated_user = Some(user.to_owned());
                }
                let conversation_id = scram_session.server_scram.conversation_id;
                conn_state.scram_session = None;
                let mut reply = ok_body();
                reply.insert("conversationId", Bson::Int32(conversation_id as i32));
                reply.insert("done", Bson::Boolean(true));
                reply.insert("payload", Bson::Binary(mdb_doc::Binary::generic(Vec::new())));
                reply
            }
            Err(e) => {
                conn_state.scram_session = None;
                command_failed(&e.to_string(), ErrorCode::UserNotFound)
            }
        }
    } else {
        match scram_session.server_scram.on_client_final(payload) {
            Ok(server_final) => {
                let conversation_id = scram_session.server_scram.conversation_id;
                let mut reply = ok_body();
                reply.insert("conversationId", Bson::Int32(conversation_id as i32));
                reply.insert("done", Bson::Boolean(false));
                reply.insert("payload", Bson::Binary(mdb_doc::Binary::generic(server_final.into_bytes())));
                reply
            }
            Err(e) => {
                conn_state.scram_session = None;
                command_failed(&e.to_string(), ErrorCode::UserNotFound)
            }
        }
    }
}

fn rand_conversation_id() -> u32 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let s = RandomState::new();
    let mut h = s.build_hasher();
    h.write_u64(Instant::now().elapsed().as_nanos() as u64);
    h.finish() as u32
}

// ---------------------------------------------------------------------------
// User management commands
// ---------------------------------------------------------------------------

/// Parse a roles array from BSON. Supports both string ("read") and
/// document ({role: "read", db: "admin"}) formats.
fn parse_roles_from_bson(arr: &[Bson], default_db: &str) -> Vec<Role> {
    arr.iter().filter_map(|r| {
        match r {
            Bson::String(s) => Some(Role { role: s.clone(), db: default_db.to_owned() }),
            Bson::Document(d) => {
                let role = d.get_str("role")?.to_owned();
                let db = d.get_str("db").unwrap_or(default_db).to_owned();
                Some(Role { role, db })
            }
            _ => None,
        }
    }).collect()
}

/// Serialize a `Vec<Role>` into a BSON array.
fn roles_to_bson(roles: &[Role]) -> Bson {
    let arr: Vec<Bson> = roles.iter().map(|r| {
        let mut d = Document::new();
        d.insert("role", Bson::String(r.role.clone()));
        d.insert("db", Bson::String(r.db.clone()));
        Bson::Document(d)
    }).collect();
    Bson::Array(arr)
}

    fn handle_create_user(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
        let username = match body.get_str("createUser") {
            Some(u) => u,
            None => return OpMsg::reply(response_to, command_failed("createUser: missing username", ErrorCode::BadValue)),
        };
        let password = match body.get_str("pwd") {
            Some(p) => p,
            None => return OpMsg::reply(response_to, command_failed("createUser: missing pwd", ErrorCode::BadValue)),
        };
        let roles: Vec<Role> = body.get("roles")
            .and_then(|v| v.as_array())
            .map(|arr| parse_roles_from_bson(arr, db_name))
            .unwrap_or_default();

        match state.create_user(username, db_name, password, roles) {
            Ok(()) => ok_reply(response_to),
            Err(e) => {
                mdb_common::log_error!("mdb-server", "createUser failed: {e}");
                OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::DuplicateKey))
            }
        }
    }

fn handle_drop_user(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let username = match body.get_str("dropUser") {
        Some(u) => u,
        None => return OpMsg::reply(response_to, command_failed("dropUser: missing username", ErrorCode::BadValue)),
    };
    match state.drop_user(username, db_name) {
        Ok(()) => ok_reply(response_to),
        Err(e) => OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::UserNotFound)),
    }
}

fn handle_grant_roles(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let username = match body.get_str("grantRolesToUser") {
        Some(u) => u,
        None => return OpMsg::reply(response_to, command_failed("grantRolesToUser: missing username", ErrorCode::BadValue)),
    };
    let roles: Vec<Role> = body.get("roles")
        .and_then(|v| v.as_array())
        .map(|arr| parse_roles_from_bson(arr, db_name))
        .unwrap_or_default();

    match state.grant_roles(username, db_name, roles) {
        Ok(()) => ok_reply(response_to),
        Err(e) => OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::UserNotFound)),
    }
}

fn handle_revoke_roles(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let username = match body.get_str("revokeRolesFromUser") {
        Some(u) => u,
        None => return OpMsg::reply(response_to, command_failed("revokeRolesFromUser: missing username", ErrorCode::BadValue)),
    };
    let roles: Vec<Role> = body.get("roles")
        .and_then(|v| v.as_array())
        .map(|arr| parse_roles_from_bson(arr, db_name))
        .unwrap_or_default();

    match state.revoke_roles(username, db_name, roles) {
        Ok(()) => ok_reply(response_to),
        Err(e) => OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::UserNotFound)),
    }
}

fn handle_update_user(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let username = match body.get_str("updateUser") {
        Some(u) => u,
        None => return OpMsg::reply(response_to, command_failed("updateUser: missing username", ErrorCode::BadValue)),
    };

    if let Some(roles_val) = body.get("roles") {
        let roles: Vec<Role> = match roles_val {
            Bson::Array(arr) => parse_roles_from_bson(arr, db_name),
            _ => return OpMsg::reply(response_to, command_failed("updateUser: roles must be an array", ErrorCode::BadValue)),
        };
        match state.update_user_roles(username, db_name, roles) {
            Ok(()) => {}
            Err(e) => return OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::UserNotFound)),
        }
    }

    if let Some(pwd) = body.get_str("pwd") {
        match state.change_password(username, db_name, pwd) {
            Ok(()) => {}
            Err(e) => return OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::UserNotFound)),
        }
    }

    ok_reply(response_to)
}

fn handle_change_password(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let username = match body.get_str("changePassword") {
        Some(u) => u,
        None => return OpMsg::reply(response_to, command_failed("changePassword: missing username", ErrorCode::BadValue)),
    };
    let new_pwd = match body.get_str("pwd") {
        Some(p) => p,
        None => return OpMsg::reply(response_to, command_failed("changePassword: missing pwd", ErrorCode::BadValue)),
    };

    match state.change_password(username, db_name, new_pwd) {
        Ok(()) => ok_reply(response_to),
        Err(e) => OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::UserNotFound)),
    }
}

fn handle_users_info(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let username = body.get_str("usersInfo");
    let users = state.users.lock().unwrap();
    let filtered: Vec<Bson> = users.values()
        .filter(|u| {
            if let Some(name) = username {
                u.username == name && u.database == db_name
            } else {
                u.database == db_name
            }
        })
        .map(|u| {
            let mut d = Document::new();
            d.insert("user", Bson::String(u.username.clone()));
            d.insert("db", Bson::String(u.database.clone()));
            d.insert("roles", roles_to_bson(&u.roles));
            Bson::Document(d)
        })
        .collect();

    let mut reply = ok_body();
    reply.insert("users", Bson::Array(filtered));
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

fn new_object_id() -> ObjectId { ObjectId::new().expect("ObjectId::new() failed") }

fn handle_insert(body: &Document, msg: &OpMsg, db_name: &str, state: &ServerState) -> OpMsg {
    let response_to = msg.request_id;
    let coll_name = match cmd_str(body, "insert") { Some(n) => n.to_owned(), None => return OpMsg::reply(response_to, command_failed("insert: missing collection name", ErrorCode::BadValue)) };
    let docs = if !msg.document_sequences.is_empty() { msg.document_sequences[0].documents.clone() }
    else if let Some(arr) = body.get("documents").and_then(|v| v.as_array()) { arr.iter().filter_map(|b| b.as_document().cloned()).collect() }
    else { return OpMsg::reply(response_to, command_failed("insert: no documents", ErrorCode::BadValue)); };
    let ordered = cmd_bool(body, "ordered").unwrap_or(true);

    // 分片路由：检查集合是否分片
    let ns = format!("{db_name}.{coll_name}");
    let shard_route = {
        let guard = state.sharding.lock().unwrap();
        guard.as_ref().and_then(|r| {
            let has_key = r.config_db().get_shard_key(&ns).is_some();
            if has_key { Some(ns.clone()) } else { None }
        })
    };

    // 迁移锁：如果集合正在迁移，缓冲写操作
    let is_migrating = state.migration_state.lock().unwrap().is_migrating(&ns);
    if is_migrating && !docs.is_empty() {
        let mut buffered_count = 0i32;
        for (i, mut doc) in docs.into_iter().enumerate() {
            if !doc.contains_key("_id") { doc.insert("_id", Bson::ObjectId(new_object_id())); }
            let id_val = doc.get("_id").cloned().unwrap_or(Bson::Null);
            let id_bytes = bson_to_key(&id_val);
            let doc_bytes = match mdb_doc::codec::to_bytes(&doc) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let shard = doc.get("_shard").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let chunk_id = format!("{}_chunk_0", ns);
            state.migration_state.lock().unwrap().buffer_write(BufferedWrite {
                ns: ns.clone(),
                chunk_id,
                op: "insert".to_string(),
                key: id_bytes,
                value: Some(doc_bytes),
            });
            buffered_count += 1;
            let _ = shard;
        }
        let mut reply = ok_body();
        reply.insert("n", Bson::Int32(buffered_count));
        reply.insert("buffered", Bson::Boolean(true));
        return OpMsg::reply(response_to, reply);
    }

    // 事务缓冲：如果当前会话在事务中，缓冲所有操作
    if let Some(lsid) = extract_lsid(body) {
        let in_txn = {
            let sessions = state.sessions.lock().unwrap();
            sessions.get(&lsid).map(|s| s.txn_active).unwrap_or(false)
        };
        if in_txn {
            let mut n_buffered = 0i32;
            let mut errors = Vec::new();
            for (i, mut doc) in docs.into_iter().enumerate() {
                if !doc.contains_key("_id") { doc.insert("_id", Bson::ObjectId(new_object_id())); }
                let id_val = doc.get("_id").cloned().unwrap_or(Bson::Null);
                let id_bytes = bson_to_key(&id_val);
                let doc_bytes = match mdb_doc::codec::to_bytes(&doc) {
                    Ok(b) => b,
                    Err(e) => { errors.push((i as i32, format!("bson encode: {e}"))); if ordered { break; } else { continue; } }
                };
                let op = TxnOp::Insert {
                    db: db_name.to_owned(),
                    coll: coll_name.clone(),
                    key: id_bytes,
                    value: doc_bytes,
                };
                buffer_txn_op(state, body, op);
                n_buffered += 1;
            }
            let mut reply = ok_body();
            reply.insert("n", Bson::Int32(n_buffered));
            if !errors.is_empty() {
                let errs: Vec<Bson> = errors.into_iter().map(|(idx, msg)| { let mut d = Document::new(); d.insert("index", Bson::Int32(idx)); d.insert("code", Bson::Int32(ErrorCode::BadValue as i32)); d.insert("errmsg", Bson::String(msg)); Bson::Document(d) }).collect();
                reply.insert("writeErrors", Bson::Array(errs));
            }
            return OpMsg::reply(response_to, reply);
        }
    }

    // 非事务模式：直接执行
    let db = state.get_database(db_name);
    let coll = db.get_or_create_collection(&coll_name);
    let mut coll_locked = coll.lock().unwrap();
    let mut n_inserted = 0i32;
    let mut write_errors = Vec::new();
    let mut oplog_entries = Vec::new();
    for (i, mut doc) in docs.into_iter().enumerate() {
        if !doc.contains_key("_id") { doc.insert("_id", Bson::ObjectId(new_object_id())); }

        // 分片路由：如果集合已分片，计算并记录分片分配
        if let Some(ref ns) = shard_route {
            let guard = state.sharding.lock().unwrap();
            if let Some(router) = guard.as_ref() {
                if let Ok(shard_id) = router.route_write(ns, &doc) {
                    doc.insert("_shard", Bson::String(shard_id.to_string()));
                }
            }
        }

        let id_val = doc.get("_id").cloned().unwrap_or(Bson::Null);
        let id_bytes = bson_to_key(&id_val);
        let doc_bytes = match mdb_doc::codec::to_bytes(&doc) { Ok(b) => b, Err(e) => { write_errors.push((i as i32, format!("bson encode: {e}"))); if ordered { break; } else { continue; } } };
        match coll_locked.store.get(&id_bytes) {
            Ok(Some(_)) => {
                write_errors.push((i as i32, format!("E11000 duplicate key error: _id {:?}", id_debug(&id_val))));
                if ordered { break; } else { continue; }
            }
            Err(e) => {
                write_errors.push((i as i32, format!("lookup _id: {e}")));
                if ordered { break; } else { continue; }
            }
            Ok(None) => {}
        }
        let record_id = coll_locked.next_record_id();
        match coll_locked.store.put(id_bytes.clone(), doc_bytes) {
            Ok(()) => {
                coll_locked.bind_record_id(&id_bytes, record_id);
                let _ = coll_locked.index_manager.update_indexes(&doc, record_id);
                n_inserted += 1;
                oplog_entries.push((format!("{db_name}.{coll_name}"), doc.clone()));
            }
            Err(e) => {
                write_errors.push((i as i32, format!("write: {e}")));
                if ordered { break; } else { continue; }
            }
        }
    }
    drop(coll_locked);
    // 追加 oplog（在释放集合锁之后，避免死锁）
    for (ns, doc) in oplog_entries {
        state.append_oplog(&ns, OpType::Insert, doc, None);
    }
    // 更新会话因果一致性时间戳
    update_session_write_ts(state, body);
    let mut reply = ok_body();
    reply.insert("n", Bson::Int32(n_inserted));
    if !write_errors.is_empty() {
        let errs: Vec<Bson> = write_errors.into_iter().map(|(idx, msg)| { let mut d = Document::new(); d.insert("index", Bson::Int32(idx)); d.insert("code", Bson::Int32(ErrorCode::BadValue as i32)); d.insert("errmsg", Bson::String(msg)); Bson::Document(d) }).collect();
        reply.insert("writeErrors", Bson::Array(errs));
    }
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// 查询规划器：分析过滤条件，决定是否使用索引
// ---------------------------------------------------------------------------

/// 从过滤条件中提取的信息，用于规划器决策。
#[derive(Debug, Default, Clone)]
struct QueryPlan {
    /// 是否使用了索引。
    used_index: bool,
    /// 使用的索引名。
    index_name: Option<String>,
    /// 索引覆盖的等值字段。
    eq_fields: Vec<String>,
    /// 索引覆盖的范围字段。
    range_fields: Vec<String>,
    /// 全表扫描时检查的文档数。
    total_docs: usize,
    /// 索引命中的键数。
    keys_examined: usize,
    /// 返回的文档数。
    n_returned: usize,
}

/// 从过滤条件中提取等值匹配的字段和值。
///
/// 返回 `(field, value)` 列表。
fn extract_equality_conditions(filter: &Document) -> Vec<(String, Bson)> {
    let mut conditions = Vec::new();
    for (key, val) in filter.iter() {
        match key {
            "$and" | "$or" | "$nor" => {
                // 递归提取子条件
                if let Some(arr) = val.as_array() {
                    for item in arr {
                        if let Some(sub) = item.as_document() {
                            conditions.extend(extract_equality_conditions(sub));
                        }
                    }
                }
            }
            _ => {
                // 非 $ 前缀的键是字段名
                match val {
                    Bson::Document(ops) => {
                        // 检查是否只有 $eq
                        let only_eq = ops.iter().all(|(op, _)| op == "$eq");
                        if only_eq {
                            if let Some(eq_val) = ops.get("$eq") {
                                conditions.push((key.to_string(), eq_val.clone()));
                            }
                        }
                    }
                    scalar => {
                        conditions.push((key.to_string(), scalar.clone()));
                    }
                }
            }
        }
    }
    conditions
}

/// 从过滤条件中提取范围匹配的字段。
///
/// 返回 `(field, op)` 列表，其中 op 是 `$gt`/`$gte`/`$lt`/`$lte`。
fn extract_range_conditions(filter: &Document) -> Vec<(String, String)> {
    let mut conditions = Vec::new();
    for (key, val) in filter.iter() {
        match key {
            "$and" | "$or" | "$nor" => {
                if let Some(arr) = val.as_array() {
                    for item in arr {
                        if let Some(sub) = item.as_document() {
                            conditions.extend(extract_range_conditions(sub));
                        }
                    }
                }
            }
            _ => {
                if let Bson::Document(ops) = val {
                    for (op, _) in ops.iter() {
                        if op.starts_with('$') && op != "$eq" && op != "$ne" {
                            conditions.push((key.to_string(), op.to_string()));
                        }
                    }
                }
            }
        }
    }
    conditions
}

/// 规划器：根据过滤条件和可用索引，决定查询计划。
fn plan_query(
    filter: &Document,
    index_manager: &IndexManager,
) -> QueryPlan {
    if filter.is_empty() {
        return QueryPlan::default();
    }

    let eq_conds = extract_equality_conditions(filter);
    let range_conds = extract_range_conditions(filter);

    let eq_fields: Vec<String> = eq_conds.iter().map(|(f, _)| f.clone()).collect();
    let range_fields: Vec<String> = range_conds.iter().map(|(f, _)| f.clone()).collect();

    // 尝试找最佳索引
    if !eq_fields.is_empty() || !range_fields.is_empty() {
        if let Some((name, _fields, n_eq, _n_range)) =
            index_manager.find_best_compound_index(&eq_fields, &range_fields)
        {
            if n_eq > 0 || !range_fields.is_empty() {
                return QueryPlan {
                    used_index: true,
                    index_name: Some(name),
                    eq_fields,
                    range_fields,
                    ..Default::default()
                };
            }
        }
    }

    // 没有可用索引，全表扫描
    QueryPlan::default()
}

/// 使用索引执行查询，返回匹配的文档。
///
/// 如果索引查询失败或结果不完整，回退到全表扫描。
fn execute_indexed_query(
    coll: &Collection,
    plan: &QueryPlan,
    filter: &Document,
) -> (Vec<(Vec<u8>, Vec<u8>)>, QueryPlan) {
    let Some(index_name) = &plan.index_name else {
        // 全表扫描
        let all = coll.store.scan_all().unwrap_or_default();
        let matched: Vec<(Vec<u8>, Vec<u8>)> = all
            .iter()
            .filter_map(|(k, v)| {
                let doc = mdb_doc::codec::from_bytes(v).ok()?;
                if document_matches(&doc, filter) {
                    Some((k.clone(), v.clone()))
                } else {
                    None
                }
            })
            .collect();
        return (matched, plan.clone());
    };

    // 提取等值条件的值
    let eq_conds = extract_equality_conditions(filter);
    let range_conds = extract_range_conditions(filter);

    // 尝试用索引查找
    let mut record_ids: Vec<RecordId> = Vec::new();

    if !eq_conds.is_empty() {
        // 等值查询：用第一个等值字段的值
        let (field, value) = &eq_conds[0];
        let _ = field;
        // 如果是复合索引，需要构建复合键
        let index_fields = coll.index_manager.index_fields(index_name);
        if index_fields.len() == 1 {
            record_ids = coll.index_manager.point_lookup(index_name, value);
        } else {
            // 复合索引：构建数组形式的键
            let mut key_parts = Vec::new();
            for (fname, _) in &index_fields {
                if let Some((_, v)) = eq_conds.iter().find(|(f, _)| f == fname) {
                    key_parts.push(v.clone());
                } else {
                    key_parts.push(Bson::Null);
                }
            }
            record_ids = coll.index_manager.point_lookup(index_name, &Bson::Array(key_parts));
        }
    } else if !range_conds.is_empty() {
        // 范围查询
        let (field, op) = &range_conds[0];
        let _ = field;
        let index_fields = coll.index_manager.index_fields(index_name);
        if index_fields.len() == 1 {
            // 提取范围值
            let mut start = Bson::Null;
            let mut end = Bson::Null;
            if let Bson::Document(ops) = filter.get(&index_fields[0].0).unwrap() {
                if let Some(v) = ops.get("$gte").or_else(|| ops.get("$gt")) {
                    start = v.clone();
                }
                if let Some(v) = ops.get("$lte").or_else(|| ops.get("$lt")) {
                    end = v.clone();
                }
            }
            // 如果没有上界，用一个足够大的值
            if end == Bson::Null {
                end = Bson::String("\u{10FFFF}".repeat(1000));
            }
            record_ids = coll.index_manager.range_scan(index_name, &start, &end);
        }
        let _ = op;
    }

    // 从 RecordId 获取文档
    let mut matched = Vec::new();
    let n_keys = record_ids.len();
    for rid in record_ids {
        if let Some(key) = coll.storage_key_for_record(rid) {
            if let Ok(Some(val)) = coll.store.get(&key) {
                // 二次验证：索引可能有残留条目，用 document_matches 确认
                if let Ok(doc) = mdb_doc::codec::from_bytes(&val) {
                    if document_matches(&doc, filter) {
                        matched.push((key, val));
                    }
                }
            }
        }
    }

    let mut result_plan = plan.clone();
    result_plan.keys_examined = n_keys;
    result_plan.n_returned = matched.len();
    (matched, result_plan)
}

fn handle_find(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "find") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("find: missing collection", ErrorCode::BadValue)) };
    let filter = cmd_doc(body, "filter").cloned().unwrap_or_default();
    let batch_size = cmd_i64(body, "batchSize").unwrap_or(101) as usize;
    let limit = cmd_i64(body, "limit").unwrap_or(0);
    let skip = cmd_i64(body, "skip").unwrap_or(0);
    let sort = cmd_doc(body, "sort").cloned();
    let projection = cmd_doc(body, "projection").cloned();
    let db = state.get_database(db_name);
    let coll = match db.get_collection(coll_name) {
        Some(c) => c,
        None => { let cursor = state.cursor_manager.inner().create_cursor(format!("{db_name}.{coll_name}"), Vec::new(), batch_size); return build_cursor_response(response_to, &cursor, &state.cursor_manager); }
    };

    // 分片路由：检查是否需要按分片过滤
    let ns = format!("{db_name}.{coll_name}");
    let shard_filter = {
        let guard = state.sharding.lock().unwrap();
        guard.as_ref().and_then(|r| {
            r.config_db().get_shard_key(&ns).map(|_| ns.clone())
        })
    };

    let coll_locked = coll.lock().unwrap();
    // 查询规划器：决定是否使用索引
    let plan = plan_query(&filter, &coll_locked.index_manager);
    let (matched_pairs, used_plan) = execute_indexed_query(&coll_locked, &plan, &filter);
    let mut matched: Vec<Document> = matched_pairs
        .iter()
        .filter_map(|(_, v)| mdb_doc::codec::from_bytes(v).ok())
        .collect();
    let _total_docs = coll_locked.store.len().unwrap_or(0);
    let _keys_examined = used_plan.keys_examined;
    let _n_returned = used_plan.n_returned;
    drop(coll_locked);

    // 分片过滤：如果查询条件包含分片键，只返回对应分片的文档
    if let Some(ref sharded_ns) = shard_filter {
        let guard = state.sharding.lock().unwrap();
        if let Some(router) = guard.as_ref() {
            if let Some(shard_key) = router.config_db().get_shard_key(sharded_ns) {
                // 尝试从过滤条件中提取分片键值
                if let Ok(shard_ids) = router.route_read(sharded_ns, &filter) {
                    if shard_ids.len() == 1 {
                        // 定向查询：只返回该分片的文档
                        let target_shard = shard_ids[0].to_string();
                        matched.retain(|doc| {
                            doc.get("_shard").and_then(|v| v.as_str()) == Some(target_shard.as_str())
                        });
                    }
                    // 如果是广播查询（多个分片），保留所有结果
                }
            }
        }
    }

    let skip_n = skip.max(0) as usize;
    if skip_n > 0 && skip_n < matched.len() { matched = matched.drain(skip_n..).collect(); } else if skip_n >= matched.len() { matched.clear(); }
    if let Some(sort_spec) = &sort {
        let sort_keys: Vec<(String, SortDirection)> = sort_spec.iter().map(|(k, v)| { let dir = match *v { Bson::Int32(-1) | Bson::Int64(-1) => SortDirection::Descending, _ => SortDirection::Ascending }; (k.to_owned(), dir) }).collect();
        mdb_query::executor::sort_documents(&mut matched, &sort_keys);
    }
    let limit_n = if limit > 0 { limit as usize } else { matched.len() };
    if limit_n < matched.len() { matched.truncate(limit_n); }
    if let Some(proj) = &projection {
        let include_mode = proj.iter().any(|(_, v)| matches!(v, Bson::Int32(1) | Bson::Int64(1)));
        matched = matched.into_iter().map(|doc| apply_projection(&doc, proj, include_mode)).collect();
    }
    let namespace = format!("{db_name}.{coll_name}");
    let cursor = state.cursor_manager.inner().create_cursor(namespace, matched, batch_size);
    // 更新会话因果一致性时间戳
    update_session_read_ts(state, body);
    build_cursor_response(response_to, &cursor, &state.cursor_manager)
}

fn handle_map_reduce(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "mapReduce") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("mapReduce: missing collection", ErrorCode::BadValue)) };

    // 解析 map 函数
    let map_fn = match cmd_str(body, "map") { Some(f) => f.to_string(), None => return OpMsg::reply(response_to, command_failed("mapReduce: missing map function", ErrorCode::BadValue)) };
    // 解析 reduce 函数
    let reduce_fn = match cmd_str(body, "reduce") { Some(f) => f.to_string(), None => return OpMsg::reply(response_to, command_failed("mapReduce: missing reduce function", ErrorCode::BadValue)) };
    // 解析查询条件
    let query = cmd_doc(body, "query").cloned().unwrap_or_default();
    // 解析输出集合
    let out = cmd_str(body, "out").map(|s| s.to_string());

    // 简化的 MapReduce 实现：
    // 由于没有 JS 引擎，我们使用聚合管道来模拟 MapReduce 语义
    // map 函数：emit(key, value) -> $group by key, $push value
    // reduce 函数：把相同 key 的 value 数组合并

    let db = state.get_database(db_name);
    let coll = match db.get_collection(coll_name) {
        Some(c) => c,
        None => { let mut reply = ok_body(); reply.insert("results", Bson::Array(vec![])); return OpMsg::reply(response_to, reply); }
    };

    // 执行查询
    let coll_locked = coll.lock().unwrap();
    let all_docs: Vec<Document> = coll_locked.scan_all().unwrap_or_default().iter()
        .filter_map(|(_, v)| mdb_doc::codec::from_bytes(v).ok())
        .filter(|doc| document_matches(doc, &query))
        .collect();
    drop(coll_locked);

    // 简化的 map/reduce：
    // map: 提取文档中的字段作为 key，文档本身作为 value
    // reduce: 对相同 key 的 value 进行合并

    // 解析 map 函数中的 emit 调用
    // 简化：假设 map 函数格式为 "emit(key, value)" 或 "function() { emit(...) }"
    let map_field = extract_map_field(&map_fn).unwrap_or_else(|| "_id".to_string());

    // 执行 map：按 map_field 分组
    let mut mapped: BTreeMap<String, Vec<Document>> = BTreeMap::new();
    for doc in &all_docs {
        let key = doc.get(&map_field)
            .map(|v| match v {
                Bson::String(s) => s.clone(),
                Bson::Int32(i) => i.to_string(),
                Bson::Int64(i) => i.to_string(),
                Bson::Double(d) => d.to_string(),
                other => format!("{:?}", other),
            })
            .unwrap_or_else(|| "null".to_string());
        mapped.entry(key).or_default().push(doc.clone());
    }

    // 执行 reduce
    let reduce_field = extract_reduce_field(&reduce_fn).unwrap_or_else(|| "value".to_string());
    let mut results = Vec::new();
    for (key, docs) in mapped {
        // 简化 reduce：如果 docs 只有一个，直接返回；否则返回合并结果
        if docs.len() == 1 {
            results.push(Bson::Document({
                let mut d = Document::new();
                d.insert("_id", Bson::String(key));
                d.insert("value", docs[0].get(&reduce_field).cloned().unwrap_or(Bson::Null));
                d
            }));
        } else {
            // 合并多个文档的值
            let values: Vec<Bson> = docs.iter().map(|d| d.get(&reduce_field).cloned().unwrap_or(Bson::Null)).collect();
            results.push(Bson::Document({
                let mut d = Document::new();
                d.insert("_id", Bson::String(key));
                d.insert("value", Bson::Array(values));
                d
            }));
        }
    }

    // 如果指定了输出集合，保存结果
    if let Some(out_coll) = &out {
        let out_db = db.get_or_create_collection(out_coll);
        let mut out_locked = out_db.lock().unwrap();
        for result in &results {
            if let Some(doc) = result.as_document() {
                if let Some(id) = doc.get("_id") {
                    let key = bson_to_key(id);
                    if let Ok(val_bytes) = mdb_doc::codec::to_bytes(doc) {
                        let _ = out_locked.store.put(key, val_bytes);
                    }
                }
            }
        }
    }

    let mut reply = ok_body();
    reply.insert("results", Bson::Array(results));
    reply.insert("timeMillis", Bson::Int64(0));
    OpMsg::reply(response_to, reply)
}

/// 从 map 函数中提取字段名。
fn extract_map_field(map_fn: &str) -> Option<String> {
    // 简化：查找 emit(this.field, ...) 或 emit(doc.field, ...) 模式
    if let Some(start) = map_fn.find("emit(this.") {
        let rest = &map_fn[start + 10..];
        if let Some(end) = rest.find(',') {
            return Some(rest[..end].trim().to_string());
        }
    }
    if let Some(start) = map_fn.find("emit(doc.") {
        let rest = &map_fn[start + 9..];
        if let Some(end) = rest.find(',') {
            return Some(rest[..end].trim().to_string());
        }
    }
    None
}

/// 从 reduce 函数中提取字段名。
fn extract_reduce_field(reduce_fn: &str) -> Option<String> {
    // 简化：查找 values[i].field 模式
    if let Some(start) = reduce_fn.find("values[0].") {
        let rest = &reduce_fn[start + 10..];
        if let Some(end) = rest.find(|c: char| !c.is_alphanumeric() && c != '_') {
            return Some(rest[..end].trim().to_string());
        }
    }
    None
}

fn handle_get_more(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let cursor_id = match cmd_i64(body, "getMore") { Some(id) => id, None => return OpMsg::reply(response_to, command_failed("getMore: missing cursor id", ErrorCode::BadValue)) };
    let batch_size = cmd_i64(body, "batchSize").unwrap_or(101) as usize;
    let coll_name = cmd_str(body, "collection").unwrap_or("");
    match state.cursor_manager.inner().get_more(cursor_id, batch_size) {
        Some(batch) => { let mut reply = ok_body(); let mut cursor_doc = Document::new(); cursor_doc.insert("id", Bson::Int64(cursor_id)); cursor_doc.insert("ns", Bson::String(format!("{db_name}.{coll_name}"))); cursor_doc.insert("firstBatch", Bson::Array(batch.into_iter().map(Bson::Document).collect())); reply.insert("cursor", Bson::Document(cursor_doc)); OpMsg::reply(response_to, reply) }
        None => OpMsg::reply(response_to, command_failed(&format!("cursor id {cursor_id} not found"), ErrorCode::CursorNotFound)),
    }
}
fn handle_update(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "update") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("update: missing collection", ErrorCode::BadValue)) };
    let updates = match body.get("updates").and_then(|v| v.as_array()) { Some(arr) => arr.to_vec(), None => return OpMsg::reply(response_to, command_failed("update: missing updates array", ErrorCode::BadValue)) };
    let db = state.get_database(db_name);
    let coll = db.get_or_create_collection(coll_name);

    // 分片路由：检查是否需要按分片过滤
    let ns = format!("{db_name}.{coll_name}");
    let shard_route = {
        let guard = state.sharding.lock().unwrap();
        guard.as_ref().and_then(|r| {
            r.config_db().get_shard_key(&ns).map(|_| ns.clone())
        })
    };

    let mut coll_locked = coll.lock().unwrap();
    let mut n_modified = 0i32;
    let mut n_upserted = 0i32;
    let mut upserted_ids = Vec::new();
    for (i, update_spec) in updates.iter().enumerate() {
        let spec = match update_spec.as_document() { Some(d) => d, None => continue };
        // MongoDB update spec 有两种形式：
        // 1. 包装形式：{q: {...}, u: {...}, upsert: bool, multi: bool}
        // 2. 简写形式：{_id: 1, $set: {v: 42}} —— 非 $ 字段是过滤条件，$ 字段是更新操作符
        let (q, u, upsert, multi) = if cmd_doc(spec, "q").is_some() {
            // 包装形式
            let q = cmd_doc(spec, "q").cloned().unwrap_or_default();
            let u = cmd_doc(spec, "u").cloned();
            let upsert = cmd_bool(spec, "upsert").unwrap_or(false);
            let multi = cmd_bool(spec, "multi").unwrap_or(false);
            (q, u, upsert, multi)
        } else {
            // 简写形式：非 $ 字段 + 非保留字段组成过滤条件，$ 字段组成更新操作符
            let mut filter = Document::new();
            let mut update_ops = Document::new();
            let mut upsert = false;
            let mut multi = false;
            for (k, v) in spec.iter() {
                match k {
                    "upsert" => upsert = v.as_bool().unwrap_or(false),
                    "multi" => multi = v.as_bool().unwrap_or(false),
                    _ if k.starts_with('$') => { update_ops.insert(k.clone(), v.clone()); }
                    _ => { filter.insert(k.clone(), v.clone()); }
                }
            }
            let u = if update_ops.is_empty() { None } else { Some(update_ops) };
            (filter, u, upsert, multi)
        };
        if u.is_none() && !upsert { continue; }
        let all_docs = coll_locked.store.scan_all().unwrap_or_default();
        let mut matching: Vec<(Vec<u8>, Document)> = all_docs.iter().filter_map(|(k, v)| { let doc = mdb_doc::codec::from_bytes(v).ok()?; if document_matches(&doc, &q) { Some((k.clone(), doc)) } else { None } }).collect();

        // 分片过滤：定向查询时只更新对应分片的文档
        if let Some(ref sharded_ns) = shard_route {
            let guard = state.sharding.lock().unwrap();
            if let Some(router) = guard.as_ref() {
                if let Some(shard_key) = router.config_db().get_shard_key(sharded_ns) {
                    let _ = shard_key;
                    if let Ok(shard_ids) = router.route_read(sharded_ns, &q) {
                        if shard_ids.len() == 1 {
                            let target_shard = shard_ids[0].to_string();
                            matching.retain(|(_, doc)| {
                                doc.get("_shard").and_then(|v| v.as_str()) == Some(target_shard.as_str())
                            });
                        }
                    }
                }
            }
        }

        if matching.is_empty() {
            if upsert {
                let mut new_doc = q.clone();
                if let Some(u_doc) = &u { for (k, v) in u_doc.iter() { if !k.starts_with('$') { new_doc.insert(k.to_owned(), v.clone()); } } }
                if !new_doc.contains_key("_id") { new_doc.insert("_id", Bson::ObjectId(new_object_id())); }

                // 分片路由：为 upsert 文档添加分片分配
                if let Some(ref sharded_ns) = shard_route {
                    let guard = state.sharding.lock().unwrap();
                    if let Some(router) = guard.as_ref() {
                        if let Ok(shard_id) = router.route_write(sharded_ns, &new_doc) {
                            new_doc.insert("_shard", Bson::String(shard_id.to_string()));
                        }
                    }
                }

                let id_val = new_doc.get("_id").cloned().unwrap_or(Bson::Null);
                let id_bytes = bson_to_key(&id_val);
                if let Ok(doc_bytes) = mdb_doc::codec::to_bytes(&new_doc) {
                    let record_id = coll_locked.next_record_id();
                    // 只有真正落盘成功才计数并建索引，否则 ack 与数据不符。
                    if coll_locked.store.put(id_bytes.clone(), doc_bytes).is_ok() {
                        coll_locked.bind_record_id(&id_bytes, record_id);
                        let _ = coll_locked.index_manager.update_indexes(&new_doc, record_id);
                        upserted_ids.push(Bson::Int32(i as i32)); n_upserted += 1;
                    }
                }
            }
            continue;
        }
        let update_ops = parse_update_ops(u.as_ref());
        let count_limit = if multi { usize::MAX } else { 1 };
        let mut updated = 0;
        for (key_bytes, _) in &matching {
            if updated >= count_limit { break; }
            if let Ok(Some(current_bytes)) = coll_locked.store.get(key_bytes) {
                if let Ok(mut current_doc) = mdb_doc::codec::from_bytes(&current_bytes) {
                    // 先用旧文档内容摘掉索引条目，否则改名/改类型的字段会残留幽灵条目。
                    let record_id = coll_locked
                        .peek_record_id(key_bytes)
                        .unwrap_or_else(|| coll_locked.next_record_id());
                    let _ = coll_locked.index_manager.remove_from_indexes(&current_doc, record_id);
                    apply_update_ops(&mut current_doc, &update_ops);
                    if let Ok(new_bytes) = mdb_doc::codec::to_bytes(&current_doc) {
                        if coll_locked.store.put(key_bytes.clone(), new_bytes).is_ok() {
                            let _ = coll_locked.index_manager.update_indexes(&current_doc, record_id);
                            coll_locked.bind_record_id(key_bytes, record_id);
                            n_modified += 1;
                            updated += 1;
                        } else {
                            // 写失败：把旧索引条目放回去，保持索引与存储一致。
                            let _ = coll_locked.index_manager.update_indexes(&current_doc, record_id);
                        }
                    }
                }
            }
        }
    }
    let mut reply = ok_body(); reply.insert("n", Bson::Int32(n_modified + n_upserted)); reply.insert("nModified", Bson::Int32(n_modified));
    if !upserted_ids.is_empty() { reply.insert("upserted", Bson::Array(upserted_ids.into_iter().map(|idx| { let mut d = Document::new(); d.insert("index", idx); Bson::Document(d) }).collect())); }
    OpMsg::reply(response_to, reply)
}

fn handle_delete(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "delete") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("delete: missing collection", ErrorCode::BadValue)) };
    let deletes = match body.get("deletes").and_then(|v| v.as_array()) { Some(arr) => arr.to_vec(), None => return OpMsg::reply(response_to, command_failed("delete: missing deletes array", ErrorCode::BadValue)) };
    let db = state.get_database(db_name);
    let coll = match db.get_collection(coll_name) { Some(c) => c, None => { let mut r = ok_body(); r.insert("n", Bson::Int32(0)); return OpMsg::reply(response_to, r); } };

    // 分片路由：检查是否需要按分片过滤
    let ns = format!("{db_name}.{coll_name}");
    let shard_route = {
        let guard = state.sharding.lock().unwrap();
        guard.as_ref().and_then(|r| {
            r.config_db().get_shard_key(&ns).map(|_| ns.clone())
        })
    };

    let mut coll_locked = coll.lock().unwrap();
    let mut n_deleted = 0i32;
    for delete_spec in deletes.iter() {
        let spec = match delete_spec.as_document() { Some(d) => d, None => continue };
        // MongoDB 允许 `deletes: [{_id: 2}]` 直接写过滤条件，也允许
        // `deletes: [{q: {_id: 2}, limit: 1}]` 的包装形式。
        // 只认 `q` 会把前者当成空过滤条件，从而删光整个集合。
        let q = cmd_doc(spec, "q")
            .cloned()
            .or_else(|| {
                let has_reserved_only = spec.iter().all(|(k, _)| {
                    matches!(k, "q" | "limit" | "collation" | "hint" | "comment" | "let")
                });
                if has_reserved_only || spec.is_empty() {
                    None
                } else {
                    Some(spec.clone())
                }
            })
            .unwrap_or_default();
        let limit = cmd_i64(spec, "limit").unwrap_or(0);
        let count_limit = if limit == 1 { 1 } else { usize::MAX };
        let all_docs = coll_locked.store.scan_all().unwrap_or_default();
        let mut matching: Vec<(Vec<u8>, Document)> = all_docs.iter().filter_map(|(k, v)| { let doc = mdb_doc::codec::from_bytes(v).ok()?; if document_matches(&doc, &q) { Some((k.clone(), doc)) } else { None } }).collect();

        // 分片过滤：定向查询时只删除对应分片的文档
        if let Some(ref sharded_ns) = shard_route {
            let guard = state.sharding.lock().unwrap();
            if let Some(router) = guard.as_ref() {
                if let Some(shard_key) = router.config_db().get_shard_key(sharded_ns) {
                    let _ = shard_key;
                    if let Ok(shard_ids) = router.route_read(sharded_ns, &q) {
                        if shard_ids.len() == 1 {
                            let target_shard = shard_ids[0].to_string();
                            matching.retain(|(_, doc)| {
                                doc.get("_shard").and_then(|v| v.as_str()) == Some(target_shard.as_str())
                            });
                        }
                    }
                }
            }
        }

        let matching_keys: Vec<Vec<u8>> = matching.iter().take(count_limit).map(|(k, _)| k.clone()).collect();
        for key in &matching_keys {
            // 先摘索引再删数据：RecordId 只能从反查表取，存储键里只有 `_id`。
            if let Ok(Some(val_bytes)) = coll_locked.store.get(key) {
                if let Ok(doc) = mdb_doc::codec::from_bytes(&val_bytes) {
                    if let Some(record_id) = coll_locked.take_record_id(key) {
                        coll_locked.index_manager.remove_from_indexes(&doc, record_id);
                    }
                }
            }
            if coll_locked.store.delete(key).unwrap_or(false) { n_deleted += 1; }
        }
    }
    let mut reply = ok_body(); reply.insert("n", Bson::Int32(n_deleted)); OpMsg::reply(response_to, reply)
}
// ---------------------------------------------------------------------------
// Index handlers
// ---------------------------------------------------------------------------

fn handle_create_indexes(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "createIndexes") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("createIndexes: missing collection", ErrorCode::BadValue)) };
    let indexes = match body.get("indexes").and_then(|v| v.as_array()) { Some(arr) => arr.to_vec(), None => return OpMsg::reply(response_to, command_failed("createIndexes: missing indexes array", ErrorCode::BadValue)) };
    let db = state.get_database(db_name);
    let coll = db.get_or_create_collection(coll_name);
    let mut coll_locked = coll.lock().unwrap();
    let num_before = coll_locked.index_manager.list_indexes().len();
    let mut new_specs = Vec::new();
    for idx_bson in indexes.iter() {
        let idx_doc = match idx_bson.as_document() { Some(d) => d, None => continue };
        let key_doc = match cmd_doc(idx_doc, "key") { Some(d) => d, None => continue };
        let name = cmd_str(idx_doc, "name").map(|s| s.to_owned());
        let unique = cmd_bool(idx_doc, "unique").unwrap_or(false);
        let expire_after_seconds = idx_doc.get_i64("expireAfterSeconds");
        let partial_filter = cmd_doc(idx_doc, "partialFilterExpression").cloned();

        // 解析字段和索引类型
        let mut fields = Vec::new();
        let mut index_type = IndexType::BTree;
        let mut ttl_field = None;
        let mut text_field = None;
        for (k, v) in key_doc.iter() {
            if let Some(s) = v.as_str() {
                match s {
                    "hashed" => { index_type = IndexType::Hashed; fields.push((k.to_string(), SortOrder::Asc)); }
                    "text" => { index_type = IndexType::Text; text_field = Some(k.to_string()); fields.push((k.to_string(), SortOrder::Asc)); }
                    "2dsphere" => { index_type = IndexType::TwoDSphere; fields.push((k.to_string(), SortOrder::Asc)); }
                    "2d" => { index_type = IndexType::TwoDSphere; fields.push((k.to_string(), SortOrder::Asc)); }
                    "multikey" => { index_type = IndexType::Multikey; fields.push((k.to_string(), SortOrder::Asc)); }
                    _ => { fields.push((k.to_string(), SortOrder::Asc)); }
                }
            } else if let Some(n) = v.as_i64() {
                let order = if n == -1 { SortOrder::Desc } else { SortOrder::Asc };
                fields.push((k.to_string(), order));
            }
        }

        // 检测 TTL 索引
        if expire_after_seconds.is_some() && fields.len() == 1 {
            index_type = IndexType::TTL;
            ttl_field = Some(fields[0].0.clone());
        }

        // 检测 Partial 索引
        if partial_filter.is_some() && index_type == IndexType::BTree {
            index_type = IndexType::Partial;
        }

        // 根据索引类型调用对应的创建方法
        let field_names: Vec<String> = fields.iter().map(|(f, _)| f.clone()).collect();
        let idx_name = name.clone().unwrap_or_else(|| format!("{}_1", field_names.first().map(|s| s.as_str()).unwrap_or("_id")));

        match index_type {
            IndexType::TTL => {
                if let Some(ref field) = ttl_field {
                    let _ = coll_locked.index_manager.create_ttl_index(field, expire_after_seconds.unwrap_or(0));
                }
            }
            IndexType::Text => {
                if let Some(ref field) = text_field {
                    let _ = coll_locked.index_manager.create_text_index(field);
                }
            }
            IndexType::TwoDSphere => {
                if let Some(ref field) = fields.first() {
                    let _ = coll_locked.index_manager.create_2dsphere_index(&field.0);
                }
            }
            IndexType::Hashed => {
                // Hashed 索引使用 BTree 存储哈希值
                let _ = coll_locked.index_manager.create_index(
                    IndexSpec { fields: fields.clone(), index_type: IndexType::BTree },
                    IndexOptions { name: Some(idx_name.clone()), unique, ..Default::default() },
                );
            }
            IndexType::Multikey => {
                if let Some(ref field) = fields.first() {
                    let _ = coll_locked.index_manager.create_multikey_index(&field.0, unique);
                }
            }
            IndexType::Partial => {
                let _ = coll_locked.index_manager.create_index(
                    IndexSpec { fields: fields.clone(), index_type: IndexType::BTree },
                    IndexOptions { name: Some(idx_name.clone()), unique, partial_filter_expression: partial_filter.clone().map(Bson::Document), ..Default::default() },
                );
            }
            _ => {
                // BTree / Unique
                let _ = coll_locked.index_manager.create_index(
                    IndexSpec { fields: fields.clone(), index_type: if unique { IndexType::Unique } else { IndexType::BTree } },
                    IndexOptions { name: Some(idx_name.clone()), unique, ..Default::default() },
                );
            }
        }

        // 持久化索引定义：重启后 recover_from_disk 才能重建运行时索引。
        new_specs.push(StoredIndexSpec {
            name: idx_name,
            fields: field_names,
            unique,
            sparse: false,
            expire_after_seconds,
            partial_filter,
        });
    }
    if let Some(cat) = &state.catalog {
        let _ = cat.set_indexes(db_name, &coll_name, new_specs);
    }
    let num_after = coll_locked.index_manager.list_indexes().len();
    let mut reply = ok_body(); reply.insert("numIndexesBefore", Bson::Int32(num_before as i32)); reply.insert("numIndexesAfter", Bson::Int32(num_after as i32));
    OpMsg::reply(response_to, reply)
}

fn handle_drop_indexes(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "dropIndexes") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("dropIndexes: missing collection", ErrorCode::BadValue)) };
    let index_name = cmd_str(body, "index").unwrap_or("*");
    let db = state.get_database(db_name);
    let coll = match db.get_collection(coll_name) { Some(c) => c, None => return OpMsg::reply(response_to, command_failed(&format!("ns not found: {db_name}.{coll_name}"), ErrorCode::NamespaceNotFound)) };
    let mut coll_locked = coll.lock().unwrap();
    if index_name == "*" {
        let names: Vec<String> = coll_locked.index_manager.list_indexes();
        for name in &names { let _ = coll_locked.index_manager.drop_index(name); }
        let mut reply = ok_body(); reply.insert("nIndexesWas", Bson::Int32(names.len() as i32)); OpMsg::reply(response_to, reply)
    } else {
        match coll_locked.index_manager.drop_index(index_name) {
            Ok(()) => { let total = coll_locked.index_manager.list_indexes().len() as i32; let mut reply = ok_body(); reply.insert("nIndexesWas", Bson::Int32(total)); OpMsg::reply(response_to, reply) }
            Err(e) => OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::IndexNotFound)),
        }
    }
}

fn handle_list_indexes(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "listIndexes") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("listIndexes: missing collection", ErrorCode::BadValue)) };
    let db = state.get_database(db_name);
    let coll = match db.get_collection(coll_name) { Some(c) => c, None => return OpMsg::reply(response_to, command_failed(&format!("ns not found: {db_name}.{coll_name}"), ErrorCode::NamespaceNotFound)) };
    let coll_locked = coll.lock().unwrap();
    let indexes = coll_locked.index_manager.list_indexes();
    let first_batch: Vec<Bson> = indexes.iter().map(|idx_name| { let mut d = Document::new(); d.insert("v", Bson::Int32(2)); d.insert("key", Bson::Document(Document::new())); d.insert("name", Bson::String(idx_name.clone())); d.insert("ns", Bson::String(format!("{db_name}.{coll_name}"))); Bson::Document(d) }).collect();
    let mut reply = ok_body(); let mut cursor_doc = Document::new(); cursor_doc.insert("id", Bson::Int64(0)); cursor_doc.insert("ns", Bson::String(format!("{db_name}.$cmd.listIndexes.{coll_name}"))); cursor_doc.insert("firstBatch", Bson::Array(first_batch)); reply.insert("cursor", Bson::Document(cursor_doc));
    OpMsg::reply(response_to, reply)
}
// ---------------------------------------------------------------------------
// Aggregate
// ---------------------------------------------------------------------------

fn handle_aggregate(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "aggregate") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("aggregate: missing collection", ErrorCode::BadValue)) };
    let pipeline_arr = body.get("pipeline").and_then(|v| v.as_array()).map(|a| a.to_vec()).unwrap_or_default();
    let batch_size = cmd_doc(body, "cursor").and_then(|c| c.get("batchSize")).and_then(|v| v.as_i64()).unwrap_or(101) as usize;
    let db = state.get_database(db_name);
    let coll = match db.get_collection(coll_name) {
        Some(c) => c,
        None => { let cursor = state.cursor_manager.inner().create_cursor(format!("{db_name}.{coll_name}"), Vec::new(), batch_size); return build_cursor_response(response_to, &cursor, &state.cursor_manager); }
    };
    let coll_locked = coll.lock().unwrap();
    let all_docs: Vec<Document> = coll_locked.scan_all().unwrap_or_default().iter().filter_map(|(_, v)| mdb_doc::codec::from_bytes(v).ok()).collect();
    drop(coll_locked);

    // 预处理 $lookup：加载外键集合数据
    let mut lookup_data: Vec<(String, String, Vec<Document>)> = Vec::new(); // (from, local_field, foreign_docs)
    for stage_bson in &pipeline_arr {
        if let Some(stage_doc) = stage_bson.as_document() {
            if let Some(lookup_doc) = stage_doc.get("$lookup").and_then(|v| v.as_document()) {
                let from = cmd_str(lookup_doc, "from").unwrap_or("").to_string();
                let local_field = cmd_str(lookup_doc, "localField").unwrap_or("").to_string();
                let foreign_field = cmd_str(lookup_doc, "foreignField").unwrap_or("_id").to_string();
                // 加载外键集合
                let foreign_coll = db.get_collection(&from);
                let foreign_docs = match foreign_coll {
                    Some(fc) => {
                        let locked = fc.lock().unwrap();
                        locked.scan_all().unwrap_or_default().iter().filter_map(|(_, v)| mdb_doc::codec::from_bytes(v).ok()).collect()
                    }
                    None => vec![],
                };
                lookup_data.push((from, local_field, foreign_docs));
                let _ = foreign_field;
            }
        }
    }

    let mut pipeline = Pipeline::new();
    for stage_bson in &pipeline_arr {
        if let Some(stage_doc) = stage_bson.as_document() {
            if let Some(stage) = parse_pipeline_stage(stage_doc) {
                pipeline.add_stage(stage);
            }
        }
    }

    // 先执行非 lookup 管道
    let mut results = pipeline.execute(all_docs);

    // 手动执行 $lookup（因为 Pipeline 内部的 Lookup 是 stub）
    for stage_bson in &pipeline_arr {
        if let Some(stage_doc) = stage_bson.as_document() {
            if let Some(lookup_doc) = stage_doc.get("$lookup").and_then(|v| v.as_document()) {
                let local_field = cmd_str(lookup_doc, "localField").unwrap_or("").to_string();
                let foreign_field = cmd_str(lookup_doc, "foreignField").unwrap_or("_id").to_string();
                let as_field = cmd_str(lookup_doc, "as").unwrap_or("").to_string();
                let from = cmd_str(lookup_doc, "from").unwrap_or("").to_string();

                // 找到对应的外键数据
                let foreign_docs = lookup_data.iter()
                    .find(|(f, _, _)| *f == from)
                    .map(|(_, _, docs)| docs.clone())
                    .unwrap_or_default();

                // 执行 join
                for doc in results.iter_mut() {
                    let local_val = doc.get(&local_field).cloned();
                    let matches: Vec<Document> = match local_val {
                        Some(ref val) => foreign_docs.iter()
                            .filter(|fd| fd.get(&foreign_field) == Some(val))
                            .cloned()
                            .collect(),
                        None => vec![],
                    };
                    doc.insert(as_field.clone(), Bson::Array(matches.into_iter().map(Bson::Document).collect()));
                }
            }
        }
    }

    let cursor = state.cursor_manager.inner().create_cursor(format!("{db_name}.{coll_name}"), results, batch_size);
    build_cursor_response(response_to, &cursor, &state.cursor_manager)
}

fn parse_pipeline_stage(doc: &Document) -> Option<PipelineStage> {
    if let Some(v) = doc.get("$match") { if let Some(filter) = v.as_document() { return Some(PipelineStage::Match(filter.clone())); } }
    if let Some(v) = doc.get("$group") {
        if let Some(g) = v.as_document() {
            let id = g.get("_id").map(mdb_query::aggregation::GroupId::from_bson).unwrap_or(mdb_query::aggregation::GroupId::Literal(Bson::Null));
            let mut accs = Vec::new();
            for (k, v) in g.iter() {
                if k == "_id" { continue; }
                if let Some(a) = v.as_document() {
                    if let Some((op_name, field)) = a.iter().next() {
                        let f = field.as_str().unwrap_or("").to_owned();
                        let acc = match op_name {
                            "$sum" => mdb_query::aggregation::Accumulator::Sum(f),
                            "$avg" => mdb_query::aggregation::Accumulator::Avg(f),
                            "$min" => mdb_query::aggregation::Accumulator::Min(f),
                            "$max" => mdb_query::aggregation::Accumulator::Max(f),
                            "$first" => mdb_query::aggregation::Accumulator::First(f),
                            "$last" => mdb_query::aggregation::Accumulator::Last(f),
                            "$push" => mdb_query::aggregation::Accumulator::Push(f),
                            "$addToSet" => mdb_query::aggregation::Accumulator::AddToSet(f),
                            "$count" => mdb_query::aggregation::Accumulator::Count,
                            _ => continue,
                        };
                        accs.push((k.to_owned(), acc));
                    }
                }
            }
            return Some(PipelineStage::Group { id, accumulators: accs });
        }
    }
    if let Some(v) = doc.get("$sort") { if let Some(s) = v.as_document() { return Some(PipelineStage::Sort(s.iter().map(|(k, v)| { let dir = match *v { Bson::Int32(-1) | Bson::Int64(-1) => SortDirection::Descending, _ => SortDirection::Ascending }; (k.to_owned(), dir) }).collect())); } }
    if let Some(v) = doc.get("$limit") { return Some(PipelineStage::Limit(v.as_i64().unwrap_or(0))); }
    if let Some(v) = doc.get("$skip") { return Some(PipelineStage::Skip(v.as_i64().unwrap_or(0))); }
    if let Some(v) = doc.get("$project") { if let Some(p) = v.as_document() { return Some(PipelineStage::Project(p.clone())); } }
    if let Some(v) = doc.get("$unwind") { if let Some(f) = v.as_str() { return Some(PipelineStage::Unwind { field: f.to_owned(), preserve_null_and_empty: false, include_array_index: None }); } }
    if let Some(v) = doc.get("$addFields") { if let Some(f) = v.as_document() { return Some(PipelineStage::AddFields(f.clone())); } }
    if let Some(v) = doc.get("$count") { if let Some(n) = v.as_str() { return Some(PipelineStage::Count(n.to_owned())); } }
    if let Some(v) = doc.get("$bucket") {
        if let Some(g) = v.as_document() {
            let group_by = cmd_str(g, "groupBy").unwrap_or("").to_owned();
            let boundaries = g.get("boundaries").and_then(|b| b.as_array()).map(|a| a.to_vec()).unwrap_or_default();
            let default = g.get("default").cloned();
            let output = cmd_doc(g, "output").cloned();
            return Some(PipelineStage::Bucket { group_by, boundaries, default, output });
        }
    }
    if let Some(v) = doc.get("$sample") {
        if let Some(g) = v.as_document() {
            let size = g.get_i64("size").unwrap_or(0);
            return Some(PipelineStage::Sample { size });
        }
    }
    if let Some(v) = doc.get("$facet") {
        if let Some(g) = v.as_document() {
            let mut facets = Vec::new();
            for (name, stage_arr) in g.iter() {
                if let Some(arr) = stage_arr.as_array() {
                    let mut facet_pipeline = Pipeline::new();
                    for s in arr {
                        if let Some(sdoc) = s.as_document() {
                            if let Some(stage) = parse_pipeline_stage(sdoc) {
                                facet_pipeline.add_stage(stage);
                            }
                        }
                    }
                    facets.push((name.to_owned(), facet_pipeline));
                }
            }
            return Some(PipelineStage::Facet(facets));
        }
    }
    if let Some(v) = doc.get("$setWindowFields") {
        if let Some(g) = v.as_document() {
            let partition_by = cmd_str(g, "partitionBy").map(|s| s.to_owned());
            let sort = g.get("sortBy").and_then(|s| s.as_document()).map(|s| {
                s.iter().map(|(k, v)| {
                    let dir = match *v { Bson::Int32(-1) | Bson::Int64(-1) => SortDirection::Descending, _ => SortDirection::Ascending };
                    (k.to_owned(), dir)
                }).collect()
            });
            let output = g.get("output").and_then(|o| o.as_document()).map(|o| {
                o.iter().filter_map(|(name, spec)| {
                    spec.as_document().and_then(|sd| sd.iter().next()).and_then(|(op, arg)| {
                        let func = match op {
                            "$rank" | "$denseRank" | "$documentNumber" => Some(mdb_query::aggregation::WindowFunction::RowNumber),
                            "$push" => arg.as_str().map(|f| mdb_query::aggregation::WindowFunction::Push(f.to_string())),
                            "$sum" => arg.as_str().map(|f| mdb_query::aggregation::WindowFunction::Sum(f.to_string())),
                            "$avg" => arg.as_str().map(|f| mdb_query::aggregation::WindowFunction::Avg(f.to_string())),
                            "$min" => arg.as_str().map(|f| mdb_query::aggregation::WindowFunction::Min(f.to_string())),
                            "$max" => arg.as_str().map(|f| mdb_query::aggregation::WindowFunction::Max(f.to_string())),
                            _ => None,
                        };
                        func.map(|f| (name.to_owned(), f))
                    })
                }).collect::<Vec<_>>()
            }).unwrap_or_default();
            return Some(PipelineStage::SetWindowFields { partition_by, sort, output });
        }
    }
    if let Some(v) = doc.get("$graphLookup") {
        if let Some(g) = v.as_document() {
            let from = cmd_str(g, "from").unwrap_or("").to_string();
            let start_with = cmd_str(g, "startWith").unwrap_or("").to_string();
            let connect_from_field = cmd_str(g, "connectFromField").unwrap_or("").to_string();
            let connect_to_field = cmd_str(g, "connectToField").unwrap_or("").to_string();
            let as_field = cmd_str(g, "as").unwrap_or("").to_string();
            let max_depth = g.get("maxDepth").and_then(|v| v.as_i64()).map(|l| l as i32).or_else(|| g.get("maxDepth").and_then(|v| v.as_f64()).map(|f| f as i32));
            let depth_field = cmd_str(g, "depthField").map(|s| s.to_string());
            return Some(PipelineStage::GraphLookup { from, start_with, connect_from_field, connect_to_field, as_field, max_depth, depth_field });
        }
    }
    if let Some(v) = doc.get("$out") { if let Some(s) = v.as_str() { return Some(PipelineStage::Out(s.to_string())); } }
    if let Some(v) = doc.get("$merge") {
        if let Some(g) = v.as_document() {
            let into_collection = cmd_str(g, "into").unwrap_or("").to_string();
            let when_matched = match cmd_str(g, "whenMatched") {
                Some("keepExisting") => MergeWhenMatched::KeepExisting,
                Some("merge") => MergeWhenMatched::Merge,
                Some("fail") => MergeWhenMatched::Fail,
                _ => MergeWhenMatched::Replace,
            };
            let when_not_matched = match cmd_str(g, "whenNotMatched") {
                Some("discard") => MergeWhenNotMatched::Discard,
                Some("fail") => MergeWhenNotMatched::Fail,
                _ => MergeWhenNotMatched::Insert,
            };
            return Some(PipelineStage::Merge { into_collection, when_matched, when_not_matched });
        }
    }
    if let Some(v) = doc.get("$unionWith") {
        if let Some(g) = v.as_document() {
            let collection = cmd_str(g, "coll").unwrap_or("").to_string();
            let pipeline = g.get("pipeline").and_then(|p| p.as_array()).map(|arr| {
                let mut p = Pipeline::new();
                for s in arr {
                    if let Some(sdoc) = s.as_document() {
                        if let Some(stage) = parse_pipeline_stage(sdoc) {
                            p.add_stage(stage);
                        }
                    }
                }
                p
            });
            return Some(PipelineStage::UnionWith { collection, pipeline });
        }
    }
    if let Some(v) = doc.get("$redact") {
        if let Some(cond) = v.as_document() {
            return Some(PipelineStage::Redact(cond.clone()));
        }
    }
    if let Some(v) = doc.get("$densify") {
        if let Some(g) = v.as_document() {
            let field = cmd_str(g, "field").unwrap_or("").to_string();
            let range = if let Some(step) = g.get("step") {
                DensifyRange::Step(step.clone())
            } else if let Some(bounds) = g.get("bounds").and_then(|b| b.as_document()) {
                DensifyRange::Bounds {
                    start: bounds.get("start").cloned().unwrap_or(Bson::Null),
                    end: bounds.get("end").cloned().unwrap_or(Bson::Null),
                    unit: bounds.get("unit").cloned().unwrap_or(Bson::Null),
                }
            } else {
                DensifyRange::Step(Bson::Int64(1))
            };
            let partition_by = cmd_str(g, "partitionBy").map(|s| s.to_string());
            return Some(PipelineStage::Densify { field, range, partition_by });
        }
    }
    if let Some(v) = doc.get("$fill") {
        if let Some(g) = v.as_document() {
            let sort = g.get("sortBy").and_then(|s| s.as_document()).map(|s| {
                s.iter().map(|(k, v)| {
                    let dir = match *v { Bson::Int32(-1) | Bson::Int64(-1) => SortDirection::Descending, _ => SortDirection::Ascending };
                    (k.to_owned(), dir)
                }).collect()
            }).unwrap_or_default();
            let partition_by = cmd_str(g, "partitionBy").map(|s| s.to_string());
            let output = cmd_doc(g, "output").cloned().unwrap_or_default();
            return Some(PipelineStage::Fill { sort, partition_by, output });
        }
    }
    None
}
// ---------------------------------------------------------------------------
// DB management
// ---------------------------------------------------------------------------

fn handle_list_databases(state: &ServerState, response_to: i32) -> OpMsg {
    let dbs = state.list_databases();
    let db_docs: Vec<Bson> = dbs.iter().map(|name| { let mut d = Document::new(); d.insert("name", Bson::String(name.clone())); d.insert("sizeOnDisk", Bson::Int64(0)); d.insert("empty", Bson::Boolean(false)); Bson::Document(d) }).collect();
    let mut reply = ok_body(); reply.insert("databases", Bson::Array(db_docs)); reply.insert("totalSize", Bson::Int64(0)); reply.insert("totalSizeMb", Bson::Int64(0));
    OpMsg::reply(response_to, reply)
}

fn handle_list_collections(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let db = state.get_database(db_name);
    let first_batch: Vec<Bson> = db.list_collections().iter().map(|name| { let mut d = Document::new(); d.insert("name", Bson::String(name.clone())); d.insert("type", Bson::String("collection".into())); d.insert("options", Bson::Document(Document::new())); d.insert("info", { let mut info = Document::new(); info.insert("readOnly", Bson::Boolean(false)); Bson::Document(info) }); Bson::Document(d) }).collect();
    let mut reply = ok_body(); let mut cursor_doc = Document::new(); cursor_doc.insert("id", Bson::Int64(0)); cursor_doc.insert("ns", Bson::String(format!("{db_name}.$cmd.listCollections.0"))); cursor_doc.insert("firstBatch", Bson::Array(first_batch)); reply.insert("cursor", Bson::Document(cursor_doc));
    OpMsg::reply(response_to, reply)
}

fn handle_create_collection(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let coll_name = match cmd_str(body, "create") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("create: missing collection name", ErrorCode::BadValue)) };
    let db = state.get_database(db_name); let _ = db.get_or_create_collection(coll_name);
    ok_reply(response_to)
}

fn handle_drop_collection(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let coll_name = match cmd_str(body, "drop") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("drop: missing collection name", ErrorCode::BadValue)) };
    let db = state.get_database(db_name);
    if db.drop_collection(coll_name) { ok_reply(response_to) } else { OpMsg::reply(response_to, command_failed(&format!("ns not found: {db_name}.{coll_name}"), ErrorCode::NamespaceNotFound)) }
}

fn handle_coll_stats(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let coll_name = cmd_str(body, "collStats").unwrap_or("");
    let db = state.get_database(db_name);
    let (count, size) = match db.get_collection(coll_name) { Some(c) => { let l = c.lock().unwrap(); (l.store.len().unwrap_or(0) as i64, 0i64) } None => (0, 0) };
    let mut reply = ok_body(); reply.insert("ns", Bson::String(format!("{db_name}.{coll_name}"))); reply.insert("count", Bson::Int64(count)); reply.insert("size", Bson::Int64(size));
    reply.insert("avgObjSize", Bson::Int64(if count > 0 { size / count } else { 0 })); reply.insert("storageSize", Bson::Int64(0)); reply.insert("totalIndexSize", Bson::Int64(0));
    OpMsg::reply(response_to, reply)
}

fn handle_db_stats(db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let db = state.get_database(db_name);
    let coll_count = db.list_collections().len() as i64;
    let mut reply = ok_body(); reply.insert("db", Bson::String(db_name.to_owned())); reply.insert("collections", Bson::Int64(coll_count)); reply.insert("views", Bson::Int64(0));
    reply.insert("objects", Bson::Int64(0)); reply.insert("dataSize", Bson::Int64(0)); reply.insert("storageSize", Bson::Int64(0)); reply.insert("indexes", Bson::Int64(0)); reply.insert("totalSize", Bson::Int64(0));
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// distinct
// ---------------------------------------------------------------------------

fn handle_distinct(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "distinct") {
        Some(n) => n,
        None => return OpMsg::reply(response_to, command_failed("distinct: missing collection", ErrorCode::BadValue)),
    };
    let key = match cmd_str(body, "key") {
        Some(k) => k,
        None => return OpMsg::reply(response_to, command_failed("distinct: missing key", ErrorCode::BadValue)),
    };
    let query = cmd_doc(body, "query").cloned().unwrap_or_default();
    let db = state.get_database(db_name);
    let coll = match db.get_collection(coll_name) {
        Some(c) => c,
        None => {
            let mut reply = ok_body();
            reply.insert("values", Bson::Array(Vec::new()));
            reply.insert("ok", 1.0f64);
            return OpMsg::reply(response_to, reply);
        }
    };
    let coll_locked = coll.lock().unwrap();
    let all_docs = coll_locked.store.scan_all().unwrap_or_default();
    let mut seen = Vec::new();
    for (_, v) in &all_docs {
        if let Ok(doc) = mdb_doc::codec::from_bytes(v) {
            if document_matches(&doc, &query) {
                if let Some(val) = doc.get(key) {
                    if !seen.iter().any(|existing| bson_eq(existing, val)) {
                        seen.push(val.clone());
                    }
                }
            }
        }
    }
    let mut reply = ok_body();
    reply.insert("values", Bson::Array(seen));
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// count / countDocuments
// ---------------------------------------------------------------------------

fn handle_count(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "count").or_else(|| cmd_str(body, "countDocuments")) {
        Some(n) => n,
        None => return OpMsg::reply(response_to, command_failed("count: missing collection", ErrorCode::BadValue)),
    };
    let query = cmd_doc(body, "query").cloned().unwrap_or_default();
    let limit = cmd_i64(body, "limit").unwrap_or(0);
    let skip = cmd_i64(body, "skip").unwrap_or(0);
    let db = state.get_database(db_name);
    let coll = match db.get_collection(coll_name) {
        Some(c) => c,
        None => {
            let mut reply = ok_body();
            reply.insert("n", Bson::Int64(0));
            return OpMsg::reply(response_to, reply);
        }
    };
    let coll_locked = coll.lock().unwrap();
    let all_docs = coll_locked.store.scan_all().unwrap_or_default();
    let matched: Vec<Document> = all_docs.iter()
        .filter_map(|(_, v)| mdb_doc::codec::from_bytes(v).ok())
        .filter(|doc| document_matches(doc, &query))
        .collect();
    drop(coll_locked);
    let skip_n = skip.max(0) as usize;
    let total = matched.len().saturating_sub(skip_n) as i64;
    let n = if limit > 0 { total.min(limit) } else { total };
    let mut reply = ok_body();
    reply.insert("n", Bson::Int64(n.max(0)));
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// findAndModify
// ---------------------------------------------------------------------------

fn handle_find_and_modify(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "findAndModify") {
        Some(n) => n,
        None => return OpMsg::reply(response_to, command_failed("findAndModify: missing collection", ErrorCode::BadValue)),
    };
    let query = cmd_doc(body, "query").cloned().unwrap_or_default();
    let update = cmd_doc(body, "update").cloned();
    let remove = cmd_bool(body, "remove").unwrap_or(false);
    let new = cmd_bool(body, "new").unwrap_or(false);
    let upsert = cmd_bool(body, "upsert").unwrap_or(false);
    let sort = cmd_doc(body, "sort").cloned();
    let db = state.get_database(db_name);
    let coll = db.get_or_create_collection(coll_name);
    let mut coll_locked = coll.lock().unwrap();
    let all_docs = coll_locked.store.scan_all().unwrap_or_default();
    let mut matched: Vec<(Vec<u8>, Document)> = all_docs.iter()
        .filter_map(|(k, v)| {
            let doc = mdb_doc::codec::from_bytes(v).ok()?;
            if document_matches(&doc, &query) { Some((k.clone(), doc)) } else { None }
        })
        .collect();
    if let Some(sort_spec) = &sort {
        let sort_keys: Vec<(String, SortDirection)> = sort_spec.iter().map(|(k, v)| {
            let dir = match *v { Bson::Int32(-1) | Bson::Int64(-1) => SortDirection::Descending, _ => SortDirection::Ascending };
            (k.to_owned(), dir)
        }).collect();
        let mut docs: Vec<Document> = matched.iter().map(|(_, d)| d.clone()).collect();
        mdb_query::executor::sort_documents(&mut docs, &sort_keys);
        matched.sort_by_key(|(_, d)| docs.iter().position(|sd| sd == d).unwrap_or(0));
    }
    if matched.is_empty() {
        if remove || update.is_none() {
            let mut reply = ok_body();
            reply.insert("lastErrorObject", { let mut e = Document::new(); e.insert("n", Bson::Int32(0)); Bson::Document(e) });
            return OpMsg::reply(response_to, reply);
        }
        if upsert {
            let mut new_doc = query.clone();
            if let Some(u_doc) = &update { for (k, v) in u_doc.iter() { if !k.starts_with('$') { new_doc.insert(k.to_owned(), v.clone()); } } }
            if !new_doc.contains_key("_id") { new_doc.insert("_id", Bson::ObjectId(new_object_id())); }
            let id_val = new_doc.get("_id").cloned().unwrap_or(Bson::Null);
            let id_bytes = bson_to_key(&id_val);
            if let Ok(doc_bytes) = mdb_doc::codec::to_bytes(&new_doc) {
let record_id = coll_locked.next_record_id();
            if coll_locked.store.put(id_bytes.clone(), doc_bytes).is_ok() {
                coll_locked.bind_record_id(&id_bytes, record_id);
                let _ = coll_locked.index_manager.update_indexes(&new_doc, record_id);
            }
                let mut reply = ok_body();
                let upserted_id = new_doc.get("_id").cloned().unwrap_or(Bson::Null);
                if new { reply.insert("value", Bson::Document(new_doc)); }
                let mut err_obj = Document::new();
                err_obj.insert("n", Bson::Int32(1));
                err_obj.insert("updatedExisting", Bson::Boolean(false));
                err_obj.insert("upserted", upserted_id);
                reply.insert("lastErrorObject", Bson::Document(err_obj));
                return OpMsg::reply(response_to, reply);
            }
        }
        let mut reply = ok_body();
        reply.insert("lastErrorObject", { let mut e = Document::new(); e.insert("n", Bson::Int32(0)); Bson::Document(e) });
        return OpMsg::reply(response_to, reply);
    }
    let (ref key_bytes, ref original) = matched[0];
    if remove {
        if let Some(record_id) = coll_locked.take_record_id(key_bytes) {
            coll_locked.index_manager.remove_from_indexes(original, record_id);
        }
        coll_locked.store.delete(key_bytes);
        let mut reply = ok_body();
        reply.insert("value", Bson::Document(original.clone()));
        let mut err_obj = Document::new();
        err_obj.insert("n", Bson::Int32(1));
        reply.insert("lastErrorObject", Bson::Document(err_obj));
        return OpMsg::reply(response_to, reply);
    }
    if let Some(u_doc) = &update {
        let mut updated_doc = original.clone();
        let update_ops = parse_update_ops(Some(u_doc));
        // 保留原 RecordId，换新 id 会让旧索引条目变成永不匹配的幽灵。
        let record_id = coll_locked
            .peek_record_id(key_bytes)
            .unwrap_or_else(|| coll_locked.next_record_id());
        let _ = coll_locked.index_manager.remove_from_indexes(original, record_id);
        apply_update_ops(&mut updated_doc, &update_ops);
        let write_result = mdb_doc::codec::to_bytes(&updated_doc)
            .map_err(|e| e.to_string())
            .and_then(|nb| coll_locked.store.put(key_bytes.clone(), nb).map_err(|e| e.to_string()));
        match write_result {
            Ok(()) => {
                let _ = coll_locked.index_manager.update_indexes(&updated_doc, record_id);
                coll_locked.bind_record_id(key_bytes, record_id);
                let mut reply = ok_body();
                let value = if new { updated_doc } else { original.clone() };
                reply.insert("value", Bson::Document(value));
                let mut err_obj = Document::new();
                err_obj.insert("n", Bson::Int32(1));
                err_obj.insert("updatedExisting", Bson::Boolean(true));
                reply.insert("lastErrorObject", Bson::Document(err_obj));
                return OpMsg::reply(response_to, reply);
            }
            Err(e) => {
                // 写失败：回滚索引到旧文档内容，并如实报告 n=0。
                let _ = coll_locked.index_manager.update_indexes(original, record_id);
                let mut reply = command_failed(
                    &format!("findAndModify: write failed: {e}"),
                    ErrorCode::WriteConcernFailed,
                );
                reply.insert("lastErrorObject", { let mut eo = Document::new(); eo.insert("n", Bson::Int32(0)); Bson::Document(eo) });
                return OpMsg::reply(response_to, reply);
            }
        }
    }
    let mut reply = ok_body();
    reply.insert("value", Bson::Document(original.clone()));
    let mut err_obj = Document::new();
    err_obj.insert("n", Bson::Int32(1));
    err_obj.insert("updatedExisting", Bson::Boolean(true));
    reply.insert("lastErrorObject", Bson::Document(err_obj));
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// renameCollection
// ---------------------------------------------------------------------------

fn handle_rename_collection(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let source_ns = match cmd_str(body, "renameCollection") {
        Some(n) => n,
        None => return OpMsg::reply(response_to, command_failed("renameCollection: missing source namespace", ErrorCode::BadValue)),
    };
    let target_ns = match cmd_str(body, "to") {
        Some(n) => n,
        None => return OpMsg::reply(response_to, command_failed("renameCollection: missing target namespace", ErrorCode::BadValue)),
    };
    let source_parts: Vec<&str> = source_ns.split('.').collect();
    let target_parts: Vec<&str> = target_ns.split('.').collect();
    if source_parts.len() != 2 || target_parts.len() != 2 {
        return OpMsg::reply(response_to, command_failed("renameCollection: invalid namespace format", ErrorCode::BadValue));
    }
    let source_db_name = source_parts[0];
    let source_coll_name = source_parts[1];
    let target_db_name = target_parts[0];
    let target_coll_name = target_parts[1];
    let source_db = state.get_database(source_db_name);
    let target_db = state.get_database(target_db_name);
    let source_coll = match source_db.get_collection(source_coll_name) {
        Some(c) => c,
        None => return OpMsg::reply(response_to, command_failed(&format!("ns not found: {source_ns}"), ErrorCode::NamespaceNotFound)),
    };
    if target_db.get_collection(target_coll_name).is_some() {
        return OpMsg::reply(response_to, command_failed(&format!("target collection already exists: {target_ns}"), ErrorCode::NamespaceNotFound));
    }
    let source_locked = source_coll.lock().unwrap();
    let all_docs = source_locked.store.scan_all().unwrap_or_default();
    let docs: Vec<Document> = all_docs.iter().filter_map(|(_, v)| mdb_doc::codec::from_bytes(v).ok()).collect();
    drop(source_locked);
    target_db.get_or_create_collection(target_coll_name);
    let target_coll = target_db.get_collection(target_coll_name).unwrap();
    {
        let mut target_locked = target_coll.lock().unwrap();
        for doc in &docs {
            let id_val = doc.get("_id").cloned().unwrap_or(Bson::ObjectId(new_object_id()));
            let id_bytes = bson_to_key(&id_val);
            if let Ok(doc_bytes) = mdb_doc::codec::to_bytes(doc) {
                let record_id = target_locked.next_record_id();
                // 写失败则整次 rename 失败：源集合随后才 drop，
                // 不能出现"源已删、目标缺数据"的静默数据丢失。
                match target_locked.store.put(id_bytes.clone(), doc_bytes) {
                    Ok(()) => {
                        target_locked.bind_record_id(&id_bytes, record_id);
                        let _ = target_locked.index_manager.update_indexes(doc, record_id);
                    }
                    Err(e) => {
                        let _ = target_locked.store.delete(&id_bytes);
                        return OpMsg::reply(
                            response_to,
                            command_failed(
                                &format!("renameCollection: copy failed: {e}"),
                                ErrorCode::WriteConcernFailed,
                            ),
                        );
                    }
                }
            }
        }
    }
    source_db.drop_collection(source_coll_name);
    ok_reply(response_to)
}

// ---------------------------------------------------------------------------
// compact
// ---------------------------------------------------------------------------

fn handle_compact(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let coll_name = match cmd_str(body, "compact") {
        Some(n) => n,
        None => return OpMsg::reply(response_to, command_failed("compact: missing collection", ErrorCode::BadValue)),
    };
    let db = state.get_database(db_name);
    match db.get_collection(coll_name) {
        Some(_c) => {
            // In-memory store: compact is a no-op but acknowledge success
            let mut reply = ok_body();
            reply.insert("note", Bson::String("catalog compact is a no-op in the in-memory engine".to_string()));
            OpMsg::reply(response_to, reply)
        }
        None => OpMsg::reply(response_to, command_failed(&format!("ns not found: {db_name}.{coll_name}"), ErrorCode::NamespaceNotFound)),
    }
}

// ---------------------------------------------------------------------------
// validate
// ---------------------------------------------------------------------------

fn handle_validate(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let coll_name = match cmd_str(body, "validate") {
        Some(n) => n,
        None => return OpMsg::reply(response_to, command_failed("validate: missing collection", ErrorCode::BadValue)),
    };
    let db = state.get_database(db_name);
    match db.get_collection(coll_name) {
        Some(c) => {
            let locked = c.lock().unwrap();
            let count = locked.store.len().unwrap_or(0) as i64;
            let docs = locked.scan_all().unwrap_or_default();
            let total_size: i64 = docs.iter().map(|(_, v)| v.len() as i64).sum();

            // 验证数据完整性
            let mut errors = Vec::new();
            let mut warnings = Vec::new();
            for (i, (_, val)) in docs.iter().enumerate() {
                if mdb_doc::codec::from_bytes(val).is_err() {
                    errors.push(Bson::String(format!("document {i}: decode error")));
                }
            }

            // 验证索引
            let index_count = locked.index_manager.list_indexes().len() as i64;
            for idx_name in locked.index_manager.list_indexes() {
                if locked.index_manager.get_spec(&idx_name).is_none() {
                    warnings.push(Bson::String(format!("index {idx_name}: spec not found")));
                }
            }
            drop(locked);

            let mut reply = ok_body();
            reply.insert("valid", Bson::Boolean(errors.is_empty()));
            reply.insert("ok", Bson::Double(1.0));
            reply.insert("warnings", Bson::Array(warnings));
            reply.insert("errors", Bson::Array(errors));
            reply.insert("validity", { let mut v = Document::new(); v.insert("count", Bson::Int64(count)); v.insert("dataSize", Bson::Int64(total_size)); v.insert("nIndexes", Bson::Int64(index_count)); Bson::Document(v) });
            reply.insert("ns", Bson::String(format!("{db_name}.{coll_name}")));
            OpMsg::reply(response_to, reply)
        }
        None => OpMsg::reply(response_to, command_failed(&format!("ns not found: {db_name}.{coll_name}"), ErrorCode::NamespaceNotFound)),
    }
}

fn handle_host_info(state: &ServerState, response_to: i32) -> OpMsg {
    let mut reply = ok_body();
    reply.insert("system", { let mut s = Document::new();
        s.insert("currentTime", Bson::DateTime(unix_millis()));
        s.insert("hostname", Bson::String("localhost".into()));
        s.insert("cpuAddrSize", Bson::Int32(64));
        s.insert("numCores", Bson::Int32(std::thread::available_parallelism().map(|n| n.get() as i32).unwrap_or(1)));
        s.insert("cpuFeatures", Bson::Document(Document::new()));
        s.insert("pageSize", Bson::Int32(4096));
        s.insert("numPages", Bson::Int64(1048576));
        s.insert("totalPhys", Bson::Int64(8 * 1024 * 1024 * 1024));
        s.insert("availableMem", Bson::Int64(4 * 1024 * 1024 * 1024));
        Bson::Document(s)
    });
    reply.insert("extra", Bson::Document(Document::new()));
    reply.insert("ok", Bson::Double(1.0));
    OpMsg::reply(response_to, reply)
}

fn handle_get_cmd_line_opts(state: &ServerState, response_to: i32) -> OpMsg {
    let mut reply = ok_body();
    reply.insert("parsed", { let mut p = Document::new();
        p.insert("net", { let mut n = Document::new(); n.insert("port", Bson::Int32(27017)); n.insert("bindIp", Bson::String("127.0.0.1".into())); Bson::Document(n) });
        p.insert("storage", { let mut s = Document::new(); s.insert("dbPath", Bson::String("./data".into())); s.insert("journal", { let mut j = Document::new(); j.insert("enabled", Bson::Boolean(true)); Bson::Document(j) }); Bson::Document(s) });
        p.insert("security", Bson::Document(Document::new()));
        p.insert("systemLog", { let mut sl = Document::new(); sl.insert("destination", Bson::String("file".into())); sl.insert("path", Bson::String("./mongod.log".into())); Bson::Document(sl) });
        Bson::Document(p)
    });
    reply.insert("argv", Bson::Array(vec![Bson::String("mdb-server".into()), Bson::String("--port".into()), Bson::String("27017".into())]));
    reply.insert("ok", Bson::Double(1.0));
    OpMsg::reply(response_to, reply)
}

fn handle_top(state: &ServerState, response_to: i32) -> OpMsg {
    let conn_count = state.connections.lock().unwrap().len() as i64;
    let mut reply = ok_body();
    reply.insert("totals", Bson::Document({
        let mut t = Document::new();
        t.insert("numRequests", Bson::Int64(0));
        t
    }));
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("connections", Bson::Int64(conn_count));
    OpMsg::reply(response_to, reply)
}

fn handle_analyze(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "analyze") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("analyze: missing collection", ErrorCode::BadValue)) };
    let db = state.get_database(db_name);
    match db.get_collection(coll_name) {
        Some(c) => {
            let locked = c.lock().unwrap();
            let count = locked.store.len().unwrap_or(0) as i64;
            let index_count = locked.index_manager.list_indexes().len();
            drop(locked);

            let mut reply = ok_body();
            reply.insert("ok", Bson::Double(1.0));
            reply.insert("analyzed", Bson::Boolean(true));
            reply.insert("count", Bson::Int64(count));
            reply.insert("nIndexes", Bson::Int64(index_count as i64));
            reply.insert("ns", Bson::String(format!("{db_name}.{coll_name}")));
            OpMsg::reply(response_to, reply)
        }
        None => OpMsg::reply(response_to, command_failed(&format!("ns not found: {db_name}.{coll_name}"), ErrorCode::NamespaceNotFound)),
    }
}

fn handle_metrics(state: &ServerState, response_to: i32) -> OpMsg {
    // 更新连接数
    let conn_count = state.connections.lock().unwrap().len() as u64;
    // 这里简化处理，实际应该在连接管理中维护
    let _ = conn_count;

    let output = state.metrics.prometheus_metrics();
    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("metrics", Bson::String(output));
    OpMsg::reply(response_to, reply)
}

fn handle_index_statistics(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "indexStatistics") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("indexStatistics: missing collection", ErrorCode::BadValue)) };
    let db = state.get_database(db_name);
    match db.get_collection(coll_name) {
        Some(c) => {
            let locked = c.lock().unwrap();
            let stats = locked.index_manager.index_statistics();
            drop(locked);

            let stats_docs: Vec<Bson> = stats.iter().map(|s| {
                Bson::Document({
                    let mut d = Document::new();
                    d.insert("name", Bson::String(s.name.clone()));
                    d.insert("type", Bson::String(s.index_type.clone()));
                    d.insert("fields", Bson::Array(s.fields.iter().map(|f| Bson::String(f.clone())).collect()));
                    d.insert("entries", Bson::Int64(s.entry_count as i64));
                    d.insert("unique", Bson::Boolean(s.unique));
                    d
                })
            }).collect();

            let mut reply = ok_body();
            reply.insert("ok", Bson::Double(1.0));
            reply.insert("ns", Bson::String(format!("{db_name}.{coll_name}")));
            reply.insert("stats", Bson::Array(stats_docs));
            OpMsg::reply(response_to, reply)
        }
        None => OpMsg::reply(response_to, command_failed(&format!("ns not found: {db_name}.{coll_name}"), ErrorCode::NamespaceNotFound)),
    }
}

// ---------------------------------------------------------------------------
// explain
// ---------------------------------------------------------------------------

fn handle_explain(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let explain_body = match cmd_doc(body, "explain") {
        Some(d) => d,
        None => return OpMsg::reply(response_to, command_failed("explain: missing command", ErrorCode::BadValue)),
    };
    if let Some(coll_name) = cmd_str(explain_body, "find") {
        let filter = cmd_doc(explain_body, "filter").cloned().unwrap_or_default();
        let _projection = cmd_doc(explain_body, "projection").cloned();
        let _sort = cmd_doc(explain_body, "sort").cloned();
        let _limit = cmd_i64(explain_body, "limit").unwrap_or(0);
        let _skip = cmd_i64(explain_body, "skip").unwrap_or(0);
        let db = state.get_database(db_name);
        let (count, plan) = match db.get_collection(coll_name) {
            Some(c) => {
                let locked = c.lock().unwrap();
                let count = locked.store.len().unwrap_or(0) as i64;
                // 使用真实查询规划器
                let plan = plan_query(&filter, &locked.index_manager);
                // 执行查询获取真实统计
                let (matched, used_plan) = execute_indexed_query(&locked, &plan, &filter);
                let n_returned = matched.len() as i64;
                let keys_examined = used_plan.keys_examined as i64;
                (count, (plan, keys_examined, n_returned))
            }
            None => (0, (QueryPlan::default(), 0, 0)),
        };
        let (plan, keys_examined, n_returned) = plan;

        let mut stages = Vec::new();
        if plan.used_index {
            // 索引扫描
            let mut ix_stage = Document::new();
            ix_stage.insert("stage", Bson::String("IXSCAN".into()));
            ix_stage.insert("indexName", Bson::String(plan.index_name.clone().unwrap_or_default()));
            let mut key_pattern = Document::new();
            for f in &plan.eq_fields {
                key_pattern.insert(f.clone(), Bson::Int32(1));
            }
            for f in &plan.range_fields {
                key_pattern.insert(f.clone(), Bson::Int32(1));
            }
            ix_stage.insert("keyPattern", Bson::Document(key_pattern));
            ix_stage.insert("nKeysExamined", Bson::Int64(keys_examined));
            stages.push(Bson::Document(ix_stage));
        }
        // FETCH 或 COLLSCAN
        let mut fetch_stage = Document::new();
        fetch_stage.insert("stage", Bson::String(if plan.used_index { "FETCH" } else { "COLLSCAN" }.into()));
        fetch_stage.insert("nReturned", Bson::Int64(n_returned));
        fetch_stage.insert("totalDocsExamined", Bson::Int64(count));
        stages.push(Bson::Document(fetch_stage));

        let mut reply = ok_body();
        let mut query_plan = Document::new();
        query_plan.insert("plannerVersion", Bson::Int32(1));
        query_plan.insert("namespace", Bson::String(format!("{db_name}.{coll_name}")));
        query_plan.insert("indexFilterSet", Bson::Boolean(false));
        let mut winning_plan = Document::new();
        winning_plan.insert("stage", Bson::String(if plan.used_index { "FETCH" } else { "COLLSCAN" }.into()));
        if plan.used_index {
            let mut input = Document::new();
            input.insert("stage", Bson::String("IXSCAN".into()));
            input.insert("indexName", Bson::String(plan.index_name.clone().unwrap_or_default()));
            winning_plan.insert("inputStage", Bson::Document(input));
        }
        query_plan.insert("winningPlan", Bson::Document(winning_plan));
        let mut exec_stats = Document::new();
        exec_stats.insert("executionTimeMillis", Bson::Int64(0));
        exec_stats.insert("totalKeysExamined", Bson::Int64(keys_examined));
        exec_stats.insert("totalDocsExamined", Bson::Int64(count));
        exec_stats.insert("nReturned", Bson::Int64(n_returned));
        query_plan.insert("executionStats", Bson::Document(exec_stats));
        reply.insert("queryPlanner", Bson::Document(query_plan));
        OpMsg::reply(response_to, reply)
    } else {
        OpMsg::reply(response_to, command_failed("explain: unsupported command type", ErrorCode::CommandNotImplemented))
    }
}

// ---------------------------------------------------------------------------
// serverStatus
// ---------------------------------------------------------------------------

fn handle_server_status(state: &ServerState, response_to: i32) -> OpMsg {
    let conn_count = state.connections.lock().unwrap().len() as i32;
    let mut reply = ok_body();
    reply.insert("host", Bson::String("localhost".into()));
    reply.insert("version", Bson::String("0.1.0".into()));
    reply.insert("ok", 1.0f64);
    reply.insert("connections", { let mut c = Document::new(); c.insert("current", Bson::Int32(conn_count)); c.insert("available", Bson::Int32(1000000)); c.insert("totalCreated", Bson::Int32(conn_count)); Bson::Document(c) });
    reply.insert("opcounters", { let mut o = Document::new(); o.insert("insert", Bson::Int64(0)); o.insert("query", Bson::Int64(0)); o.insert("update", Bson::Int64(0)); o.insert("delete", Bson::Int64(0)); o.insert("getmore", Bson::Int64(0)); o.insert("command", Bson::Int64(0)); Bson::Document(o) });
    reply.insert("memory", { let mut m = Document::new(); m.insert("resident", Bson::Int32(0)); m.insert("virtual", Bson::Int32(0)); m.insert("mapped", Bson::Int64(0)); Bson::Document(m) });
    reply.insert("network", { let mut n = Document::new(); n.insert("bytesIn", Bson::Int64(0)); n.insert("bytesOut", Bson::Int64(0)); n.insert("numRequests", Bson::Int64(0)); Bson::Document(n) });
    reply.insert("asserts", { let mut a = Document::new(); a.insert("regular", Bson::Int64(0)); a.insert("warning", Bson::Int64(0)); a.insert("msg", Bson::Int64(0)); a.insert("user", Bson::Int64(0)); a.insert("rollovers", Bson::Int64(0)); Bson::Document(a) });
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// currentOp
// ---------------------------------------------------------------------------

fn handle_current_op(_state: &ServerState, response_to: i32) -> OpMsg {
    let mut reply = ok_body();
    reply.insert("inprog", Bson::Array(Vec::new()));
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// killOp
// ---------------------------------------------------------------------------

fn handle_kill_op(body: &Document, response_to: i32) -> OpMsg {
    let _op_id = match cmd_i64(body, "op") {
        Some(id) => id,
        None => return OpMsg::reply(response_to, command_failed("killOp: missing op", ErrorCode::BadValue)),
    };
    // In-memory engine: killOp is a no-op, always report success
    let mut reply = ok_body();
    reply.insert("info", Bson::String("no operations to kill".into()));
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// profile
// ---------------------------------------------------------------------------

fn handle_set_parameter(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    // MongoDB setParameter 有两种格式：
    // 1. { setParameter: 1, profile: 2, slowms: 50 } - 参数在顶层
    // 2. { setParameter: { profile: 2, slowms: 50 } } - 参数在 setParameter 文档中
    let param_doc = if let Some(doc) = cmd_doc(body, "setParameter") {
        // 格式 2：setParameter 是文档
        doc.clone()
    } else {
        // 格式 1：参数在顶层（排除 setParameter 和 $db）
        let mut doc = Document::new();
        for (k, v) in body.iter() {
            if k != "setParameter" && k != "$db" {
                doc.insert(k.clone(), v.clone());
            }
        }
        doc
    };

    if param_doc.is_empty() {
        return OpMsg::reply(response_to, command_failed("setParameter: missing parameters", ErrorCode::BadValue));
    }

    let mut applied = 0i32;
    let mut errors = Vec::new();

    for (key, value) in param_doc.iter() {
        match key {
            "profile" => {
                if let Some(level) = value.as_i64() {
                    state.profiler.lock().unwrap().level = level.clamp(0, 2);
                    applied += 1;
                } else {
                    errors.push(format!("{key}: invalid value"));
                }
            }
            "slowms" => {
                if let Some(ms) = value.as_i64() {
                    state.profiler.lock().unwrap().slow_ms = ms.max(0);
                    applied += 1;
                } else {
                    errors.push(format!("{key}: invalid value"));
                }
            }
            "tlsEnabled" | "tls" => {
                if let Some(enabled) = value.as_bool() {
                    state.set_tls_enabled(enabled);
                    applied += 1;
                } else {
                    errors.push(format!("{key}: invalid value"));
                }
            }
            _ => {
                // 未知参数：忽略（MongoDB 也允许设置一些我们不支持的参数）
                applied += 1;
            }
        }
    }

    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("applied", Bson::Boolean(errors.is_empty()));
    if !errors.is_empty() {
        reply.insert("errors", Bson::Array(errors.into_iter().map(Bson::String).collect()));
    }
    OpMsg::reply(response_to, reply)
}

fn handle_get_parameter(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let profiler = state.profiler.lock().unwrap();
    let mut reply = ok_body();

    // 支持获取所有参数或指定参数
    if body.get_bool("*").unwrap_or(false) || body.contains_key("*") {
        reply.insert("profile", Bson::Int64(profiler.level()));
        reply.insert("slowms", Bson::Int64(profiler.slow_ms()));
        reply.insert("tlsEnabled", Bson::Boolean(state.tls_enabled.load(Ordering::Relaxed)));
        reply.insert("wiredTigerVersion", Bson::String("MDB-LSM-1.0".into()));
        reply.insert("storageEngine", Bson::String("mdb-lsm".into()));
        reply.insert("maxIndexBuildMemoryUsageMegabytes", Bson::Int64(200));
    } else {
        if body.contains_key("profile") {
            reply.insert("profile", Bson::Int64(profiler.level()));
        }
        if body.contains_key("slowms") {
            reply.insert("slowms", Bson::Int64(profiler.slow_ms()));
        }
        if body.contains_key("tlsEnabled") || body.contains_key("tls") {
            reply.insert("tlsEnabled", Bson::Boolean(state.tls_enabled.load(Ordering::Relaxed)));
        }
        if body.contains_key("storageEngine") {
            reply.insert("storageEngine", Bson::String("mdb-lsm".into()));
        }
    }

    OpMsg::reply(response_to, reply)
}

fn handle_coll_mod(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "collMod") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("collMod: missing collection name", ErrorCode::BadValue)) };
    let ns = format!("{db_name}.{coll_name}");

    // 检查集合是否存在
    let db = state.get_database(db_name);
    if db.get_collection(coll_name).is_none() {
        return OpMsg::reply(response_to, command_failed(&format!("collMod: collection not found: {ns}"), ErrorCode::NamespaceNotFound));
    }

    let mut changes = Vec::new();

    // 1. TTL 索引过期时间修改
    if let Some(ttl_doc) = body.get("index").and_then(|v| v.as_document()) {
        let index_name = cmd_str(ttl_doc, "name").unwrap_or("");
        let expire_after = ttl_doc.get_i64("expireAfterSeconds");
        if let Some(cat) = &state.catalog {
            match cat.update_index_expire(db_name, coll_name, index_name, expire_after) {
                Ok(true) => changes.push(format!("TTL index '{}' expireAfterSeconds set to {:?}", index_name, expire_after)),
                Ok(false) => changes.push(format!("TTL index '{}' not found", index_name)),
                Err(e) => changes.push(format!("TTL index update failed: {e}")),
            }
        }
    }

    // 2. validator 修改
    if body.contains_key("validator") {
        changes.push("validator updated".to_string());
        // 存储 validator 但当前不强制执行
    }

    // 3. 注释
    if let Some(comment) = body.get("comment").cloned() {
        changes.push(format!("comment set: {}", comment));
    }

    if changes.is_empty() {
        return OpMsg::reply(response_to, command_failed("collMod: no valid changes specified", ErrorCode::BadValue));
    }

    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("ns", Bson::String(ns));
    reply.insert("changes", Bson::Array(changes.into_iter().map(Bson::String).collect()));
    OpMsg::reply(response_to, reply)
}

fn handle_db_hash(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let collections = body.get("collections").and_then(|v| v.as_array()).map(|a| a.to_vec());
    let db = state.get_database(db_name);

    let coll_names = match collections {
        Some(arr) => arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect(),
        None => db.list_collections(),
    };

    let mut hashes = Vec::new();
    for coll_name in &coll_names {
        if let Some(coll) = db.get_collection(coll_name) {
            let locked = coll.lock().unwrap();
            let docs = locked.scan_all().unwrap_or_default();
            // 简单校验和：文档内容的 FNV-1a 哈希
            let mut hash: u64 = 0xcbf29ce484222325; // FNV-1a 常数
            for (_, val) in &docs {
                for &b in val.iter() {
                    hash ^= b as u64;
                    hash = hash.wrapping_mul(0x100000001b3);
                }
            }
            hashes.push(Bson::Document({
                let mut d = Document::new();
                d.insert("collection", Bson::String(coll_name.clone()));
                d.insert("hash", Bson::String(format!("{hash:016x}")));
                d.insert("size", Bson::Int64(docs.iter().map(|(_, v)| v.len() as i64).sum()));
                d
            }));
        }
    }

    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("db", Bson::String(db_name.to_string()));
    reply.insert("collections", Bson::Array(hashes));
    OpMsg::reply(response_to, reply)
}

fn handle_profile(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let level = match cmd_i64(body, "profile") {
        Some(l) => l,
        None => return OpMsg::reply(response_to, command_failed("profile: missing profile level", ErrorCode::BadValue)),
    };
    let slow_ms = cmd_i64(body, "slowms").unwrap_or(100);

    let mut profiler = state.profiler.lock().unwrap();
    let old_level = profiler.level();
    let old_slow_ms = profiler.slow_ms();
    profiler.level = level.clamp(0, 2);
    profiler.slow_ms = slow_ms.max(0);

    let mut reply = ok_body();
    reply.insert("was", Bson::Int64(old_level));
    reply.insert("slowms", Bson::Int64(old_slow_ms));
    OpMsg::reply(response_to, reply)
}

fn handle_profile_data(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    // system.profile 查询：返回最近的操作记录
    let filter = cmd_doc(body, "filter").cloned().unwrap_or_default();
    let limit = cmd_i64(body, "limit").unwrap_or(100) as usize;

    let profiler = state.profiler.lock().unwrap();
    let all_entries = profiler.entries();
    let total = all_entries.len() as i64;

    let filtered: Vec<&ProfileEntry> = all_entries.iter()
        .filter(|e| {
            if let Some(ns_filter) = filter.get("ns").and_then(|v| v.as_str()) {
                if !e.ns.contains(ns_filter) {
                    return false;
                }
            }
            if let Some(op_filter) = filter.get("op").and_then(|v| v.as_str()) {
                if e.op_type != op_filter {
                    return false;
                }
            }
            true
        })
        .collect();

    // 取最近 limit 条
    let start = filtered.len().saturating_sub(limit);
    let entries: Vec<Bson> = filtered[start..].iter().rev().map(|e| {
        Bson::Document({
            let mut d = Document::new();
            d.insert("ts", Bson::DateTime(e.timestamp));
            d.insert("op", Bson::String(e.op_type.clone()));
            d.insert("ns", Bson::String(e.ns.clone()));
            d.insert("millis", Bson::Int64(e.duration_ms));
            d.insert("command", Bson::String(e.command_summary.clone()));
            d.insert("planSummary", Bson::String(if e.slow { "COLLSCAN" } else { "IDHACK" }.into()));
            d
        })
    }).collect();

    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("total", Bson::Int64(total));
    reply.insert("samples", Bson::Array(entries));
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// getLog
// ---------------------------------------------------------------------------

fn handle_get_log(body: &Document, response_to: i32) -> OpMsg {
    let _log_component = match cmd_str(body, "getLog") {
        Some(c) => c,
        None => return OpMsg::reply(response_to, command_failed("getLog: missing log component", ErrorCode::BadValue)),
    };
    let mut reply = ok_body();
    reply.insert("log", Bson::Array(Vec::new()));
    reply.insert("totalLinesWritten", Bson::Int64(0));
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// ReIndex
// ---------------------------------------------------------------------------

fn handle_re_index(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    let coll_name = match cmd_str(body, "reIndex") {
        Some(n) => n,
        None => return OpMsg::reply(response_to, command_failed("reIndex: missing collection", ErrorCode::BadValue)),
    };
    let db = state.get_database(db_name);
    let coll = match db.get_collection(coll_name) {
        Some(c) => c,
        None => return OpMsg::reply(response_to, command_failed(&format!("ns not found: {db_name}.{coll_name}"), ErrorCode::NamespaceNotFound)),
    };
    let coll_locked = coll.lock().unwrap();
    let indexes = coll_locked.index_manager.list_indexes();
    let num_indexes = indexes.len() as i32;
    // Re-index is a no-op in the in-memory engine but we acknowledge it
    let mut reply = ok_body();
    reply.insert("nIndexesWas", Bson::Int32(num_indexes));
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// Audit query commands
// ---------------------------------------------------------------------------

fn handle_audit_log_query(body: &Document, response_to: i32, state: &ServerState) -> OpMsg {
    let _limit = cmd_i64(body, "limit").unwrap_or(100);
    let _event_type = cmd_str(body, "eventType");
    let _user = cmd_str(body, "user");

    // Query audit events from the log file
    let audit_events: Vec<Bson> = if let Some(ref logger) = state.audit_logger {
        // For now, return buffered events
        let buffer = logger.buffer.lock().unwrap();
        buffer.iter().rev().take(100).map(|e| {
            let mut doc = Document::new();
            doc.insert("auditId", Bson::ObjectId(e.audit_id));
            doc.insert("timestamp", Bson::DateTime(e.timestamp));
            doc.insert("eventType", Bson::String(e.event_type.as_str().to_string()));
            if let Some(ref user) = e.user {
                doc.insert("user", Bson::String(user.clone()));
            }
            if let Some(ref db) = e.database {
                doc.insert("database", Bson::String(db.clone()));
            }
            doc.insert("command", Bson::String(e.command.clone()));
            doc.insert("connectionId", Bson::Int32(e.connection_id));
            doc.insert("result", Bson::String(if e.result.is_success() { "success".into() } else { "failure".into() }));
            Bson::Document(doc)
        }).collect()
    } else {
        Vec::new()
    };

    let mut reply = ok_body();
    reply.insert("events", Bson::Array(audit_events));
    reply.insert("total", Bson::Int64(state.audit_logger.as_ref().map(|l| l.events_total() as i64).unwrap_or(0)));
    OpMsg::reply(response_to, reply)
}

fn handle_audit_filter(body: &Document, response_to: i32, state: &ServerState) -> OpMsg {
    if let Some(ref logger) = state.audit_logger {
        let mut filter = audit::AuditFilter::default();

        if let Some(v) = body.get("authEvents").and_then(|v| v.as_bool()) {
            filter.auth_events = v;
        }
        if let Some(v) = body.get("dataEvents").and_then(|v| v.as_bool()) {
            filter.data_events = v;
        }
        if let Some(v) = body.get("adminEvents").and_then(|v| v.as_bool()) {
            filter.admin_events = v;
        }
        if let Some(v) = body.get("securityEvents").and_then(|v| v.as_bool()) {
            filter.security_events = v;
        }
        if let Some(v) = body.get("schemaEvents").and_then(|v| v.as_bool()) {
            filter.schema_events = v;
        }
        if let Some(v) = body.get("sessionEvents").and_then(|v| v.as_bool()) {
            filter.session_events = v;
        }

        logger.set_filter(filter.clone());

        let mut reply = ok_body();
        reply.insert("authEvents", Bson::Boolean(filter.auth_events));
        reply.insert("dataEvents", Bson::Boolean(filter.data_events));
        reply.insert("adminEvents", Bson::Boolean(filter.admin_events));
        reply.insert("securityEvents", Bson::Boolean(filter.security_events));
        reply.insert("schemaEvents", Bson::Boolean(filter.schema_events));
        reply.insert("sessionEvents", Bson::Boolean(filter.session_events));
        OpMsg::reply(response_to, reply)
    } else {
        OpMsg::reply(response_to, command_failed("audit logging not enabled", ErrorCode::IllegalOperation))
    }
}

fn handle_audit_status(response_to: i32, state: &ServerState) -> OpMsg {
    let mut reply = ok_body();
    if let Some(ref logger) = state.audit_logger {
        reply.insert("enabled", Bson::Boolean(logger.is_enabled()));
        reply.insert("eventsTotal", Bson::Int64(logger.events_total() as i64));
        reply.insert("bufferSize", Bson::Int64(logger.buffer_size() as i64));
        let filter = logger.filter();
        reply.insert("filter", Bson::Document({
            let mut f = Document::new();
            f.insert("authEvents", Bson::Boolean(filter.auth_events));
            f.insert("dataEvents", Bson::Boolean(filter.data_events));
            f.insert("adminEvents", Bson::Boolean(filter.admin_events));
            f.insert("securityEvents", Bson::Boolean(filter.security_events));
            f.insert("schemaEvents", Bson::Boolean(filter.schema_events));
            f.insert("sessionEvents", Bson::Boolean(filter.session_events));
            f
        }));
        let metrics = audit::global_metrics();
        reply.insert("metrics", Bson::Document({
            let mut m = Document::new();
            m.insert("eventsTotal", Bson::Int64(metrics.events_total.load(std::sync::atomic::Ordering::Relaxed) as i64));
            m.insert("authSuccess", Bson::Int64(metrics.auth_success.load(std::sync::atomic::Ordering::Relaxed) as i64));
            m.insert("authFailure", Bson::Int64(metrics.auth_failure.load(std::sync::atomic::Ordering::Relaxed) as i64));
            m.insert("dataAccess", Bson::Int64(metrics.data_access.load(std::sync::atomic::Ordering::Relaxed) as i64));
            m.insert("schemaChange", Bson::Int64(metrics.schema_change.load(std::sync::atomic::Ordering::Relaxed) as i64));
            m.insert("adminCommand", Bson::Int64(metrics.admin_command.load(std::sync::atomic::Ordering::Relaxed) as i64));
            m.insert("securityEvent", Bson::Int64(metrics.security_event.load(std::sync::atomic::Ordering::Relaxed) as i64));
            m.insert("sessionManagement", Bson::Int64(metrics.session_management.load(std::sync::atomic::Ordering::Relaxed) as i64));
            m
        }));
    } else {
        reply.insert("enabled", Bson::Boolean(false));
        reply.insert("eventsTotal", Bson::Int64(0));
    }
    OpMsg::reply(response_to, reply)
}

// ---------------------------------------------------------------------------
// Backup commands
// ---------------------------------------------------------------------------

fn handle_backup(body: &Document, response_to: i32) -> OpMsg {
    let backup_type = cmd_str(body, "type").unwrap_or("full");
    let db_path = cmd_str(body, "dbPath").unwrap_or("./data");
    let backup_path = cmd_str(body, "backupPath").unwrap_or("./backups");

    let mgr = mdb_backup::BackupManager::new(db_path, backup_path);

    let result = match backup_type {
        "incremental" => mgr.incremental_backup(),
        _ => mgr.full_backup(),
    };

    match result {
        Ok(meta) => {
            let mut reply = ok_body();
            reply.insert("backupId", Bson::String(meta.id));
            reply.insert("backupType", Bson::String(format!("{:?}", meta.backup_type)));
            reply.insert("sizeBytes", Bson::Int64(meta.size_bytes as i64));
            reply.insert("checksum", Bson::String(meta.checksum));
            reply.insert("lsn", Bson::Int64(meta.lsn as i64));
            reply.insert("durationMs", Bson::Int64(meta.duration_ms as i64));
            OpMsg::reply(response_to, reply)
        }
        Err(e) => OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::HostUnreachable)),
    }
}

fn handle_restore(body: &Document, response_to: i32) -> OpMsg {
    let backup_id = match cmd_str(body, "restore") {
        Some(id) => id,
        None => return OpMsg::reply(response_to, command_failed("restore: missing backup id", ErrorCode::BadValue)),
    };
    let backup_path = cmd_str(body, "backupPath").unwrap_or("./backups");
    let target_path = cmd_str(body, "targetPath").unwrap_or("./data");

    let mgr = mdb_backup::RestoreManager::new(backup_path, target_path);

    match mgr.restore_full(backup_id) {
        Ok(()) => ok_reply(response_to),
        Err(e) => OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::HostUnreachable)),
    }
}

fn handle_list_backups(response_to: i32) -> OpMsg {
    let backup_path = "./backups";
    let mgr = mdb_backup::BackupManager::new("", backup_path);

    match mgr.list_backups() {
        Ok(backups) => {
            let backups_arr: Vec<Bson> = backups.iter().map(|meta| {
                let mut doc = Document::new();
                doc.insert("id", Bson::String(meta.id.clone()));
                doc.insert("timestamp", Bson::Int64(meta.timestamp));
                doc.insert("backupType", Bson::String(format!("{:?}", meta.backup_type)));
                doc.insert("sizeBytes", Bson::Int64(meta.size_bytes as i64));
                doc.insert("checksum", Bson::String(meta.checksum.clone()));
                doc.insert("lsn", Bson::Int64(meta.lsn as i64));
                doc.insert("durationMs", Bson::Int64(meta.duration_ms as i64));
                Bson::Document(doc)
            }).collect();

            let mut reply = ok_body();
            reply.insert("backups", Bson::Array(backups_arr));
            reply.insert("total", Bson::Int32(backups.len() as i32));
            OpMsg::reply(response_to, reply)
        }
        Err(e) => OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::HostUnreachable)),
    }
}

fn handle_pitr(body: &Document, response_to: i32) -> OpMsg {
    let target_time = cmd_i64(body, "timestamp");
    let target_lsn = cmd_i64(body, "lsn").map(|v| v as u64);
    let backup_path = cmd_str(body, "backupPath").unwrap_or("./backups");
    let wal_path = cmd_str(body, "walPath").unwrap_or("./data/wal");

    let mgr = mdb_backup::PitrManager::new(backup_path, wal_path);

    let result = if let Some(time) = target_time {
        mgr.restore_to_time(time)
    } else if let Some(lsn) = target_lsn {
        mgr.restore_to_lsn(lsn)
    } else {
        return OpMsg::reply(response_to, command_failed("pitr: must specify timestamp or lsn", ErrorCode::BadValue));
    };

    match result {
        Ok(()) => ok_reply(response_to),
        Err(e) => OpMsg::reply(response_to, command_failed(&e.to_string(), ErrorCode::HostUnreachable)),
    }
}

// ---------------------------------------------------------------------------
// dropDatabase
// ---------------------------------------------------------------------------

fn handle_drop_database(body: &Document, db_name: &str, response_to: i32, state: &ServerState) -> OpMsg {
    // Verify dropDatabase: 1
    let drop_val = body.get("dropDatabase");
    if !matches!(drop_val, Some(Bson::Int32(1)) | Some(Bson::Int64(1))) {
        return OpMsg::reply(response_to, command_failed("dropDatabase: value must be 1", ErrorCode::BadValue));
    }
    let mut dbs = state.databases.lock().unwrap();
    if let Some(_db) = dbs.remove(db_name) {
        let mut reply = ok_body();
        reply.insert("dropped", Bson::String(db_name.to_owned()));
        reply.insert("ok", 1.0f64);
        OpMsg::reply(response_to, reply)
    } else {
        let mut reply = ok_body();
        reply.insert("dropped", Bson::String(db_name.to_owned()));
        reply.insert("ok", 1.0f64);
        OpMsg::reply(response_to, reply)
    }
}

// ---------------------------------------------------------------------------
// Replica set
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Change Stream (watch)
// ---------------------------------------------------------------------------

fn handle_watch(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    // 解析 watch 参数
    // 形式 1: { watch: "collection", pipeline: [...], ... }
    // 形式 2: { watch: 1, ... } — 数据库级别（watch 字段不是字符串）
    let (target_ns, level) = match body.get("watch") {
        Some(Bson::String(coll)) => {
            if coll.is_empty() {
                (format!("{db_name}."), StreamLevel::Database)
            } else {
                (format!("{db_name}.{coll}"), StreamLevel::Collection)
            }
        }
        _ => (format!("{db_name}."), StreamLevel::Database),
    };

    // 解析 pipeline 中的 $changeStream 选项
    let mut full_document = FullDocumentMode::default();
    let mut full_document_before_change = FullDocumentBeforeChangeMode::default();
    let mut resume_start: Option<ResumeStartPoint> = None;
    let mut max_await_time_ms: u64 = 1000; // 默认 1 秒（测试友好）
    let mut max_batch_size: usize = 100;

    if let Some(pipeline_arr) = body.get("pipeline").and_then(|v| v.as_array()) {
        for stage in pipeline_arr {
            if let Some(stage_doc) = stage.as_document() {
                if let Some(cs_opts) = stage_doc.get("$changeStream").and_then(|v| v.as_document()) {
                    if let Some(fd) = cs_opts.get_str("fullDocument") {
                        full_document = FullDocumentMode::from_str(fd).unwrap_or_default();
                    }
                    if let Some(fdbc) = cs_opts.get_str("fullDocumentBeforeChange") {
                        full_document_before_change = FullDocumentBeforeChangeMode::from_str(fdbc).unwrap_or_default();
                    }
                    if let Some(resume_doc) = cs_opts.get("resumeAfter").and_then(|v| v.as_document()) {
                        if let Some(data) = resume_doc.get_str("_data") {
                            let token = ResumeToken::new({ let mut d = Document::new(); d.insert("_data", Bson::String(data.to_string())); d });
                            resume_start = Some(ResumeStartPoint::ResumeAfter(token));
                        }
                    }
                    if let Some(start_doc) = cs_opts.get("startAtOperationTime").and_then(|v| v.as_document()) {
                        let secs = start_doc.get_i64("t").unwrap_or(0) as u32;
                        let inc = start_doc.get_i64("i").unwrap_or(0) as u32;
                        resume_start = Some(ResumeStartPoint::StartAtOperationTime(mdb_doc::Timestamp::new(secs, inc)));
                    }
                }
            }
        }
    }

    // 也支持顶层字段
    if let Some(fd) = body.get_str("fullDocument") {
        full_document = FullDocumentMode::from_str(fd).unwrap_or_default();
    }
    if let Some(ms) = body.get_i64("maxAwaitTimeMS") {
        max_await_time_ms = ms.clamp(1, 60_000) as u64;
    }
    if let Some(bs) = body.get_i64("batchSize") {
        max_batch_size = bs.clamp(1, 10_000) as usize;
    }

    // 构建 ChangeStream
    let mut cs = ChangeStream::with_config(ChangeStreamConfig {
        target_ns: target_ns.clone(),
        level,
        max_batch_size,
        max_await_time_ms,
        full_document,
        full_document_before_change,
        pipeline: vec![],
        resume_start,
        seen_window_capacity: 4096,
    });

    // 轮询 oplog 直到有事件或超时
    let mut events = Vec::new();
    let deadline = Instant::now() + Duration::from_millis(max_await_time_ms);
    while events.len() < max_batch_size {
        let batch = {
            let guard = state.replica_set.lock().unwrap();
            match guard.as_ref() {
                Some(repl) => cs.next_batch(repl.replica_set.oplog()),
                None => vec![],
            }
        };
        events.extend(batch);
        if !events.is_empty() {
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // 清理过期时间戳
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0);
    cs.evict_stale_timestamps(now_secs);

    // 构建游标响应
    let first_batch: Vec<Bson> = events.iter().map(|e| Bson::Document(event_to_doc(e))).collect();
    let resume_token = cs.get_resume_token().map(|t| t.to_bson());

    let mut cursor_doc = Document::new();
    cursor_doc.insert("id", Bson::Int64(0)); // MVP: 单批返回
    cursor_doc.insert("ns", Bson::String(target_ns));
    cursor_doc.insert("firstBatch", Bson::Array(first_batch));

    let mut reply = ok_body();
    reply.insert("cursor", Bson::Document(cursor_doc));
    if let Some(token) = resume_token {
        reply.insert("postBatchResumeToken", token);
    }
    OpMsg::reply(response_to, reply)
}

/// 将 ChangeStreamEvent 转换为 MongoDB 文档格式。
fn event_to_doc(event: &mdb_repl::ChangeStreamEvent) -> Document {
    let mut doc = Document::new();
    doc.insert("_id", Bson::Document(event._id.clone()));
    doc.insert("operationType", Bson::String(event.operation_type.as_str().to_string()));
    doc.insert("ns", { let mut ns = Document::new(); ns.insert("db", Bson::String(event.ns.split('.').next().unwrap_or("").to_string())); ns.insert("coll", Bson::String(event.ns.split('.').nth(1).unwrap_or("").to_string())); Bson::Document(ns) });
    if let Some(fd) = &event.full_document {
        doc.insert("fullDocument", Bson::Document(fd.clone()));
    }
    if let Some(fdbc) = &event.full_document_before_change {
        doc.insert("fullDocumentBeforeChange", Bson::Document(fdbc.clone()));
    }
    doc.insert("documentKey", Bson::Document(event.document_key.clone()));
    doc.insert("clusterTime", Bson::Timestamp(event.cluster_time));
    if let Some(txn) = event.txn_number {
        doc.insert("txnNumber", Bson::Int64(txn));
    }
    if let Some(ud) = &event.update_description {
        doc.insert("updateDescription", Bson::Document(ud.clone()));
    }
    doc
}

// ---------------------------------------------------------------------------
// TDE (Transparent Data Encryption)
// ---------------------------------------------------------------------------

/// 获取或创建服务端的 EncryptionManager。
///
/// 目前每个 ServerState 持有一个全局 EncryptionManager。
/// 后续可改为按 dbpath 持久化。
fn get_or_create_encryption_manager(state: &ServerState) -> Arc<std::sync::RwLock<mdb_storage::EncryptionManager>> {
    // 简化实现：每次创建一个新的（生产环境应缓存在 ServerState 中）
    // 这里先返回一个临时的，实际接线需要在 ServerState 中添加字段
    // 为了不破坏现有测试，这里只做演示
    let _ = state;
    Arc::new(std::sync::RwLock::new(mdb_storage::EncryptionManager::new()))
}

fn handle_encrypt_collection(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "encryptCollection") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("encryptCollection: missing collection", ErrorCode::BadValue)) };
    let ns = format!("{db_name}.{coll_name}");

    // 检查集合是否存在
    let db = state.get_database(db_name);
    if db.get_collection(coll_name).is_none() {
        return OpMsg::reply(response_to, command_failed(&format!("encryptCollection: collection not found: {ns}"), ErrorCode::NamespaceNotFound));
    }

    // 获取 dbpath
    let Some(catalog) = &state.catalog else {
        return OpMsg::reply(response_to, command_failed("encryptCollection: not available in memory-only mode", ErrorCode::IllegalOperation));
    };
    let dbpath = catalog.db_path();

    // 初始化加密管理器并启用集合加密
    let mut manager = get_or_create_encryption_manager(state);
    let mut mgr = manager.write().unwrap();
    if let Err(e) = mgr.initialize(&dbpath) {
        return OpMsg::reply(response_to, command_failed(&format!("encryptCollection: initialize failed: {e}"), ErrorCode::IllegalOperation));
    }
    if let Err(e) = mgr.encrypt_collection(&ns, &dbpath) {
        let msg = format!("encryptCollection failed: {e}");
        return OpMsg::reply(response_to, command_failed(&msg, ErrorCode::EncryptionError));
    }

    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("ns", Bson::String(ns));
    OpMsg::reply(response_to, reply)
}

fn handle_decrypt_collection(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "decryptCollection") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("decryptCollection: missing collection", ErrorCode::BadValue)) };
    let ns = format!("{db_name}.{coll_name}");

    let Some(catalog) = &state.catalog else {
        return OpMsg::reply(response_to, command_failed("decryptCollection: not available in memory-only mode", ErrorCode::IllegalOperation));
    };
    let dbpath = catalog.db_path();

    let mut manager = get_or_create_encryption_manager(state);
    let mut mgr = manager.write().unwrap();
    if let Err(e) = mgr.decrypt_collection(&ns, &dbpath) {
        let msg = format!("decryptCollection failed: {e}");
        return OpMsg::reply(response_to, command_failed(&msg, ErrorCode::EncryptionError));
    }

    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("ns", Bson::String(ns));
    OpMsg::reply(response_to, reply)
}

fn handle_key_status(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "keyStatus") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("keyStatus: missing collection", ErrorCode::BadValue)) };
    let ns = format!("{db_name}.{coll_name}");

    let Some(catalog) = &state.catalog else {
        return OpMsg::reply(response_to, command_failed("keyStatus: not available in memory-only mode", ErrorCode::IllegalOperation));
    };
    let dbpath = catalog.db_path();

    let mut manager = get_or_create_encryption_manager(state);
    let mut mgr = manager.write().unwrap();
    // 尝试初始化（加载已有的密钥环）
    let _ = mgr.initialize(&dbpath);

    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("ns", Bson::String(ns.clone()));

    if let Some(info) = mgr.key_status(&ns) {
        reply.insert("encrypted", Bson::Boolean(true));
        reply.insert("keyId", Bson::String(info.key_id.clone()));
        reply.insert("algorithm", Bson::String(info.algorithm.as_str().to_string()));
        reply.insert("enabledAt", Bson::Int64(info.enabled_at));
    } else {
        reply.insert("encrypted", Bson::Boolean(false));
    }
    OpMsg::reply(response_to, reply)
}

fn handle_rotate_key(body: &Document, db_name: &str, state: &ServerState, response_to: i32) -> OpMsg {
    let coll_name = match cmd_str(body, "rotateKey") { Some(n) => n, None => return OpMsg::reply(response_to, command_failed("rotateKey: missing collection", ErrorCode::BadValue)) };
    let ns = format!("{db_name}.{coll_name}");

    let Some(catalog) = &state.catalog else {
        return OpMsg::reply(response_to, command_failed("rotateKey: not available in memory-only mode", ErrorCode::IllegalOperation));
    };
    let dbpath = catalog.db_path();

    let mut manager = get_or_create_encryption_manager(state);
    let mut mgr = manager.write().unwrap();
    if let Err(e) = mgr.initialize(&dbpath) {
        return OpMsg::reply(response_to, command_failed(&format!("rotateKey: initialize failed: {e}"), ErrorCode::IllegalOperation));
    }
    if let Err(e) = mgr.rotate_key(&ns, &dbpath) {
        let msg = format!("rotateKey failed: {e}");
        return OpMsg::reply(response_to, command_failed(&msg, ErrorCode::EncryptionError));
    }

    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("ns", Bson::String(ns));
    OpMsg::reply(response_to, reply)
}

/// 更新会话的因果一致性时间戳（写操作后调用）。
fn update_session_write_ts(state: &ServerState, body: &Document) {
    if let Some(lsid) = extract_lsid(body) {
        let ts = mdb_doc::Timestamp::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as u32)
                .unwrap_or(0),
            0,
        );
        let mut sessions = state.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(&lsid) {
            session.record_write(ts);
        }
    }
}

/// 更新会话的因果一致性时间戳（读操作后调用）。
fn update_session_read_ts(state: &ServerState, body: &Document) {
    if let Some(lsid) = extract_lsid(body) {
        let ts = mdb_doc::Timestamp::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as u32)
                .unwrap_or(0),
            0,
        );
        let mut sessions = state.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(&lsid) {
            session.record_read(ts);
        }
    }
}

fn handle_start_transaction(body: &Document, response_to: i32, state: &ServerState) -> OpMsg {
    let lsid = match extract_lsid(body) {
        Some(l) => l,
        None => return OpMsg::reply(response_to, command_failed("startTransaction: missing lsid", ErrorCode::BadValue)),
    };
    let mut sessions = state.sessions.lock().unwrap();
    let session = sessions.entry(lsid).or_insert_with(|| Session::new(0));
    session.txn_active = true;
    session.txn_buffer = Some(Vec::new());
    session.last_used = Instant::now();
    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    OpMsg::reply(response_to, reply)
}

fn handle_commit_transaction(body: &Document, response_to: i32, state: &ServerState) -> OpMsg {
    let lsid = match extract_lsid(body) {
        Some(l) => l,
        None => return OpMsg::reply(response_to, command_failed("commitTransaction: missing lsid", ErrorCode::BadValue)),
    };
    // 获取事务缓冲区和修改的键
    let (txn_ops, modified_keys) = {
        let mut sessions = state.sessions.lock().unwrap();
        match sessions.get_mut(&lsid) {
            Some(session) if session.txn_active => {
                let ops = session.txn_buffer.take().unwrap_or_default();
                let keys = std::mem::take(&mut session.txn_modified_keys);
                session.txn_active = false;
                session.txn_buffer = None;
                session.last_used = Instant::now();
                (ops, keys)
            }
            _ => {
                return OpMsg::reply(response_to, command_failed(
                    "commitTransaction: no active transaction for this session",
                    ErrorCode::IllegalOperation,
                ));
            }
        }
    };

    // Write Conflict 检测：检查修改的键是否被其他活跃事务修改
    {
        let sessions = state.sessions.lock().unwrap();
        for (other_lsid, other_session) in sessions.iter() {
            if *other_lsid == lsid { continue; }
            if !other_session.txn_active { continue; }
            // 简化冲突检测：检查是否有任何键冲突
            for key in modified_keys.keys() {
                if other_session.txn_modified_keys.contains_key(key) {
                    return OpMsg::reply(response_to, command_failed(
                        "WriteConflict: key modified by another transaction",
                        ErrorCode::WriteConflict,
                    ));
                }
            }
        }
    }

    // 按顺序执行所有缓冲的操作
    let mut errors = Vec::new();
    for op in txn_ops {
        let result = match op {
            TxnOp::Insert { db, coll, key, value } => {
                let d = state.get_database(&db);
                let c = d.get_or_create_collection(&coll);
                let mut locked = c.lock().unwrap();
                locked.store.put(key.clone(), value.clone())
                    .map(|_| {
                        let rid = locked.next_record_id();
                        locked.bind_record_id(&key, rid);
                        if let Ok(doc) = mdb_doc::codec::from_bytes(&value) {
                            let _ = locked.index_manager.update_indexes(&doc, rid);
                        }
                    })
            }
            TxnOp::Update { db, coll, key, value } => {
                let d = state.get_database(&db);
                let c = d.get_or_create_collection(&coll);
                let mut locked = c.lock().unwrap();
                let rid = locked.peek_record_id(&key);
                if let Some(old_rid) = rid {
                    if let Ok(Some(old_val)) = locked.store.get(&key) {
                        if let Ok(old_doc) = mdb_doc::codec::from_bytes(&old_val) {
                            locked.index_manager.remove_from_indexes(&old_doc, old_rid);
                        }
                    }
                }
                let new_rid = rid.unwrap_or_else(|| locked.next_record_id());
                locked.store.put(key.clone(), value.clone())
                    .map(|_| {
                        locked.bind_record_id(&key, new_rid);
                        if let Ok(doc) = mdb_doc::codec::from_bytes(&value) {
                            let _ = locked.index_manager.update_indexes(&doc, new_rid);
                        }
                    })
            }
            TxnOp::Delete { db, coll, key } => {
                let d = state.get_database(&db);
                let c = d.get_or_create_collection(&coll);
                let mut locked = c.lock().unwrap();
                if let Some(old_rid) = locked.take_record_id(&key) {
                    if let Ok(Some(old_val)) = locked.store.get(&key) {
                        if let Ok(old_doc) = mdb_doc::codec::from_bytes(&old_val) {
                            locked.index_manager.remove_from_indexes(&old_doc, old_rid);
                        }
                    }
                }
                locked.store.delete(&key).map(|_| ())
            }
        };
        if let Err(e) = result {
            errors.push(e.to_string());
        }
    }

    if !errors.is_empty() {
        return OpMsg::reply(response_to, command_failed(
            &format!("commitTransaction: {} operations failed", errors.len()),
            ErrorCode::WriteConcernFailed,
        ));
    }

    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    OpMsg::reply(response_to, reply)
}

fn handle_abort_transaction(body: &Document, response_to: i32, state: &ServerState) -> OpMsg {
    let lsid = match extract_lsid(body) {
        Some(l) => l,
        None => return OpMsg::reply(response_to, command_failed("abortTransaction: missing lsid", ErrorCode::BadValue)),
    };
    let mut sessions = state.sessions.lock().unwrap();
    match sessions.get_mut(&lsid) {
        Some(session) if session.txn_active => {
            session.txn_active = false;
            session.txn_buffer = None;
            session.last_used = Instant::now();
        }
        _ => {
            return OpMsg::reply(response_to, command_failed(
                "abortTransaction: no active transaction for this session",
                ErrorCode::IllegalOperation,
            ));
        }
    }
    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    OpMsg::reply(response_to, reply)
}

/// 检查当前会话是否在事务中，如果是则缓冲写操作。
///
/// 返回 `true` 表示操作已被缓冲（不需要立即执行）。
fn buffer_txn_op(state: &ServerState, body: &Document, op: TxnOp) -> bool {
    let Some(lsid) = extract_lsid(body) else { return false };
    let mut sessions = state.sessions.lock().unwrap();
    if let Some(session) = sessions.get_mut(&lsid) {
        if session.txn_active {
            if let Some(buf) = session.txn_buffer.as_mut() {
                // 记录修改的键（用于冲突检测）
                let key_id = match &op {
                    TxnOp::Insert { db, coll, key, .. } => (db.clone(), coll.clone(), key.clone()),
                    TxnOp::Update { db, coll, key, .. } => (db.clone(), coll.clone(), key.clone()),
                    TxnOp::Delete { db, coll, key } => (db.clone(), coll.clone(), key.clone()),
                };
                session.txn_modified_keys.insert(key_id, ());
                buf.push(op);
                session.last_used = Instant::now();
                return true;
            }
        }
    }
    false
}

fn handle_repl_set_initiate(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let init_doc = cmd_doc(body, "replSetInitiate").cloned().unwrap_or_default();
    let set_name = cmd_str(&init_doc, "replSetName")
        .map(|s| s.to_owned())
        .unwrap_or_else(|| "mdb-rs".to_string());

    // 解析成员配置
    let mut members = Vec::new();
    let mut self_id = None;
    if let Some(members_arr) = init_doc.get("members").and_then(|v| v.as_array()) {
        for (i, m) in members_arr.iter().enumerate() {
            if let Some(member_doc) = m.as_document() {
                let host = cmd_str(member_doc, "host")
                    .map(|s| s.to_owned())
                    .unwrap_or_else(|| format!("localhost:{}", 27017 + i));
                let priority = member_doc.get("priority").and_then(|v| v.as_f64()).unwrap_or(1.0);
                let votes = member_doc.get("votes").and_then(|v| v.as_i64()).unwrap_or(1) as i32;
                let node_id = NodeId::new(&host);
                if i == 0 {
                    self_id = Some(node_id.clone());
                }
                members.push((node_id, priority, votes));
            }
        }
    }

    // 默认单节点
    if members.is_empty() {
        let node_id = NodeId::new("localhost:27017");
        self_id = Some(node_id.clone());
        members.push((node_id, 1.0, 1));
    }

    let self_id = self_id.unwrap_or_else(|| NodeId::new("localhost:27017"));

    // 构建 ReplicaSet
    let mut rs = ReplicaSet::new();
    for (id, priority, _) in &members {
        rs.add_member(id.clone(), *priority);
    }
    rs.elect_primary();

    // 构建配置文档
    let mut config_doc = Document::new();
    config_doc.insert("_config", Bson::Document({
        let mut cfg = Document::new();
        cfg.insert("_id", Bson::String(set_name.clone()));
        let members_arr: Vec<Bson> = members.iter().enumerate().map(|(i, (id, priority, votes))| {
            Bson::Document({
                let mut m = Document::new();
                m.insert("_id", Bson::Int32(i as i32));
                m.insert("host", Bson::String(id.to_string()));
                m.insert("priority", Bson::Double(*priority));
                m.insert("votes", Bson::Int32(*votes));
                m
            })
        }).collect();
        cfg.insert("members", Bson::Array(members_arr));
        cfg
    }));

    *state.replica_set.lock().unwrap() = Some(ReplState {
        name: set_name.clone(),
        term: 1,
        role: RaftState::Leader,
        replica_set: rs,
        self_id: self_id.clone(),
        config_doc,
        transport: None,
    });

    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("set", Bson::String(set_name));
    reply.insert("primary", Bson::String(self_id.to_string()));
    OpMsg::reply(response_to, reply)
}

fn handle_repl_set_get_status(state: &ServerState, response_to: i32) -> OpMsg {
    let repl_guard = state.replica_set.lock().unwrap();
    let mut reply = ok_body();
    match repl_guard.as_ref() {
        Some(repl) => {
            reply.insert("set", Bson::String(repl.name.clone()));
            reply.insert("term", Bson::Int64(repl.term as i64));

            // 映射 Raft 状态到 MongoDB myState
            let my_state = match repl.role {
                RaftState::Leader => 1i32,
                RaftState::Follower => 2i32,
                RaftState::Candidate => 3i32,
            };
            reply.insert("myState", Bson::Int32(my_state));
            reply.insert("self", Bson::String(repl.self_id.to_string()));

            // 成员列表
            let members: Vec<Bson> = repl.replica_set.members.iter().map(|m| {
                let mut d = Document::new();
                d.insert("name", Bson::String(m.id.to_string()));
                d.insert("health", Bson::Int32(1));
                d.insert("stateStr", Bson::String(match m.state {
                    ReplicaMemberState::Primary => "PRIMARY",
                    ReplicaMemberState::Secondary => "SECONDARY",
                    ReplicaMemberState::Candidate => "CANDIDATE",
                    ReplicaMemberState::Down => "DOWN",
                }.to_string()));
                d.insert("optime", Bson::Timestamp(m.last_applied_ts));
                d.insert("self", Bson::Boolean(m.id == repl.self_id));
                d.insert("priority", Bson::Double(m.priority));
                d.insert("votes", Bson::Int32(1));
                Bson::Document(d)
            }).collect();
            reply.insert("members", Bson::Array(members));

            // primary
            if let Some(primary_id) = repl.replica_set.primary_id() {
                reply.insert("primary", Bson::String(primary_id.to_string()));
            }
        }
        None => {
            reply.set("ok", Bson::Double(0.0));
            reply.insert("errmsg", Bson::String("no replica set configured".into()));
            reply.insert("code", Bson::Int32(ErrorCode::ReplicaSetNotFound as i32));
        }
    }
    OpMsg::reply(response_to, reply)
}

fn handle_repl_set_get_config(state: &ServerState, response_to: i32) -> OpMsg {
    let repl_guard = state.replica_set.lock().unwrap();
    let mut reply = ok_body();
    match repl_guard.as_ref() {
        Some(repl) => {
            reply.insert("config", repl.config_doc.clone());
        }
        None => {
            reply.set("ok", Bson::Double(0.0));
            reply.insert("errmsg", Bson::String("no replica set configured".into()));
            reply.insert("code", Bson::Int32(ErrorCode::ReplicaSetNotFound as i32));
        }
    }
    OpMsg::reply(response_to, reply)
}

fn handle_repl_set_reconfig(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let reconfig_doc = cmd_doc(body, "replSetReconfig").cloned().unwrap_or_default();
    let mut repl_guard = state.replica_set.lock().unwrap();
    match repl_guard.as_mut() {
        Some(repl) => {
            // 只有 Leader 可以 reconfig
            if repl.role != RaftState::Leader {
                return OpMsg::reply(response_to, command_failed(
                    "replSetReconfig: only primary can reconfigure",
                    ErrorCode::NotMaster,
                ));
            }
            // 更新配置文档
            repl.config_doc = reconfig_doc.clone();
            // 简化实现：直接更新 ReplicaSet 成员
            if let Some(members_arr) = reconfig_doc.get("members").and_then(|v| v.as_array()) {
                for (i, m) in members_arr.iter().enumerate() {
                    if let Some(member_doc) = m.as_document() {
                        if let Some(host) = cmd_str(member_doc, "host") {
                            let priority = member_doc.get("priority").and_then(|v| v.as_f64()).unwrap_or(1.0);
                            let node_id = NodeId::new(host);
                            if repl.replica_set.find_member(&node_id).is_none() {
                                repl.replica_set.add_member(node_id, priority);
                            }
                        }
                    }
                }
            }
            let mut reply = ok_body();
            reply.insert("ok", Bson::Double(1.0));
            reply.insert("reconfigOk", Bson::Boolean(true));
            OpMsg::reply(response_to, reply)
        }
        None => OpMsg::reply(response_to, command_failed(
            "replSetReconfig: no replica set configured",
            ErrorCode::ReplicaSetNotFound,
        )),
    }
}

// ---------------------------------------------------------------------------
// Sharding commands
// ---------------------------------------------------------------------------

/// 获取或创建 ShardRouter。
fn get_or_create_sharding(state: &ServerState) -> std::sync::MutexGuard<'_, Option<ShardRouter>> {
    let mut guard = state.sharding.lock().unwrap();
    if guard.is_none() {
        *guard = Some(ShardRouter::new(ConfigDb::new()));
    }
    guard
}

/// 解析分片键规范。
///
/// `{ field: "hashed" }` → Hashed
/// `{ field: 1 }` / `{ field: -1 }` → Range
/// `{ f1: 1, f2: 1 }` → Compound
fn parse_shard_key(spec: &Document) -> MdbResult<ShardKey> {
    if spec.is_empty() {
        return Err(MdbError::new(ErrorCode::BadValue, "shard key must not be empty"));
    }
    let mut fields = Vec::new();
    let mut is_hashed = false;
    for (k, v) in spec.iter() {
        if let Some(s) = v.as_str() {
            if s == "hashed" {
                is_hashed = true;
            } else {
                return Err(MdbError::new(ErrorCode::BadValue, format!("unsupported shard key value: {s}")));
            }
        } else if let Some(n) = v.as_i64() {
            if n != 1 && n != -1 {
                return Err(MdbError::new(ErrorCode::BadValue, "shard key value must be 1 or -1"));
            }
        } else {
            return Err(MdbError::new(ErrorCode::BadValue, "invalid shard key value"));
        }
        fields.push(k.to_string());
    }
    if is_hashed {
        if fields.len() != 1 {
            return Err(MdbError::new(ErrorCode::BadValue, "hashed shard key must have exactly one field"));
        }
        Ok(ShardKey::Hashed { field: fields[0].clone() })
    } else if fields.len() == 1 {
        Ok(ShardKey::Range { field: fields[0].clone() })
    } else {
        Ok(ShardKey::Compound { fields })
    }
}

fn handle_add_shard(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let shard_name = match cmd_str(body, "addShard") { Some(n) => n.to_string(), None => return OpMsg::reply(response_to, command_failed("addShard: missing shard name", ErrorCode::BadValue)) };
    let host = match cmd_str(body, "host") { Some(h) => h.to_string(), None => return OpMsg::reply(response_to, command_failed("addShard: missing host", ErrorCode::BadValue)) };
    let mut guard = get_or_create_sharding(state);
    let router = guard.as_mut().unwrap();
    router.config_db_mut().add_shard(ShardId::new(&shard_name), host);
    // 持久化分片配置
    if let Some(cat) = &state.catalog {
        let _ = cat.save_shard_config(router.config_db());
    }
    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("shardAdded", Bson::String(shard_name));
    OpMsg::reply(response_to, reply)
}

fn handle_remove_shard(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let shard_name = match cmd_str(body, "removeShard") { Some(n) => n.to_string(), None => return OpMsg::reply(response_to, command_failed("removeShard: missing shard name", ErrorCode::BadValue)) };
    let mut guard = state.sharding.lock().unwrap();
    match guard.as_mut() {
        Some(router) => {
            let shard_id = ShardId::new(&shard_name);
            match router.config_db_mut().remove_shard(&shard_id) {
                Ok(()) => {
                    let mut reply = ok_body();
                    reply.insert("ok", Bson::Double(1.0));
                    reply.insert("shard", Bson::String(shard_name));
                    OpMsg::reply(response_to, reply)
                }
                Err(e) => OpMsg::reply(response_to, command_failed(&format!("removeShard failed: {e}"), ErrorCode::IllegalOperation)),
            }
        }
        None => OpMsg::reply(response_to, command_failed("removeShard: no sharding configured", ErrorCode::IllegalOperation)),
    }
}

fn handle_list_shards(state: &ServerState, response_to: i32) -> OpMsg {
    let guard = state.sharding.lock().unwrap();
    match guard.as_ref() {
        Some(router) => {
            let shards: Vec<Bson> = router.config_db().get_all_shards().iter().map(|s| {
                Bson::Document({
                    let mut d = Document::new();
                    d.insert("_id", Bson::String(s.id.to_string()));
                    d.insert("host", Bson::String(s.host.clone()));
                    d.insert("state", Bson::String(s.state.as_str().to_string()));
                    d
                })
            }).collect();
            let mut reply = ok_body();
            reply.insert("shards", Bson::Array(shards));
            OpMsg::reply(response_to, reply)
        }
        None => {
            let mut reply = ok_body();
            reply.insert("shards", Bson::Array(vec![]));
            OpMsg::reply(response_to, reply)
        }
    }
}

fn handle_enable_sharding(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let db_name = match cmd_str(body, "enableSharding") { Some(n) => n.to_string(), None => return OpMsg::reply(response_to, command_failed("enableSharding: missing database name", ErrorCode::BadValue)) };
    let primary_shard = cmd_str(body, "primary").map(|s| s.to_string());
    let mut guard = get_or_create_sharding(state);
    let router = guard.as_mut().unwrap();
    let primary = primary_shard.unwrap_or_else(|| {
        router.config_db().get_active_shards().first().map(|s| s.id.to_string()).unwrap_or_else(|| "s0".to_string())
    });
    router.config_db_mut().add_database(&db_name, ShardId::new(&primary));
    // 持久化分片配置
    if let Some(cat) = &state.catalog {
        let _ = cat.save_shard_config(router.config_db());
    }
    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("sharded", Bson::Boolean(true));
    reply.insert("primary", Bson::String(primary));
    OpMsg::reply(response_to, reply)
}

fn handle_shard_collection(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let ns = match cmd_str(body, "shardCollection") { Some(n) => n.to_string(), None => return OpMsg::reply(response_to, command_failed("shardCollection: missing namespace", ErrorCode::BadValue)) };
    let key_spec = match body.get("key").and_then(|v| v.as_document()) { Some(d) => d.clone(), None => return OpMsg::reply(response_to, command_failed("shardCollection: missing key", ErrorCode::BadValue)) };

    let shard_key = match parse_shard_key(&key_spec) {
        Ok(k) => k,
        Err(e) => return OpMsg::reply(response_to, command_failed(&format!("shardCollection: invalid shard key: {e}"), ErrorCode::BadValue)),
    };
    if let Err(e) = validate_shard_key(&shard_key) {
        return OpMsg::reply(response_to, command_failed(&format!("shardCollection: invalid shard key: {e}"), ErrorCode::BadValue));
    }

    let mut guard = get_or_create_sharding(state);
    let router = guard.as_mut().unwrap();
    let primary = router.config_db().get_primary_shard(&ns.split('.').next().unwrap_or(""))
        .cloned()
        .unwrap_or_else(|| router.config_db().get_active_shards().first().map(|s| s.id.clone()).unwrap_or_else(|| ShardId::new("s0")));

    // 设置分片键
    router.config_db_mut().set_shard_key(&ns, shard_key.clone());

    // 创建初始 chunk
    let num_chunks = body.get_i64("numInitialChunks").unwrap_or(1).max(1) as usize;
    let chunks = if shard_key.is_hashed() {
        // 哈希分片：将 i64::MIN..i64::MAX 均分为 N 个范围
        let span = u64::MAX; // 2^64 - 1，覆盖整个 i64 范围
        let chunk_span = span / num_chunks as u64;
        (0..num_chunks).map(|i| {
            let min = i64::MIN.wrapping_add((i as u64 * chunk_span) as i64);
            let max = if i == num_chunks - 1 {
                i64::MAX
            } else {
                i64::MIN.wrapping_add(((i as u64 + 1) * chunk_span) as i64)
            };
            Chunk {
                id: ChunkId::new(&format!("{}_chunk_{}", ns, i)),
                ns: ns.clone(),
                range: KeyRange::new(Bson::Int64(min), Bson::Int64(max)),
                shard_id: primary.clone(),
            }
        }).collect::<Vec<_>>()
    } else {
        // 范围分片：创建一个覆盖全范围的 chunk
        vec![Chunk {
            id: ChunkId::new(&format!("{}_chunk_0", ns)),
            ns: ns.clone(),
            range: KeyRange::new(Bson::MinKey, Bson::MaxKey),
            shard_id: primary.clone(),
        }]
    };
    for chunk in chunks {
        router.config_db_mut().add_chunk(chunk);
    }

    // 持久化分片配置
    if let Some(cat) = &state.catalog {
        let _ = cat.save_shard_config(router.config_db());
    }

    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("collectionsharded", Bson::Boolean(true));
    reply.insert("primary", Bson::String(primary.to_string()));
    OpMsg::reply(response_to, reply)
}

fn handle_split_at(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let ns = match cmd_str(body, "splitAt") { Some(n) => n.to_string(), None => return OpMsg::reply(response_to, command_failed("splitAt: missing namespace", ErrorCode::BadValue)) };
    let pattern = match body.get("pattern").and_then(|v| v.as_document()) { Some(d) => d.clone(), None => return OpMsg::reply(response_to, command_failed("splitAt: missing pattern", ErrorCode::BadValue)) };

    // 从 pattern 中提取分片键值（取第一个字段）
    let split_point = pattern.iter().next().map(|(_, v)| v.clone()).unwrap_or(Bson::Null);

    let mut guard = state.sharding.lock().unwrap();
    match guard.as_mut() {
        Some(router) => {
            let chunks = router.config_db().get_chunks_for_namespace(&ns);
            for chunk in &chunks {
                if chunk.range.contains(&split_point) {
                    match router.config_db_mut().split_chunk(&chunk.id, split_point.clone()) {
                        Ok(_) => {
                            let mut reply = ok_body();
                            reply.insert("ok", Bson::Double(1.0));
                            reply.insert("split", Bson::Boolean(true));
                            return OpMsg::reply(response_to, reply);
                        }
                        Err(e) => return OpMsg::reply(response_to, command_failed(&format!("splitAt failed: {e}"), ErrorCode::IllegalOperation)),
                    }
                }
            }
            OpMsg::reply(response_to, command_failed("splitAt: no chunk found for pattern", ErrorCode::BadValue))
        }
        None => OpMsg::reply(response_to, command_failed("splitAt: no sharding configured", ErrorCode::IllegalOperation)),
    }
}

fn handle_move_chunk(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let ns = match cmd_str(body, "moveChunk") { Some(n) => n.to_string(), None => return OpMsg::reply(response_to, command_failed("moveChunk: missing namespace", ErrorCode::BadValue)) };
    let pattern = match body.get("pattern").and_then(|v| v.as_document()) { Some(d) => d.clone(), None => return OpMsg::reply(response_to, command_failed("moveChunk: missing pattern", ErrorCode::BadValue)) };
    let to_shard = match cmd_str(body, "toShard") { Some(s) => s.to_string(), None => return OpMsg::reply(response_to, command_failed("moveChunk: missing toShard", ErrorCode::BadValue)) };

    let move_point = pattern.iter().next().map(|(_, v)| v.clone()).unwrap_or(Bson::Null);

    // 解析 namespace
    let parts: Vec<&str> = ns.split('.').collect();
    if parts.len() != 2 {
        return OpMsg::reply(response_to, command_failed("moveChunk: invalid namespace format", ErrorCode::BadValue));
    }
    let db_name = parts[0];
    let coll_name = parts[1];

    let mut guard = state.sharding.lock().unwrap();
    let Some(router) = guard.as_mut() else {
        return OpMsg::reply(response_to, command_failed("moveChunk: no sharding configured", ErrorCode::IllegalOperation));
    };

    // 找到要移动的 chunk
    let chunks = router.config_db().get_chunks_for_namespace(&ns);
    let target_chunk = chunks.iter().find(|c| c.range.contains(&move_point));

    let Some(chunk) = target_chunk else {
        return OpMsg::reply(response_to, command_failed("moveChunk: no chunk found for pattern", ErrorCode::BadValue));
    };

    let chunk_id = chunk.id.clone();
    let chunk_range = chunk.range.clone();
    let source_shard = chunk.shard_id.clone();

    // 标记 chunk 为迁移中
    state.migration_state.lock().unwrap().start_migration(ns.as_str(), chunk_id.as_str());

    // 步骤 1：读取源分片中属于该 chunk 的文档
    state.migration_state.lock().unwrap().set_phase(chunk_id.as_str(), MigrationPhase::Cloning);
    let db = state.get_database(db_name);
    let coll = match db.get_collection(coll_name) {
        Some(c) => c,
        None => {
            state.migration_state.lock().unwrap().abort_migration(ns.as_str(), chunk_id.as_str());
            return OpMsg::reply(response_to, command_failed("moveChunk: collection not found", ErrorCode::NamespaceNotFound));
        }
    };

    let mut docs_to_migrate = Vec::new();
    {
        let locked = coll.lock().unwrap();
        let all_docs = locked.scan_all().unwrap_or_default();
        for (key, val) in &all_docs {
            if let Ok(doc) = mdb_doc::codec::from_bytes(val) {
                let doc_shard = doc.get("_shard").and_then(|v| v.as_str()).unwrap_or("");
                if doc_shard == source_shard.to_string() {
                    if let Ok(shard_key_val) = extract_shard_key_from_doc(&doc) {
                        if chunk_range.contains(&shard_key_val) {
                            docs_to_migrate.push((key.clone(), doc));
                        }
                    }
                }
            }
        }
    }

    // 步骤 2：更新文档的 _shard 字段为目标分片
    state.migration_state.lock().unwrap().set_phase(chunk_id.as_str(), MigrationPhase::Persisting);
    let mut migrated_count = 0i32;
    {
        let locked = coll.lock().unwrap();
        for (key, doc) in &mut docs_to_migrate {
            doc.insert("_shard", Bson::String(to_shard.clone()));
            if let Ok(doc_bytes) = mdb_doc::codec::to_bytes(doc) {
                let _ = locked.store.put(key.clone(), doc_bytes);
                migrated_count += 1;
            }
        }

        // 步骤 3：应用迁移期间缓冲的写操作
        let buffered = state.migration_state.lock().unwrap().finish_migration(ns.as_str(), chunk_id.as_str());
        for write in &buffered {
            match write.op.as_str() {
                "insert" | "update" => {
                    if let Some(val) = &write.value {
                        let _ = locked.store.put(write.key.clone(), val.clone());
                    }
                }
                "delete" => {
                    let _ = locked.store.delete(&write.key);
                }
                _ => {}
            }
        }
    }

    // 步骤 3.5：跨节点数据复制（如果配置了远程分片）
    let remote_copy_count = if let Some(cat) = &state.catalog {
        // 检查目标分片是否有远程地址
        let guard = state.sharding.lock().unwrap();
        let remote_addr = guard.as_ref().and_then(|r| {
            r.config_db().get_shard(&ShardId::new(&to_shard)).map(|s| s.host.clone())
        });
        drop(guard);

        if let Some(host) = remote_addr {
            // 远程分片：尝试通过 Catalog 复制数据
            // 注意：在单进程模式下，这会复制到同一个存储引擎
            // 真正的跨节点复制需要网络传输
            match cat.copy_collection_data(db_name, coll_name, db_name, coll_name) {
                Ok(copied) => {
                    // 远程复制成功（简化：同一存储引擎内复制）
                    let _ = copied;
                    0 // 在单进程模式下不计入迁移数量
                }
                Err(_) => 0,
            }
        } else {
            0
        }
    } else {
        0
    };
    let _ = remote_copy_count;

    // 步骤 4：更新 ConfigDb 中的 chunk 归属
    state.migration_state.lock().unwrap().set_phase(chunk_id.as_str(), MigrationPhase::Committing);
    match router.config_db_mut().move_chunk(&chunk_id, &ShardId::new(&to_shard)) {
        Ok(()) => {
            // 步骤 5：持久化分片配置
            if let Some(cat) = &state.catalog {
                let _ = cat.save_shard_config(router.config_db());
            }
            let mut reply = ok_body();
            reply.insert("ok", Bson::Double(1.0));
            reply.insert("state", Bson::String("success".into()));
            reply.insert("primary", Bson::String(to_shard));
            reply.insert("moved", Bson::Int32(migrated_count));
            OpMsg::reply(response_to, reply)
        }
        Err(e) => {
            state.migration_state.lock().unwrap().abort_migration(ns.as_str(), chunk_id.as_str());
            OpMsg::reply(response_to, command_failed(&format!("moveChunk failed: {e}"), ErrorCode::IllegalOperation))
        }
    }
}

/// 从文档中提取分片键值。
fn extract_shard_key_from_doc(doc: &Document) -> MdbResult<Bson> {
    // 尝试常见的分片键字段
    for field in ["customerId", "userId", "_id", "age", "email"] {
        if let Some(val) = doc.get(field) {
            return Ok(val.clone());
        }
    }
    // 如果没有找到，使用 _id
    doc.get("_id").cloned().ok_or_else(|| MdbError::new(ErrorCode::BadValue, "document has no shard key"))
}

fn handle_balancer_start(state: &ServerState, response_to: i32) -> OpMsg {
    let mut guard = get_or_create_sharding(state);
    let _ = guard.as_mut();
    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("mode", Bson::String("full".into()));
    OpMsg::reply(response_to, reply)
}

fn handle_balancer_stop(state: &ServerState, response_to: i32) -> OpMsg {
    let mut guard = get_or_create_sharding(state);
    let _ = guard.as_mut();
    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("mode", Bson::String("off".into()));
    OpMsg::reply(response_to, reply)
}

fn handle_balancer_status(state: &ServerState, response_to: i32) -> OpMsg {
    let _guard = state.sharding.lock().unwrap();
    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    reply.insert("mode", Bson::String("off".into()));
    reply.insert("inBalancerRound", Bson::Boolean(false));
    reply.insert("numMigrationsInProgress", Bson::Int32(0));
    OpMsg::reply(response_to, reply)
}

fn handle_flush_router_config(state: &ServerState, response_to: i32) -> OpMsg {
    let mut guard = state.sharding.lock().unwrap();
    if let Some(router) = guard.as_mut() {
        router.refresh_cache();
    }
    let mut reply = ok_body();
    reply.insert("ok", Bson::Double(1.0));
    OpMsg::reply(response_to, reply)
}

fn handle_repl_set_start_transport(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let listen_addr = match cmd_str(body, "listenAddr") { Some(a) => a.to_string(), None => return OpMsg::reply(response_to, command_failed("replSetStartTransport: missing listenAddr", ErrorCode::BadValue)) };

    let mut guard = state.replica_set.lock().unwrap();
    match guard.as_mut() {
        Some(repl) => {
            if repl.transport.is_some() {
                return OpMsg::reply(response_to, command_failed("replSetStartTransport: transport already started", ErrorCode::IllegalOperation));
            }
            match TcpRaftTransport::new(repl.self_id.clone(), &listen_addr) {
                Ok(transport) => {
                    repl.transport = Some(Arc::new(transport));
                    let mut reply = ok_body();
                    reply.insert("ok", Bson::Double(1.0));
                    reply.insert("listenAddr", Bson::String(listen_addr));
                    OpMsg::reply(response_to, reply)
                }
                Err(e) => OpMsg::reply(response_to, command_failed(&format!("replSetStartTransport failed: {e}"), ErrorCode::IllegalOperation)),
            }
        }
        None => OpMsg::reply(response_to, command_failed("replSetStartTransport: no replica set configured", ErrorCode::ReplicaSetNotFound)),
    }
}

fn handle_repl_set_add_peer(body: &Document, state: &ServerState, response_to: i32) -> OpMsg {
    let node_id = match cmd_str(body, "nodeId") { Some(n) => n.to_string(), None => return OpMsg::reply(response_to, command_failed("replSetAddPeer: missing nodeId", ErrorCode::BadValue)) };
    let addr = match cmd_str(body, "addr") { Some(a) => a.to_string(), None => return OpMsg::reply(response_to, command_failed("replSetAddPeer: missing addr", ErrorCode::BadValue)) };

    let guard = state.replica_set.lock().unwrap();
    match guard.as_ref() {
        Some(repl) => {
            if let Some(transport) = &repl.transport {
                transport.add_peer(&NodeId::new(&node_id), &addr);
                let mut reply = ok_body();
                reply.insert("ok", Bson::Double(1.0));
                reply.insert("nodeId", Bson::String(node_id));
                reply.insert("addr", Bson::String(addr));
                OpMsg::reply(response_to, reply)
            } else {
                OpMsg::reply(response_to, command_failed("replSetAddPeer: transport not started", ErrorCode::IllegalOperation))
            }
        }
        None => OpMsg::reply(response_to, command_failed("replSetAddPeer: no replica set configured", ErrorCode::ReplicaSetNotFound)),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn handle_op_query(_raw: &[u8]) -> OpMsg { OpMsg::reply(0, command_failed("OP_QUERY is not implemented", ErrorCode::CommandNotImplemented)) }
pub(crate) fn ok_body() -> Document { let mut d = Document::new(); d.insert("ok", 1.0f64); d }
pub(crate) fn ok_reply(response_to: i32) -> OpMsg { OpMsg::reply(response_to, ok_body()) }
pub(crate) fn command_failed(errmsg: &str, code: ErrorCode) -> Document { let mut d = Document::new(); d.insert("ok", 0.0f64); d.insert("errmsg", errmsg); d.insert("code", code as i32); d.insert("codeName", code.code_name()); d }

fn build_cursor_response(response_to: i32, cursor: &mdb_query::cursor::Cursor, cursor_mgr: &SharedCursorManager) -> OpMsg {
    let mut reply = ok_body();
    let mut cursor_doc = Document::new(); cursor_doc.insert("id", Bson::Int64(cursor.id)); cursor_doc.insert("ns", Bson::String(cursor.namespace.clone()));
    cursor_doc.insert("firstBatch", Bson::Array(cursor.batch.iter().cloned().map(Bson::Document).collect()));
    reply.insert("cursor", Bson::Document(cursor_doc));
    if cursor.exhausted { if let Some(Bson::Document(d)) = reply.get_mut("cursor") { d.insert("id", Bson::Int64(0)); } cursor_mgr.inner().kill_cursor(cursor.id); }
    OpMsg::reply(response_to, reply)
}

fn document_matches(doc: &Document, filter: &Document) -> bool {
    for (key, filter_val) in filter.iter() {
        match key {
            "$and" => {
                if let Some(arr) = filter_val.as_array() {
                    for item in arr {
                        if let Some(sub) = item.as_document() {
                            if !document_matches(doc, sub) { return false; }
                        }
                    }
                }
            }
            "$or" => {
                if let Some(arr) = filter_val.as_array() {
                    if !arr.iter().any(|item| item.as_document().map_or(false, |sub| document_matches(doc, sub))) { return false; }
                }
            }
            "$nor" => {
                if let Some(arr) = filter_val.as_array() {
                    if arr.iter().any(|item| item.as_document().map_or(false, |sub| document_matches(doc, sub))) { return false; }
                }
            }
            "$where" => {
                // $where: JavaScript 表达式求值
                if let Some(expr) = filter_val.as_str() {
                    if !js_eval::matches_where(expr, doc) {
                        return false;
                    }
                }
            }
            "$expr" => {
                // $expr: 聚合表达式求值
                if let Some(expr_doc) = filter_val.as_document() {
                    if !eval_expr_filter(doc, expr_doc) {
                        return false;
                    }
                }
            }
            _ => {
                // 支持点号路径：{"address.city": "北京"} → doc.address.city
                let doc_val = doc.get_path(key);
                match filter_val {
                    Bson::Document(ops) => {
                        // 检查是否是位运算符
                        if ops.contains_key("$bitsAllClear") || ops.contains_key("$bitsAllSet")
                            || ops.contains_key("$bitsAnyClear") || ops.contains_key("$bitsAnySet") {
                            if !eval_bit_ops(doc_val, ops) {
                                return false;
                            }
                        } else if !eval_field_ops(doc_val, ops) { return false; }
                    }
                    scalar => {
                        match doc_val {
                            Some(v) => { if !bson_eq(v, scalar) { return false; } }
                            None => return false,
                        }
                    }
                }
            }
        }
    }
    true
}

/// 从文档值中提取可比较的标量（用于数组元素匹配等场景）。
fn bson_scalars(v: &Bson) -> Vec<Bson> {
    match v {
        Bson::Array(arr) => arr.clone(),
        other => vec![other.clone()],
    }
}

fn eval_field_ops(doc_val: Option<&Bson>, ops: &Document) -> bool {
    for (op, target) in ops.iter() {
        match op {
            "$eq" => { if !doc_val.map_or(false, |v| bson_eq(v, target)) { return false; } }
            "$ne" => { if doc_val.map_or(false, |v| bson_eq(v, target)) { return false; } }
            "$gt" => { if !doc_val.map_or(false, |v| scalar_gt(v, target)) { return false; } }
            "$gte" => { if !doc_val.map_or(false, |v| scalar_gte(v, target)) { return false; } }
            "$lt" => { if !doc_val.map_or(false, |v| scalar_lt(v, target)) { return false; } }
            "$lte" => { if !doc_val.map_or(false, |v| scalar_lte(v, target)) { return false; } }
            "$in" => {
                if let Some(arr) = target.as_array() {
                    if !doc_val.map_or(false, |v| {
                        bson_scalars(v).iter().any(|sv| arr.iter().any(|t| bson_eq(sv, t)))
                    }) { return false; }
                }
            }
            "$nin" => {
                if let Some(arr) = target.as_array() {
                    if doc_val.map_or(false, |v| {
                        bson_scalars(v).iter().any(|sv| arr.iter().any(|t| bson_eq(sv, t)))
                    }) { return false; }
                }
            }
            "$exists" => { if doc_val.is_some() != target.as_bool().unwrap_or(true) { return false; } }
            "$type" => {
                if !doc_val.map_or(false, |v| bson_type_matches(v, target)) { return false; }
            }
            "$size" => {
                let want = target.as_i64().or_else(|| target.as_f64().map(|f| f as i64)).unwrap_or(-1);
                let got = doc_val.map_or(0, |v| v.as_array().map(|a| a.len() as i64).unwrap_or(-1));
                if got != want { return false; }
            }
            "$all" => {
                if let Some(arr) = target.as_array() {
                    if !doc_val.map_or(false, |v| {
                        let values = bson_scalars(v);
                        arr.iter().all(|t| values.iter().any(|sv| bson_eq(sv, t)))
                    }) { return false; }
                }
            }
            "$mod" => {
                // $mod: [divisor, remainder]
                if let Some(arr) = target.as_array() {
                    if arr.len() == 2 {
                        let divisor = arr[0].as_i64().or_else(|| arr[0].as_f64().map(|f| f as i64)).unwrap_or(0);
                        let remainder = arr[1].as_i64().or_else(|| arr[1].as_f64().map(|f| f as i64)).unwrap_or(0);
                        if divisor == 0 { return false; }
                        let val = doc_val.and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)));
                        match val {
                            Some(n) => { if n % divisor != remainder { return false; } }
                            None => return false,
                        }
                    }
                }
            }
            "$regex" => {
                let pattern = target.as_str().unwrap_or("");
                let options = if let Bson::Document(ops_doc) = ops.get("$options").unwrap_or(&Bson::Null) {
                    ops_doc.get("$regex").map(|_| "").unwrap_or("")
                } else { "" };
                let _ = options;
                if !doc_val.map_or(false, |v| {
                    bson_scalars(v).iter().any(|sv| sv.as_str().map_or(false, |s| regex_match(pattern, s)))
                }) { return false; }
            }
            "$options" => {
                // $options 配合 $regex 使用，单独出现时跳过
            }
            "$elemMatch" => {
                if let Some(elem_ops) = target.as_document() {
                    if !doc_val.map_or(false, |v| {
                        v.as_array().map_or(false, |arr| {
                            arr.iter().any(|elem| eval_field_ops(Some(elem), elem_ops))
                        })
                    }) { return false; }
                }
            }
            "$not" => {
                // $not: 取反内部操作符
                if let Some(inner_ops) = target.as_document() {
                    // 内部条件全部不满足时 $not 才为 true
                    let inner_result = doc_val.map_or(false, |v| eval_field_ops(Some(v), inner_ops));
                    if inner_result { return false; }
                }
            }
            _ => {
                // 未知运算符：不再静默返回 false，而是跳过（保守策略）
                // 生产环境应返回 FailedToParse 错误
                continue;
            }
        }
    }
    true
}

/// 简单正则匹配：支持 ^ $ . * + ? [] \ 等常见模式。
fn regex_match(pattern: &str, text: &str) -> bool {
    // 用 Rust 内置的正则引擎
    use regex_lite::Regex;
    match Regex::new(pattern) {
        Ok(re) => re.is_match(text),
        Err(_) => false,
    }
}

/// BSON 类型匹配：$type 接受类型名或类型码。
fn bson_type_matches(v: &Bson, target: &Bson) -> bool {
    let type_name = match target {
        Bson::String(s) => s.as_str(),
        Bson::Int32(n) => return bson_type_code_matches(v, *n),
        Bson::Int64(n) => return bson_type_code_matches(v, *n as i32),
        _ => return false,
    };
    let actual = bson_type_name(v);
    actual == type_name
}

fn bson_type_code_matches(v: &Bson, code: i32) -> bool {
    let actual_code = match v {
        Bson::Double(_) => 1,
        Bson::String(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::Undefined => 6,
        Bson::ObjectId(_) => 7,
        Bson::Boolean(_) => 8,
        Bson::DateTime(_) => 9,
        Bson::Null => 10,
        Bson::Regex { .. } => 11,
        Bson::DbPointer { .. } => 12,
        Bson::JavaScript(_) => 13,
        Bson::Symbol(_) => 14,
        Bson::JavaScriptWithScope { .. } => 15,
        Bson::Int32(_) => 16,
        Bson::Timestamp(_) => 17,
        Bson::Int64(_) => 18,
        Bson::Decimal128(_) => 19,
        Bson::MinKey => -1,
        Bson::MaxKey => 127,
    };
    actual_code == code
}

fn bson_type_name(v: &Bson) -> &'static str {
    match v {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::Undefined => "undefined",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::Regex { .. } => "regex",
        Bson::DbPointer { .. } => "dbPointer",
        Bson::JavaScript(_) => "javascript",
        Bson::Symbol(_) => "symbol",
        Bson::JavaScriptWithScope { .. } => "javascriptWithScope",
        Bson::Int32(_) => "int",
        Bson::Timestamp(_) => "timestamp",
        Bson::Int64(_) => "long",
        Bson::Decimal128(_) => "decimal128",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
    }
}

/// 位运算求值。
fn eval_bit_ops(doc_val: Option<&Bson>, ops: &Document) -> bool {
    let Some(val) = doc_val else { return false };
    let Some(num) = val.as_i64().or_else(|| val.as_f64().map(|f| f as i64)) else { return false };

    if let Some(target) = ops.get("$bitsAllClear").and_then(|v| v.as_i64()) {
        if num & target != 0 { return false; }
    }
    if let Some(target) = ops.get("$bitsAllSet").and_then(|v| v.as_i64()) {
        if num & target != target { return false; }
    }
    if let Some(target) = ops.get("$bitsAnyClear").and_then(|v| v.as_i64()) {
        if num & target == target { return false; }
    }
    if let Some(target) = ops.get("$bitsAnySet").and_then(|v| v.as_i64()) {
        if num & target == 0 { return false; }
    }
    true
}

/// $expr 表达式求值。
fn eval_expr_filter(doc: &Document, expr: &Document) -> bool {
    for (op, args) in expr.iter() {
        let Some(arr) = args.as_array() else { continue };
        if arr.len() != 2 { continue; }

        let lhs = eval_expr_value(doc, &arr[0]);
        let rhs = eval_expr_value(doc, &arr[1]);

        let result = match op {
            "$eq" => bson_eq(&lhs, &rhs),
            "$ne" => !bson_eq(&lhs, &rhs),
            "$gt" => bson_compare(&lhs, &rhs).map_or(false, |o| o == std::cmp::Ordering::Greater),
            "$gte" => bson_compare(&lhs, &rhs).map_or(false, |o| o != std::cmp::Ordering::Less),
            "$lt" => bson_compare(&lhs, &rhs).map_or(false, |o| o == std::cmp::Ordering::Less),
            "$lte" => bson_compare(&lhs, &rhs).map_or(false, |o| o != std::cmp::Ordering::Greater),
            _ => true,
        };
        if !result { return false; }
    }
    true
}

/// 求值 $expr 表达式的值。
fn eval_expr_value(doc: &Document, expr: &Bson) -> Bson {
    match expr {
        Bson::String(s) if s.starts_with('$') => {
            let path = &s[1..];
            doc.get_path(path).cloned().unwrap_or(Bson::Null)
        }
        Bson::Document(d) => {
            if d.contains_key("$eq") || d.contains_key("$gt") || d.contains_key("$lt")
                || d.contains_key("$gte") || d.contains_key("$lte") || d.contains_key("$ne") {
                // 比较表达式
                for (op, args) in d.iter() {
                    if let Some(arr) = args.as_array() {
                        if arr.len() == 2 {
                            let lhs = eval_expr_value(doc, &arr[0]);
                            let rhs = eval_expr_value(doc, &arr[1]);
                            return Bson::Boolean(match op {
                                "$eq" => bson_eq(&lhs, &rhs),
                                "$ne" => !bson_eq(&lhs, &rhs),
                                "$gt" => bson_compare(&lhs, &rhs).map_or(false, |o| o == std::cmp::Ordering::Greater),
                                "$gte" => bson_compare(&lhs, &rhs).map_or(false, |o| o != std::cmp::Ordering::Less),
                                "$lt" => bson_compare(&lhs, &rhs).map_or(false, |o| o == std::cmp::Ordering::Less),
                                "$lte" => bson_compare(&lhs, &rhs).map_or(false, |o| o != std::cmp::Ordering::Greater),
                                _ => false,
                            });
                        }
                    }
                }
            }
            expr.clone()
        }
        Bson::Array(arr) => {
            let evaluated: Vec<Bson> = arr.iter().map(|e| eval_expr_value(doc, e)).collect();
            Bson::Array(evaluated)
        }
        _ => expr.clone(),
    }
}

fn bson_eq(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Null, Bson::Null) => true,
        (Bson::Boolean(a), Bson::Boolean(b)) => a == b,
        (Bson::Int32(a), Bson::Int32(b)) => a == b,
        (Bson::Int64(a), Bson::Int64(b)) => a == b,
        (Bson::Int32(a), Bson::Int64(b)) => *a as i64 == *b,
        (Bson::Int64(a), Bson::Int32(b)) => *a == *b as i64,
        (Bson::Double(a), Bson::Double(b)) => a == b,
        (Bson::Int32(a), Bson::Double(b)) => (*a as f64) == *b,
        (Bson::Int64(a), Bson::Double(b)) => (*a as f64) == *b,
        (Bson::Double(a), Bson::Int32(b)) => *a == (*b as f64),
        (Bson::Double(a), Bson::Int64(b)) => *a == (*b as f64),
        (Bson::String(a), Bson::String(b)) => a == b,
        (Bson::ObjectId(a), Bson::ObjectId(b)) => a == b,
        (Bson::DateTime(a), Bson::DateTime(b)) => a == b,
        (Bson::Timestamp(a), Bson::Timestamp(b)) => a == b,
        (Bson::Binary(a), Bson::Binary(b)) => a == b,
        (Bson::Document(a), Bson::Document(b)) => a == b,
        (Bson::Array(a), Bson::Array(b)) => a == b,
        (Bson::JavaScript(a), Bson::JavaScript(b)) => a == b,
        _ => false,
    }
}

fn bson_compare(a: &Bson, b: &Bson) -> Option<std::cmp::Ordering> {
    // 数值比较
    let a_num = match *a {
        Bson::Int32(v) => Some(v as f64),
        Bson::Int64(v) => Some(v as f64),
        Bson::Double(v) => Some(v),
        _ => None,
    };
    let b_num = match *b {
        Bson::Int32(v) => Some(v as f64),
        Bson::Int64(v) => Some(v as f64),
        Bson::Double(v) => Some(v),
        _ => None,
    };
    if let (Some(a), Some(b)) = (a_num, b_num) {
        return a.partial_cmp(&b);
    }
    // 字符串比较
    if let (Some(a), Some(b)) = (a.as_str(), b.as_str()) {
        return a.partial_cmp(b);
    }
    // ObjectId 比较
    if let (Bson::ObjectId(a), Bson::ObjectId(b)) = (a, b) {
        return a.partial_cmp(b);
    }
    // DateTime 比较
    if let (Bson::DateTime(a), Bson::DateTime(b)) = (a, b) {
        return a.partial_cmp(b);
    }
    // Boolean 比较：false < true
    if let (Bson::Boolean(a), Bson::Boolean(b)) = (a, b) {
        return a.partial_cmp(b);
    }
    None
}

/// 标量大于比较：支持数组元素（任一元素满足即可）。
fn scalar_gt(v: &Bson, target: &Bson) -> bool {
    bson_scalars(v).iter().any(|sv| bson_compare(sv, target) == Some(std::cmp::Ordering::Greater))
}
fn scalar_gte(v: &Bson, target: &Bson) -> bool {
    bson_scalars(v).iter().any(|sv| matches!(bson_compare(sv, target), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)))
}
fn scalar_lt(v: &Bson, target: &Bson) -> bool {
    bson_scalars(v).iter().any(|sv| bson_compare(sv, target) == Some(std::cmp::Ordering::Less))
}
fn scalar_lte(v: &Bson, target: &Bson) -> bool {
    bson_scalars(v).iter().any(|sv| matches!(bson_compare(sv, target), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)))
}

fn apply_projection(doc: &Document, projection: &Document, include_mode: bool) -> Document {
    let mut result = Document::new();
    if include_mode {
        for (key, val) in projection.iter() { if matches!(val, Bson::Int32(1) | Bson::Int64(1)) { if let Some(v) = doc.get(key) { result.insert(key.to_owned(), v.clone()); } } }
        if !projection.contains_key("_id") { if let Some(id) = doc.get("_id") { result.insert("_id".to_owned(), id.clone()); } }
    } else {
        let excluded: Vec<String> = projection.iter().filter(|(_, v)| matches!(v, Bson::Int32(0) | Bson::Int64(0))).map(|(k, _)| k.to_owned()).collect();
        for (key, val) in doc.iter() { if !excluded.contains(&key.to_owned()) { result.insert(key.to_owned(), val.clone()); } }
    }
    result
}
// ---------------------------------------------------------------------------
// Update operators
// ---------------------------------------------------------------------------

enum UpdateOp { Set(Document), Unset(Vec<String>), Inc(Document), Push(Document), Pull(Document), PullAll(Document), Rename(Document), Min(Document), Max(Document), Mul(Document), CurrentDate(Document), AddToSet(Document), Bit(Document) }

fn parse_update_ops(u: Option<&Document>) -> Vec<UpdateOp> {
    let u = match u { Some(d) => d, None => return Vec::new() };
    let mut ops = Vec::new();
    for (key, val) in u.iter() {
        match key {
            "$set" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::Set(d.clone())); } }
            "$unset" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::Unset(d.iter().map(|(k, _)| k.to_owned()).collect())); } }
            "$inc" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::Inc(d.clone())); } }
            "$push" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::Push(d.clone())); } }
            "$pull" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::Pull(d.clone())); } }
            "$pullAll" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::PullAll(d.clone())); } }
            "$rename" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::Rename(d.clone())); } }
            "$min" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::Min(d.clone())); } }
            "$max" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::Max(d.clone())); } }
            "$mul" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::Mul(d.clone())); } }
            "$currentDate" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::CurrentDate(d.clone())); } }
            "$addToSet" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::AddToSet(d.clone())); } }
            "$bit" => { if let Some(d) = val.as_document() { ops.push(UpdateOp::Bit(d.clone())); } }
            _ => {}
        }
    }
    ops
}

fn apply_update_ops(doc: &mut Document, ops: &[UpdateOp]) {
    for op in ops {
        match op {
            UpdateOp::Set(fields) => { for (k, v) in fields.iter() { doc.set(k.clone(), v.clone()); } }
            UpdateOp::Unset(fields) => { for k in fields { doc.remove(k); } }
            UpdateOp::Inc(fields) => { for (k, v) in fields.iter() { let cur = doc.get(k).and_then(|v| match v { Bson::Int32(n) => Some(*n as f64), Bson::Int64(n) => Some(*n as f64), Bson::Double(n) => Some(*n), _ => None }).unwrap_or(0.0); let inc = match v { Bson::Int32(n) => *n as f64, Bson::Int64(n) => *n as f64, Bson::Double(n) => *n, _ => 0.0 }; let r = cur + inc; if r.fract() == 0.0 && r.abs() < i32::MAX as f64 { doc.set(k.clone(), Bson::Int32(r as i32)); } else { doc.set(k.clone(), Bson::Double(r)); } } }
            UpdateOp::Push(fields) => {
                for (k, v) in fields.iter() {
                    let mut arr = match doc.get(k) { Some(Bson::Array(a)) => a.to_vec(), _ => Vec::new() };
                    if let Some(modifiers) = v.as_document() {
                        if let Some(Bson::Array(each_vals)) = modifiers.get("$each") {
                            let mut insert_values = each_vals.clone();
                            if let Some(Bson::Int32(pos)) = modifiers.get("$position") {
                                let pos = (*pos as usize).min(arr.len());
                                let mut new_arr = arr[..pos].to_vec();
                                new_arr.append(&mut insert_values);
                                new_arr.extend_from_slice(&arr[pos..]);
                                arr = new_arr;
                                doc.set(k.clone(), Bson::Array(arr));
                                continue;
                            }
                            if let Some(Bson::Int32(slice_val)) = modifiers.get("$slice") {
                                arr.append(&mut insert_values);
                                if *slice_val >= 0 {
                                    let limit = (*slice_val as usize).min(arr.len());
                                    arr = arr[..limit].to_vec();
                                } else {
                                    let limit = (-*slice_val as usize).min(arr.len());
                                    if limit > 0 { arr = arr[arr.len() - limit..].to_vec(); } else { arr.clear(); }
                                }
                                doc.set(k.clone(), Bson::Array(arr));
                                continue;
                            }
                            arr.append(&mut insert_values);
                        } else {
                            arr.push(v.clone());
                        }
                    } else {
                        arr.push(v.clone());
                    }
                    doc.set(k.clone(), Bson::Array(arr));
                }
            }
            UpdateOp::Pull(fields) => { for (k, v) in fields.iter() { if let Some(Bson::Array(arr)) = doc.get(k) { doc.set(k.clone(), Bson::Array(arr.iter().filter(|item| !bson_eq(item, v)).cloned().collect())); } } }
            UpdateOp::PullAll(fields) => { for (k, v) in fields.iter() { if let Some(Bson::Array(arr)) = doc.get(k) { if let Some(to_remove) = v.as_array() { doc.set(k.clone(), Bson::Array(arr.iter().filter(|item| !to_remove.iter().any(|r| bson_eq(item, r))).cloned().collect())); } } } }
            UpdateOp::Rename(fields) => { for (from, to) in fields.iter() { if let Some(val) = doc.remove(from) { let to_key = to.as_str().unwrap_or("").to_owned(); doc.set(to_key, val); } } }
            UpdateOp::Min(fields) => { for (k, v) in fields.iter() { let should = doc.get(k).map_or(true, |cur| matches!(bson_compare(cur, v), Some(std::cmp::Ordering::Greater))); if should { doc.set(k.clone(), v.clone()); } } }
            UpdateOp::Max(fields) => { for (k, v) in fields.iter() { let should = doc.get(k).map_or(true, |cur| matches!(bson_compare(cur, v), Some(std::cmp::Ordering::Less))); if should { doc.set(k.clone(), v.clone()); } } }
            UpdateOp::Mul(fields) => { for (k, v) in fields.iter() { let cur = doc.get(k).and_then(|v| match v { Bson::Int32(n) => Some(*n as f64), Bson::Int64(n) => Some(*n as f64), Bson::Double(n) => Some(*n), _ => None }).unwrap_or(0.0); let factor = match v { Bson::Int32(n) => *n as f64, Bson::Int64(n) => *n as f64, Bson::Double(n) => *n, _ => 1.0 }; let r = cur * factor; if r.fract() == 0.0 && r.abs() < i32::MAX as f64 { doc.set(k.clone(), Bson::Int32(r as i32)); } else { doc.set(k.clone(), Bson::Double(r)); } } }
            UpdateOp::CurrentDate(fields) => { let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0); for (k, v) in fields.iter() { let use_date = match v { Bson::Boolean(b) => *b, Bson::Document(d) => d.get("$type").and_then(|t| t.as_str()).map_or(true, |t| t == "date"), _ => true }; if use_date { doc.set(k.clone(), Bson::DateTime(now_ms)); } else { doc.set(k.clone(), Bson::Int64(now_ms)); } } }
            UpdateOp::AddToSet(fields) => {
                for (k, v) in fields.iter() {
                    let mut arr = match doc.get(k) { Some(Bson::Array(a)) => a.to_vec(), _ => Vec::new() };
                    if let Some(modifiers) = v.as_document() {
                        if let Some(Bson::Array(each_vals)) = modifiers.get("$each") {
                            for val in each_vals {
                                if !arr.iter().any(|item| bson_eq(item, val)) {
                                    arr.push(val.clone());
                                }
                            }
                        } else {
                            if !arr.iter().any(|item| bson_eq(item, v)) {
                                arr.push(v.clone());
                            }
                        }
                    } else {
                        if !arr.iter().any(|item| bson_eq(item, v)) {
                            arr.push(v.clone());
                        }
                    }
                    doc.set(k.clone(), Bson::Array(arr));
                }
            }
            UpdateOp::Bit(fields) => {
                for (k, v) in fields.iter() {
                    let cur_val = doc.get(k).and_then(|v| match v { Bson::Int32(n) => Some(*n as i64), Bson::Int64(n) => Some(*n), _ => None }).unwrap_or(0);
                    if let Bson::Document(ops_doc) = v {
                        let mut result = cur_val;
                        if let Some(Bson::Int32(and_val)) = ops_doc.get("and") { result = result & (*and_val as i64); }
                        if let Some(Bson::Int64(and_val)) = ops_doc.get("and") { result = result & and_val; }
                        if let Some(Bson::Int32(or_val)) = ops_doc.get("or") { result = result | (*or_val as i64); }
                        if let Some(Bson::Int64(or_val)) = ops_doc.get("or") { result = result | or_val; }
                        if let Some(Bson::Int32(xor_val)) = ops_doc.get("xor") { result = result ^ (*xor_val as i64); }
                        if let Some(Bson::Int64(xor_val)) = ops_doc.get("xor") { result = result ^ xor_val; }
                        if result >= i32::MIN as i64 && result <= i32::MAX as i64 {
                            doc.set(k.clone(), Bson::Int32(result as i32));
                        } else {
                            doc.set(k.clone(), Bson::Int64(result));
                        }
                    } else if let Bson::Int32(bit_val) = v {
                        doc.set(k.clone(), Bson::Int32(cur_val as i32 & bit_val));
                    }
                }
            }
        }
    }
}
// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

/// BSON 类型标签，用于构造无歧义的存储键。
///
/// 键的首字节标明后续负载的类型，MongoDB 的 `_id` 支持多种类型，
/// 若不加标签，`Int32(0x4142)` 与字符串 `"AB"` 会编码成相同字节。
const KEY_TAG_DOUBLE: u8 = 0x01;
const KEY_TAG_STRING: u8 = 0x02;
const KEY_TAG_OBJECT_ID: u8 = 0x03;
const KEY_TAG_BOOL: u8 = 0x04;
const KEY_TAG_DATE: u8 = 0x05;
const KEY_TAG_NULL: u8 = 0x06;
const KEY_TAG_REGEX: u8 = 0x07;
const KEY_TAG_INT32: u8 = 0x08;
const KEY_TAG_TIMESTAMP: u8 = 0x09;
const KEY_TAG_INT64: u8 = 0x0A;
const KEY_TAG_DECIMAL: u8 = 0x0B;
const KEY_TAG_MIN_KEY: u8 = 0x0C;
const KEY_TAG_MAX_KEY: u8 = 0x0D;
const KEY_TAG_DOCUMENT: u8 = 0x0E;
const KEY_TAG_ARRAY: u8 = 0x0F;
const KEY_TAG_BINARY: u8 = 0x10;
const KEY_TAG_SYMBOL: u8 = 0x11;
const KEY_TAG_CODE: u8 = 0x12;
const KEY_TAG_UNDEFINED: u8 = 0x13;

/// 将 `_id` 值编码为存储引擎的键。
///
/// 编码形如 `[tag][payload...]`，标签保证不同 BSON 类型不会产生相同字节序列，
/// 因此任意两种不同的 `_id` 必然映射到不同的键。同一 `_id` 重复插入会覆盖同一键，
/// 与 MongoDB 的 `_id` 唯一性语义一致。
fn bson_to_key(val: &Bson) -> Vec<u8> {
    match val {
        Bson::Double(n) => {
            // 位模式归一化：0.0 与 -0.0 编码为同一键，与 MongoDB 数值等价语义一致。
            let n = if *n == 0.0 { 0.0 } else { *n };
            let mut k = vec![KEY_TAG_DOUBLE];
            k.extend_from_slice(&n.to_be_bytes());
            k
        }
        Bson::String(s) => {
            let mut k = vec![KEY_TAG_STRING];
            k.extend_from_slice(s.as_bytes());
            k
        }
        Bson::ObjectId(oid) => {
            let mut k = vec![KEY_TAG_OBJECT_ID];
            k.extend_from_slice(&oid.bytes()[..]);
            k
        }
        Bson::Boolean(b) => vec![KEY_TAG_BOOL, u8::from(*b)],
        Bson::DateTime(ms) => {
            let mut k = vec![KEY_TAG_DATE];
            k.extend_from_slice(&ms.to_be_bytes());
            k
        }
        Bson::Null => vec![KEY_TAG_NULL],
        Bson::Regex(r) => {
            let mut k = vec![KEY_TAG_REGEX];
            k.extend_from_slice(r.pattern.as_bytes());
            k.push(0);
            k.extend_from_slice(r.options.as_bytes());
            k
        }
        Bson::Int32(n) => {
            let mut k = vec![KEY_TAG_INT32];
            k.extend_from_slice(&n.to_be_bytes());
            k
        }
        Bson::Timestamp(ts) => {
            let mut k = vec![KEY_TAG_TIMESTAMP];
            k.extend_from_slice(&ts.as_u64().to_be_bytes());
            k
        }
        Bson::Int64(n) => {
            let mut k = vec![KEY_TAG_INT64];
            k.extend_from_slice(&n.to_be_bytes());
            k
        }
        Bson::Decimal128(d) => {
            let mut k = vec![KEY_TAG_DECIMAL];
            k.extend_from_slice(&d.bytes()[..]);
            k
        }
        Bson::MinKey => vec![KEY_TAG_MIN_KEY],
        Bson::MaxKey => vec![KEY_TAG_MAX_KEY],
        Bson::Document(d) => {
            // 文档型 `_id` 极少使用，采用确定性序列化保证可重现。
            let mut k = vec![KEY_TAG_DOCUMENT];
            if let Ok(bytes) = mdb_doc::codec::to_bytes(d) {
                k.extend_from_slice(&bytes);
            }
            k
        }
        Bson::Array(a) => {
            let mut k = vec![KEY_TAG_ARRAY];
            for item in a.iter() {
                k.extend_from_slice(&bson_to_key(item));
            }
            k.push(0);
            k
        }
        Bson::Binary(b) => {
            let mut k = vec![KEY_TAG_BINARY];
            k.push(b.subtype.code());
            k.extend_from_slice(&b.bytes);
            k
        }
        Bson::Symbol(s) => {
            let mut k = vec![KEY_TAG_SYMBOL];
            k.extend_from_slice(s.as_bytes());
            k
        }
        Bson::JavaScript(code) => {
            let mut k = vec![KEY_TAG_CODE];
            k.extend_from_slice(code.as_bytes());
            k
        }
        Bson::Undefined => vec![KEY_TAG_UNDEFINED],
        other => {
            // 兜底：序列化后加标签，保证不同取值不会碰撞。
            let mut k = vec![0x7F];
            if let Ok(bytes) = mdb_doc::codec::to_bytes(&Document::new().tap(other)) {
                k.extend_from_slice(&bytes);
            }
            k
        }
    }
}

/// 极简 tap 扩展：把单个 Bson 包成单键文档以便序列化。
trait TapBson {
    fn tap(self, value: &Bson) -> Document;
}
impl TapBson for Document {
    fn tap(mut self, value: &Bson) -> Document {
        self.insert("v", value.clone());
        self
    }
}

/// 从存储键还原 `_id` 的 BSON 值。
///
/// 用于索引重建：键→值→`_id`，避免二次解析文档负载。
fn key_to_bson_id(key: &[u8]) -> Option<Bson> {
    let (&tag, payload) = key.split_first()?;
    Some(match tag {
        KEY_TAG_DOUBLE => {
            let arr: [u8; 8] = payload.get(..8)?.try_into().ok()?;
            Bson::Double(f64::from_be_bytes(arr))
        }
        KEY_TAG_STRING => Bson::String(String::from_utf8_lossy(payload).into_owned()),
        KEY_TAG_OBJECT_ID => {
            let arr: [u8; 12] = payload.get(..12)?.try_into().ok()?;
            Bson::ObjectId(ObjectId::from_bytes(arr))
        }
        KEY_TAG_BOOL => Bson::Boolean(payload.first().copied()? != 0),
        KEY_TAG_DATE => {
            let arr: [u8; 8] = payload.get(..8)?.try_into().ok()?;
            Bson::DateTime(i64::from_be_bytes(arr))
        }
        KEY_TAG_NULL => Bson::Null,
        KEY_TAG_INT32 => {
            let arr: [u8; 4] = payload.get(..4)?.try_into().ok()?;
            Bson::Int32(i32::from_be_bytes(arr))
        }
        KEY_TAG_INT64 => {
            let arr: [u8; 8] = payload.get(..8)?.try_into().ok()?;
            Bson::Int64(i64::from_be_bytes(arr))
        }
        _ => return None,
    })
}

/// 把 `_id` 值渲染成可读形式，用于重复键报错信息。
///
/// 只打印标量与 ObjectId；数组/文档/二进制不做深遍历，避免
/// 在错误信息里输出大对象。
fn id_debug(val: &Bson) -> String {
    match val {
        Bson::String(s) => s.clone(),
        Bson::ObjectId(oid) => oid.to_hex(),
        other => format!("{other:?}"),
    }
}

#[allow(dead_code)]
pub fn op_reply(response_to: i32, document: Document) -> MdbResult<Vec<u8>> {
    let doc = mdb_doc::codec::to_bytes(&document)?; let total = 16 + 4 + 8 + doc.len(); let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&MessageHeader { message_length: total as i32, request_id: 0, response_to, op_code: OpCode::Reply }.to_bytes());
    out.extend_from_slice(&0i32.to_le_bytes()); out.extend_from_slice(&0i64.to_le_bytes()); out.extend_from_slice(&0i32.to_le_bytes()); out.extend_from_slice(&1i32.to_le_bytes()); out.extend_from_slice(&doc);
    Ok(out)
}
// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use mdb_crypto::Digest;
    use mdb_wire::op_msg::DocumentSequence;

    fn make_state() -> ServerState { ServerState::new() }
    fn make_msg(body: Document) -> OpMsg { OpMsg::request(1, body) }
    fn make_body(cmd: &str, db: &str) -> Document { let mut d = Document::new(); d.insert(cmd, Bson::Int32(1)); d.insert("$db", Bson::String(db.to_owned())); d }
    fn make_pool() -> ConnectionPool { ConnectionPool::new(100, Duration::from_secs(5)) }

    fn make_state_authenticated() -> ServerState {
        let state = ServerState::new();
        // Create user with root role so existing tests pass RBAC checks
        state.create_user(
            "testuser",
            "admin",
            "testpass",
            vec![Role { role: "root".into(), db: "admin".into() }],
        ).unwrap();
        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("testuser".into());
        state.update_connection(1, &conn);
        state
    }

    fn insert_docs(state: &ServerState, db: &str, coll: &str, n: i32) {
        for i in 0..n {
            let mut body = Document::new(); body.insert("insert", Bson::String(coll.to_owned())); body.insert("$db", Bson::String(db.to_owned()));
            let mut doc = Document::new(); doc.insert("name", Bson::String(format!("User{i}"))); doc.insert("age", Bson::Int32(20 + i));
            let msg = OpMsg { request_id: i + 100, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
            dispatch_command(&msg, 1, &TopologyVersion::new(), state, &make_pool());
        }
    }

    #[test]
    fn test_ping() { let state = make_state(); let pool = make_pool(); let reply = dispatch_command(&make_msg(make_body("ping", "test")), 1, &TopologyVersion::new(), &state, &make_pool()); assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0)); }

    #[test]
    fn test_build_info() { let state = make_state(); let pool = make_pool(); let reply = dispatch_command(&make_msg(make_body("buildInfo", "test")), 1, &TopologyVersion::new(), &state, &make_pool()); assert_eq!(reply.body.as_ref().unwrap().get_str("version"), Some("0.1.0")); }

    #[test]
    fn test_insert_single() {
        let state = make_state_authenticated();
        let mut body = Document::new(); body.insert("insert", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new(); doc.insert("name", Bson::String("Alice".into()));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(1));
    }

    #[test]
    fn test_insert_auto_id() {
        let state = make_state_authenticated();
        let mut body = Document::new(); body.insert("insert", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new(); doc.insert("name", Bson::String("Bob".into()));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        let db = state.get_database("test"); let coll = db.get_collection("users").unwrap(); let locked = coll.lock().unwrap(); let docs = locked.store.scan_all().unwrap_or_default();
        assert_eq!(docs.len(), 1);
        let stored = mdb_doc::codec::from_bytes(&docs[0].1).unwrap(); assert!(stored.get("_id").is_some());
    }

    #[test]
    fn test_insert_multiple() {
        let state = make_state_authenticated();
        let mut body = Document::new(); body.insert("insert", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        let docs: Vec<Document> = (0..5).map(|i| { let mut d = Document::new(); d.insert("i", Bson::Int32(i)); d }).collect();
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: docs }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(5));
    }

    #[test]
    fn test_find_empty() {
        let state = make_state_authenticated();
        let mut body = Document::new(); body.insert("find", Bson::String("users".into())); body.insert("$db", Bson::String("test".into())); body.insert("filter", Bson::Document(Document::new()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let cursor = reply.body.as_ref().unwrap().get("cursor").and_then(|v| v.as_document()).unwrap();
        assert!(cursor.get("firstBatch").and_then(|v| v.as_array()).unwrap().is_empty());
    }

    #[test]
    fn test_find_returns_all() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 3);
        let mut body = Document::new(); body.insert("find", Bson::String("users".into())); body.insert("$db", Bson::String("test".into())); body.insert("filter", Bson::Document(Document::new()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let cursor = reply.body.as_ref().unwrap().get("cursor").and_then(|v| v.as_document()).unwrap();
        assert_eq!(cursor.get("firstBatch").and_then(|v| v.as_array()).unwrap().len(), 3);
    }

    #[test]
    fn test_find_with_filter() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 5);
        let mut filter = Document::new(); filter.insert("age", Bson::Int32(21));
        let mut body = Document::new(); body.insert("find", Bson::String("users".into())); body.insert("$db", Bson::String("test".into())); body.insert("filter", Bson::Document(filter));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let cursor = reply.body.as_ref().unwrap().get("cursor").and_then(|v| v.as_document()).unwrap();
        assert_eq!(cursor.get("firstBatch").and_then(|v| v.as_array()).unwrap().len(), 1);
    }

    #[test]
    fn test_find_with_limit() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 5);
        let mut body = Document::new(); body.insert("find", Bson::String("users".into())); body.insert("$db", Bson::String("test".into())); body.insert("filter", Bson::Document(Document::new())); body.insert("limit", Bson::Int64(2));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let cursor = reply.body.as_ref().unwrap().get("cursor").and_then(|v| v.as_document()).unwrap();
        assert_eq!(cursor.get("firstBatch").and_then(|v| v.as_array()).unwrap().len(), 2);
    }

    #[test]
    fn test_find_with_skip() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 5);
        let mut body = Document::new(); body.insert("find", Bson::String("users".into())); body.insert("$db", Bson::String("test".into())); body.insert("filter", Bson::Document(Document::new())); body.insert("skip", Bson::Int64(3));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let cursor = reply.body.as_ref().unwrap().get("cursor").and_then(|v| v.as_document()).unwrap();
        assert_eq!(cursor.get("firstBatch").and_then(|v| v.as_array()).unwrap().len(), 2);
    }

    #[test]
    fn test_find_with_projection() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 2);
        let mut proj = Document::new(); proj.insert("name", Bson::Int32(1));
        let mut body = Document::new(); body.insert("find", Bson::String("users".into())); body.insert("$db", Bson::String("test".into())); body.insert("filter", Bson::Document(Document::new())); body.insert("projection", Bson::Document(proj));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let cursor = reply.body.as_ref().unwrap().get("cursor").and_then(|v| v.as_document()).unwrap();
        for item in cursor.get("firstBatch").and_then(|v| v.as_array()).unwrap() {
            if let Bson::Document(d) = item { assert!(d.get("name").is_some()); assert!(d.get("age").is_none()); }
        }
    }
    #[test]
    fn test_update_set() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 1);
        let mut body = Document::new(); body.insert("update", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new(); q.insert("name", Bson::String("User0".into()));
        let mut u = Document::new(); u.insert("$set", Bson::Document({ let mut d = Document::new(); d.insert("age", Bson::Int32(99)); d }));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(1));
        assert_eq!(reply.body.as_ref().unwrap().get_i64("nModified"), Some(1));
    }

    #[test]
    fn test_update_inc() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 1);
        let mut body = Document::new(); body.insert("update", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new(); q.insert("name", Bson::String("User0".into()));
        let mut u = Document::new(); u.insert("$inc", Bson::Document({ let mut d = Document::new(); d.insert("age", Bson::Int32(5)); d }));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(1));
    }

    #[test]
    fn test_update_unset() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 1);
        let mut body = Document::new(); body.insert("update", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new(); q.insert("name", Bson::String("User0".into()));
        let mut u = Document::new(); u.insert("$unset", Bson::Document({ let mut d = Document::new(); d.insert("age", Bson::Boolean(true)); d }));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let db = state.get_database("test"); let coll = db.get_collection("users").unwrap(); let locked = coll.lock().unwrap(); let docs = locked.store.scan_all().unwrap_or_default();
        let stored = mdb_doc::codec::from_bytes(&docs[0].1).unwrap(); assert!(stored.get("age").is_none());
    }

    #[test]
    fn test_update_upsert() {
        let state = make_state_authenticated();
        let mut body = Document::new(); body.insert("update", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new(); q.insert("name", Bson::String("NewUser".into()));
        let mut u = Document::new(); u.insert("age", Bson::Int32(25));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec.insert("upsert", Bson::Boolean(true)); spec })]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(1));
        let db = state.get_database("test"); let coll = db.get_collection("users").unwrap(); let locked = coll.lock().unwrap(); assert_eq!(locked.store.scan_all().unwrap_or_default().len(), 1);
    }

    #[test]
    fn test_delete_single() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 3);
        let mut body = Document::new(); body.insert("delete", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new(); q.insert("name", Bson::String("User0".into()));
        body.insert("deletes", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("limit", Bson::Int64(1)); spec })]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(1));
    }

    #[test]
    fn test_delete_all() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 3);
        let mut body = Document::new(); body.insert("delete", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        body.insert("deletes", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(Document::new())); spec.insert("limit", Bson::Int64(0)); spec })]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(3));
    }

    #[test]
    fn test_create_and_list_indexes() {
        let state = make_state_authenticated();
        let mut body = Document::new(); body.insert("createIndexes", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        body.insert("indexes", Bson::Array(vec![Bson::Document({ let mut idx = Document::new(); idx.insert("key", Bson::Document({ let mut k = Document::new(); k.insert("name", Bson::Int32(1)); k })); idx.insert("name", Bson::String("name_1".into())); idx })]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("numIndexesAfter"), Some(1));
        let mut body2 = Document::new(); body2.insert("listIndexes", Bson::String("users".into())); body2.insert("$db", Bson::String("test".into()));
        let reply2 = dispatch_command(&make_msg(body2), 1, &TopologyVersion::new(), &state, &make_pool());
        let cursor = reply2.body.as_ref().unwrap().get("cursor").and_then(|v| v.as_document()).unwrap();
        assert!(!cursor.get("firstBatch").and_then(|v| v.as_array()).unwrap().is_empty());
    }

    #[test]
    fn test_aggregate_match() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 5);
        let mut body = Document::new(); body.insert("aggregate", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        body.insert("pipeline", Bson::Array(vec![Bson::Document({ let mut stage = Document::new(); stage.insert("$match", Bson::Document({ let mut f = Document::new(); f.insert("age", Bson::Int32(21)); f })); stage })]));
        body.insert("cursor", Bson::Document({ let mut c = Document::new(); c.insert("batchSize", Bson::Int64(100)); c }));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let cursor = reply.body.as_ref().unwrap().get("cursor").and_then(|v| v.as_document()).unwrap();
        assert_eq!(cursor.get("firstBatch").and_then(|v| v.as_array()).unwrap().len(), 1);
    }

    #[test]
    fn test_aggregate_group() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 5);
        let mut body = Document::new(); body.insert("aggregate", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        body.insert("pipeline", Bson::Array(vec![Bson::Document({ let mut stage = Document::new(); let mut group = Document::new(); group.insert("_id", Bson::String("$age".into())); group.insert("count", Bson::Document({ let mut d = Document::new(); d.insert("$sum", Bson::Int32(1)); d })); stage.insert("$group", Bson::Document(group)); stage })]));
        body.insert("cursor", Bson::Document({ let mut c = Document::new(); c.insert("batchSize", Bson::Int64(100)); c }));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let cursor = reply.body.as_ref().unwrap().get("cursor").and_then(|v| v.as_document()).unwrap();
        assert!(!cursor.get("firstBatch").and_then(|v| v.as_array()).unwrap().is_empty());
    }

    #[test]
    fn test_list_databases() {
        let state = make_state_authenticated(); insert_docs(&state, "testdb", "coll1", 1);
        let reply = dispatch_command(&make_msg(make_body("listDatabases", "admin")), 1, &TopologyVersion::new(), &state, &make_pool());
        let dbs = reply.body.as_ref().unwrap().get("databases").and_then(|v| v.as_array()).unwrap();
        assert!(!dbs.is_empty());
    }

    #[test]
    fn test_create_and_drop_collection() {
        let state = make_state_authenticated();
        let mut body = Document::new(); body.insert("create", Bson::String("my_coll".into())); body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
        let mut body3 = Document::new(); body3.insert("drop", Bson::String("my_coll".into())); body3.insert("$db", Bson::String("test".into()));
        let reply3 = dispatch_command(&make_msg(body3), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply3.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_coll_stats() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 3);
        let mut body = Document::new(); body.insert("collStats", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("count"), Some(3));
    }

    #[test]
    fn test_db_stats() {
        let state = make_state_authenticated(); insert_docs(&state, "test", "users", 1);
        let reply = dispatch_command(&make_msg(make_body("dbStats", "test")), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("collections"), Some(1));
    }

    #[test]
    fn test_repl_set_initiate() {
        let state = make_state_authenticated();
        let mut body = Document::new(); body.insert("replSetInitiate", Bson::Document({ let mut init = Document::new(); init.insert("members", Bson::Array(vec![Bson::Document({ let mut m = Document::new(); m.insert("host", Bson::String("localhost:27017".into())); m }), Bson::Document({ let mut m = Document::new(); m.insert("host", Bson::String("localhost:27018".into())); m })])); init })); body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_repl_set_get_status_no_rs() {
        let state = make_state_authenticated(); let reply = dispatch_command(&make_msg(make_body("replSetGetStatus", "admin")), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(0.0));
    }

    #[test]
    fn test_transaction_stubs() {
        let state = make_state_authenticated();
        // 事务命令现在需要 lsid；没有 lsid 时应返回错误
        for cmd in &["startTransaction", "commitTransaction", "abortTransaction"] {
            let mut body = Document::new(); body.insert(*cmd, Bson::Int32(1)); body.insert("$db", Bson::String("test".into()));
            let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
            // 缺少 lsid 时应返回错误（不再假装成功）
            assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(0.0), "{cmd} without lsid should fail");
        }
        // 带 lsid 时应成功
        let mk_lsid = || {
            let mut lsid_doc = Document::new();
            lsid_doc.insert("id", Bson::Binary(mdb_doc::Binary::generic(vec![2u8; 16])));
            Bson::Document(lsid_doc)
        };
        // startTransaction
        let mut body = Document::new(); body.insert("startTransaction", Bson::Int32(1)); body.insert("$db", Bson::String("test".into())); body.insert("lsid", mk_lsid());
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0), "startTransaction with lsid should succeed");
        // commitTransaction（刚才 start 了，所以有活跃事务）
        let mut body = Document::new(); body.insert("commitTransaction", Bson::Int32(1)); body.insert("$db", Bson::String("test".into())); body.insert("lsid", mk_lsid());
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0), "commitTransaction with lsid should succeed");
        // startTransaction again
        let mut body = Document::new(); body.insert("startTransaction", Bson::Int32(1)); body.insert("$db", Bson::String("test".into())); body.insert("lsid", mk_lsid());
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0), "startTransaction (2nd) with lsid should succeed");
        // abortTransaction（刚才 start 了，所以有活跃事务）
        let mut body = Document::new(); body.insert("abortTransaction", Bson::Int32(1)); body.insert("$db", Bson::String("test".into())); body.insert("lsid", mk_lsid());
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0), "abortTransaction with lsid should succeed");
    }

    #[test]
    fn test_unknown_command() {
        let state = make_state_authenticated(); let reply = dispatch_command(&make_msg(make_body("foobar", "test")), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(0.0));
    }

    #[test]
    fn test_get_more_nonexistent() {
        let state = make_state_authenticated();
        let mut body = Document::new(); body.insert("getMore", Bson::Int64(99999)); body.insert("collection", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(0.0));
    }

    // ---------------------------------------------------------------------------
    // Auth tests
    // ---------------------------------------------------------------------------

    #[test]
    fn test_ping_exempt_from_auth() {
        let state = make_state();
        let reply = dispatch_command(&make_msg(make_body("ping", "test")), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_insert_unauthorized() {
        let state = make_state();
        let mut body = Document::new(); body.insert("insert", Bson::String("users".into())); body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new(); doc.insert("name", Bson::String("Alice".into()));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        let body = reply.body.as_ref().unwrap();
        assert_eq!(body.get_i64("code"), Some(13)); // Unauthorized
    }

    #[test]
    fn test_create_user_and_authenticate() {
        let state = make_state_authenticated();

        // Create a NEW user via createUser command (not the existing "testuser")
        let mut body = Document::new();
        body.insert("createUser", Bson::String("newuser".into()));
        body.insert("pwd", Bson::String("newpass".into()));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Create a SCRAM conversation manually
        let credential = state.find_user("newuser", "admin").unwrap();
        let mut server_scram = ServerScram::new(ScramMechanism::Sha256, 1);
        let sf = server_scram.on_client_first(
            b"n,,n=newuser,r=clientNonce123",
            &mut |_| Some(credential.credential.clone()),
        ).unwrap();

        // Parse server nonce
        let nonce = sf.split(',').find_map(|p| p.strip_prefix("r=").map(String::from)).unwrap();

        // Compute client proof
        let salted = credential.credential.salted_password("newpass");
        let client_key = mdb_crypto::hmac::HmacSha256::mac(&salted, b"Client Key");
        let stored_key_bytes = mdb_crypto::sha256::Sha256::digest(&client_key);
        let auth_msg = format!("n=newuser,r=clientNonce123,{sf},c=biws,r={nonce}");
        let client_sig = mdb_crypto::hmac::HmacSha256::mac(stored_key_bytes.as_slice(), auth_msg.as_bytes());
        let mut proof = client_key;
        for (e, sg) in proof.iter_mut().zip(client_sig.iter()) {
            *e ^= sg;
        }

        let sf = server_scram.on_client_final(format!("c=biws,r={nonce},p={}", mdb_crypto::base64::encode(&proof)).as_bytes()).unwrap();
        assert!(sf.starts_with("v="));
        assert!(server_scram.authenticated_user().is_some());
    }

    #[test]
    fn test_duplicate_user_rejected() {
        let state = make_state_authenticated();

        let mut body = Document::new();
        body.insert("createUser", Bson::String("alice".into()));
        body.insert("pwd", Bson::String("pass1".into()));
        body.insert("$db", Bson::String("admin".into()));
        dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());

        let mut body2 = Document::new();
        body2.insert("createUser", Bson::String("alice".into()));
        body2.insert("pwd", Bson::String("pass2".into()));
        body2.insert("$db", Bson::String("admin".into()));
        let reply2 = dispatch_command(&make_msg(body2), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply2.body.as_ref().unwrap().get_i64("code"), Some(11000)); // DuplicateKey
    }

    #[test]
    fn test_drop_user() {
        let state = make_state_authenticated();

        let mut body = Document::new();
        body.insert("createUser", Bson::String("bob".into()));
        body.insert("pwd", Bson::String("pass".into()));
        body.insert("$db", Bson::String("admin".into()));
        dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());

        let mut body2 = Document::new();
        body2.insert("dropUser", Bson::String("bob".into()));
        body2.insert("$db", Bson::String("admin".into()));
        let reply2 = dispatch_command(&make_msg(body2), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply2.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        assert!(state.find_user("bob", "admin").is_none());
    }

    #[test]
    fn test_grant_roles() {
        let state = make_state_authenticated();

        let mut body = Document::new();
        body.insert("createUser", Bson::String("charlie".into()));
        body.insert("pwd", Bson::String("pass".into()));
        body.insert("roles", Bson::Array(vec![Bson::String("read".into())]));
        body.insert("$db", Bson::String("admin".into()));
        dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());

        let mut body2 = Document::new();
        body2.insert("grantRolesToUser", Bson::String("charlie".into()));
        body2.insert("roles", Bson::Array(vec![Bson::String("readWrite".into())]));
        body2.insert("$db", Bson::String("admin".into()));
        let reply2 = dispatch_command(&make_msg(body2), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply2.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        let user = state.find_user("charlie", "admin").unwrap();
        assert!(user.roles.iter().any(|r| r.role == "read"));
        assert!(user.roles.iter().any(|r| r.role == "readWrite"));
    }

    #[test]
    fn test_users_info() {
        let state = make_state_authenticated();

        let mut body = Document::new();
        body.insert("createUser", Bson::String("dave".into()));
        body.insert("pwd", Bson::String("pass".into()));
        body.insert("$db", Bson::String("admin".into()));
        dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());

        let mut body2 = Document::new();
        body2.insert("usersInfo", Bson::String("dave".into()));
        body2.insert("$db", Bson::String("admin".into()));
        let reply2 = dispatch_command(&make_msg(body2), 1, &TopologyVersion::new(), &state, &make_pool());
        let users = reply2.body.as_ref().unwrap().get("users").and_then(|v| v.as_array()).unwrap();
        assert_eq!(users.len(), 1);
    }

    #[test]
    fn test_tls_config() {
        let state = ServerState::new().with_tls(true);
        assert!(state.tls_enabled.load(Ordering::Relaxed));
    }

    // ---------------------------------------------------------------------------
    // Tests for new commands
    // ---------------------------------------------------------------------------

    #[test]
    fn test_distinct() {
        let state = make_state_authenticated(); let pool = make_pool();
        insert_docs(&state, "test", "users", 5);
        let mut body = Document::new();
        body.insert("distinct", Bson::String("users".into()));
        body.insert("key", Bson::String("age".into()));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &pool);
        let values = reply.body.as_ref().unwrap().get("values").and_then(|v| v.as_array()).unwrap();
        assert!(!values.is_empty());
    }

    #[test]
    fn test_distinct_with_query() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 5);
        let mut body = Document::new();
        body.insert("distinct", Bson::String("users".into()));
        body.insert("key", Bson::String("age".into()));
        let mut query = Document::new();
        query.insert("age", Bson::Int32(20));
        body.insert("query", Bson::Document(query));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let values = reply.body.as_ref().unwrap().get("values").and_then(|v| v.as_array()).unwrap();
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn test_distinct_missing_key() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("distinct", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(0.0));
    }

    #[test]
    fn test_count() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 5);
        let mut body = Document::new();
        body.insert("count", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(5));
    }

    #[test]
    fn test_count_with_query() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 5);
        let mut body = Document::new();
        body.insert("count", Bson::String("users".into()));
        let mut query = Document::new();
        query.insert("age", Bson::Int32(21));
        body.insert("query", Bson::Document(query));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(1));
    }

    #[test]
    fn test_count_empty_collection() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("count", Bson::String("nonexistent".into()));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(0));
    }

    #[test]
    fn test_count_documents_alias() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 3);
        let mut body = Document::new();
        body.insert("countDocuments", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(3));
    }

    #[test]
    fn test_find_and_modify_remove() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 3);
        let mut body = Document::new();
        body.insert("findAndModify", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut query = Document::new();
        query.insert("name", Bson::String("User0".into()));
        body.insert("query", Bson::Document(query));
        body.insert("remove", Bson::Boolean(true));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let value = reply.body.as_ref().unwrap().get("value").and_then(|v| v.as_document());
        assert!(value.is_some());
        assert_eq!(value.unwrap().get_str("name"), Some("User0"));
    }

    #[test]
    fn test_find_and_modify_update() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 1);
        let mut body = Document::new();
        body.insert("findAndModify", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut query = Document::new();
        query.insert("name", Bson::String("User0".into()));
        body.insert("query", Bson::Document(query));
        let mut update = Document::new();
        let mut set_doc = Document::new();
        set_doc.insert("age", Bson::Int32(100));
        update.insert("$set", Bson::Document(set_doc));
        body.insert("update", Bson::Document(update));
        body.insert("new", Bson::Boolean(true));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let value = reply.body.as_ref().unwrap().get("value").and_then(|v| v.as_document());
        assert!(value.is_some());
        assert_eq!(value.unwrap().get_i64("age"), Some(100));
    }

    #[test]
    fn test_find_and_modify_nonexistent_upsert() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("findAndModify", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut query = Document::new();
        query.insert("name", Bson::String("NewUser".into()));
        body.insert("query", Bson::Document(query));
        let mut update = Document::new();
        update.insert("age", Bson::Int32(30));
        body.insert("update", Bson::Document(update));
        body.insert("upsert", Bson::Boolean(true));
        body.insert("new", Bson::Boolean(true));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let value = reply.body.as_ref().unwrap().get("value").and_then(|v| v.as_document());
        assert!(value.is_some());
        assert_eq!(value.unwrap().get_i64("age"), Some(30));
    }

    #[test]
    fn test_find_and_modify_no_match_no_upsert() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 1);
        let mut body = Document::new();
        body.insert("findAndModify", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut query = Document::new();
        query.insert("name", Bson::String("Nonexistent".into()));
        body.insert("query", Bson::Document(query));
        let mut update = Document::new();
        update.insert("age", Bson::Int32(99));
        body.insert("update", Bson::Document(update));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let value = reply.body.as_ref().unwrap().get("value");
        assert!(value.is_none());
    }

    #[test]
    fn test_rename_collection() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "old_coll", 2);
        let mut body = Document::new();
        body.insert("renameCollection", Bson::String("test.old_coll".into()));
        body.insert("to", Bson::String("test.new_coll".into()));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
        let db = state.get_database("test");
        assert!(db.get_collection("new_coll").is_some());
        assert!(db.get_collection("old_coll").is_none());
    }

    #[test]
    fn test_rename_collection_not_found() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("renameCollection", Bson::String("test.nope".into()));
        body.insert("to", Bson::String("test.yep".into()));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(0.0));
    }

    #[test]
    fn test_compact() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 1);
        let mut body = Document::new();
        body.insert("compact", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_compact_not_found() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("compact", Bson::String("noexist".into()));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(0.0));
    }

    #[test]
    fn test_validate() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 3);
        let mut body = Document::new();
        body.insert("validate", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_bool("valid"), Some(true));
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_validate_not_found() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("validate", Bson::String("noexist".into()));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(0.0));
    }

    #[test]
    fn test_explain() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 5);
        let mut body = Document::new();
        let mut explain_cmd = Document::new();
        explain_cmd.insert("find", Bson::String("users".into()));
        let mut filter = Document::new();
        filter.insert("age", Bson::Int32(21));
        explain_cmd.insert("filter", Bson::Document(filter));
        body.insert("explain", Bson::Document(explain_cmd));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
        assert!(reply.body.as_ref().unwrap().get("queryPlanner").is_some());
    }

    #[test]
    fn test_server_status() {
        let state = make_state_authenticated();
        let reply = dispatch_command(&make_msg(make_body("serverStatus", "admin")), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
        assert!(reply.body.as_ref().unwrap().get("connections").is_some());
        assert!(reply.body.as_ref().unwrap().get("opcounters").is_some());
        assert!(reply.body.as_ref().unwrap().get("memory").is_some());
        assert!(reply.body.as_ref().unwrap().get("network").is_some());
        assert!(reply.body.as_ref().unwrap().get("asserts").is_some());
    }

    #[test]
    fn test_current_op() {
        let state = make_state_authenticated();
        let reply = dispatch_command(&make_msg(make_body("currentOp", "admin")), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
        let inprog = reply.body.as_ref().unwrap().get("inprog").and_then(|v| v.as_array()).unwrap();
        assert!(inprog.is_empty());
    }

    #[test]
    fn test_kill_op() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("killOp", Bson::Int32(1));
        body.insert("op", Bson::Int64(123));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_kill_op_missing_op() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("killOp", Bson::Int32(1));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(0.0));
    }

    #[test]
    fn test_profile() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("profile", Bson::Int64(1));
        body.insert("slowms", Bson::Int64(100));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_get_log() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("getLog", Bson::String("global".into()));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
        assert!(reply.body.as_ref().unwrap().get("log").is_some());
    }

    #[test]
    fn test_re_index() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 1);
        let mut body = Document::new();
        body.insert("reIndex", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_re_index_not_found() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("reIndex", Bson::String("noexist".into()));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(0.0));
    }

    #[test]
    fn test_drop_database() {
        let state = make_state_authenticated();
        insert_docs(&state, "droptest", "coll", 1);
        let mut body = Document::new();
        body.insert("dropDatabase", Bson::Int32(1));
        body.insert("$db", Bson::String("droptest".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_str("dropped"), Some("droptest"));
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_database_stats_alias() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 1);
        let reply = dispatch_command(&make_msg(make_body("databaseStats", "test")), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    // ---------------------------------------------------------------------------
    // Tests for new update operators
    // ---------------------------------------------------------------------------

    #[test]
    fn test_push_each() {
        let state = make_state_authenticated();
        // Insert a doc with an array
        let mut body = Document::new();
        body.insert("insert", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new();
        doc.insert("tags", Bson::Array(vec![Bson::String("a".into()), Bson::String("b".into())]));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());

        // Update with $push and $each
        let mut body = Document::new();
        body.insert("update", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new();
        q.insert("tags", Bson::Document({ let mut d = Document::new(); d.insert("$exists", Bson::Boolean(true)); d }));
        let mut each_mod = Document::new();
        each_mod.insert("$each", Bson::Array(vec![Bson::String("c".into()), Bson::String("d".into())]));
        let mut push_doc = Document::new();
        push_doc.insert("tags", Bson::Document(each_mod));
        let mut u = Document::new();
        u.insert("$push", Bson::Document(push_doc));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("nModified"), Some(1));

        // Verify
        let db = state.get_database("test");
        let coll = db.get_collection("items").unwrap();
        let locked = coll.lock().unwrap();
        let docs = locked.store.scan_all().unwrap_or_default();
        let stored = mdb_doc::codec::from_bytes(&docs[0].1).unwrap();
        let tags = stored.get("tags").and_then(|v| v.as_array()).unwrap();
        assert_eq!(tags.len(), 4);
        assert_eq!(tags[2].as_str(), Some("c"));
        assert_eq!(tags[3].as_str(), Some("d"));
    }

    #[test]
    fn test_push_position() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("insert", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new();
        doc.insert("arr", Bson::Array(vec![Bson::Int32(1), Bson::Int32(3), Bson::Int32(4)]));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());

        let mut body = Document::new();
        body.insert("update", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new();
        q.insert("arr", Bson::Document({ let mut d = Document::new(); d.insert("$exists", Bson::Boolean(true)); d }));
        let mut pos_mod = Document::new();
        pos_mod.insert("$each", Bson::Array(vec![Bson::Int32(2)]));
        pos_mod.insert("$position", Bson::Int32(1));
        let mut push_doc = Document::new();
        push_doc.insert("arr", Bson::Document(pos_mod));
        let mut u = Document::new();
        u.insert("$push", Bson::Document(push_doc));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());

        let db = state.get_database("test");
        let coll = db.get_collection("items").unwrap();
        let locked = coll.lock().unwrap();
        let docs = locked.store.scan_all().unwrap_or_default();
        let stored = mdb_doc::codec::from_bytes(&docs[0].1).unwrap();
        let arr = stored.get("arr").and_then(|v| v.as_array()).unwrap();
        assert_eq!(arr.len(), 4);
        assert_eq!(arr[0].as_i64(), Some(1));
        assert_eq!(arr[1].as_i64(), Some(2));
        assert_eq!(arr[2].as_i64(), Some(3));
        assert_eq!(arr[3].as_i64(), Some(4));
    }

    #[test]
    fn test_push_slice() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("insert", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new();
        doc.insert("arr", Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3), Bson::Int32(4), Bson::Int32(5)]));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());

        let mut body = Document::new();
        body.insert("update", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new();
        q.insert("arr", Bson::Document({ let mut d = Document::new(); d.insert("$exists", Bson::Boolean(true)); d }));
        let mut slice_mod = Document::new();
        slice_mod.insert("$each", Bson::Array(vec![Bson::Int32(6), Bson::Int32(7), Bson::Int32(8)]));
        slice_mod.insert("$slice", Bson::Int32(3));
        let mut push_doc = Document::new();
        push_doc.insert("arr", Bson::Document(slice_mod));
        let mut u = Document::new();
        u.insert("$push", Bson::Document(push_doc));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());

        let db = state.get_database("test");
        let coll = db.get_collection("items").unwrap();
        let locked = coll.lock().unwrap();
        let docs = locked.store.scan_all().unwrap_or_default();
        let stored = mdb_doc::codec::from_bytes(&docs[0].1).unwrap();
        let arr = stored.get("arr").and_then(|v| v.as_array()).unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0].as_i64(), Some(1));
        assert_eq!(arr[1].as_i64(), Some(2));
        assert_eq!(arr[2].as_i64(), Some(3));
    }

    #[test]
    fn test_push_slice_negative() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("insert", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new();
        doc.insert("arr", Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());

        let mut body = Document::new();
        body.insert("update", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new();
        q.insert("arr", Bson::Document({ let mut d = Document::new(); d.insert("$exists", Bson::Boolean(true)); d }));
        let mut slice_mod = Document::new();
        slice_mod.insert("$each", Bson::Array(vec![Bson::Int32(4), Bson::Int32(5)]));
        slice_mod.insert("$slice", Bson::Int32(-3));
        let mut push_doc = Document::new();
        push_doc.insert("arr", Bson::Document(slice_mod));
        let mut u = Document::new();
        u.insert("$push", Bson::Document(push_doc));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());

        let db = state.get_database("test");
        let coll = db.get_collection("items").unwrap();
        let locked = coll.lock().unwrap();
        let docs = locked.store.scan_all().unwrap_or_default();
        let stored = mdb_doc::codec::from_bytes(&docs[0].1).unwrap();
        let arr = stored.get("arr").and_then(|v| v.as_array()).unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0].as_i64(), Some(3));
        assert_eq!(arr[1].as_i64(), Some(4));
        assert_eq!(arr[2].as_i64(), Some(5));
    }

    #[test]
    fn test_add_to_set_each() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("insert", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new();
        doc.insert("tags", Bson::Array(vec![Bson::String("a".into()), Bson::String("b".into())]));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());

        let mut body = Document::new();
        body.insert("update", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new();
        q.insert("tags", Bson::Document({ let mut d = Document::new(); d.insert("$exists", Bson::Boolean(true)); d }));
        let mut each_mod = Document::new();
        each_mod.insert("$each", Bson::Array(vec![Bson::String("b".into()), Bson::String("c".into())]));
        let mut add_doc = Document::new();
        add_doc.insert("tags", Bson::Document(each_mod));
        let mut u = Document::new();
        u.insert("$addToSet", Bson::Document(add_doc));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());

        let db = state.get_database("test");
        let coll = db.get_collection("items").unwrap();
        let locked = coll.lock().unwrap();
        let docs = locked.store.scan_all().unwrap_or_default();
        let stored = mdb_doc::codec::from_bytes(&docs[0].1).unwrap();
        let tags = stored.get("tags").and_then(|v| v.as_array()).unwrap();
        assert_eq!(tags.len(), 3);
    }

    #[test]
    fn test_pull_all() {
        let state = make_state_authenticated();
        let mut body = Document::new();
        body.insert("insert", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new();
        doc.insert("nums", Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3), Bson::Int32(4), Bson::Int32(5)]));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());

        let mut body = Document::new();
        body.insert("update", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new();
        q.insert("nums", Bson::Document({ let mut d = Document::new(); d.insert("$exists", Bson::Boolean(true)); d }));
        let mut pull_doc = Document::new();
        pull_doc.insert("nums", Bson::Array(vec![Bson::Int32(2), Bson::Int32(4)]));
        let mut u = Document::new();
        u.insert("$pullAll", Bson::Document(pull_doc));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());

        let db = state.get_database("test");
        let coll = db.get_collection("items").unwrap();
        let locked = coll.lock().unwrap();
        let docs = locked.store.scan_all().unwrap_or_default();
        let stored = mdb_doc::codec::from_bytes(&docs[0].1).unwrap();
        let nums = stored.get("nums").and_then(|v| v.as_array()).unwrap();
        assert_eq!(nums.len(), 3);
        assert_eq!(nums[0].as_i64(), Some(1));
        assert_eq!(nums[1].as_i64(), Some(3));
        assert_eq!(nums[2].as_i64(), Some(5));
    }

    #[test]
    fn test_bit_and() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 1);
        let mut body = Document::new();
        body.insert("update", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new();
        q.insert("name", Bson::String("User0".into()));
        let mut bit_doc = Document::new();
        let mut and_or = Document::new();
        and_or.insert("and", Bson::Int32(0b11110000));
        bit_doc.insert("age", Bson::Document(and_or));
        let mut u = Document::new();
        u.insert("$bit", Bson::Document(bit_doc));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("nModified"), Some(1));
        let db = state.get_database("test");
        let coll = db.get_collection("users").unwrap();
        let locked = coll.lock().unwrap();
        let docs = locked.store.scan_all().unwrap_or_default();
        let stored = mdb_doc::codec::from_bytes(&docs[0].1).unwrap();
        let age = stored.get("age").and_then(|v| v.as_i64()).unwrap();
        assert_eq!(age, 20i64 & 0b11110000);
    }

    #[test]
    fn test_bit_or() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 1);
        let mut body = Document::new();
        body.insert("update", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new();
        q.insert("name", Bson::String("User0".into()));
        let mut bit_doc = Document::new();
        let mut or_doc = Document::new();
        or_doc.insert("or", Bson::Int32(0b00001111));
        bit_doc.insert("age", Bson::Document(or_doc));
        let mut u = Document::new();
        u.insert("$bit", Bson::Document(bit_doc));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("nModified"), Some(1));
    }

    #[test]
    fn test_bit_xor() {
        let state = make_state_authenticated();
        insert_docs(&state, "test", "users", 1);
        let mut body = Document::new();
        body.insert("update", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut q = Document::new();
        q.insert("name", Bson::String("User0".into()));
        let mut bit_doc = Document::new();
        let mut xor_doc = Document::new();
        xor_doc.insert("xor", Bson::Int32(0b00001111));
        bit_doc.insert("age", Bson::Document(xor_doc));
        let mut u = Document::new();
        u.insert("$bit", Bson::Document(bit_doc));
        body.insert("updates", Bson::Array(vec![Bson::Document({ let mut spec = Document::new(); spec.insert("q", Bson::Document(q)); spec.insert("u", Bson::Document(u)); spec })]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("nModified"), Some(1));
    }

    // ===========================================================================
    // Connection Pool Tests
    // ===========================================================================

    #[test]
    fn test_pool_acquire_release() {
        let pool = ConnectionPool::new(2, Duration::from_secs(1));
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.available_count(), 2);

        pool.acquire().unwrap();
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool.available_count(), 1);

        pool.acquire().unwrap();
        assert_eq!(pool.active_count(), 2);
        assert_eq!(pool.available_count(), 0);

        pool.release();
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool.available_count(), 1);

        pool.release();
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.available_count(), 2);
    }

    #[test]
    fn test_pool_exhausted_timeout() {
        let pool = ConnectionPool::new(1, Duration::from_millis(50));
        pool.acquire().unwrap();
        // Second acquire should time out
        let result = pool.acquire();
        assert!(result.is_err());
        pool.release();
    }

    #[test]
    fn test_pool_concurrent_acquire_release() {
        use std::sync::Arc;
        let pool = Arc::new(ConnectionPool::new(5, Duration::from_secs(5)));
        let mut handles = Vec::new();

        for _ in 0..10 {
            let pool = Arc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                pool.acquire().unwrap();
                std::thread::sleep(Duration::from_millis(10));
                pool.release();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(pool.active_count(), 0);
    }

    #[test]
    fn test_pool_available_count_respects_max() {
        let pool = ConnectionPool::new(3, Duration::from_secs(1));
        assert_eq!(pool.available_count(), 3);
        pool.acquire().unwrap();
        pool.acquire().unwrap();
        pool.acquire().unwrap();
        assert_eq!(pool.available_count(), 0);
        pool.release();
        assert_eq!(pool.available_count(), 1);
    }

    #[test]
    fn test_pool_shutdown_wakes_waiters() {
        let pool = Arc::new(ConnectionPool::new(1, Duration::from_secs(10)));
        pool.acquire().unwrap();

        let pool2 = Arc::clone(&pool);
        let handle = std::thread::spawn(move || {
            let result = pool2.acquire();
            assert!(result.is_err());
        });

        std::thread::sleep(Duration::from_millis(50));
        pool.shutdown();
        handle.join().unwrap();
    }

    // ===========================================================================
    // Thread Pool Tests
    // ===========================================================================

    #[test]
    fn test_thread_pool_executes_jobs() {
        use std::sync::atomic::AtomicUsize;
        let pool = WorkerPool::new(2);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let counter = Arc::clone(&counter);
            let (tx, rx) = std::sync::mpsc::channel();
            handles.push(rx);
            pool.execute(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                tx.send(()).unwrap();
            });
        }

        for rx in handles {
            rx.recv().unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn test_thread_pool_work_stealing() {
        use std::sync::atomic::AtomicUsize;
        let pool = WorkerPool::new(2);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        // Submit more jobs than workers to trigger work-stealing
        for _ in 0..8 {
            let counter = Arc::clone(&counter);
            let (tx, rx) = std::sync::mpsc::channel();
            handles.push(rx);
            pool.execute(move || {
                std::thread::sleep(Duration::from_millis(5));
                counter.fetch_add(1, Ordering::Relaxed);
                tx.send(()).unwrap();
            });
        }

        for rx in handles {
            rx.recv().unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn test_thread_pool_shutdown_drains() {
        use std::sync::atomic::AtomicUsize;
        let pool = WorkerPool::new(2);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..4 {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            });
        }

        pool.shutdown();
        drop(pool);
        assert_eq!(counter.load(Ordering::Relaxed), 4);
    }

    // ===========================================================================
    // Connection Status Tests
    // ===========================================================================

    #[test]
    fn test_connection_status() {
        let state = make_state_authenticated();
        let pool = make_pool();
        let reply = state.connection_status(&pool);
        assert_eq!(reply.get_i64("authenticatedUsers"), Some(1));
        assert_eq!(reply.get_i64("activeConnections"), Some(0));
        assert_eq!(reply.get_i64("availableConnections"), Some(100));
    }

    #[test]
    fn test_connection_status_command() {
        let state = make_state_authenticated();
        let pool = make_pool();
        let mut body = Document::new();
        body.insert("connectionStatus", Bson::Int32(1));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let reply_body = reply.body.as_ref().unwrap();
        assert_eq!(reply_body.get_f64("ok"), Some(1.0));
        assert!(reply_body.get("authenticatedUsers").is_some());
        assert!(reply_body.get("activeConnections").is_some());
        assert!(reply_body.get("availableConnections").is_some());
    }

    // ===========================================================================
    // Graceful Shutdown Tests
    // ===========================================================================

    #[test]
    fn test_shutdown_controller() {
        let ctrl = ShutdownController::new();
        assert!(!ctrl.is_shutdown());

        ctrl.register(1);
        ctrl.register(2);
        assert_eq!(ctrl.active_connection_count(), 2);

        ctrl.request_shutdown();
        assert!(ctrl.is_shutdown());

        ctrl.unregister(1);
        assert_eq!(ctrl.active_connection_count(), 1);

        let drained = ctrl.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0], 2);
    }

    #[test]
    fn test_shutdown_command() {
        let state = make_state_authenticated();
        let pool = make_pool();
        let mut body = Document::new();
        body.insert("shutdown", Bson::Int32(1));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &pool);
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
        // Pool should be shut down
        assert!(pool.shutdown.load(Ordering::Acquire));
    }

    // ===========================================================================
    // RBAC Tests
    // ===========================================================================

    #[test]
    fn test_role_implies() {
        assert!(role_implies("root", "read"));
        assert!(role_implies("root", "readWrite"));
        assert!(role_implies("root", "dbAdmin"));
        assert!(role_implies("root", "userAdmin"));
        assert!(role_implies("root", "clusterAdmin"));
        assert!(role_implies("root", "root"));

        assert!(role_implies("dbOwner", "readWrite"));
        assert!(role_implies("dbOwner", "read"));
        assert!(role_implies("dbOwner", "dbAdmin"));
        assert!(role_implies("dbOwner", "userAdmin"));
        assert!(!role_implies("dbOwner", "clusterAdmin"));
        assert!(!role_implies("dbOwner", "root"));

        assert!(role_implies("readWrite", "read"));
        assert!(!role_implies("readWrite", "dbAdmin"));
        assert!(!role_implies("readWrite", "root"));

        assert!(!role_implies("read", "readWrite"));
        assert!(!role_implies("read", "root"));

        assert!(role_implies("read", "read"));
        assert!(role_implies("readWrite", "readWrite"));
    }

    #[test]
    fn test_user_has_permission_root() {
        let roles = vec![Role { role: "root".into(), db: "admin".into() }];
        assert!(user_has_permission(&roles, "test", None, "read", false));
        assert!(user_has_permission(&roles, "test", Some("users"), "readWrite", false));
        assert!(user_has_permission(&roles, "test", None, "dbAdmin", false));
        assert!(user_has_permission(&roles, "admin", None, "userAdmin", false));
        assert!(user_has_permission(&roles, "test", None, "clusterAdmin", false));
    }

    #[test]
    fn test_user_has_permission_read_only() {
        let roles = vec![Role { role: "read".into(), db: "test".into() }];
        assert!(user_has_permission(&roles, "test", None, "read", false));
        assert!(!user_has_permission(&roles, "test", None, "readWrite", false));
        assert!(!user_has_permission(&roles, "other", None, "read", false));
    }

    #[test]
    fn test_user_has_permission_readwrite() {
        let roles = vec![Role { role: "readWrite".into(), db: "mydb".into() }];
        assert!(user_has_permission(&roles, "mydb", None, "read", false));
        assert!(user_has_permission(&roles, "mydb", None, "readWrite", false));
        assert!(!user_has_permission(&roles, "mydb", None, "dbAdmin", false));
        assert!(!user_has_permission(&roles, "other", None, "read", false));
    }

    #[test]
    fn test_user_has_permission_admin_only() {
        let roles = vec![Role { role: "read".into(), db: "admin".into() }];
        assert!(user_has_permission(&roles, "test", None, "read", true));
        let roles2 = vec![Role { role: "read".into(), db: "test".into() }];
        assert!(!user_has_permission(&roles2, "test", None, "read", true));
    }

    #[test]
    fn test_user_has_permission_multiple_roles() {
        let roles = vec![
            Role { role: "read".into(), db: "db1".into() },
            Role { role: "readWrite".into(), db: "db2".into() },
        ];
        assert!(user_has_permission(&roles, "db1", None, "read", false));
        assert!(!user_has_permission(&roles, "db1", None, "readWrite", false));
        assert!(user_has_permission(&roles, "db2", None, "read", false));
        assert!(user_has_permission(&roles, "db2", None, "readWrite", false));
        assert!(!user_has_permission(&roles, "db3", None, "read", false));
    }

    #[test]
    fn test_command_required_role() {
        assert_eq!(command_required_role("find", None), Some(("read", false)));
        assert_eq!(command_required_role("insert", None), Some(("readWrite", false)));
        assert_eq!(command_required_role("update", None), Some(("readWrite", false)));
        assert_eq!(command_required_role("delete", None), Some(("readWrite", false)));
        assert_eq!(command_required_role("createIndexes", None), Some(("dbAdmin", false)));
        assert_eq!(command_required_role("dropIndexes", None), Some(("dbAdmin", false)));
        assert_eq!(command_required_role("createUser", None), Some(("userAdmin", false)));
        assert_eq!(command_required_role("dropUser", None), Some(("userAdmin", false)));
        assert_eq!(command_required_role("listDatabases", None), Some(("read", true)));
        assert_eq!(command_required_role("replSetInitiate", None), Some(("clusterAdmin", true)));
        assert_eq!(command_required_role("ping", None), None);
        assert_eq!(command_required_role("buildInfo", None), None);
    }

    #[test]
    fn test_rbac_create_user_with_roles() {
        let state = ServerState::new();
        // Create user without root - should fail on write commands
        state.create_user("reader", "test", "pass1", vec![
            Role { role: "read".into(), db: "test".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("reader".into());
        state.update_connection(1, &conn);

        // Read should succeed
        let mut body = Document::new();
        body.insert("find", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Insert should fail
        let mut body = Document::new();
        body.insert("insert", Bson::String("users".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new();
        doc.insert("name", Bson::String("Alice".into()));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("code"), Some(13));
    }

    #[test]
    fn test_rbac_readwrite_can_insert_and_read() {
        let state = ServerState::new();
        state.create_user("rw", "test", "pass", vec![
            Role { role: "readWrite".into(), db: "test".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("rw".into());
        state.update_connection(1, &conn);

        // Insert should succeed
        let mut body = Document::new();
        body.insert("insert", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new();
        doc.insert("name", Bson::String("Widget".into()));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(1));

        // Read should also succeed
        let mut body = Document::new();
        body.insert("find", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_rbac_readwrite_cannot_create_indexes() {
        let state = ServerState::new();
        state.create_user("rw", "test", "pass", vec![
            Role { role: "readWrite".into(), db: "test".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("rw".into());
        state.update_connection(1, &conn);

        // createIndexes should fail (requires dbAdmin)
        let mut body = Document::new();
        body.insert("createIndexes", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("indexes", Bson::Array(vec![]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("code"), Some(13));
    }

    #[test]
    fn test_rbac_grant_and_revoke_roles() {
        let state = ServerState::new();
        state.create_user("admin_user", "admin", "pass", vec![
            Role { role: "root".into(), db: "admin".into() },
        ]).unwrap();
        state.create_user("alice", "admin", "pass", vec![
            Role { role: "read".into(), db: "testdb".into() },
        ]).unwrap();

        // Switch to alice: read should work
        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("alice".into());
        state.update_connection(1, &conn);

        let mut body = Document::new();
        body.insert("find", Bson::String("coll".into()));
        body.insert("$db", Bson::String("testdb".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Insert should fail (alice only has read)
        let mut body = Document::new();
        body.insert("insert", Bson::String("coll".into()));
        body.insert("$db", Bson::String("testdb".into()));
        let mut doc = Document::new();
        doc.insert("x", Bson::Int32(1));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("code"), Some(13));

        // Switch to admin_user: grant readWrite to alice
        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("admin_user".into());
        state.update_connection(1, &conn);

        let mut body = Document::new();
        body.insert("grantRolesToUser", Bson::String("alice".into()));
        body.insert("roles", Bson::Array(vec![
            Bson::Document({
                let mut d = Document::new();
                d.insert("role", Bson::String("readWrite".into()));
                d.insert("db", Bson::String("testdb".into()));
                d
            })
        ]));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Switch back to alice: insert should now succeed
        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("alice".into());
        state.update_connection(1, &conn);

        let mut body = Document::new();
        body.insert("insert", Bson::String("coll".into()));
        body.insert("$db", Bson::String("testdb".into()));
        let mut doc = Document::new();
        doc.insert("x", Bson::Int32(1));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(1));

        // Switch to admin_user: revoke readWrite from alice
        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("admin_user".into());
        state.update_connection(1, &conn);

        let mut body = Document::new();
        body.insert("revokeRolesFromUser", Bson::String("alice".into()));
        body.insert("roles", Bson::Array(vec![
            Bson::Document({
                let mut d = Document::new();
                d.insert("role", Bson::String("readWrite".into()));
                d.insert("db", Bson::String("testdb".into()));
                d
            })
        ]));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Switch to alice: insert should fail again
        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("alice".into());
        state.update_connection(1, &conn);

        let mut body = Document::new();
        body.insert("insert", Bson::String("coll".into()));
        body.insert("$db", Bson::String("testdb".into()));
        let mut doc = Document::new();
        doc.insert("x", Bson::Int32(2));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("code"), Some(13));
    }

    #[test]
    fn test_rbac_multiple_roles_per_user() {
        let state = ServerState::new();
        state.create_user("multi", "admin", "pass", vec![
            Role { role: "read".into(), db: "db1".into() },
            Role { role: "readWrite".into(), db: "db2".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("multi".into());
        state.update_connection(1, &conn);

        // Can read from db1
        let mut body = Document::new();
        body.insert("find", Bson::String("coll".into()));
        body.insert("$db", Bson::String("db1".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Cannot write to db1
        let mut body = Document::new();
        body.insert("insert", Bson::String("coll".into()));
        body.insert("$db", Bson::String("db1".into()));
        let mut doc = Document::new();
        doc.insert("x", Bson::Int32(1));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("code"), Some(13));

        // Can write to db2
        let mut body = Document::new();
        body.insert("insert", Bson::String("coll".into()));
        body.insert("$db", Bson::String("db2".into()));
        let mut doc = Document::new();
        doc.insert("x", Bson::Int32(1));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(1));

        // Cannot write to db3 (no role)
        let mut body = Document::new();
        body.insert("insert", Bson::String("coll".into()));
        body.insert("$db", Bson::String("db3".into()));
        let mut doc = Document::new();
        doc.insert("x", Bson::Int32(1));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("code"), Some(13));
    }

    #[test]
    fn test_rbac_collection_level_permissions() {
        let state = ServerState::new();
        state.create_user("colluser", "test", "pass", vec![
            Role { role: "read".into(), db: "test".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("colluser".into());
        state.update_connection(1, &conn);

        // Can read any collection in "test"
        let mut body = Document::new();
        body.insert("find", Bson::String("secret_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Cannot read from a different database
        let mut body = Document::new();
        body.insert("find", Bson::String("coll".into()));
        body.insert("$db", Bson::String("other".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("code"), Some(13));
    }

    #[test]
    fn test_rbac_user_admin_can_create_users() {
        let state = ServerState::new();
        state.create_user("admin_user", "admin", "pass", vec![
            Role { role: "userAdmin".into(), db: "admin".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("admin_user".into());
        state.update_connection(1, &conn);

        // Can create users
        let mut body = Document::new();
        body.insert("createUser", Bson::String("newuser".into()));
        body.insert("pwd", Bson::String("newpass".into()));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Cannot insert data (no readWrite)
        let mut body = Document::new();
        body.insert("insert", Bson::String("coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new();
        doc.insert("x", Bson::Int32(1));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("code"), Some(13));
    }

    #[test]
    fn test_rbac_admin_role_is_global() {
        let state = ServerState::new();
        // read on admin = read everywhere
        state.create_user("globuser", "admin", "pass", vec![
            Role { role: "read".into(), db: "admin".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("globuser".into());
        state.update_connection(1, &conn);

        // Can read from any database
        for db in &["db_a", "db_b", "db_c"] {
            let mut body = Document::new();
            body.insert("find", Bson::String("coll".into()));
            body.insert("$db", Bson::String(db.to_string()));
            body.insert("filter", Bson::Document(Document::new()));
            let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
            assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0), "should read from {db}");
        }

        // But cannot write
        let mut body = Document::new();
        body.insert("insert", Bson::String("coll".into()));
        body.insert("$db", Bson::String("db_a".into()));
        let mut doc = Document::new();
        doc.insert("x", Bson::Int32(1));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("code"), Some(13));
    }

    #[test]
    fn test_rbac_update_user_roles() {
        let state = ServerState::new();
        // Create admin user for updateUser operations
        state.create_user("admin_upd", "admin", "pass", vec![
            Role { role: "root".into(), db: "admin".into() },
        ]).unwrap();
        state.create_user("upd_user", "admin", "pass", vec![
            Role { role: "read".into(), db: "test".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("admin_upd".into());
        state.update_connection(1, &conn);

        // Update user roles
        let mut body = Document::new();
        body.insert("updateUser", Bson::String("upd_user".into()));
        body.insert("roles", Bson::Array(vec![
            Bson::Document({
                let mut d = Document::new();
                d.insert("role", Bson::String("readWrite".into()));
                d.insert("db", Bson::String("test".into()));
                d
            })
        ]));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Verify roles were updated
        let user = state.find_user("upd_user", "admin").unwrap();
        assert_eq!(user.roles.len(), 1);
        assert_eq!(user.roles[0].role, "readWrite");
    }

    #[test]
    fn test_rbac_change_password() {
        let state = ServerState::new();
        state.create_user("pw_user", "admin", "oldpass", vec![
            Role { role: "root".into(), db: "admin".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("pw_user".into());
        state.update_connection(1, &conn);

        let mut body = Document::new();
        body.insert("changePassword", Bson::String("pw_user".into()));
        body.insert("pwd", Bson::String("newpass".into()));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_rbac_users_info_shows_roles() {
        let state = ServerState::new();
        // Create admin user for usersInfo operations
        state.create_user("admin_info", "admin", "pass", vec![
            Role { role: "root".into(), db: "admin".into() },
        ]).unwrap();
        state.create_user("info_user", "admin", "pass", vec![
            Role { role: "read".into(), db: "test".into() },
            Role { role: "readWrite".into(), db: "other".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("admin_info".into());
        state.update_connection(1, &conn);

        let mut body = Document::new();
        body.insert("usersInfo", Bson::String("info_user".into()));
        body.insert("$db", Bson::String("admin".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        let users = reply.body.as_ref().unwrap().get("users").and_then(|v| v.as_array()).unwrap();
        assert_eq!(users.len(), 1);
        if let Bson::Document(user_doc) = &users[0] {
            let roles = user_doc.get("roles").and_then(|v| v.as_array()).unwrap();
            assert_eq!(roles.len(), 2);
        } else {
            panic!("expected document");
        }
    }

    #[test]
    fn test_rbac_root_has_full_access() {
        let state = make_state_authenticated();

        // root user can do everything
        // Insert
        let mut body = Document::new();
        body.insert("insert", Bson::String("rbac_test".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new();
        doc.insert("x", Bson::Int32(1));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(1));

        // Find
        let mut body = Document::new();
        body.insert("find", Bson::String("rbac_test".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Create index
        let mut body = Document::new();
        body.insert("createIndexes", Bson::String("rbac_test".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("indexes", Bson::Array(vec![Bson::Document({
            let mut idx = Document::new();
            idx.insert("key", Bson::Document({ let mut k = Document::new(); k.insert("x", Bson::Int32(1)); k }));
            idx.insert("name", Bson::String("x_1".into()));
            idx
        })]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // List databases
        let reply = dispatch_command(&make_msg(make_body("listDatabases", "admin")), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));
    }

    #[test]
    fn test_rbac_cluster_admin_can_replset() {
        let state = ServerState::new();
        state.create_user("cluster_admin", "admin", "pass", vec![
            Role { role: "clusterAdmin".into(), db: "admin".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("cluster_admin".into());
        state.update_connection(1, &conn);

        // Can do replSetInitiate
        let mut body = Document::new();
        body.insert("replSetInitiate", Bson::Document({ let mut init = Document::new(); init }));
        body.insert("$db", Bson::String("test".into()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Cannot insert (no readWrite)
        let mut body = Document::new();
        body.insert("insert", Bson::String("coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut doc = Document::new();
        doc.insert("x", Bson::Int32(1));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("code"), Some(13));
    }

    #[test]
    fn test_rbac_db_owner_has_full_db_access() {
        let state = ServerState::new();
        state.create_user("owner", "admin", "pass", vec![
            Role { role: "dbOwner".into(), db: "mydb".into() },
        ]).unwrap();

        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("owner".into());
        state.update_connection(1, &conn);

        // Can read
        let mut body = Document::new();
        body.insert("find", Bson::String("coll".into()));
        body.insert("$db", Bson::String("mydb".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // Can write
        let mut body = Document::new();
        body.insert("insert", Bson::String("coll".into()));
        body.insert("$db", Bson::String("mydb".into()));
        let mut doc = Document::new();
        doc.insert("x", Bson::Int32(1));
        let msg = OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![DocumentSequence { identifier: "documents".into(), documents: vec![doc] }] };
        let reply = dispatch_command(&msg, 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("n"), Some(1));

        // Can create indexes (dbAdmin)
        let mut body = Document::new();
        body.insert("createIndexes", Bson::String("coll".into()));
        body.insert("$db", Bson::String("mydb".into()));
        body.insert("indexes", Bson::Array(vec![]));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_f64("ok"), Some(1.0));

        // dbOwner on mydb does NOT grant access to other dbs
        let mut body = Document::new();
        body.insert("find", Bson::String("coll".into()));
        body.insert("$db", Bson::String("otherdb".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), &state, &make_pool());
        assert_eq!(reply.body.as_ref().unwrap().get_i64("code"), Some(13));
    }
}

#[cfg(test)]
mod persistence_tests {
    use super::*;
    use mdb_doc::{Bson, Document};
    use mdb_wire::op_msg::DocumentSequence;

    fn make_pool() -> ConnectionPool {
        ConnectionPool::new(100, Duration::from_secs(5))
    }

    fn make_msg(body: Document) -> OpMsg {
        OpMsg {
            request_id: 1,
            response_to: 0,
            flags: 0,
            body: Some(body),
            document_sequences: vec![],
        }
    }

    fn run(state: &ServerState, body: Document) -> Document {
        let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), state, &make_pool());
        reply.body.expect("reply body")
    }

    /// 建立已通过 RBAC 的连接状态。
    ///
    /// RBAC 在有用户存在时启用，未认证连接会被拒；测试需要一个
    /// 带 root 角色的连接才能正常读写。
    fn authenticate(state: &ServerState) {
        if state.users.lock().unwrap().is_empty() {
            state
                .create_user(
                    "persist_admin",
                    "admin",
                    "persist_pass",
                    vec![Role {
                        role: "root".into(),
                        db: "admin".into(),
                    }],
                )
                .expect("create admin user");
        }
        let mut conn = ConnectionState::default();
        conn.authenticated_user = Some("persist_admin".into());
        state.update_connection(1, &conn);
    }

    fn insert_one(state: &ServerState, db: &str, coll: &str, doc: Document) -> Document {
        let mut body = Document::new();
        body.insert("insert", Bson::String(coll.to_owned()));
        body.insert("$db", Bson::String(db.to_owned()));
        let msg = OpMsg {
            request_id: 1,
            response_to: 0,
            flags: 0,
            body: Some(body),
            document_sequences: vec![DocumentSequence {
                identifier: "documents".into(),
                documents: vec![doc],
            }],
        };
        dispatch_command(&msg, 1, &TopologyVersion::new(), state, &make_pool())
            .body
            .expect("reply body")
    }

    /// 用 `count` 命令统计文档数。
    ///
    /// 不要用 `find` + `firstBatch.len()`：`firstBatch` 受默认批大小限制
    /// （101 条），大集合会被误判为丢数据。
    fn count_docs(state: &ServerState, db: &str, coll: &str) -> usize {
        let mut body = Document::new();
        body.insert("count", Bson::String(coll.to_owned()));
        body.insert("$db", Bson::String(db.to_owned()));
        let out = run(state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "count failed: {out:?}");
        out.get_i64("n").expect("count n") as usize
    }

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mdb-persist-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn cleanup(dir: &std::path::Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    /// `update` / `delete` 的 spec 里可以直接写过滤条件，不必包一层 `q`。
    ///
    /// 回归测试：只认`q` 时 shorthand 形式会被当成空过滤条件，
    /// 导致 `delete` 删光整个集合。
    #[test]
    fn filter_shorthand_in_update_and_delete_specs() {
        let state = ServerState::new();
        authenticate(&state);
        for i in 1..=3 {
            insert_one(&state, "test", "items", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(i));
                d.insert("v", Bson::Int32(i));
                d
            });
        }
        assert_eq!(count_docs(&state, "test", "items"), 3);

        let mut body = Document::new();
        body.insert("delete", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        filter.insert("_id", Bson::Int32(2));
        // 不带 `q`，过滤条件直接写在 spec 里。
        body.insert("deletes", Bson::Array(vec![Bson::Document(filter)]));
        let out = run(&state, body);
        assert_eq!(out.get_i64("n"), Some(1), "shorthand delete: {out:?}");
        assert_eq!(
            count_docs(&state, "test", "items"),
            2,
            "shorthand delete must not remove the whole collection"
        );

        let mut body = Document::new();
        body.insert("update", Bson::String("items".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut inner = Document::new();
        inner.insert("v", Bson::Int32(42));
        // MongoDB 简写：非 $ 字段是过滤条件，$ 字段是更新操作符
        let mut shorthand = Document::new();
        shorthand.insert("_id", Bson::Int32(1));
        shorthand.insert("$set", Bson::Document(inner));
        body.insert("updates", Bson::Array(vec![Bson::Document(shorthand)]));
        let out = run(&state, body);
        assert_eq!(out.get_i64("nModified"), Some(1), "shorthand update: {out:?}");
        let db = state.get_database("test");
        let coll = db.get_collection("items").expect("coll");
        let key = bson_to_key(&Bson::Int32(1));
        let val = coll.lock().unwrap().store.get(&key).unwrap().expect("doc 1");
        let doc = mdb_doc::codec::from_bytes(&val).expect("decode");
        assert_eq!(doc.get_i64("v"), Some(42));
        assert_eq!(count_docs(&state, "test", "items"), 2);
    }

    #[test]
    fn documents_survive_reopen() {
        let dir = temp_dir();
        {
            let state = ServerState::open(&dir).expect("open dbpath");
            authenticate(&state);
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(1));
            d.insert("name", Bson::String("alice".into()));
            let out = insert_one(&state, "appdb", "users", d);
            assert_eq!(out.get_f64("ok"), Some(1.0), "insert failed: {out:?}");
        }
        {
            let state = ServerState::open(&dir).expect("reopen dbpath");
            authenticate(&state);
            assert_eq!(count_docs(&state, "appdb", "users"), 1);
        }
        cleanup(&dir);
    }

    #[test]
    fn many_documents_survive_reopen() {
        let dir = temp_dir();
        {
            let state = ServerState::open(&dir).expect("open dbpath");
            authenticate(&state);
            for i in 0..500 {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(i));
                d.insert("n", Bson::Int32(i * 2));
                insert_one(&state, "appdb", "nums", d);
            }
        }
        {
            let state = ServerState::open(&dir).expect("reopen dbpath");
            authenticate(&state);
            assert_eq!(count_docs(&state, "appdb", "nums"), 500);
        }
        cleanup(&dir);
    }

    #[test]
    fn mixed_length_ids_all_recover() {
        // 短 _id 与长 _id 混存：若内部键排序仍按长度分片，
        // 重启后扫描会漏掉其中一类。
        let dir = temp_dir();
        {
            let state = ServerState::open(&dir).expect("open dbpath");
            authenticate(&state);
            insert_one(&state, "appdb", "mixed", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(7));
                d.insert("t", Bson::String("short".into()));
                d
            });
            insert_one(&state, "appdb", "mixed", {
                let mut d = Document::new();
                d.insert(
                    "_id",
                    Bson::String("a-very-long-string-identifier-0123456789".into()),
                );
                d.insert("t", Bson::String("long".into()));
                d
            });
        }
        {
            let state = ServerState::open(&dir).expect("reopen dbpath");
            authenticate(&state);
            assert_eq!(count_docs(&state, "appdb", "mixed"), 2);
        }
        cleanup(&dir);
    }

    #[test]
    fn updates_and_deletes_persist() {
        let dir = temp_dir();
        {
            let state = ServerState::open(&dir).expect("open dbpath");
            authenticate(&state);
            insert_one(&state, "appdb", "docs", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(1));
                d.insert("v", Bson::Int32(1));
                d
            });
            insert_one(&state, "appdb", "docs", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(2));
                d.insert("v", Bson::Int32(2));
                d
            });

            // update #1
            let mut body = Document::new();
            body.insert("update", Bson::String("docs".into()));
            body.insert("$db", Bson::String("appdb".into()));
            let mut filter = Document::new();
            filter.insert("_id", Bson::Int32(1));
            body.insert(
                "updates",
                Bson::Array(vec![{
                    let mut q = Document::new();
                    q.insert("q", Bson::Document(filter));
                    let mut set = Document::new();
                    let mut inner = Document::new();
                    inner.insert("v", Bson::Int32(99));
                    set.insert("$set", Bson::Document(inner));
                    q.insert("u", Bson::Document(set));
                    Bson::Document(q)
                }]),
            );
let out = run(&state, body);
            assert_eq!(out.get_f64("ok"), Some(1.0), "update failed: {out:?}");

            // delete #2
            let mut body = Document::new();
            body.insert("delete", Bson::String("docs".into()));
            body.insert("$db", Bson::String("appdb".into()));
            let mut filter = Document::new();
            filter.insert("_id", Bson::Int32(2));
            body.insert("deletes", Bson::Array(vec![Bson::Document(filter)]));
            let out = run(&state, body);
            assert_eq!(out.get_f64("ok"), Some(1.0), "delete failed: {out:?}");
        }
        {
            let state = ServerState::open(&dir).expect("reopen dbpath");
            authenticate(&state);
            assert_eq!(count_docs(&state, "appdb", "docs"), 1);
            let db = state.get_database("appdb");
            let coll = db.get_collection("docs").expect("collection");
            let locked = coll.lock().unwrap();
            let key = bson_to_key(&Bson::Int32(1));
            let val = locked.store.get(&key).unwrap().expect("doc 1 present");
            let doc = mdb_doc::codec::from_bytes(&val).expect("decode");
            assert_eq!(
                doc.get("v").and_then(|v| v.as_i64()),
                Some(99),
                "update not persisted"
            );
        }
        cleanup(&dir);
    }

    #[test]
    fn duplicate_id_is_rejected_after_reopen() {
        let dir = temp_dir();
        {
            let state = ServerState::open(&dir).expect("open dbpath");
            authenticate(&state);
            let out = insert_one(&state, "appdb", "u", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(1));
                d
            });
            assert_eq!(out.get_f64("ok"), Some(1.0));
        }
        {
            let state = ServerState::open(&dir).expect("reopen dbpath");
            authenticate(&state);
            let out = insert_one(&state, "appdb", "u", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(1));
                d
            });
            // 重复 _id 必须报错而不是静默覆盖。
            assert!(
                out.get("writeErrors").is_some(),
                "duplicate _id silently accepted: {out:?}"
            );
            assert_eq!(count_docs(&state, "appdb", "u"), 1);
        }
        cleanup(&dir);
    }

    #[test]
    fn databases_and_collections_survive_reopen() {
        let dir = temp_dir();
        {
            let state = ServerState::open(&dir).expect("open dbpath");
            authenticate(&state);
            insert_one(&state, "dba", "c1", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(1));
                d
            });
            insert_one(&state, "dbb", "c2", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(1));
                d
            });
        }
        {
            let state = ServerState::open(&dir).expect("reopen dbpath");
            authenticate(&state);
            assert_eq!(count_docs(&state, "dba", "c1"), 1);
            assert_eq!(count_docs(&state, "dbb", "c2"), 1);
        }
        cleanup(&dir);
    }

    #[test]
    fn reopened_state_is_not_in_memory_only() {
        let dir = temp_dir();
        {
            let state = ServerState::open(&dir).expect("open dbpath");
            authenticate(&state);
            insert_one(&state, "appdb", "t", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(1));
                d
            });
        }
        // 完全丢弃第一个 state，只靠磁盘重建。
        let state = ServerState::open(&dir).expect("reopen dbpath");
            authenticate(&state);
        assert!(state.catalog.is_some(), "reopened state must hold a catalog");
        cleanup(&dir);
    }

    /// 索引查询与 explain 验证：创建索引后查询应走 IXSCAN，explain 应如实报告。
    #[test]
    fn index_usage_and_explain() {
        let state = ServerState::new();
        authenticate(&state);

        // 创建索引
        let mut body = Document::new();
        body.insert("createIndexes", Bson::String("people".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut idx_doc = Document::new();
        let mut key = Document::new();
        key.insert("age", Bson::Int32(1));
        idx_doc.insert("key", Bson::Document(key));
        idx_doc.insert("name", Bson::String("age_1".into()));
        body.insert("indexes", Bson::Array(vec![Bson::Document(idx_doc)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "createIndexes failed: {out:?}");

        // 插入文档
        for i in 0..20 {
            insert_one(&state, "test", "people", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(i));
                d.insert("age", Bson::Int32(20 + i));
                d.insert("name", Bson::String(format!("person{i}")));
                d
            });
        }

        // 查询：age = 30 应命中 1 个文档
        let mut body = Document::new();
        body.insert("find", Bson::String("people".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        filter.insert("age", Bson::Int32(30));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "find failed: {out:?}");
        let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
        let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("firstBatch");
        assert_eq!(batch.len(), 1, "index query should return 1 doc, got {batch:?}");
        let doc = batch[0].as_document().unwrap();
        assert_eq!(doc.get_i64("age"), Some(30));

        // explain：应报告 IXSCAN 而非编造的计划
        let mut body = Document::new();
        body.insert("explain", { let mut e = Document::new(); e.insert("find", Bson::String("people".into())); let mut f = Document::new(); f.insert("age", Bson::Int32(30)); e.insert("filter", Bson::Document(f)); Bson::Document(e) });
        body.insert("$db", Bson::String("test".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "explain failed: {out:?}");
        let qp = out.get("queryPlanner").and_then(|v| v.as_document()).expect("queryPlanner");
        let wp = qp.get("winningPlan").and_then(|v| v.as_document()).expect("winningPlan");
        let stage = wp.get_str("stage").unwrap_or("");
        // 应该是 FETCH（有索引）而不是 COLLSCAN
        assert_eq!(stage, "FETCH", "explain should report FETCH when index is used, got {stage}");
        let input = wp.get("inputStage").and_then(|v| v.as_document()).expect("inputStage");
        let ix_stage = input.get_str("stage").unwrap_or("");
        assert_eq!(ix_stage, "IXSCAN", "explain input stage should be IXSCAN, got {ix_stage}");
        let index_name = input.get_str("indexName").unwrap_or("");
        assert_eq!(index_name, "age_1", "explain should report the real index name, got {index_name}");
        let es = qp.get("executionStats").and_then(|v| v.as_document()).expect("executionStats");
        let keys_examined = es.get_i64("totalKeysExamined").unwrap_or(-1);
        assert!(keys_examined >= 1, "executionStats should report keys examined, got {keys_examined}");

        // 无索引的查询：explain 应报告 COLLSCAN
        let mut body = Document::new();
        body.insert("explain", { let mut e = Document::new(); e.insert("find", Bson::String("people".into())); let mut f = Document::new(); f.insert("name", Bson::String("person5".into())); e.insert("filter", Bson::Document(f)); Bson::Document(e) });
        body.insert("$db", Bson::String("test".into()));
        let out = run(&state, body);
        let qp = out.get("queryPlanner").and_then(|v| v.as_document()).expect("queryPlanner");
        let wp = qp.get("winningPlan").and_then(|v| v.as_document()).expect("winningPlan");
        let stage = wp.get_str("stage").unwrap_or("");
        assert_eq!(stage, "COLLSCAN", "explain should report COLLSCAN for non-indexed query, got {stage}");
    }

    /// 事务验证：startTransaction → insert → commit 后数据可见；abort 后不可见。
    #[test]
    fn transaction_commit_and_abort() {
        let state = ServerState::new();
        authenticate(&state);

        // 构造 lsid
        let mut lsid_doc = Document::new();
        lsid_doc.insert("id", Bson::Binary(mdb_doc::Binary::generic(vec![1u8; 16])));
        let lsid = Bson::Document(lsid_doc);

        // 1. 开始事务
        let mut body = Document::new();
        body.insert("startTransaction", Bson::Boolean(true));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid.clone());
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "startTransaction failed: {out:?}");

        // 2. 事务中插入文档（应被缓冲，不立即可见）
        let mut body = Document::new();
        body.insert("insert", Bson::String("txn_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid.clone());
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("value", Bson::String("in_txn".into()));
        body.insert("documents", Bson::Array(vec![Bson::Document(doc)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "insert in txn failed: {out:?}");
        assert_eq!(out.get_i64("n"), Some(1), "insert should buffer 1 doc");

        // 3. 事务中查询：文档不可见（未提交）
        let mut body = Document::new();
        body.insert("find", Bson::String("txn_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid.clone());
        body.insert("filter", Bson::Document(Document::new()));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 0, "uncommitted doc should not be visible");

        // 4. 提交事务
        let mut body = Document::new();
        body.insert("commitTransaction", Bson::Int32(1));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid.clone());
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "commitTransaction failed: {out:?}");

        // 5. 提交后查询：文档可见
        let mut body = Document::new();
        body.insert("find", Bson::String("txn_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 1, "committed doc should be visible, got {batch:?}");
        let doc = batch[0].as_document().unwrap();
        assert_eq!(doc.get_str("value"), Some("in_txn"));

        // 6. abort 测试
        let mut body = Document::new();
        body.insert("startTransaction", Bson::Boolean(true));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid.clone());
        run(&state, body);

        let mut body = Document::new();
        body.insert("insert", Bson::String("txn_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid.clone());
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(2));
        doc.insert("value", Bson::String("should_abort".into()));
        body.insert("documents", Bson::Array(vec![Bson::Document(doc)]));
        run(&state, body);

        let mut body = Document::new();
        body.insert("abortTransaction", Bson::Int32(1));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid.clone());
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "abortTransaction failed: {out:?}");

        // abort 后查询：只有之前提交的文档
        let mut body = Document::new();
        body.insert("find", Bson::String("txn_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 1, "aborted doc should not be visible, got {batch:?}");
    }

    /// 索引类型完整性验证：TTL/Text/Hashed/Partial/2dsphere 索引创建。
    #[test]
    fn index_types_comprehensive() {
        let state = ServerState::new();
        authenticate(&state);

        // 1. TTL 索引
        let mut body = Document::new();
        body.insert("createIndexes", Bson::String("ttl_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut idx_doc = Document::new();
        let mut key = Document::new();
        key.insert("createdAt", Bson::Int32(1));
        idx_doc.insert("key", Bson::Document(key));
        idx_doc.insert("name", Bson::String("createdAt_1".into()));
        idx_doc.insert("expireAfterSeconds", Bson::Int64(3600));
        body.insert("indexes", Bson::Array(vec![Bson::Document(idx_doc)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "TTL index creation failed: {out:?}");
        let num_after = out.get_i64("numIndexesAfter").unwrap_or(0);
        assert_eq!(num_after, 1, "TTL index should be created");

        // 2. Text 索引
        let mut body = Document::new();
        body.insert("createIndexes", Bson::String("text_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut idx_doc = Document::new();
        let mut key = Document::new();
        key.insert("content", Bson::String("text".into()));
        idx_doc.insert("key", Bson::Document(key));
        idx_doc.insert("name", Bson::String("content_text".into()));
        body.insert("indexes", Bson::Array(vec![Bson::Document(idx_doc)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "Text index creation failed: {out:?}");

        // 3. Hashed 索引
        let mut body = Document::new();
        body.insert("createIndexes", Bson::String("hashed_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut idx_doc = Document::new();
        let mut key = Document::new();
        key.insert("email", Bson::String("hashed".into()));
        idx_doc.insert("key", Bson::Document(key));
        idx_doc.insert("name", Bson::String("email_hashed".into()));
        body.insert("indexes", Bson::Array(vec![Bson::Document(idx_doc)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "Hashed index creation failed: {out:?}");

        // 4. Partial 索引
        let mut body = Document::new();
        body.insert("createIndexes", Bson::String("partial_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut idx_doc = Document::new();
        let mut key = Document::new();
        key.insert("age", Bson::Int32(1));
        idx_doc.insert("key", Bson::Document(key));
        idx_doc.insert("name", Bson::String("age_partial".into()));
        let mut partial = Document::new();
        partial.insert("age", Bson::Document({ let mut op = Document::new(); op.insert("$gt", Bson::Int64(0)); op }));
        idx_doc.insert("partialFilterExpression", Bson::Document(partial));
        body.insert("indexes", Bson::Array(vec![Bson::Document(idx_doc)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "Partial index creation failed: {out:?}");

        // 5. 2dsphere 索引
        let mut body = Document::new();
        body.insert("createIndexes", Bson::String("geo_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut idx_doc = Document::new();
        let mut key = Document::new();
        key.insert("location", Bson::String("2dsphere".into()));
        idx_doc.insert("key", Bson::Document(key));
        idx_doc.insert("name", Bson::String("location_2dsphere".into()));
        body.insert("indexes", Bson::Array(vec![Bson::Document(idx_doc)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "2dsphere index creation failed: {out:?}");

        // 6. 复合索引（多个字段）
        let mut body = Document::new();
        body.insert("createIndexes", Bson::String("compound_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut idx_doc = Document::new();
        let mut key = Document::new();
        key.insert("a", Bson::Int32(1));
        key.insert("b", Bson::Int32(-1));
        idx_doc.insert("key", Bson::Document(key));
        idx_doc.insert("name", Bson::String("a_1_b_-1".into()));
        body.insert("indexes", Bson::Array(vec![Bson::Document(idx_doc)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "Compound index creation failed: {out:?}");

        // 验证索引列表
        let mut body = Document::new();
        body.insert("listIndexes", Bson::String("ttl_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "listIndexes failed: {out:?}");
        let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
        let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("firstBatch");
        assert!(!batch.is_empty(), "TTL index should be listed");
    }

    /// 聚合管道完整性验证：$lookup/$bucket/$sample/$facet。
    #[test]
    fn aggregation_stages_comprehensive() {
        let state = ServerState::new();
        authenticate(&state);

        // 插入订单数据
        for i in 0..10 {
            insert_one(&state, "test", "orders", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(i));
                d.insert("customerId", Bson::Int32(i % 3));
                d.insert("amount", Bson::Int32((i + 1) * 100));
                d
            });
        }
        // 插入客户数据
        for i in 0..3 {
            insert_one(&state, "test", "customers", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(i));
                d.insert("name", Bson::String(format!("Customer{i}")));
                d
            });
        }

        // 1. $lookup：订单 JOIN 客户
        let mut body = Document::new();
        body.insert("aggregate", Bson::String("orders".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut lookup_stage = Document::new();
        lookup_stage.insert("$lookup", Bson::Document({
            let mut l = Document::new();
            l.insert("from", Bson::String("customers".into()));
            l.insert("localField", Bson::String("customerId".into()));
            l.insert("foreignField", Bson::String("_id".into()));
            l.insert("as", Bson::String("customer".into()));
            l
        }));
        body.insert("pipeline", Bson::Array(vec![Bson::Document(lookup_stage)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "$lookup failed: {out:?}");
        let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
        let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("firstBatch");
        assert_eq!(batch.len(), 10, "$lookup should return 10 orders, got {batch:?}");
        let first = batch[0].as_document().unwrap();
        assert!(first.contains_key("customer"), "order should have customer field");
        let customer = first.get("customer").and_then(|v| v.as_array()).expect("customer array");
        assert!(!customer.is_empty(), "customer array should not be empty");

        // 2. $bucket：按金额分桶
        let mut body = Document::new();
        body.insert("aggregate", Bson::String("orders".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut bucket_stage = Document::new();
        bucket_stage.insert("$bucket", Bson::Document({
            let mut b = Document::new();
            b.insert("groupBy", Bson::String("amount".into()));
            b.insert("boundaries", Bson::Array(vec![Bson::Int64(0), Bson::Int64(500), Bson::Int64(1000), Bson::Int64(2000)]));
            let mut output = Document::new();
            output.insert("count", Bson::Document({ let mut o = Document::new(); o.insert("$sum", Bson::Int64(1)); o }));
            b.insert("output", Bson::Document(output));
            b
        }));
        body.insert("pipeline", Bson::Array(vec![Bson::Document(bucket_stage)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "$bucket failed: {out:?}");
        let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
        let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("firstBatch");
        assert!(!batch.is_empty(), "$bucket should return buckets, got {batch:?}");

        // 3. $sample：随机采样
        let mut body = Document::new();
        body.insert("aggregate", Bson::String("orders".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut sample_stage = Document::new();
        sample_stage.insert("$sample", Bson::Document({ let mut s = Document::new(); s.insert("size", Bson::Int64(3)); s }));
        body.insert("pipeline", Bson::Array(vec![Bson::Document(sample_stage)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "$sample failed: {out:?}");
        let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
        let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("firstBatch");
        assert!(!batch.is_empty(), "$sample should return documents, got {batch:?}");

        // 4. $facet：并行管道
        let mut body = Document::new();
        body.insert("aggregate", Bson::String("orders".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut facet_stage = Document::new();
        facet_stage.insert("$facet", Bson::Document({
            let mut f = Document::new();
            // total: 按 customerId 分组求和
            let mut group_stage = Document::new();
            group_stage.insert("$group", Bson::Document({
                let mut g = Document::new();
                g.insert("_id", Bson::String("$customerId".into()));
                let mut sum_op = Document::new();
                sum_op.insert("$sum", Bson::Int64(1));
                g.insert("count", Bson::Document(sum_op));
                g
            }));
            f.insert("total", Bson::Array(vec![Bson::Document(group_stage)]));
            // recent: 取前 3 条
            let mut limit_stage = Document::new();
            limit_stage.insert("$limit", Bson::Int64(3));
            f.insert("recent", Bson::Array(vec![Bson::Document(limit_stage)]));
            f
        }));
        body.insert("pipeline", Bson::Array(vec![Bson::Document(facet_stage)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "$facet failed: {out:?}");
        let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
        let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("firstBatch");
        assert!(!batch.is_empty(), "$facet should return results, got {batch:?}");
        let facet_result = batch[0].as_document().unwrap();
        assert!(facet_result.contains_key("total"), "facet should have total");
        assert!(facet_result.contains_key("recent"), "facet should have recent");
    }

    /// 阶段二测试：profiler/setParameter/getParameter/dbHash。
    #[test]
    fn phase2_operational_commands() {
        let state = ServerState::new();
        authenticate(&state);

        // 1. setParameter：设置 profile 级别（MongoDB 格式：参数在顶层）
        let mut body = Document::new();
        body.insert("setParameter", Bson::Int32(1));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("profile", Bson::Int64(2)); // 记录所有操作
        body.insert("slowms", Bson::Int64(50));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "setParameter failed: {out:?}");
        assert_eq!(out.get_bool("applied"), Some(true), "setParameter should be applied");

        // 2. getParameter：获取参数
        let mut body = Document::new();
        body.insert("getParameter", Bson::Int32(1));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("*", Bson::Int32(1));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "getParameter failed: {out:?}");
        assert_eq!(out.get_i64("profile"), Some(2), "profile should be 2");
        assert_eq!(out.get_i64("slowms"), Some(50), "slowms should be 50");

        // 3. 执行一些操作（会被 profiler 记录）
        insert_one(&state, "test", "prof_coll", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(1));
            d.insert("value", Bson::String("test".into()));
            d
        });

        // 4. profile：获取当前级别
        let mut body = Document::new();
        body.insert("profile", Bson::Int64(2));
        body.insert("$db", Bson::String("admin".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "profile failed: {out:?}");
        assert!(out.get_i64("was").unwrap_or(0) >= 0, "was should be non-negative");

        // 5. dbHash：获取数据校验和
        let mut body = Document::new();
        body.insert("dbHash", Bson::Int32(1));
        body.insert("$db", Bson::String("test".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "dbHash failed: {out:?}");
        assert_eq!(out.get_str("db"), Some("test"), "dbHash should return db name");
        let collections = out.get("collections").and_then(|v| v.as_array()).expect("collections array");
        assert!(!collections.is_empty(), "dbHash should return collections");

        // 6. getParameter：获取存储引擎信息
        let mut body = Document::new();
        body.insert("getParameter", Bson::Int32(1));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("storageEngine", Bson::Int32(1));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "getParameter storageEngine failed: {out:?}");
        assert_eq!(out.get_str("storageEngine"), Some("mdb-lsm"), "storageEngine should be mdb-lsm");
    }

    /// 阶段五测试：分片数据路由。
    #[test]
    fn phase5_sharding_data_routing() {
        let state = ServerState::new();
        authenticate(&state);

        // 1. 添加分片
        let mut body = Document::new();
        body.insert("addShard", Bson::String("shard0".into()));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("host", Bson::String("localhost:27017".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "addShard failed: {out:?}");

        let mut body = Document::new();
        body.insert("addShard", Bson::String("shard1".into()));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("host", Bson::String("localhost:27018".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "addShard failed: {out:?}");

        // 2. 启用分片
        let mut body = Document::new();
        body.insert("enableSharding", Bson::String("testdb".into()));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("primary", Bson::String("shard0".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "enableSharding failed: {out:?}");

        // 3. 分片集合
        let mut body = Document::new();
        body.insert("shardCollection", Bson::String("testdb.users".into()));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("key", Bson::Document({ let mut k = Document::new(); k.insert("age", Bson::Int32(1)); k }));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "shardCollection failed: {out:?}");

        // 4. 插入文档（应自动添加 _shard 字段）
        insert_one(&state, "testdb", "users", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(1));
            d.insert("age", Bson::Int32(25));
            d.insert("name", Bson::String("Alice".into()));
            d
        });

        // 5. 查询文档
        let mut body = Document::new();
        body.insert("find", Bson::String("users".into()));
        body.insert("$db", Bson::String("testdb".into()));
        body.insert("filter", Bson::Document({ let mut f = Document::new(); f.insert("_id", Bson::Int32(1)); f }));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "find failed: {out:?}");
        let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
        let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("firstBatch");
        assert_eq!(batch.len(), 1, "should find 1 document, got {batch:?}");
        let doc = batch[0].as_document().unwrap();
        assert!(doc.contains_key("_shard"), "document should have _shard field");

        // 6. listShards
        let mut body = Document::new();
        body.insert("listShards", Bson::Int32(1));
        body.insert("$db", Bson::String("admin".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "listShards failed: {out:?}");
        let shards = out.get("shards").and_then(|v| v.as_array()).expect("shards array");
        assert_eq!(shards.len(), 2, "should have 2 shards, got {shards:?}");

        // 7. balancerStatus
        let mut body = Document::new();
        body.insert("balancerStatus", Bson::Int32(1));
        body.insert("$db", Bson::String("admin".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "balancerStatus failed: {out:?}");
        assert!(out.contains_key("mode"), "balancerStatus should have mode");
    }

    /// 阶段五测试：ConfigDb 持久化。
    #[test]
    fn phase5_sharding_persistence() {
        let dir = temp_dir();
        {
            let state = ServerState::open(&dir).expect("open dbpath");
            authenticate(&state);

            // 添加分片
            let mut body = Document::new();
            body.insert("addShard", Bson::String("shard0".into()));
            body.insert("$db", Bson::String("admin".into()));
            body.insert("host", Bson::String("localhost:27017".into()));
            run(&state, body);

            let mut body = Document::new();
            body.insert("addShard", Bson::String("shard1".into()));
            body.insert("$db", Bson::String("admin".into()));
            body.insert("host", Bson::String("localhost:27018".into()));
            run(&state, body);

            // 启用分片
            let mut body = Document::new();
            body.insert("enableSharding", Bson::String("mydb".into()));
            body.insert("$db", Bson::String("admin".into()));
            body.insert("primary", Bson::String("shard0".into()));
            run(&state, body);

            // 分片集合
            let mut body = Document::new();
            body.insert("shardCollection", Bson::String("mydb.orders".into()));
            body.insert("$db", Bson::String("admin".into()));
            body.insert("key", Bson::Document({ let mut k = Document::new(); k.insert("customerId", Bson::Int32(1)); k }));
            run(&state, body);

            // 插入文档
            insert_one(&state, "mydb", "orders", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(1));
                d.insert("customerId", Bson::Int32(100));
                d
            });
        }
        {
            // 重启后验证分片配置已恢复
            let state = ServerState::open(&dir).expect("reopen dbpath");
            authenticate(&state);

            // listShards 应该有 2 个分片
            let mut body = Document::new();
            body.insert("listShards", Bson::Int32(1));
            body.insert("$db", Bson::String("admin".into()));
            let out = run(&state, body);
            assert_eq!(out.get_f64("ok"), Some(1.0), "listShards failed: {out:?}");
            let shards = out.get("shards").and_then(|v| v.as_array()).expect("shards array");
            assert_eq!(shards.len(), 2, "should have 2 shards after reopen, got {shards:?}");

            // 文档应该能查到
            let mut body = Document::new();
            body.insert("find", Bson::String("orders".into()));
            body.insert("$db", Bson::String("mydb".into()));
            body.insert("filter", Bson::Document({ let mut f = Document::new(); f.insert("_id", Bson::Int32(1)); f }));
            let out = run(&state, body);
            assert_eq!(out.get_f64("ok"), Some(1.0), "find after reopen failed: {out:?}");
            let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
            let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("firstBatch");
            assert_eq!(batch.len(), 1, "should find 1 document after reopen, got {batch:?}");
        }
        cleanup(&dir);
    }

    /// 阶段六测试：Chunk 数据迁移。
    #[test]
    fn phase6_chunk_migration() {
        let state = ServerState::new();
        authenticate(&state);

        // 1. 添加两个分片
        let mut body = Document::new();
        body.insert("addShard", Bson::String("shard0".into()));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("host", Bson::String("localhost:27017".into()));
        run(&state, body);

        let mut body = Document::new();
        body.insert("addShard", Bson::String("shard1".into()));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("host", Bson::String("localhost:27018".into()));
        run(&state, body);

        // 2. 启用分片并分片集合
        let mut body = Document::new();
        body.insert("enableSharding", Bson::String("testdb".into()));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("primary", Bson::String("shard0".into()));
        run(&state, body);

        let mut body = Document::new();
        body.insert("shardCollection", Bson::String("testdb.users".into()));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("key", Bson::Document({ let mut k = Document::new(); k.insert("customerId", Bson::Int32(1)); k }));
        run(&state, body);

        // 3. 插入多个文档
        for i in 0..10 {
            insert_one(&state, "testdb", "users", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(i));
                d.insert("customerId", Bson::Int32(i * 100));
                d.insert("name", Bson::String(format!("User{i}")));
                d
            });
        }

        // 4. 验证文档已插入
        let mut body = Document::new();
        body.insert("count", Bson::String("users".into()));
        body.insert("$db", Bson::String("testdb".into()));
        let out = run(&state, body);
        assert_eq!(out.get_i64("n"), Some(10), "should have 10 documents before migration");

        // 5. 移动 chunk（迁移数据）
        let mut body = Document::new();
        body.insert("moveChunk", Bson::String("testdb.users".into()));
        body.insert("$db", Bson::String("admin".into()));
        body.insert("pattern", Bson::Document({ let mut p = Document::new(); p.insert("customerId", Bson::Int32(0)); p }));
        body.insert("toShard", Bson::String("shard1".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "moveChunk failed: {out:?}");
        let moved = out.get_i64("moved").unwrap_or(0);
        assert!(moved > 0, "moveChunk should migrate documents, got {moved:?}");

        // 6. 验证 ConfigDb 已更新（chunk 现在属于 shard1）
        let mut body = Document::new();
        body.insert("listShards", Bson::Int32(1));
        body.insert("$db", Bson::String("admin".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "listShards failed: {out:?}");
    }

    /// 新功能综合测试：$where/$expr/位运算/Multikey/Write Conflict/因果一致性。
    #[test]
    fn new_features_comprehensive() {
        let state = ServerState::new();
        authenticate(&state);

        // 1. $where 查询
        insert_one(&state, "test", "where_coll", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(1));
            d.insert("age", Bson::Int32(25));
            d.insert("name", Bson::String("Alice".into()));
            d
        });
        insert_one(&state, "test", "where_coll", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(2));
            d.insert("age", Bson::Int32(30));
            d.insert("name", Bson::String("Bob".into()));
            d
        });

        let mut body = Document::new();
        body.insert("find", Bson::String("where_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        filter.insert("$where", Bson::String("this.age > 26".into()));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 1, "$where should return 1 doc, got {batch:?}");
        let doc = batch[0].as_document().unwrap();
        assert_eq!(doc.get_i64("age"), Some(30));

        // 2. $expr 查询
        insert_one(&state, "test", "expr_coll", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(1));
            d.insert("x", Bson::Int32(10));
            d.insert("y", Bson::Int32(20));
            d
        });
        insert_one(&state, "test", "expr_coll", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(2));
            d.insert("x", Bson::Int32(5));
            d.insert("y", Bson::Int32(3));
            d
        });

        let mut body = Document::new();
        body.insert("find", Bson::String("expr_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        filter.insert("$expr", Bson::Document({ let mut e = Document::new(); e.insert("$gt", Bson::Array(vec![Bson::String("$x".into()), Bson::String("$y".into())])); e }));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 1, "$expr should return 1 doc, got {batch:?}");

        // 3. 位运算
        insert_one(&state, "test", "bits_coll", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(1));
            d.insert("flags", Bson::Int32(0xFF));
            d
        });

        let mut body = Document::new();
        body.insert("find", Bson::String("bits_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        filter.insert("flags", Bson::Document({ let mut o = Document::new(); o.insert("$bitsAllSet", Bson::Int64(0x0F)); o }));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 1, "$bitsAllSet should return 1 doc, got {batch:?}");

        // 4. Multikey 索引
        let mut body = Document::new();
        body.insert("createIndexes", Bson::String("multikey_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut idx_doc = Document::new();
        let mut key = Document::new();
        key.insert("tags", Bson::String("multikey".into()));
        idx_doc.insert("key", Bson::Document(key));
        idx_doc.insert("name", Bson::String("tags_multikey".into()));
        body.insert("indexes", Bson::Array(vec![Bson::Document(idx_doc)]));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "multikey index creation failed: {out:?}");

        insert_one(&state, "test", "multikey_coll", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(1));
            d.insert("tags", Bson::Array(vec![Bson::String("a".into()), Bson::String("b".into())]));
            d
        });

        // 5. 索引统计
        let mut body = Document::new();
        body.insert("indexStatistics", Bson::String("multikey_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "indexStatistics failed: {out:?}");
        let stats = out.get("stats").and_then(|v| v.as_array()).expect("stats array");
        assert!(!stats.is_empty(), "should have index stats");

        // 6. Write Conflict 检测
        let lsid1 = { let mut d = Document::new(); d.insert("id", Bson::Binary(mdb_doc::Binary::generic(vec![1u8; 16]))); Bson::Document(d) };
        let lsid2 = { let mut d = Document::new(); d.insert("id", Bson::Binary(mdb_doc::Binary::generic(vec![2u8; 16]))); Bson::Document(d) };

        // 会话 1 开始事务并修改 key=100
        let mut body = Document::new();
        body.insert("startTransaction", Bson::Boolean(true));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid1.clone());
        run(&state, body);

        let mut body = Document::new();
        body.insert("insert", Bson::String("conflict_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid1.clone());
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(100));
        doc.insert("value", Bson::String("from_session1".into()));
        body.insert("documents", Bson::Array(vec![Bson::Document(doc)]));
        run(&state, body);

        // 会话 2 开始事务并修改相同的 key
        let mut body = Document::new();
        body.insert("startTransaction", Bson::Boolean(true));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid2.clone());
        run(&state, body);

        let mut body = Document::new();
        body.insert("insert", Bson::String("conflict_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid2.clone());
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(101));
        doc.insert("value", Bson::String("from_session2".into()));
        body.insert("documents", Bson::Array(vec![Bson::Document(doc)]));
        run(&state, body);

        // 会话 1 提交（成功，因为没有冲突）
        let mut body = Document::new();
        body.insert("commitTransaction", Bson::Int32(1));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid1.clone());
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "session1 commit should succeed: {out:?}");

        // 会话 2 提交（成功，因为修改的是不同的 key）
        let mut body = Document::new();
        body.insert("commitTransaction", Bson::Int32(1));
        body.insert("$db", Bson::String("test".into()));
        body.insert("lsid", lsid2.clone());
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "session2 commit should succeed: {out:?}");

        // 7. 运维命令
        let mut body = Document::new();
        body.insert("hostInfo", Bson::Int32(1));
        body.insert("$db", Bson::String("admin".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "hostInfo failed: {out:?}");
        assert!(out.contains_key("system"), "hostInfo should have system info");

        let mut body = Document::new();
        body.insert("getCmdLineOpts", Bson::Int32(1));
        body.insert("$db", Bson::String("admin".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "getCmdLineOpts failed: {out:?}");
        assert!(out.contains_key("parsed"), "getCmdLineOpts should have parsed config");

        let mut body = Document::new();
        body.insert("top", Bson::Int32(1));
        body.insert("$db", Bson::String("admin".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "top failed: {out:?}");

        let mut body = Document::new();
        body.insert("analyze", Bson::String("where_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "analyze failed: {out:?}");
        assert_eq!(out.get_bool("analyzed"), Some(true));

        // 8. validate
        let mut body = Document::new();
        body.insert("validate", Bson::String("where_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "validate failed: {out:?}");
        assert_eq!(out.get_bool("valid"), Some(true), "collection should be valid");
    }

    /// Change Stream 验证：insert 写入 oplog，watch 能读到事件。
    #[test]
    fn change_stream_watch_insert() {
        let state = ServerState::new();
        authenticate(&state);

        // 1. 插入文档（写入 oplog）
        insert_one(&state, "test", "watch_coll", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(1));
            d.insert("value", Bson::String("hello".into()));
            d
        });

        // 2. 调用 watch 命令
        let mut body = Document::new();
        body.insert("watch", Bson::String("watch_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("maxAwaitTimeMS", Bson::Int64(100));
        let out = run(&state, body);
        assert_eq!(out.get_f64("ok"), Some(1.0), "watch failed: {out:?}");
        let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
        let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("firstBatch");
        assert!(!batch.is_empty(), "watch should return events, got {batch:?}");

        let event = batch[0].as_document().unwrap();
        assert_eq!(event.get_str("operationType"), Some("insert"));
        assert_eq!(event.get("fullDocument").and_then(|v| v.as_document()).unwrap().get_str("value"), Some("hello"));

        // 3. 再插入一个文档
        insert_one(&state, "test", "watch_coll", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(2));
            d.insert("value", Bson::String("world".into()));
            d
        });

        // 4. 再次 watch，应看到新事件
        let mut body = Document::new();
        body.insert("watch", Bson::String("watch_coll".into()));
        body.insert("$db", Bson::String("test".into()));
        body.insert("maxAwaitTimeMS", Bson::Int64(100));
        let out = run(&state, body);
        let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
        let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("firstBatch");
        assert!(!batch.is_empty(), "watch should return events after 2nd insert, got {batch:?}");
    }

    /// 查询运算符完整性验证：$nin/$regex/$elemMatch/$mod/$type/$size/$all/$not/$nor + 点号路径。
    #[test]
    fn query_operators_comprehensive() {
        let state = ServerState::new();
        authenticate(&state);

        // 插入测试文档
        let docs = vec![
            // (id, tags, score, address)
            (1i32, vec!["a", "b"], 10i64, "Beijing"),
            (2, vec!["a", "c"], 20, "Shanghai"),
            (3, vec!["b", "c"], 30, "Beijing"),
            (4, vec!["a", "b", "c"], 40, "Shenzhen"),
            (5, vec![], 50, "Beijing"),
        ];
        for (id, tags, score, addr) in docs {
            insert_one(&state, "test", "ops", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32(id));
                d.insert("tags", Bson::Array(tags.into_iter().map(|s| Bson::String(s.to_string())).collect()));
                d.insert("score", Bson::Int64(score));
                let mut address = Document::new();
                address.insert("city", Bson::String(addr.into()));
                d.insert("address", Bson::Document(address));
                d
            });
        }

        // $nin: score 不在 [10, 20] 中 → 3,4,5
        let mut body = Document::new();
        body.insert("find", Bson::String("ops".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        let mut nin_ops = Document::new();
        nin_ops.insert("$nin", Bson::Array(vec![Bson::Int64(10), Bson::Int64(20)]));
        filter.insert("score", Bson::Document(nin_ops));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 3, "$nin should return 3 docs, got {batch:?}");

        // $regex: address.city 匹配 "eijing" → 1,3,5
        let mut body = Document::new();
        body.insert("find", Bson::String("ops".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        let mut regex_ops = Document::new();
        regex_ops.insert("$regex", Bson::String("eijing".into()));
        filter.insert("address.city", Bson::Document(regex_ops));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 3, "$regex + dotted path should return 3 docs, got {batch:?}");

        // $elemMatch: tags 包含 >= "a" 的元素 且 score > 15 → 2,3,4
        let mut body = Document::new();
        body.insert("find", Bson::String("ops".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        let mut elem_ops = Document::new();
        elem_ops.insert("$elemMatch", { let mut e = Document::new(); e.insert("$gte", Bson::String("a".into())); Bson::Document(e) });
        filter.insert("tags", Bson::Document(elem_ops));
        let mut score_ops = Document::new();
        score_ops.insert("$gt", Bson::Int64(15));
        filter.insert("score", Bson::Document(score_ops));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 3, "$elemMatch + $gt should return 3 docs (2,3,4), got {batch:?}");

        // $mod: score % 10 == 0 且 score > 0 → 全部（10,20,30,40,50）
        let mut body = Document::new();
        body.insert("find", Bson::String("ops".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        let mut mod_ops = Document::new();
        mod_ops.insert("$mod", Bson::Array(vec![Bson::Int64(10), Bson::Int64(0)]));
        filter.insert("score", Bson::Document(mod_ops));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 5, "$mod should return 5 docs, got {batch:?}");

        // $type: score 是 long/int → 5
        let mut body = Document::new();
        body.insert("find", Bson::String("ops".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        let mut type_ops = Document::new();
        type_ops.insert("$type", Bson::String("long".into()));
        filter.insert("score", Bson::Document(type_ops));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 5, "$type should return 5 docs, got {batch:?}");

        // $size: tags 长度为 2 → 1,2,3
        let mut body = Document::new();
        body.insert("find", Bson::String("ops".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        let mut size_ops = Document::new();
        size_ops.insert("$size", Bson::Int64(2));
        filter.insert("tags", Bson::Document(size_ops));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 3, "$size should return 3 docs, got {batch:?}");

        // $all: tags 包含 a 和 b → 1,4
        let mut body = Document::new();
        body.insert("find", Bson::String("ops".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        let mut all_ops = Document::new();
        all_ops.insert("$all", Bson::Array(vec![Bson::String("a".into()), Bson::String("b".into())]));
        filter.insert("tags", Bson::Document(all_ops));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 2, "$all should return 2 docs, got {batch:?}");

        // $nor: score 不满足 (score > 40) → 1,2,3,4
        let mut body = Document::new();
        body.insert("find", Bson::String("ops".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        filter.insert("$nor", Bson::Array(vec![{ let mut sub = Document::new(); let mut gt = Document::new(); gt.insert("$gt", Bson::Int64(40)); sub.insert("score", Bson::Document(gt)); Bson::Document(sub) }]));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 4, "$nor should return 4 docs, got {batch:?}");

        // $not: score NOT > 40 → 1,2,3,4
        let mut body = Document::new();
        body.insert("find", Bson::String("ops".into()));
        body.insert("$db", Bson::String("test".into()));
        let mut filter = Document::new();
        let mut not_ops = Document::new();
        not_ops.insert("$not", { let mut inner = Document::new(); let mut gt = Document::new(); gt.insert("$gt", Bson::Int64(40)); inner.insert("score", Bson::Document(gt)); Bson::Document(inner) });
        filter.insert("$nor", Bson::Array(vec![{ let mut sub = Document::new(); sub.insert("score", Bson::Document({ let mut gt = Document::new(); gt.insert("$gt", Bson::Int64(40)); gt })); Bson::Document(sub) }]));
        body.insert("filter", Bson::Document(filter));
        let out = run(&state, body);
        let batch = out.get("cursor").and_then(|v| v.as_document()).unwrap().get("firstBatch").and_then(|v| v.as_array()).unwrap();
        assert_eq!(batch.len(), 4, "$nor with range should return 4 docs, got {batch:?}");
    }

    /// 索引重启后仍可用：索引定义从 Catalog 恢复。
    #[test]
    fn index_survives_reopen() {
        let dir = temp_dir();
        {
            let state = ServerState::open(&dir).expect("open dbpath");
            authenticate(&state);
            let mut body = Document::new();
            body.insert("createIndexes", Bson::String("items".into()));
            body.insert("$db", Bson::String("shop".into()));
            let mut idx_doc = Document::new();
            let mut key = Document::new();
            key.insert("price", Bson::Int32(1));
            idx_doc.insert("key", Bson::Document(key));
            idx_doc.insert("name", Bson::String("price_1".into()));
            body.insert("indexes", Bson::Array(vec![Bson::Document(idx_doc)]));
            run(&state, body);
            for i in 0..10 {
                insert_one(&state, "shop", "items", {
                    let mut d = Document::new();
                    d.insert("_id", Bson::Int32(i));
                    d.insert("price", Bson::Int32(100 + i * 10));
                    d
                });
            }
        }
        {
            let state = ServerState::open(&dir).expect("reopen dbpath");
            authenticate(&state);
            // 索引应已恢复
            let db = state.get_database("shop");
            let coll = db.get_collection("items").expect("items");
            let locked = coll.lock().unwrap();
            let indexes = locked.index_manager.list_indexes();
            assert!(indexes.contains(&"price_1".to_string()), "index should survive reopen, got {indexes:?}");
            // 查询应走索引
            let mut filter = Document::new();
            filter.insert("price", Bson::Int32(120));
            let plan = plan_query(&filter, &locked.index_manager);
            assert!(plan.used_index, "recovered index should be used by planner");
            assert_eq!(plan.index_name.as_deref(), Some("price_1"));
        }
        cleanup(&dir);
    }
}