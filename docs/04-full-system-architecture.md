# MDB 全系统架构设计

| 项 | 值 |
|---|---|
| 文档版本 | 0.1.0 |
| 编写日期 | 2026-09-29 |
| 涵盖模块 | 存储引擎、CRUD、查询引擎、索引、事务、副本集、分片、Change Stream、GridFS、Compass |

---

## 0. 系统总览

```text
┌─────────────────────────────────────────────────────────────────┐
│                    MDB Compass (Electron + React)                │
├─────────────────────────────────────────────────────────────────┤
│                         mdb-server                              │
│  ┌──────────┬──────────┬──────────┬──────────┬────────────────┐ │
│  │  insert  │  find    │  update  │  delete  │ aggregate      │ │
│  └────┬─────┴────┬─────┴────┬─────┴────┬─────┴───────┬────────┘ │
│       │          │          │          │              │          │
│  ┌────┴──────────┴──────────┴──────────┴──────────────┴────────┐ │
│  │                      mdb-query                             │ │
│  │  (查询解析 → 优化 → 执行器 → 游标管理 → 聚合管道)           │ │
│  └────┬────────────────────────────────────────────────────────┘ │
│       │                                                         │
│  ┌────┴─────────────────────────────────────────────────────┐    │
│  │                      mdb-txn                             │    │
│  │  (MVCC 快照隔离, 写冲突检测, 2PC, 会话管理)              │    │
│  └────┬──────────────────────┬──────────────────────────────┘    │
│       │                      │                                  │
│  ┌────┴──────────┐    ┌──────┴──────┐                           │
│  │   mdb-index   │    │  mdb-repl   │                           │
│  │ (B-Tree,      │    │ (Raft 选举, │                           │
│  │  TTL, Unique, │    │  Oplog,     │                           │
│  │  Text, 2dsph) │    │  ChangeStr) │                           │
│  └────┬──────────┘    └──────┬──────┘                           │
│       │                      │                                  │
│  ┌────┴──────────────────────┴──────────────────────────────┐    │
│  │                    mdb-storage                           │    │
│  │  (LSM-Tree: WAL + MemTable + SSTable + Compaction)       │    │
│  └──────────────────────────────────────────────────────────┘    │
│       │                                                         │
│  ┌────┴──────────┐                                              │
│  │   mdb-router  │  (分片路由, 配置服务器, 均衡器)              │
│  └───────────────┘                                              │
├─────────────────────────────────────────────────────────────────┤
│  mdb-common (错误码, 配置, 日志, 指标)                          │
│  mdb-crypto (SM3, SM4, CRC32C, SCRAM, HMAC)                    │
│  mdb-doc (BSON 编解码, 文档模型)                                │
│  mdb-wire (MongoDB Wire Protocol)                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. 存储引擎 (mdb-storage)

### 1.1 架构

LSM-Tree: WAL → MemTable → L0 SST → L1..Ln SST (Compaction)

### 1.2 核心类型

```rust
// 对外接口
pub trait StorageEngine: Send + Sync {
    fn get(&self, cf: &str, key: &[u8], snapshot: u64) -> Result<Option<Value>>;
    fn put(&self, cf: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn delete(&self, cf: &str, key: &[u8]) -> Result<()>;
    fn scan(&self, cf: &str, start: &[u8], end: &[u8], snapshot: u64) -> Result<ScanIterator>;
    fn flush(&self, cf: &str) -> Result<()>;
    fn create_cf(&self, name: &str) -> Result<()>;
    fn drop_cf(&self, name: &str) -> Result<()>;
    fn snapshot(&self) -> u64;
    fn compact(&self, cf: &str) -> Result<()>;
}

// 内部类型
pub struct Value { pub data: Vec<u8>, pub seqno: u64, pub tombstone: bool }
pub struct Snapshot { pub seqno: u64 }
pub struct ScanIterator { /* 流式迭代器 */ }
```

### 1.3 写入路径

```
put(key, value)
  → 分配全局递增 seqno
  → WAL 追加 (seqno + key + value + crc32c)
  → 组提交窗口等待
  → fsync(WAL) → 写确认
  → MemTable.insert(key, seqno, value)
  → 若 MemTable 超阈值 → 冻结 + 异步刷盘为 L0 SST
```

### 1.4 读取路径

```
get(key, snapshot)
  → 查 MemTable (BTreeMap)
  → 查 L0 SST (Bloom filter → Index Block → Data Block)
  → 查 L1..Ln SST (区间剪枝)
  → 返回第一个 seqno <= snapshot 的可见版本
```

---

## 2. CRUD 操作

### 2.1 insert 命令

```
insert: <collection>
documents: [<doc1>, <doc2>, ...]
ordered: true/false
writeConcern: { w: 1 }
```

流程：
1. 校验文档有效性（BSON 大小、字段名合法）
2. 自动注入 `_id`（若缺失）
3. 逐文档写入存储引擎（ordered 时遇错停止）
4. 返回 `{ ok, n, writeErrors, writeConcernError }`

### 2.2 find 命令

```
find: <collection>
filter: { <query> }
sort: { <field>: 1/-1 }
projection: { <field>: 1 }
limit: N
skip: N
batchSize: N
noCursorTimeout: true
```

流程：
1. 解析查询 → 查询计划（索引选择）
2. 创建游标（tailable 或一次性）
3. 返回首批结果 + cursorId
4. 后续 getMore 获取更多

### 2.3 update 命令

```
update: <collection>
updates: [
  { q: <query>, u: <update>, upsert: true, multi: true }
]
```

支持操作符：`$set`, `$unset`, `$inc`, `$push`, `$pull`, `$rename`, `$min`, `$max`, `$mul`, `$currentDate`, `$addToSet`

### 2.4 delete 命令

```
delete: <collection>
deletes: [
  { q: <query>, limit: 0/1 }
]
```

---

## 3. 查询引擎 (mdb-query)

### 3.1 查询操作符

**比较**: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`
**逻辑**: `$and`, `$or`, `$not`, `$nor`, `$exists`, `$type`
**元素**: `$regex`, `$mod`
**数组**: `$all`, `$elemMatch`, `$size`
**位运算**: `$bit`
**地理**: `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere`

### 3.2 聚合管道

```rust
pub enum PipelineStage {
    Match(Document),        // $match
    Group(GroupSpec),       // $group
    Sort(SortSpec),         // $sort
    Project(Document),      // $project
    Limit(i64),             // $limit
    Skip(i64),              // $skip
    Unwind(String),         // $unwind
    Lookup(LookupSpec),     // $lookup
    AddFields(Document),    // $addFields
    Bucket(BucketSpec),     // $bucket
    Count(String),          // $count
    Out(String),            // $out
    Merge(MergeSpec),       // $merge
    Facet(FacetSpec),       // $facet
    SetWindowFields(WFSpec),// $setWindowFields
}

pub struct AggregationPipeline {
    stages: Vec<PipelineStage>,
}
```

### 3.3 游标

```rust
pub struct Cursor {
    id: i64,
    namespace: String,
    first_batch: Vec<Document>,
    exhausted: bool,
    batch_size: i32,
    no_timeout: bool,
}
```

---

## 4. 索引 (mdb-index)

### 4.1 索引类型

| 类型 | 说明 | MongoDB 兼容 |
|------|------|-------------|
| B-Tree | 默认，支持点查+范围+排序 | ✅ |
| Unique | 唯一约束 | ✅ |
| TTL | 按日期字段自动过期 | ✅ |
| Multikey | 数组字段自动多键 | ✅ |
| Text | 全文搜索 | ✅ |
| 2dsphere | 地理空间 | ✅ |
| Hashed | 哈希分片键 | ✅ |
| Partial | 条件索引 | ✅ |

### 4.2 B-Tree 结构

```rust
pub struct BTreeIndex {
    name: String,
    key_spec: IndexKeySpec,
    root: Option<NodeId>,
    pages: PageCache,
    // 支持重复键（多键索引）
}

struct BTreeNode {
    keys: Vec<InternalKey>,
    children: Vec<NodeId>,
    is_leaf: bool,
    next: Option<NodeId>,  // 叶子链表，支持范围扫描
}

struct InternalKey {
    key: Vec<u8>,
    seqno: u64,
    ptr: RecordPtr,
}
```

### 4.3 索引管理命令

```javascript
db.collection.createIndex({ field: 1 }, { unique: true, expireAfterSeconds: 3600 })
db.collection.dropIndex("index_name")
db.collection.getIndexes()
db.collection.reIndex()
```

---

## 5. 事务 (mdb-txn)

### 5.1 MVCC 快照隔离

```rust
pub struct TransactionManager {
    active_txns: HashMap<TxnId, Transaction>,
    next_txn_id: AtomicU64,
    snapshot_manager: SnapshotManager,
}

pub struct Transaction {
    id: TxnId,
    snapshot_seqno: u64,
    write_set: Vec<WriteOp>,
    read_set: HashSet<Key>,
    state: TxnState,
}

pub enum TxnState {
    Active,
    Preparing,    // 2PC Phase 1
    Committed,
    Aborted,
}
```

### 5.2 写冲突检测

- 基于 key 级别的乐观锁
- 冲突时返回 `WriteConflict` 错误
- 驱动自动重试整个事务

### 5.3 会话管理

```
startSession → startTransaction → commitTransaction / abortTransaction
```

---

## 6. 副本集 (mdb-repl)

### 6.1 Raft 选举

```rust
pub struct RaftNode {
    id: NodeId,
    state: RaftState,       // Follower / Candidate / Leader
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
}

pub enum RaftState { Follower, Candidate, Leader }
```

### 6.2 Oplog

```rust
pub struct OplogEntry {
    ts: Timestamp,       // { t: seconds, i: inc }
    op: OpType,          // "i"=insert, "u"=update, "d"=delete, "c"=command
    ns: String,          // "db.collection"
    o: Document,         // 文档内容或更新表达式
    o2: Option<Document>, // update 的查询条件
    ui: ObjectId,        // 唯一标识
}

pub enum OpType { Insert, Update, Delete, Command }
```

### 6.3 复制流

- Primary 写入 oplog → Secondary 拉取 oplog → 重放
- 心跳检测：每 2 秒发送 `hello`
- 延迟告警：`secondaryDelayMembs` 超阈值

### 6.4 故障转移

```
Primary 失败 → 多数 Secondary 发起选举 → 新 Primary 选出
→ 旧 Primary 恢复后降级为 Secondary
```

---

## 7. 分片 (mdb-router)

### 7.1 架构

```text
┌──────────┐
│ mdb-cli  │  (mongosh 等效)
└────┬─────┘
     │
┌────┴─────┐
│mongos/mdb│  (mdb-router, 路由器)
└────┬─────┘
     │
┌────┴────┬────────┬────────┐
│ Shard 1 │ Shard 2│ Shard 3│  (mdb-server 实例)
└─────────┴────────┴────────┘
     │
┌────┴─────────────────────┐
│  Config Server (mdb)     │  (元数据存储)
│  (chunk 分布, shard key) │
└──────────────────────────┘
```

### 7.2 Shard Key

```javascript
sh.shardCollection("db.collection", { field: "hashed" })     // 哈希分片
sh.shardCollection("db.collection", { field: 1 })            // 范围分片
sh.shardCollection("db.collection", { field1: 1, field2: 1 })// 复合分片
```

### 7.3 路由逻辑

```rust
pub struct ShardRouter {
    config_db: ConfigDb,          // 配置数据库
    chunk_map: ChunkMap,          // chunk → shard 映射
    routing_table: RoutingTable,  // 缓存的路由表
}

impl ShardRouter {
    pub fn route(&self, ns: &str, key: &Bson) -> Result<ShardId>;
    pub fn target_chunks(&self, ns: &str, key_range: &KeyRange) -> Vec<ChunkId>;
}
```

### 7.4 均衡器

- 自动迁移 chunk 使各 shard 负载均衡
- 迁移期间使用 `moveChunk` 命令
- 支持 `disableBalancing` / `enableBalancing`

---

## 8. Change Stream

### 8.1 基于 Oplog 的变更流

```javascript
const cs = db.collection.watch([], { resumeAfter: resumeToken })
cs.on("change", (change) => {
    // change.operationType: "insert" | "update" | "replace" | "delete" | ...
    // change.fullDocument: 完整文档
    // change.documentKey: { _id: ... }
    // change.resumeToken: { _data: "..." }
})
```

### 8.2 恢复令牌

```rust
pub struct ResumeToken {
    data: Document,  // { ts: Timestamp, txnNumber: i64, _id: ObjectId }
}

impl ResumeToken {
    pub fn from_change_event(event: &ChangeStreamEvent) -> Self;
    pub fn to_bson(&self) -> Bson;
    pub fn from_bson(b: &Bson) -> Result<Self>;
}
```

### 8.3 Change Stream 事件

```rust
pub struct ChangeStreamEvent {
    _id: ResumeToken,
    operation_type: OperationType,
    full_namespace: String,
    destination_namespace: Option<String>,
    full_document: Option<Document>,
    document_key: Document,
    update_description: Option<UpdateDescription>,
    cluster_time: Timestamp,
    txn_number: Option<i64>,
    wall_time: Option<i64>,
}
```

---

## 9. GridFS

### 9.1 集合结构

```javascript
// fs.files: 文件元数据
{
    _id: ObjectId,
    length: i64,          // 文件总大小
    chunkSize: i32,       // 分块大小（默认 255KB）
    filename: String,
    contentType: String,
    metadata: Document,
    uploadDate: DateTime
}

// fs.chunks: 文件数据块
{
    _id: ObjectId,
    files_id: ObjectId,   // 关联 fs.files._id
    n: i32,               // 块序号（从 0 开始）
    data: Binary           // 块数据
}
```

### 9.2 API

```javascript
// 上传
db.fs.files.insertOne({ filename: "test.pdf", length: 1024, chunkSize: 255000, ... })
db.fs.chunks.insertOne({ files_id: ..., n: 0, data: BinData(0, "...") })

// 下载
const cursor = db.fs.chunks.find({ files_id: file_id }).sort({ n: 1 })
// 流式拼接各块数据

// 流式上传（bucket 对象）
const bucket = new GridFSBucket(db)
bucket.openUploadStream("file.txt")  // 返回可写流
bucket.openDownloadStream(fileId)    // 返回可读流
```

---

## 10. MDB Compass (Electron + React)

### 10.1 技术栈

- **后端**: Electron + Node.js
- **前端**: React 18 + TypeScript
- **UI 库**: Ant Design 5.x
- **状态管理**: Zustand
- **数据库驱动**: mongodb (Node.js driver, 兼容 MDB)
- **图表**: Recharts (性能指标)
- **终端**: xterm.js ( mdb shell)

### 10.2 功能模块

| 模块 | 说明 |
|------|------|
| 连接管理 | 连接/断开 MDB 实例，管理连接配置 |
| 数据库浏览 | 树形展示数据库/集合/索引 |
| 文档查看 | JSON/BSON 查看器，支持编辑 |
| 查询构建器 | 可视化查询构建，支持所有操作符 |
| 索引管理 | 创建/删除/查看索引，分析查询计划 |
| 聚合管道构建器 | 拖拽式聚合管道构建 |
| 性能监控 | 实时指标图表（连接数、操作数、存储使用） |
| 复制集状态 | 显示 primary/secondary 状态、延迟 |
| 分片状态 | 显示 chunk 分布、balancer 状态 |
| GridFS 管理 | 上传/下载/预览文件 |

### 10.3 项目结构

```
mdb-compass/
├── package.json
├── electron/
│   ├── main.ts              # Electron 主进程
│   ├── preload.ts           # 预加载脚本
│   └── ipc-handlers.ts      # IPC 处理器
├── src/
│   ├── App.tsx
│   ├── components/
│   │   ├── ConnectionPanel/
│   │   ├── DatabaseTree/
│   │   ├── DocumentViewer/
│   │   ├── QueryBuilder/
│   │   ├── IndexManager/
│   │   ├── AggregationBuilder/
│   │   ├── PerformanceCharts/
│   │   ├── ReplicaSetStatus/
│   │   ├── ShardingStatus/
│   │   └── GridFSManager/
│   ├── stores/
│   │   ├── connectionStore.ts
│   │   ├── databaseStore.ts
│   │   └── queryStore.ts
│   └── services/
│       └── mdbService.ts
└── electron-builder.yml
```

---

## 11. 命令兼容矩阵

| 命令 | MongoDB | MDB | 状态 |
|------|---------|-----|------|
| insert | ✅ | ✅ | Phase 2 |
| find | ✅ | ✅ | Phase 2 |
| update | ✅ | ✅ | Phase 2 |
| delete | ✅ | ✅ | Phase 2 |
| aggregate | ✅ | ✅ | Phase 3 |
| createIndex | ✅ | ✅ | Phase 4 |
| dropIndex | ✅ | ✅ | Phase 4 |
| getIndexes | ✅ | ✅ | Phase 4 |
| startTransaction | ✅ | ✅ | Phase 5 |
| commitTransaction | ✅ | ✅ | Phase 5 |
| abortTransaction | ✅ | ✅ | Phase 5 |
| replSetInitiate | ✅ | ✅ | Phase 6 |
| shardCollection | ✅ | ✅ | Phase 7 |
| watch | ✅ | ✅ | Phase 8 |
| gridfs.* | ✅ | ✅ | Phase 9 |
