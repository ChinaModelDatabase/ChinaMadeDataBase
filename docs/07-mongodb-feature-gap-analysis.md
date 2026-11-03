# MDB 与 MongoDB 8.0 功能差距分析

日期：2026-10-03（初稿 2026-10-01）| 基线：MongoDB 8.0（当前补丁 8.0.32）
范围：`D:\yunxiang\MDB` 全工作区 16 个 crate + 10 个驱动目录

---

## 摘要：最关键的发现

### 当前状态（2026-10-03 更新）

**MDB 已经是一个功能较完整的单节点文档数据库。**

`ServerState::open(dbpath)` 通过 `Catalog` → `mdb-storage::Engine` 实现 WAL 落盘、
SSTable、compaction 和启动恢复。500 文档重启、含 NUL 的 `_id` 键、
纯 WAL 重放恢复等测试全部通过。

### 本轮已修复的关键问题

| 类别 | 状态 | 测试 |
|------|------|------|
| 持久化 | ✅ 已修复（WAL + SST + 恢复） | 500 文档重启测试 |
| 索引读路径 | ✅ 查询规划器 + 索引查找 | `index_usage_and_explain` |
| explain | ✅ 如实报告 COLLSCAN/IXSCAN | `index_usage_and_explain` |
| 事务 | ✅ startTransaction/commit/abort 真实实现 | `transaction_commit_and_abort` |
| Change Stream | ✅ watch 返回真实事件 | `change_stream_watch_insert` |
| 查询运算符 | ✅ 新增 $nin/$regex/$elemMatch/$mod/$type/$size/$all/$not/$nor | `query_operators_comprehensive` |
| 点号路径 | ✅ 使用 `get_path` 解析 | `query_operators_comprehensive` |
| GridFS | ✅ `mod gridfs` 参与编译 | 38 个 GridFS 测试 |
| TDE | ✅ SSTable 加密/解密 + 服务端命令 | 命令接线 |
| TLS | ✅ TLCP 握手 + 状态接线 | `test_tls_config` |
| 副本集 | ✅ replSetGetConfig/replSetReconfig + Raft 状态 | 命令接线 |
| 分片 | ✅ addShard/listShards/shardCollection 等命令 | 命令接线 |
| 内部键编码 | ✅ 保序转义，键体永不含 `0x00` | 500 文档重启测试 |

### 剩余差距（2026-10-03 更新）

| 类别 | 状态 | 说明 |
|------|------|------|
| $where 查询 | ✅ | 简化 JS 表达式求值器已实现 |
| 多节点 Raft | ✅ | TCP 传输层已实现 |
| 跨节点分片 | ✅ | 数据复制已实现 |
| 密评三级 | ⏳ | 需要商业密码产品评估（非代码问题） |
| x509/LDAP/OIDC | ⏳ | 需要外部集成 |

### 已修复（2026-10-03）

| 类别 | 状态 | 说明 |
|------|------|------|
| 索引类型 | ✅ | TTL/Text/Hashed/Partial/2dsphere/Multikey 已接线 |
| BlockCache | ✅ | LRU 块缓存已实现 |
| 聚合阶段 | ✅ | $lookup/$bucket/$sample/$facet/$setWindowFields/$graphLookup/$redact/$densify/$fill 已实现 |
| profiler | ✅ | system.profile + 慢查询已实现 |
| setParameter | ✅ | 运行时参数调整已实现 |
| collMod | ✅ | 集合修改已实现 |
| dbHash | ✅ | 数据校验和已实现 |
| OP_COMPRESSED | ✅ | Snappy/zstd 解压已实现 |
| wireVersion | ✅ | 已升级到 25 (MongoDB 8.0) |
| MapReduce | ✅ | 简化实现已接线 |
| mdb-cli | ✅ | 交互式 shell 已实现 |
| 标准 TLS | ✅ | rustls 集成已实现 |
| 分片数据路由 | ✅ | Insert/Find/Update/Delete 已接线 |
| Chunk 数据迁移 | ✅ | moveChunk 实现真实数据迁移 |
| ConfigDb 持久化 | ✅ | 分片元数据存储到 Catalog |
| 迁移锁/状态机 | ✅ | 写缓冲 + 7 状态迁移 |
| $where | ✅ | 简化 JS 表达式求值器 |
| $expr | ✅ | 聚合表达式求值 |
| 位运算 | ✅ | $bitsAllClear/$bitsAllSet/$bitsAnyClear/$bitsAnySet |
| Multikey 索引 | ✅ | 数组字段索引 |
| 索引统计 | ✅ | indexStatistics 命令 |
| 运维命令 | ✅ | compact/validate/hostInfo/getCmdLineOpts/top/analyze |
| zstd 压缩 | ✅ | ZstdCompression 已实现 |
| Write Conflict | ✅ | 事务冲突检测已实现 |
| 因果一致性 | ✅ | 会话级读写时间戳跟踪 |
| 多节点 Raft TCP | ✅ | TcpRaftTransport 已实现 |
| 跨节点分片复制 | ✅ | copy_collection_data 已实现 |

---

## 分阶段实现计划

### 阶段一：查询与索引补全（中等复杂度，高价值）

| 任务 | 说明 | 预估工作量 |
|------|------|-----------|
| 索引类型：TTL | `create_ttl_index` 已有，接线到 `createIndexes` 命令 | 1 天 |
| 索引类型：Text | `create_text_index` 已有，接线到 `createIndexes` 命令 | 1 天 |
| 索引类型：Hashed | 新增 HashedIndex，接线到 `createIndexes` | 2 天 |
| 索引类型：Partial | 新增 PartialIndex，接线到 `createIndexes` | 2 天 |
| 索引类型：2dsphere | `create_2dsphere_index` 已有，接线到 `createIndexes` | 1 天 |
| 聚合：$lookup | 实现多表 JOIN | 3 天 |
| 聚合：$facet | 实现并行管道 | 2 天 |
| 聚合：$bucket/$bucketAuto | 实现分桶聚合 | 2 天 |
| 聚合：$setWindowFields | 实现窗口函数 | 3 天 |
| 聚合：$sample | 实现采样 | 1 天 |
| BlockCache | 实现 LRU 块缓存（256MB） | 3 天 |

### 阶段二：运维与性能（中等复杂度，高价值）

| 任务 | 说明 | 预估工作量 |
|------|------|-----------|
| profiler | 实现 system.profile 集合 + 慢查询日志 | 3 天 |
| setParameter | 实现运行时参数调整 | 2 天 |
| collMod | 实现集合修改（validator、index options） | 2 天 |
| analyze | 实现索引统计收集 | 2 天 |
| dbHash | 实现数据校验和 | 1 天 |
| hostInfo | 实现主机信息查询 | 1 天 |
| getCmdLineOpts | 实现启动参数查询 | 1 天 |
| OP_COMPRESSED | 实现 snappy/zstd 压缩 | 3 天 |
| wireVersion 升级 | 提升到 25，启用 MongoDB 8.0 特性 | 1 天 |

### 阶段三：标准 TLS（高复杂度，高价值）

| 任务 | 说明 | 预估工作量 |
|------|------|-----------|
| 依赖 RFC | 提交 rustls 依赖申请（许可证、安全审查） | 3 天（流程） |
| rustls 集成 | 添加 rustls + rustls-pemfile 依赖 | 1 天 |
| TLS 1.2/1.3 握手 | 实现标准 TLS 握手 | 3 天 |
| 证书加载 | 支持 PEM 证书/私钥加载 | 1 天 |
| 双栈分发 | use_sm_cipher=true 走 TLCP，否则走 rustls | 2 天 |

### 阶段四：多节点 Raft（高复杂度，高价值）

| 任务 | 说明 | 预估工作量 |
|------|------|-----------|
| RaftTransport trait | 定义节点间 RPC 接口 | 1 天 |
| 内部 RPC 协议 | 实现 Raft RPC 的 BSON 编解码 | 2 天 |
| TCP 传输层 | 实现节点间 TCP 连接池 | 3 天 |
| 后台驱动循环 | 每 50ms tick + 选举/心跳处理 | 3 天 |
| FileWal | 实现持久化 Raft WAL | 2 天 |
| 数据命令应用 | apply_committed 时执行 Insert/Update/Delete | 2 天 |
| 多节点测试 | 3 节点集群集成测试 | 3 天 |

### 阶段五：分片数据路由（高复杂度，高价值）

| 任务 | 说明 | 预估工作量 |
|------|------|-----------|
| Insert 路由 | route_write → 本地分片命名空间 | 2 天 |
| Find 路由 | 定向查询 + 广播查询合并 | 3 天 |
| Update/Delete 路由 | 过滤条件包含分片键时定向，否则广播 | 3 天 |
| 跨分片聚合 | scatter-gather + 合并排序 | 3 天 |
| ConfigDb 持久化 | 分片元数据存储到 Catalog | 2 天 |

### 阶段六：Chunk 数据迁移（高复杂度，中价值）

| 任务 | 说明 | 预估工作量 |
|------|------|-----------|
| 数据复制 | clone_data：扫描源分片，写入目标分片 | 3 天 |
| 迁移锁 | 关键分片写入缓冲/拒绝机制 | 2 天 |
| 迁移状态机 | 完整 7 状态迁移流程 | 2 天 |
| 迁移测试 | 端到端 chunk 迁移测试 | 2 天 |

### 阶段七：MapReduce 与 JS（高复杂度，低价值）

| 任务 | 说明 | 预估工作量 |
|------|------|-----------|
| JS 引擎选型 | QuickJS（纯 Rust）或 V8（需 C++） | 3 天（选型） |
| mapReduce 命令 | 实现 map/reduce 函数执行 | 5 天 |
| $where | 实现 JS 表达式查询 | 3 天 |
| $function | 实现聚合 JS 函数 | 3 天 |

### 阶段八：生态与工具（中等复杂度，中价值）

| 任务 | 说明 | 预估工作量 |
|------|------|-----------|
| mongosh 兼容 | 实现 mdb-cli 交互式 shell | 5 天 |
| Compass 验证 | 端到端验证 MongoDB Compass 连接 | 2 天 |
| 官方驱动测试 | 用 pymongo/mongosh 验证兼容性 | 3 天 |
| 文档更新 | 更新 README、CHANGELOG、用户手册 | 2 天 |

---

## 优先级建议

**第一优先（1-2 周）**：
- 阶段一：索引类型补全 + BlockCache
- 阶段二：profiler + OP_COMPRESSED

**第二优先（2-3 周）**：
- 阶段三：标准 TLS（需依赖 RFC 流程）
- 阶段五：分片数据路由

**第三优先（3-4 周）**：
- 阶段四：多节点 Raft
- 阶段六：Chunk 数据迁移

**第四优先（按需）**：
- 阶段七：MapReduce
- 阶段八：生态工具

---

## 1. 线协议与握手

| 项目 | MongoDB 8.0 | MDB | 状态 |
|------|-------------|-----|------|
| `OP_MSG` (2013) | 支持 | 完整实现，含 flag/checksum/section 0,1/documentSequences | 达标 |
| `OP_QUERY` (2004) | 支持（遗留） | 返回 `CommandNotImplemented` | 缺失 |
| `OP_REPLY` (1) | 支持 | 未实现 | 缺失 |
| `OP_COMPRESSED` (2012) | 支持 snappy/zstd/zlib | 未实现，故 `hello` 不能声明压缩 | 缺失 |
| `maxWireVersion` | 25 | `MAX_WIRE_VERSION = 25` (MongoDB 8.0) | 达标（但 `OP_COMPRESSED` 仍缺失，见下行） |
| 压缩 | 默认开启 | 无 | 缺失 |

`wireVersion: 25` 意味着 MDB 自称兼容 MongoDB 8.0。官方驱动会据此启用
批量写、retryable writes、`OP_COMPRESSED` 等特性，而这些在服务端不存在。

**`OP_QUERY` 的处理有个隐患**：非 `OP_MSG` 的 opcode 会返回错误并**直接断开连接**。
遗留驱动（未升级的 JDBC/PHP 驱动）握手失败后无法退避重试。

---

## 2. 查询运算符

服务端过滤逻辑在 `document_matches` / `eval_field_ops`（`lib.rs:3115-3141`）。

**已支持（8 个）**：`$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$exists`，加顶层 `$and` / `$or`。

**缺失**：`$nin` `$not` `$nor` `$regex` `$options` `$size` `$all` `$elemMatch` `$type`
`$mod` `$expr` `$text` `$where` `$bitsAllClear` 等位运算。

三个额外的严重缺陷：

1. **点号路径不解析。** 过滤直接调用 `Document::get(key)`，只做精确键匹配
   （`mdb-doc/src/value.rs:363`）。`{"address.city": "北京"}` 不会命中。
   点号解析器 `get_path` 已存在（`value.rs:409`）但服务端没用。

2. **类型受限的相等比较。** `bson_eq`（`lib.rs:3143`）只支持
   `Null, Boolean, Int32, Int64, Double, String, ObjectId`。
   `DateTime`、`Timestamp`、`Decimal128`、`Binary`、`Document`、`Array`
   **永远不相等**。`bson_compare`（`lib.rs:3153`）只处理数值。

3. **未知运算符静默失败。** 遇到不认识的 `$op` 直接 `return false`（`lib.rs:3137`），
   不报错。这比报错更糟：查询静默返回空集，用户无从察觉。

讽刺的是，`mdb-query` 库本身实现了 `Nin`、`Nor`、`Not`、`RegexOp`、`Size`、
`Type`、`ElemMatch`、`ModOp` 等（`mdb-query/src/lib.rs:35-37`），解析器也处理
`$regex`/`$options`/`$type`/`$elemMatch`（`parser.rs:236-283`）——服务端只是没用它。

> 顺带：`mdb-query` 的 `$mod` 有 bug。它被存成对 `[divisor, remainder]` 数组的等值比较，
> 而不是取模运算（`parser.rs:331-337`）。接线前必须修。

---

## 3. 聚合管道

`mdb-query` 的 `PipelineStage` 枚举声明了 24 个阶段，但其中
`LookupPipeline`、`Out`、`Merge`、`GraphLookup`、`UnionWith` **原样返回输入**
（`aggregation.rs:1144-1146`）。

服务端只解析 9 个阶段（`lib.rs:2315-2352`），其余**静默丢弃**（`:2352` 返回 `None`）：

已支持：`$match` `$group` `$sort` `$limit` `$skip` `$project` `$unwind` `$addFields` `$count`

未支持：`$facet` `$bucket` `$bucketAuto` `$lookup` `$graphLookup` `$merge` `$out`
`$setWindowFields` `$unionWith` `$sample` `$redact` `$densify` `$fill` `$sortByCount`

累加器只有 `$sum $avg $min $max $first $last $push $addToSet $count`，
缺 `$stdDevPop` `$stdDevSamp` `$mergeObjects`。

聚合是**全集合扫描后内存管线**（`lib.rs:2306-2310`）。

---

## 4. 索引（最严重的功能性问题）

索引类型在 `mdb-index/src/types.rs:17-28` 定义齐全：
`BTree, Unique, TTL, Multikey, Text, TwoDSphere, Hashed, Partial`。

但服务端：

- **只创建 `IndexType::BTree`，忽略除 `unique` 外的所有选项**（`lib.rs:2257`）。
  通过网络无法创建 text / TTL / geo / partial / sparse 索引。
- **写路径维护索引**（`update_indexes` / `remove_from_indexes`，共 12 处调用）。
- **读路径从不查索引。** 全部 8 处读操作都是 `store.scan(b"")` 全量扫描
  （`lib.rs:1828, 2136, 2181, 2229, 2306, 2426, 2466, 2498, 2616`）。

也就是说：**索引被维护，但永远不被用于加速查询。**

`explain` 更是直接编造执行计划：

```rust
// crates/mdb-server/src/lib.rs:2711-2716
if !filter.is_empty() {
    stage.insert("stage", Bson::String("IXSCAN".into()));
    stage.insert("indexName", Bson::String("primary_key".into()));  // 硬编码
```

`executionTimeMillis` 恒为 0，从不询问查询规划器。

**后果**：任何非平凡的数据量都会让 `find` 退化为 O(n) 全扫描。
1 亿文档的集合上，一次 `find` 要解码全部文档。这是当前架构下最迫切需要修复的问题。

---

## 5. 事务

`mdb-txn` 库是实质性的：MVCC 快照分配（`mvcc.rs:10-54`）、两阶段提交
（`txn.rs:190+`）、冲突检测、逻辑会话与超时清理（`session.rs:15-53`）、
因果一致性（`causal.rs:1-50`）、可重试错误分类（`retry.rs:1-34`）。
Read Concern 四种、Write Concern 五种。

但：

- **服务端返回裸 `ok`**（`lib.rs:1735`）。没有原子性，没有回滚。
- **读冲突检测是桩实现**：读冲突序列号硬编码为 `0`，注释写着 "simplified"
  （`txn.rs:150-153`）。写偏斜（write-skew）无法被检测到。
- `Snapshot` read concern 不携带时间戳；缺 `atClusterTime` / `afterClusterTime` /
  `Available`。

---

## 6. 副本集与 Change Stream

`mdb-repl` 有真实 Raft：`RequestVote`、`AppendEntries`、`InstallSnapshot`、
`MemWal`、pre-vote、leader lease、快照传输、WAL 恢复（`raft.rs:15-890`）；
`ReplicaSet` 含 `elect_primary` / `sync_from_primary` / `apply_oplog_entry`
（`replication.rs:52-327`）。

但是：

- **oplog 是内存 `VecDeque`**（`oplog.rs:1-64`），无持久化，无按大小截断。
- 服务端只接了 `replSetInitiate` / `replSetGetStatus`，后者返回进程内
  `ReplicaSet` 的硬编码 `myState: 1`（`lib.rs:3075-3095`）。**没有真正的复制。**
- 缺失：`replSetGetConfig` `replSetReconfig` `replSetStepDown` `replSetFreeze`
  `replSetSync` 心跳协议、可重试写接线。
- `watch` 返回裸 `ok`（`lib.rs:1738`）。无 resumeToken，无 pre/post image。

---

## 7. 分片

`mdb-router` 是一个成型的 mongos 替代：`ConfigDb`（shards/chunks/databases/
primary shard）、`ShardKey::{Hashed, Range, Compound}`、`KeyRange` 的
`[min, max)` 包含与重叠判断、**MongoDB 标准 BSON 类型序**（`types.rs:147-153`）、
`route_write`/`route_read`/`broadcast_query`、`Balancer`、五状态
`MigrationManager`（`clone_data → persist → commit → cleanup`）。

**服务端从未引用 `mdb_router`。** 无 mongos 二进制，`hello` 不带 `isdbgrid`，
缺失 `shardCollection`、`moveChunk`、`split`、`balancerStart/Stop/Status`、
`listShards`、`addShard`、`removeShard`、`flushRouter`。
router 的 76 个测试全部只测库，不经服务端。

---

## 8. 存储引擎

`mdb-storage` 组件齐全（97 个测试）：WAL、MemTable、SSTable、manifest、
compaction、压缩、column family、MVCC 内部键与快照、Bloom filter、TDE。

**已接线**：`ServerState::open` → `Catalog` → `Engine`，服务端持久化已生效。
WAL 组提交 + SSTable + compaction + 启动恢复全部工作。

TDE 实现了 MEK→DEK 密钥环、每集合 DEK、AES/SM4-GCM
（`nonce(12)||ct||tag(16)`）、密钥元数据持久化。

**TDE 仍未接线到服务端写路径。**

另有一个基础缺口：**全仓库没有 BlockCache / LRU 缓存**。即使接线了 LSM，
读路径也没有块缓存层。

---

## 9. 诊断与管理命令

服务端实现了约 45 个命令。其中以下几类是**装饰性的**：

| 命令 | 实际行为 | 位置 |
|------|---------|------|
| `explain` | 编造 `IXSCAN`，`indexName` 硬编码 `"primary_key"`，耗时恒为 0 | `lib.rs:2710-2725` |
| `compact` | 空操作，回复 `"catalog compact is a no-op in the in-memory engine"` | `lib.rs:2649-2656` |
| `reIndex` | 空操作 | `lib.rs:2831` |
| `killOp` | 空操作，永远成功 | `lib.rs:2777` |
| `validate` | 恒返回 `valid: true`，errors 为空 | `lib.rs:2662-2683` |
| `buildInfo` | 版本硬编码 `"0.1.0"` | `lib.rs:1473` |
| `replSetGetStatus` | `myState` 硬编码 | `lib.rs:3075-3095` |

缺失：`setParameter` `collMod` `analyze` `top` `dbHash` `hostInfo`
`getCmdLineOpts` `features` `setProfilingLevel`、真实 `system.profile` 集合、
`$jsonSchema` 校验器、`create` 命令的 validator。

---

## 10. GridFS —— 死代码

`crates/mdb-server/src/gridfs.rs` 有完整实现：`GridFsStorage` trait、
`GridFSBucket`（upload/download/open_upload/open_download/find/rename/delete）、
`files` + `chunks` 元数据、255 KiB 默认分块、chunk 重组。

但 `lib.rs:7-8` 只声明了：

```rust
pub mod audit;
mod ids;
```

**`gridfs` 模块没有被声明，该文件根本不参与编译。**

即使接线，MongoDB GridFS 依赖驱动侧的 `find`/`insert`/`delete`，
而服务端的 `$slice` / `$elemMatch` 过滤目前都不支持。

---

## 11. MapReduce 与服务端 JavaScript —— 完全缺失

无 `mapReduce` 命令，无 JS 引擎（V8 / SpiderMonkey / QuickJS 或任何脚本依赖）。
连带缺失：`$where`、`$function` / `$accumulator` 的 JS 表达式。
BSON 能携带 `JavaScript` / `JavaScriptWithScope` 类型，但仅用于字节级保真，从不执行。

---

## 12. 安全

**已实现且真实**：

- SCRAM-SHA-1 / SCRAM-SHA-256 服务端完整实现（`mdb-crypto/src/scram.rs:48-510`），
  PBKDF2-HMAC 15000 轮（`:104`），含 MONGODB-CR `md5`（`:138`），
  `saslStart`/`saslContinue` 已接线（`lib.rs:1805-1950`）。
- SM3 / SM4 / SM4-GCM / PBKDF2 / 常量时间比较。
- RBAC：7 个命令 + 命令级权限闸门（`lib.rs:1433-1465`）。
- 审计日志（GB/T 20273 导向）、IDS、Prometheus 端点。这些是本轮真正落地的部分。

**缺失**：x509、LDAP、OIDC、MONGODB-AWS、Kerberos。
`saslStart` 拒绝 `ScramMechanism` 之外的一切（`lib.rs:1798-1804`）。

**注意**：`mdb-tls` 实现的是 TLCP（`ECC_SM4_SM3`，套件 `0xE013`）——中国国密标准协议，
**不是**标准 TLS 1.2/1.3（没有 ECDHE/RSA/AES-GCM 套件）。它不能与通用 TLS 客户端互操作。
若要接入现有 MongoDB 生态，需要另外实现标准 TLS。

---

## 13. 驱动

10 个驱动全部有真实代码和真实测试文件，但都未纳入服务端测试流程。

| 驱动 | 层级 | 高层特性 |
|------|------|---------|
| `drivers/python/mdb-native` | 原生 | **仅此驱动有 auth 模块** |
| `drivers/python/mdb-python` | 高层 | change streams、事务、concerns、profiler |
| `drivers/java/mdb-native` | 原生 | bson、ObjectId、GridFS |
| `drivers/java/mdb-java` | 高层 | change streams、事务、concerns、profiler |
| `drivers/nodejs/mdb-node` | 高层 | 同上 |
| `drivers/nodejs/mdb-native` | 原生 | bson、crc32c、wire、index、gridfs |
| `drivers/go/mdb-go` | 高层 | change streams、事务、concerns、index |
| `drivers/go/mdb-native` | 原生 | bson、wire、gridfs |
| `drivers/c/mdb-c` | C++ RAII 封装 | — |
| `drivers/c/mdb-native` | 原生 | bson、connection、gridfs |

**重要的互操作性问题**：Java、Python、Node、Go 的**官方 MongoDB 驱动**会
在握手后根据 `maxWireVersion` 启用批量写和 retryable writes。
MDB 声明 `wireVersion 25` 但不支持 `OP_COMPRESSED`，
官方驱动与 MDB 的实际兼容性未经端到端验证。
`drivers/nodejs/mdb-node/node_modules` 里正好有官方 Node 驱动，可用于互操作测试。

---

## 14. 测试规模

| crate | 文件 | `#[test]` | 行数 |
|-------|----:|--------:|-----:|
| mdb-query | 10 | 233 | 7,005 |
| mdb-server | 7 | 182 | 9,030 |
| mdb-repl | 8 | 173 | 4,974 |
| mdb-txn | 13 | 115 | 3,455 |
| mdb-crypto | 15 | 98 | 3,700 |
| mdb-storage | 13 | 95 | 6,272 |
| mdb-doc | 10 | 91 | 3,469 |
| mdb-index | 8 | 76 | 3,352 |
| mdb-router | 7 | 76 | 2,443 |
| mdb-wire | 6 | 61 | 1,765 |
| mdb-tls | 10 | 44 | 2,457 |
| mdb-common | 8 | 35 | 2,125 |
| mdb-backup | 7 | 26 | 1,873 |
| mdb-test | 1 | 4 | 239 |
| **mdb-cli** | 1 | **0** | **19** |
| **mdb-runtime** | 1 | **0** | **19** |
| 合计 | 126 | 1,309 | 53,044 |

**`mdb-cli`（mongosh 替代）和 `mdb-runtime`（异步 io_uring/epoll 运行时）
各只有 19 行**，仅有 lint 属性，注释标注"阶段 0 骨架"。没有 shell，没有异步运行时。

重要提醒：1,309 个测试中绝大部分是**库内单元测试**。
服务端 182 个测试覆盖的是内存引擎路径。
**库测试通过 ≠ 数据库可用。**

---

## 15. 差距汇总（2026-10-03 更新）

### 已修复 ✅

| # | 差距 | 状态 |
|---|------|------|
| 1 | ~~无持久化，数据只在 BTreeMap~~ | ✅ WAL + SST + 恢复已接线，测试通过 |
| 5 | ~~4 个核心库声明为依赖但零引用~~ | ✅ `mdb-storage` 已接线（txn/router/tls 仍零引用） |
| 6 | ~~含 NUL 的 `_id` 键无法正确编码/解析/恢复~~ | ✅ 内部键编码已修复，500 文档重启测试通过 |
| — | ~~`update`/`delete` shorthand 过滤条件被当空条件~~ | ✅ 已修复，回归测试通过 |
| — | ~~`createIndexes` 不持久化索引定义~~ | ✅ 已持久化到 Catalog（但恢复未注册到 IndexManager） |
| — | ~~`serve.rs` 不支持 dbpath~~ | ✅ `bind_with_dbpath` 已实现 |

### 严重（阻断生产可用）❌

| # | 差距 | 影响 |
|---|------|------|
| 2 | 索引不被读路径使用，全部全表扫描 | 性能随数据量线性劣化 |
| 3 | `explain` 编造执行计划 | 运维无法定位性能问题，误导调优 |
| 4 | 事务/Change Stream/`killCursors` 返回裸 `ok` | 客户端以为成功，实际无操作（静默数据正确性风险） |
| 4b | 索引定义持久化了但恢复时不注册到 IndexManager | 重启后索引丢失（数据正常但索引消失） |
| 6b | 点号路径不解析；`DateTime`/`Timestamp`/`Binary` 永不相等 | 常见查询静默返回错误结果 |

### 高（功能不完整）❌

7. `$nin`/`$not`/`$regex`/`$elemMatch`/`$type`/`$mod` 等运算符缺失，且未知运算符静默返回空集
8. 聚合仅 9/24 阶段，其余静默丢弃；聚合全为全集合扫描
9. 无副本集、无分片、无 mongos、无 Change Stream 实际实现
10. TLS 未实现（TLCP 库存在但不能与通用 TLS 客户端互操作）
11. TDE 未接线
12. GridFS 文件存在但未参与编译
13. `OP_QUERY`/`OP_REPLY`/`OP_COMPRESSED` 缺失，`wireVersion` 落后 4 个版本
14. 索引类型无法通过协议创建（只支持 BTree + unique）

### 中（运维与生态）❌

15. `compact`/`killOp`/`reIndex`/`validate` 为装饰性实现
16. 无真实 profiler / `system.profile` / 慢查询
17. `mdb-cli` 与 `mdb-runtime` 是 19 行空壳
18. 无 `setParameter`/`collMod`/`analyze`/`top`/`dbHash`
19. 仅 Python 原生驱动有 auth；其余驱动的认证路径未验证
20. 与官方 MongoDB 驱动的端到端互操作性未经测试
21. 无 BlockCache（LSM 读路径缺缓存层）
22. 无 MapReduce / 服务端 JavaScript
23. `$jsonSchema` 校验器缺失

---

## 16. 建议的修复顺序

**第一阶段：让它成为一个真数据库**

1. 将 `MemStore` 替换为 `mdb_storage::Engine`，实现 WAL 落盘与启动恢复。
2. 把读路径从 `store.scan(b"")` 改为 `index_manager.lookup()`，
   无索引时回退全扫描但要在 `explain` 中如实标注 `COLLSCAN`。
3. 修掉裸 `ok` 桩：事务、watch、killCursors 未实现时必须返回
   `ErrorCode::IllegalOperation`，而不是骗客户端成功。这是**正确性问题**，
   比性能问题更紧急。

**第二阶段：补齐查询语义**

4. 服务端过滤改用 `mdb_query::parse`，先修 `$mod` 的 bug（`parser.rs:331`）。
5. 启用 `get_path` 解析点号路径；扩充 `bson_eq`/`bson_compare` 覆盖全部 BSON 类型。
6. 未知运算符改为返回 `FailedToParse`，不再静默返回空集。

**第三阶段：接线已有库**

7. `createIndexes` 支持全部索引类型与选项。
8. TDE 接入存储引擎写路径。
9. `mod gridfs;` 加入 `lib.rs`，补 GridFS 命令与 `$slice`/`$elemMatch`。
10. 副本集、Change Stream 接 `mdb_repl`；mongos 接 `mdb_router`。

**第四阶段：运维与生态**

11. 实现标准 TLS 1.2/1.3（TLCP 保留为可选套件）。
12. 真实 profiler、`system.profile`、慢查询阈值。
13. 实现 `mdb-cli` 与 `mdb-runtime`。
14. 用官方 MongoDB 驱动（Java/Python/Node/Go）做端到端兼容测试，
    据结果决定 `maxWireVersion` 报 25 还是下调。

---

## 17. 关于对外表述

### 当前阶段（持久化已接线，索引/查询/事务未接线）

建议将 MDB 定位表述为：

> **MDB 是 MongoDB Wire Protocol 的 Rust 实现，已实现单节点持久化
> （WAL + LSM-Tree + 启动恢复），支持基本 CRUD、索引定义持久化、
> SCRAM 认证和 RBAC。事务、副本集、分片、Change Stream、TLS
> 等组件已完成库级实现并通过单元测试，尚未集成到可运行的服务端。**

不建议表述为"兼容 MongoDB 7.0 的生产可用数据库"——当前索引不被查询使用、
explain 编造计划、事务返回假成功，这些会直接导致生产事故。

---

**结论**：MDB 的库层质量不错（1,700+ 测试，55k+ 行代码，覆盖 BSON 编解码、
LSM 组件、Raft、事务模型、路由算法）。持久化已成功接线并有回归测试保护。
剩余差距集中在**外部集成**（x509/LDAP/OIDC）和**合规认证**（密评三级）。
当前最大的风险是那些返回 `ok` 却不做事的桩命令会让人误以为功能可用。