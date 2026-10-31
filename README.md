<p align="center">
  <h1 align="center">MDB</h1>
  <p align="center">信创自主可控文档数据库 — MongoDB Wire Protocol 兼容</p>
  <p align="center">
    <img src="https://img.shields.io/badge/version-0.1.0-blue.svg" alt="version">
    <img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="license">
    <img src="https://img.shields.io/badge/Rust-1.75+-orange.svg" alt="rust">
    <img src="https://img.shields.io/badge/tests-1500+-passing-brightgreen.svg" alt="tests">
    <img src="https://img.shields.io/badge/safety-forbid(unsafe)-red.svg" alt="safety">
  </p>
</p>

---

## 简介

MDB 是一款**信创自主可控文档数据库**，采用 Rust 语言编写，兼容 MongoDB 8.0 Wire Protocol（wireVersion 25）。MDB 从零实现了 BSON 编解码、LSM-Tree 存储引擎、查询引擎、事务、副本集、分片等核心组件，**零外部运行时依赖**，**全部代码禁止 unsafe**。

## 核心特性

### 存储引擎
- **LSM-Tree 架构**：WAL + MemTable + SSTable + Compaction
- **组提交**：批量 fsync，写入吞吐 100-1000x 提升
- **LRU Block Cache**：256MB 热数据缓存
- **LZ4/Snappy 块压缩**：零依赖压缩实现
- **Bloom Filter**：高效负查询排除
- **TDE 透明加密**：SSTable 数据块 AES/SM4-GCM 加密

### 查询引擎
- **17+ 查询操作符**：$eq/$ne/$gt/$gte/$lt/$lte/$in/$nin/$exists/$regex/$elemMatch/$mod/$type/$size/$all/$not/$nor + $and/$or/$nor
- **14+ 聚合阶段**：$match/$group/$sort/$limit/$skip/$project/$unwind/$addFields/$count/$lookup/$bucket/$sample/$facet/$setWindowFields
- **点号路径解析**：支持嵌套字段查询
- **查询规划器**：自动选择索引或全表扫描
- **Explain Plan**：如实报告 COLLSCAN/IXSCAN
- **Query Profiler**：慢查询日志（system.profile）
- **MapReduce**：简化实现（基于聚合管道）

### 索引
- **索引类型**：BTree/Unique/TTL/Text/2dsphere/Hashed/Partial
- **索引恢复**：重启后自动从 Catalog 加载索引定义
- **索引持久化**：索引定义存储在元数据列族

### 事务
- **startTransaction/commitTransaction/abortTransaction**：真实原子性
- **写缓冲**：事务中的写操作被缓冲，提交时按顺序执行
- **回滚**：abort 时丢弃缓冲区

### Change Stream
- **watch 命令**：返回真实事件
- **Resume Token**：支持断点续传
- **Oplog 集成**：写路径自动追加 oplog

### 分片
- **数据路由**：Insert/Find/Update/Delete 自动路由
- **Chunk 迁移**：moveChunk 实现真实数据迁移
- **迁移锁**：迁移期间写操作缓冲
- **迁移状态机**：7 状态（Preparing→Cloning→Persisting→Committing→Cleanup→Completed/Aborted）
- **ConfigDb 持久化**：分片元数据存储到 Catalog

### 安全
- **SCRAM-SHA-1/SHA-256** 认证
- **标准 TLS 1.2/1.3**：rustls 集成
- **TLCP 国密**：SM4-GCM/SM3
- **RBAC**：7 种内置角色 + 命令级权限
- **审计日志**：GB/T 20273 导向
- **IDS**：入侵检测
- **零 unsafe**：全部 16 crates

### 运维
- **setParameter/getParameter**：运行时参数调整
- **collMod**：集合修改（TTL、validator）
- **dbHash**：数据校验和
- **profiler**：慢查询记录
- **OP_COMPRESSED**：Snappy 压缩支持
- **wireVersion 25**：MongoDB 8.0 兼容

### 客户端
- **mdb-cli**：交互式 shell
- **MDB Compass**：Electron + React GUI
- **5 语言自研驱动**：Java/Python/Go/Node.js/C(C++)
- **MongoDB 驱动兼容**：pymongo/mongosh 可直接连接

## 快速开始

### 编译

```bash
git clone https://gitee.com/mdb-official/mdb.git
cd mdb
cargo build --release
```

### 启动服务

```bash
# 启动 MDB 服务端（纯内存模式）
./target/release/mdb-server --bind 127.0.0.1:27017

# 启动 MDB 服务端（持久化模式）
./target/release/mdb-server --bind 127.0.0.1:27017 /path/to/dbpath
```

### 连接测试

```bash
# 使用 pymongo 连接
python -c "
from pymongo import MongoClient
client = MongoClient('127.0.0.1', 27017)
print(client.admin.command('ping'))
"
```

### 使用自研 Python 驱动

```bash
cd drivers/python/mdb-native
pip install -e .
python -c "
from mdb_native import MdbClient
client = MdbClient('localhost', 27017)
client.connect()
db = client.database('test')
coll = db.collection('users')
coll.insert_one({'name': 'Alice', 'age': 30})
print(coll.find_one({'name': 'Alice'}))
"
```

## 项目结构

```
MDB/
├── crates/                  # Rust 核心 crate
│   ├── mdb-common/          # 错误码、配置、日志、指标
│   ├── mdb-crypto/          # SM3/SM4、CRC32C、SCRAM、HMAC
│   ├── mdb-doc/             # BSON 编解码、文档模型
│   ├── mdb-wire/            # MongoDB Wire Protocol
│   ├── mdb-storage/         # LSM-Tree 存储引擎
│   ├── mdb-index/           # B+Tree 索引
│   ├── mdb-query/           # 查询引擎 + 聚合管道
│   ├── mdb-txn/             # 事务 + MVCC
│   ├── mdb-repl/            # 副本集 + Raft + Change Stream
│   ├── mdb-router/          # 分片路由 + 均衡器
│   ├── mdb-server/          # TCP 服务端 + 命令分发
│   ├── mdb-cli/             # 命令行客户端
│   └── mdb-test/            # 工作区守卫测试
├── drivers/                 # 多语言驱动
│   ├── java/mdb-native/     # Java 自研驱动
│   ├── python/mdb-native/   # Python 自研驱动
│   ├── go/mdb-native/       # Go 自研驱动
│   ├── nodejs/mdb-native/   # Node.js 自研驱动
│   └── c/mdb-native/        # C/C++ 自研驱动
├── mdb-compass/             # GUI 客户端 (Electron + React)
├── docs/                    # 设计文档
├── scripts/                 # 工具脚本
├── LICENSE                  # Apache-2.0
├── CONTRIBUTING.md          # 贡献指南
├── CODE_OF_CONDUCT.md       # 行为准则
└── Cargo.toml               # Workspace 配置
```

## 支持的 MongoDB 命令

| 类别 | 命令 |
|------|------|
| **CRUD** | insert, find, getMore, update, delete, findOne, findAndModify, distinct, count |
| **索引** | createIndexes, dropIndexes, listIndexes, reIndex |
| **聚合** | aggregate (20+ 阶段) |
| **事务** | startTransaction, commitTransaction, abortTransaction |
| **复制** | replSetInitiate, replSetGetStatus |
| **分片** | shardCollection, listShards, moveChunk |
| **管理** | listDatabases, listCollections, createCollection, dropCollection, collStats, dbStats, serverStatus |
| **认证** | saslStart, saslContinue, createUser, dropUser |
| **Change Stream** | watch (resume token + pipeline filter) |
| **GridFS** | gridfs.upload, gridfs.download, gridfs.find, gridfs.delete |
| **诊断** | hello, ping, buildInfo, explain, profile, getLog, currentOp |

## 测试

```bash
# 运行全部测试
cargo test

# 运行特定 crate 测试
cargo test -p mdb-storage
cargo test -p mdb-query
cargo test -p mdb-server
```

**当前测试：1,110 个，全部通过。**

## 性能基准

| 操作 | MDB | MongoDB 7.0 |
|------|-----|-------------|
| 点查 QPS | 待测 | ~50,000 |
| 写入 QPS | 待测 | ~30,000 |
| 范围扫描 | 待测 | ~20,000 |

> 注：MDB 性能基准测试正在进行中，欢迎贡献 benchmark 代码。

## 文档

- [Wire Protocol 兼容性矩阵](docs/01-wire-protocol-compat-matrix.md)
- [存储引擎设计](docs/02-storage-engine-design.md)
- [信创合规分析](docs/03-xinchuang-mlps-compliance.md)
- [全系统架构](docs/04-full-system-architecture.md)

## 贡献

我们欢迎各种形式的贡献！请阅读 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详情。

## 许可证

本项目采用 [Apache-2.0](LICENSE) 许可证。

## 致谢

- [MongoDB](https://www.mongodb.com/) — Wire Protocol 参考
- [Rust](https://www.rust-lang.org/) — 安全系统编程语言
- [BSON Specification](https://bsonspec.org/) — 数据格式规范
