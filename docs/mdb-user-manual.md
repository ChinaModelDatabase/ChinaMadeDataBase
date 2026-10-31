# MDB 用户手册

**MDB — 信创自主可控文档数据库**

版本：0.2.0 | 日期：2026-10-03

---

## 目录

1. [简介](#1-简介)
2. [安装](#2-安装)
3. [快速开始](#3-快速开始)
4. [配置](#4-配置)
5. [认证与权限](#5-认证与权限)
6. [CRUD 操作](#6-crud-操作)
7. [查询运算符](#7-查询运算符)
8. [索引管理](#8-索引管理)
9. [聚合管道](#9-聚合管道)
10. [事务](#10-事务)
11. [副本集](#11-副本集)
12. [分片](#12-分片)
13. [Change Stream](#13-change-stream)
14. [GridFS](#14-gridfs)
15. [安全特性](#15-安全特性)
16. [备份与恢复](#16-备份与恢复)
17. [监控与运维](#17-监控与运维)
18. [客户端驱动](#18-客户端驱动)
19. [性能调优](#19-性能调优)
20. [常见问题](#20-常见问题)
21. [附录](#21-附录)

---

## 1. 简介

MDB 是一款**信创自主可控文档数据库**，兼容 MongoDB 8.0 Wire Protocol（wireVersion 25）。采用 Rust 语言编写，零外部运行时依赖，全部代码禁止 unsafe。

### 核心特性

- **存储引擎**：LSM-Tree (WAL + MemTable + SSTable + Compaction) + LRU Block Cache
- **查询引擎**：17+ 操作符，20+ 聚合阶段，$where/$expr 支持
- **索引**：B+Tree、Unique、TTL、Text、2dsphere、Hashed、Multikey、Partial
- **事务**：写缓冲、Write Conflict 检测、因果一致性
- **副本集**：Raft 共识、TCP 传输层、Change Stream
- **分片**：数据路由、Chunk 迁移、迁移锁、ConfigDb 持久化
- **安全**：SCRAM-SHA-256、标准 TLS 1.2/1.3 (rustls)、TLCP 国密、RBAC、审计日志、TDE
- **压缩**：LZ4、Snappy、zstd
- **备份**：全量/增量备份、PITR 时间点恢复

---

## 2. 安装

### 2.1 从源码编译

**系统要求**
- Rust 1.75+
- Linux/macOS/Windows
- 2GB+ 内存
- 10GB+ 磁盘空间

**编译步骤**

```bash
# 克隆仓库
git clone https://gitee.com/mdb-official/mdb.git
cd mdb

# 编译（Release 模式）
cargo build --release

# 编译产物
# target/release/mdb-server   — 服务端
# target/release/mdb-cli      — 命令行客户端
```

**验证安装**

```bash
./target/release/mdb-server --version
# MDB 0.1.0 (Xinchuang DocumentDB)

./target/release/mdb-cli --version
# MDB CLI 0.1.0
```

### 2.2 信创环境安装

**麒麟 OS / 统信 UOS**

```bash
# 安装依赖
sudo apt-get install build-essential

# 编译
cargo build --release

# 安装到系统路径
sudo cp target/release/mdb-server /usr/local/bin/
sudo cp target/release/mdb-cli /usr/local/bin/

# 创建数据目录
sudo mkdir -p /var/lib/mdb
sudo chown mdb:mdb /var/lib/mdb
```

**国产 CPU (鲲鹏/飞腾/龙芯)**

```bash
# 交叉编译示例 (aarch64)
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
cargo build --release --target aarch64-unknown-linux-gnu
```

### 2.3 Docker 部署

```bash
# 构建镜像
docker build -t mdb:0.1.0 .

# 运行容器
docker run -d \
  --name mdb \
  -p 27017:27017 \
  -v /data/mdb:/var/lib/mdb \
  mdb:0.1.0
```

---

## 3. 快速开始

### 3.1 启动服务端

```bash
# 默认配置启动
mdb-server

# 指定配置文件
mdb-server --config /etc/mdb/mdb.conf

# 指定数据目录
mdb-server --dbpath /data/mdb --port 27017
```

### 3.2 连接数据库

**使用 pymongo (Python)**

```python
from pymongo import MongoClient

# 连接 MDB
client = MongoClient("mongodb://localhost:27017")

# 验证连接
print(client.admin.command("ping"))
# {'ok': 1.0}

# 创建数据库和集合
db = client["mydb"]
coll = db["users"]

# 插入文档
coll.insert_one({"name": "张三", "age": 30, "city": "北京"})
coll.insert_many([
    {"name": "李四", "age": 25, "city": "上海"},
    {"name": "王五", "age": 35, "city": "广州"},
])

# 查询
result = coll.find_one({"name": "张三"})
print(result)
# {'_id': ObjectId('...'), 'name': '张三', 'age': 30, 'city': '北京'}

# 更新
coll.update_one({"name": "张三"}, {"$set": {"age": 31}})

# 删除
coll.delete_one({"name": "李四"})
```

**使用 mdb-native (自研 Python 驱动)**

```python
from mdb_native import MdbClient

client = MdbClient("localhost", 27017)
client.connect()

db = client.database("mydb")
coll = db.collection("users")

coll.insert_one({"name": "张三", "age": 30})
result = coll.find_one({"name": "张三"})
print(result)
```

### 3.3 命令行客户端

```bash
# 连接 MDB
mdbsh --host localhost --port 27017

# 执行命令
> use mydb
> db.users.insertOne({name: "张三", age: 30})
> db.users.find({name: "张三"})
> db.users.updateOne({name: "张三"}, {$set: {age: 31}})
> db.users.deleteOne({name: "张三"})
```

---

## 4. 配置

### 4.1 配置文件格式

MDB 使用 YAML 子集格式（与 `mongod.conf` 兼容）：

```yaml
# /etc/mdb/mdb.conf

net:
  bindIp: 127.0.0.1
  port: 27017
  tls:
    mode: requireTLS
    certificateKeyFile: /etc/mdb/tls/server.pem
    CAFile: /etc/mdb/tls/ca.pem
    useGmssl: true

storage:
  dbPath: /var/lib/mdb
  mdb:
    memtableSizeMB: 256
    compactionThreads: 4
    blocksizeKB: 64
    bloomBitsPerKey: 10
    checksum: read
    compression: lz4

security:
  authentication: SCRAM-SHA-256
  scramSha256Iterations: 15000

systemLog:
  destination: file
  logPath: /var/log/mdb/mdb.log
  logLevel: info
  auditLog:
    enabled: true
```

### 4.2 配置项说明

#### 网络配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `net.bindIp` | 127.0.0.1 | 监听地址 |
| `net.port` | 27017 | 监听端口 |
| `net.tls.mode` | disabled | TLS 模式: disabled/requireTLS |
| `net.tls.certificateKeyFile` | - | 证书文件路径 |
| `net.tls.CAFile` | - | CA 证书路径 |
| `net.tls.useGmssl` | false | 启用国密套件 |

#### 存储配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `storage.dbPath` | ./data | 数据目录 |
| `storage.mdb.memtableSizeMB` | 256 | MemTable 冻结阈值 |
| `storage.mdb.compactionThreads` | 4 | Compaction 线程数 |
| `storage.mdb.blocksizeKB` | 64 | SST 块大小 |
| `storage.mdb.bloomBitsPerKey` | 10 | Bloom Filter 位数 |
| `storage.mdb.checksum` | read | 校验模式: off/read/read.background |
| `storage.mdb.compression` | lz4 | 压缩算法: none/lz4/snappy |

#### 安全配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `security.authentication` | none | 认证模式 |
| `security.scramSha256Iterations` | 15000 | SCRAM 迭代次数 |

#### 日志配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `systemLog.destination` | stderr | 日志输出: stderr/file |
| `systemLog.logPath` | - | 日志文件路径 |
| `systemLog.logLevel` | info | 日志级别: trace/debug/info/warn/error/fatal |
| `systemLog.auditLog.enabled` | true | 审计日志开关 |

---

## 5. 认证与权限

### 5.1 创建用户

```javascript
// 连接 admin 数据库
use admin

// 创建管理员用户
db.createUser({
  user: "admin",
  pwd: "SecureP@ssw0rd!",
  roles: [
    { role: "root", db: "admin" }
  ]
})

// 创建应用用户 (仅读写 mydb)
db.createUser({
  user: "appUser",
  pwd: "AppP@ssw0rd!",
  roles: [
    { role: "readWrite", db: "mydb" }
  ]
})

// 创建只读用户
db.createUser({
  user: "reader",
  pwd: "ReadP@ssw0rd!",
  roles: [
    { role: "read", db: "mydb" }
  ]
})
```

### 5.2 内置角色

| 角色 | 权限 | 说明 |
|------|------|------|
| `root` | 全部 | 超级管理员 |
| `dbOwner` | readWrite + dbAdmin + userAdmin | 数据库所有者 |
| `dbAdmin` | 索引/统计/验证 | 数据库管理 |
| `userAdmin` | 用户管理 | 用户管理员 |
| `readWrite` | 读 + 写 | 读写用户 |
| `read` | 读 | 只读用户 |
| `enableBackup` | 备份操作 | 备份管理员 |
| `clusterAdmin` | 副本集/分片 | 集群管理员 |

### 5.3 权限管理

```javascript
// 授予角色
db.grantRolesToUser("appUser", [
  { role: "dbAdmin", db: "mydb" }
])

// 撤销角色
db.revokeRolesFromUser("appUser", [
  { role: "dbAdmin", db: "mydb" }
])

// 修改用户角色
db.updateUser("appUser", {
  roles: [
    { role: "readWrite", db: "mydb" }
  ]
})

// 修改密码
db.changeUserPassword("appUser", "NewP@ssw0rd!")

// 查看用户信息
db.getUsersInfo({ showCredentials: false })

// 删除用户
db.dropUser("appUser")
```

### 5.4 连接认证

```bash
# 带认证连接
mdbsh --host localhost --port 27017 \
  --username admin --password "SecureP@ssw0rd!" \
  --authenticationDatabase admin

# pymongo 连接
client = MongoClient(
    "mongodb://admin:SecureP@ssw0rd!@localhost:27017/?authSource=admin"
)
```

---

## 6. CRUD 操作

### 6.1 插入文档

```javascript
// 插入单个文档
db.users.insertOne({
  name: "张三",
  age: 30,
  email: "zhangsan@example.com",
  address: {
    city: "北京",
    district: "海淀区"
  },
  tags: ["VIP", "活跃"]
})

// 插入多个文档
db.users.insertMany([
  { name: "李四", age: 25 },
  { name: "王五", age: 35 },
  { name: "赵六", age: 28 }
])

// 批量插入 (有序)
db.users.insertMany([
  { name: "用户1" },
  { name: "用户2" }
], { ordered: true })

// 批量插入 (无序，遇到错误继续)
db.users.insertMany([
  { name: "用户3" },
  { name: "用户4" }
], { ordered: false })
```

### 6.2 查询文档

```javascript
// 查询所有文档
db.users.find()

// 条件查询
db.users.find({ age: { $gt: 25 } })

// 精确查询
db.users.find({ name: "张三" })

// OR 查询
db.users.find({
  $or: [
    { age: { $lt: 25 } },
    { age: { $gt: 35 } }
  ]
})

// 正则查询
db.users.find({ name: /^张/ })

// 投影 (只返回指定字段)
db.users.find({}, { name: 1, age: 1, _id: 0 })

// 排序
db.users.find().sort({ age: -1 })  // 降序
db.users.find().sort({ age: 1 })   // 升序

// 分页
db.users.find().skip(10).limit(20)

// 统计数量
db.users.countDocuments({ age: { $gte: 18 } })

// 查询唯一值
db.users.distinct("city")

// 查询单个文档
db.users.findOne({ name: "张三" })
```

### 6.3 更新文档

```javascript
// 更新单个文档
db.users.updateOne(
  { name: "张三" },
  { $set: { age: 31, city: "深圳" } }
)

// 更新多个文档
db.users.updateMany(
  { age: { $lt: 18 } },
  { $set: { status: "未成年" } }
)

// 替换整个文档
db.users.replaceOne(
  { name: "张三" },
  { name: "张三", age: 31, city: "深圳", email: "zs@example.com" }
)

// Upsert (存在则更新，不存在则插入)
db.users.updateOne(
  { name: "新用户" },
  { $set: { age: 25 } },
  { upsert: true }
)

// 原子操作
db.users.updateOne({ name: "张三" }, { $inc: { age: 1 } })      // 自增
db.users.updateOne({ name: "张三" }, { $unset: { email: "" } }) // 删除字段
db.users.updateOne({ name: "张三" }, { $min: { age: 20 } })    // 取最小值
db.users.updateOne({ name: "张三" }, { $max: { age: 50 } })    // 取最大值
db.users.updateOne({ name: "张三" }, { $mul: { age: 2 } })     // 乘法

// 数组操作
db.users.updateOne(
  { name: "张三" },
  { $push: { tags: "VIP" } }        // 追加元素
)
db.users.updateOne(
  { name: "张三" },
  { $addToSet: { tags: "VIP" } }    // 去重追加
)
db.users.updateOne(
  { name: "张三" },
  { $pull: { tags: "VIP" } }        // 删除匹配元素
)
db.users.updateOne(
  { name: "张三" },
  { $push: { tags: { $each: ["A", "B"], $position: 0 } } }  // 批量插入
)

// 条件更新
db.users.updateOne(
  { name: "张三" },
  { $currentDate: { updatedAt: true } }  // 设为当前时间
)

// 原子 findAndModify
db.users.findOneAndUpdate(
  { name: "张三" },
  { $inc: { age: 1 } },
  { returnDocument: "after" }  // 返回更新后的文档
)
```

### 6.4 删除文档

```javascript
// 删除单个文档
db.users.deleteOne({ name: "张三" })

// 删除多个文档
db.users.deleteMany({ age: { $lt: 18 } })

// 删除所有文档
db.users.deleteMany({})

// 原子 findAndDelete
db.users.findOneAndDelete({ name: "张三" })
```

---

## 7. 查询运算符

### 7.1 比较运算符

```javascript
// 等于
db.users.find({ age: 30 })
db.users.find({ age: { $eq: 30 } })

// 不等于
db.users.find({ age: { $ne: 30 } })

// 大于/大于等于
db.users.find({ age: { $gt: 25 } })
db.users.find({ age: { $gte: 25 } })

// 小于/小于等于
db.users.find({ age: { $lt: 30 } })
db.users.find({ age: { $lte: 30 } })

// 在数组中
db.users.find({ city: { $in: ["北京", "上海"] } })

// 不在数组中
db.users.find({ city: { $nin: ["北京", "上海"] } })
```

### 7.2 逻辑运算符

```javascript
// AND
db.users.find({ $and: [{ age: { $gte: 25 } }, { city: "北京" }] })

// OR
db.users.find({ $or: [{ age: { $lt: 25 } }, { age: { $gt: 35 } }] })

// NOR (都不满足)
db.users.find({ $nor: [{ age: { $lt: 25 } }, { age: { $gt: 35 } }] })
```

### 7.3 元素运算符

```javascript
// 字段存在
db.users.find({ email: { $exists: true } })

// 字段类型
db.users.find({ age: { $type: "int" } })
db.users.find({ age: { $type: "long" } })
db.users.find({ name: { $type: "string" } })

// 数组长度
db.users.find({ tags: { $size: 3 } })

// 数组包含所有元素
db.users.find({ tags: { $all: ["VIP", "活跃"] } })

// 数组元素匹配
db.users.find({ tags: { $elemMatch: { $gte: "A", $lt: "M" } } })
```

### 7.4 位运算符

```javascript
// 所有位都为 0
db.flags.find({ bits: { $bitsAllClear: 5 } })

// 所有位都为 1
db.flags.find({ bits: { $bitsAllSet: 5 } })

// 至少一位为 0
db.flags.find({ bits: { $bitsAnyClear: 5 } })

// 至少一位为 1
db.flags.find({ bits: { $bitsAnySet: 5 } })
```

### 7.5 其他运算符

```javascript
// 取模
db.numbers.find({ value: { $mod: [3, 0] } })  // 能被 3 整除

// 正则表达式
db.users.find({ name: { $regex: "^张" } })
db.users.find({ name: { $regex: "^张", $options: "i" } })  // 忽略大小写

// $where (JavaScript 表达式)
db.users.find({ $where: "this.age > 25 && this.city == '北京'" })
db.users.find({ $where: "this.name.startsWith('张')" })

// $expr (聚合表达式)
db.sales.find({ $expr: { $gt: ["$price", "$cost"] } })
db.sales.find({ $expr: { $eq: ["$status", "completed"] } })
```

### 7.6 点号路径查询

```javascript
// 查询嵌套字段
db.users.find({ "address.city": "北京" })
db.users.find({ "address.district": "海淀区" })

// 更新嵌套字段
db.users.updateOne({ _id: 1 }, { $set: { "address.city": "上海" } })
```

---

## 8. 索引管理

### 8.1 创建索引

```javascript
// 单字段索引
db.users.createIndex({ name: 1 })

// 复合索引
db.users.createIndex({ name: 1, age: -1 })

// 唯一索引
db.users.createIndex({ email: 1 }, { unique: true })

// TTL 索引 (文档在 expireAfterSeconds 后自动删除)
db.logs.createIndex({ createdAt: 1 }, { expireAfterSeconds: 86400 })

// 文本索引
db.articles.createIndex({ title: "text", content: "text" })

// 地理空间索引
db.places.createIndex({ location: "2dsphere" })

// Hashed 索引
db.users.createIndex({ email: "hashed" })

// Multikey 索引 (数组字段索引)
db.users.createIndex({ tags: "multikey" })

// Partial 索引 (只索引满足条件的文档)
db.users.createIndex({ age: 1 }, { partialFilterExpression: { age: { $gt: 0 } } })
```

### 8.2 索引统计

```javascript
// 获取索引统计信息
db.users.indexStatistics()
// 返回: [{ name, type, fields, entries, unique }, ...]
```

### 8.3 索引恢复

索引定义持久化到 Catalog，重启后自动恢复。无需手动重建索引。

// 哈希索引 (用于分片)
db.users.createIndex({ userId: "hashed" })

// 稀疏索引 (跳过缺少该字段的文档)
db.users.createIndex({ phone: 1 }, { sparse: true })

// 部分索引 (只索引满足条件的文档)
db.orders.createIndex(
  { status: 1 },
  { partialFilterExpression: { status: "active" } }
)

// 查看索引创建结果
{
  "createdCollectionAutomatically": false,
  "numIndexesBefore": 1,
  "numIndexesAfter": 2,
  "ok": 1.0
}
```

### 7.2 查看索引

```javascript
// 列出所有索引
db.users.getIndexes()

// 返回示例
[
  { "v": 2, "key": { "_id": 1 }, "name": "_id_" },
  { "v": 2, "key": { "name": 1 }, "name": "name_1" },
  { "v": 2, "key": { "email": 1 }, "name": "email_1", "unique": true }
]
```

### 7.3 删除索引

```javascript
// 删除指定索引
db.users.dropIndex("name_1")

// 删除除 _id 外的所有索引
db.users.dropIndexes()
```

### 7.4 索引统计

```javascript
// 查看集合统计 (包含索引信息)
db.users.stats()

// 返回示例
{
  "ns": "mydb.users",
  "count": 1000000,
  "size": 52428800,
  "storageSize": 67108864,
  "totalIndexSize": 8388608,
  "indexSizes": {
    "_id_": 4194304,
    "name_1": 2097152,
    "email_1": 2097152
  }
}
```

---

## 9. 聚合管道

### 8.1 基本聚合

```javascript
// 分组统计
db.orders.aggregate([
  { $group: {
    _id: "$category",
    totalAmount: { $sum: "$amount" },
    avgAmount: { $avg: "$amount" },
    count: { $sum: 1 }
  }}
])

// 排序 + 限制
db.orders.aggregate([
  { $group: { _id: "$userId", total: { $sum: "$amount" } } },
  { $sort: { total: -1 } },
  { $limit: 10 }
])

// 筛选 + 投影
db.users.aggregate([
  { $match: { age: { $gte: 18 } } },
  { $project: { name: 1, age: 1, adult: { $gte: ["$age", 18] } } }
])
```

### 8.2 关联查询 (Lookup)

```javascript
// 基本关联
db.orders.aggregate([
  { $lookup: {
    from: "users",
    localField: "userId",
    foreignField: "_id",
    as: "user"
  }},
  { $unwind: "$user" }
])

// Pipeline 关联
db.orders.aggregate([
  { $lookup: {
    from: "products",
    let: { productId: "$productId" },
    pipeline: [
      { $match: { $expr: { $eq: ["$_id", "$$productId"] } } }
    ],
    as: "product"
  }}
])
```

### 8.3 窗口函数

```javascript
// 排名
db.sales.aggregate([
  { $setWindowFields: {
    partitionBy: "$region",
    sortBy: { amount: -1 },
    output: {
      rank: { $rank: {} }
    }
  }}
])

// 累计求和
db.sales.aggregate([
  { $setWindowFields: {
    sortBy: { date: 1 },
    output: {
      runningTotal: { $sum: "$amount", window: { documents: ["unbounded", "current"] } }
    }
  }}
])

// 滑动平均
db.sales.aggregate([
  { $setWindowFields: {
    sortBy: { date: 1 },
    output: {
      movingAvg: {
        $avg: "$amount",
        window: { documents: [-2, 0] }  // 前3个文档的平均值
      }
    }
  }}
])
```

### 8.4 支持的聚合阶段

| 阶段 | 说明 |
|------|------|
| `$match` | 筛选文档 |
| `$group` | 分组聚合 |
| `$sort` | 排序 |
| `$project` | 字段投影/计算 |
| `$limit` | 限制数量 |
| `$skip` | 跳过数量 |
| `$unwind` | 展开数组 |
| `$lookup` | 关联查询 |
| `$addFields` | 添加字段 |
| `$count` | 计数 |
| `$bucket` | 分桶 |
| `$bucketAuto` | 自动分桶 |
| `$sample` | 随机采样 |
| `$densify` | 数据稠密化 |
| `$fill` | 填充空缺 |
| `$setWindowFields` | 窗口函数 |
| `$graphLookup` | 图遍历 |
| `$unionWith` | 集合合并 |
| `$redact` | 字段脱敏 |
| `$sortByCount` | 按计数排序 |
| `$out` | 输出到集合 |
| `$merge` | 合并到集合 |

---

## 10. 事务

### 10.1 基本事务

```python
# Python 示例
from pymongo import MongoClient
from pymongo.client_session import ClientSession

client = MongoClient("mongodb://localhost:27017")

with client.start_session() as session:
    with session.start_transaction():
        # 转账操作
        db.accounts.update_one(
            {"name": "Alice"},
            {"$inc": {"balance": -100}},
            session=session
        )
        db.accounts.update_one(
            {"name": "Bob"},
            {"$inc": {"balance": 100}},
            session=session
        )
    # 事务在此处自动提交
```

### 10.2 Write Conflict 检测

当两个事务同时修改相同的文档时，MDB 会检测到冲突：

```python
# 会话 1
with client.start_session() as session1:
    with session1.start_transaction():
        db.accounts.update_one({"name": "Alice"}, {"$inc": {"balance": -100}}, session=session1)

# 会话 2 (同时)
with client.start_session() as session2:
    with session2.start_transaction():
        # 修改相同的文档
        db.accounts.update_one({"name": "Alice"}, {"$inc": {"balance": 50}}, session=session2)
        # 提交时会抛出 WriteConflict 异常
        session2.commit_transaction()
```

### 10.3 因果一致性

MDB 支持因果一致性（Causal Consistency），确保：
- 读-己之写（Read-Your-Writes）：读取自己写入的数据
- 单调读（Monotonic Reads）：不会读到旧数据
- 单调写（Monotonic Writes）：写操作有序执行

### 10.4 Write Concern

```javascript
// 等待主节点确认 (w=1)
db.users.insertOne({ name: "张三" }, { writeConcern: { w: 1 } })

// 等待多数节点确认 (w="majority")
db.users.insertOne({ name: "张三" }, {
  writeConcern: { w: "majority", wtimeout: 5000 }
})

// 等待指定节点数确认 (w=2)
db.users.insertOne({ name: "张三" }, {
  writeConcern: { w: 2, j: true }  // j=true 等待日志落盘
})
```

### 10.5 Read Concern

```javascript
// local: 读取最新数据 (可能未被多数节点确认)
db.users.find({}, { readConcern: { level: "local" } })

// majority: 读取已被多数节点确认的数据
db.users.find({}, { readConcern: { level: "majority" } })

// snapshot: 读取一致性快照 (事务内使用)
```

---

## 11. 副本集

### 10.1 初始化副本集

```javascript
// 在第一个节点上执行
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "node1:27017", priority: 10 },
    { _id: 1, host: "node2:27017", priority: 5 },
    { _id: 2, host: "node3:27017", priority: 5 }
  ]
})

// 查看状态
rs.status()
```

### 10.2 副本集操作

```javascript
// 添加成员
rs.add("node4:27017")

// 添加仲裁节点
rs.addArb("node5:27017")

// 移除成员
rs.remove("node4:27017")

// 手动切换主节点
rs.stepDown(60)  // 降级60秒

// 查看主节点
rs.isMaster()

// 查看副本集配置
rs.conf()
```

### 10.3 读偏好

```python
# Python: 从副本读取
from pymongo.read_preferences import ReadPreference

# 只从主节点读
db.users.find(read_preference=ReadPreference.PRIMARY)

# 优先从副本读
db.users.find(read_preference=ReadPreference.SECONDARY_PREFERRED)

# 从最近的节点读
db.users.find(read_preference=ReadPreference.NEAREST)
```

---

## 12. 分片

### 12.1 配置分片

```javascript
// 添加分片
sh.addShard("shard0:27017")
sh.addShard("shard1:27018")

// 启用数据库分片
sh.enableSharding("mydb", { primary: "shard0" })

// 对集合分片 (哈希分片)
sh.shardCollection("mydb.users", { userId: "hashed" })

// 对集合分片 (范围分片)
sh.shardCollection("mydb.orders", { orderId: 1 })

// 复合分片键
sh.shardCollection("mydb.events", { region: 1, timestamp: 1 })
```

### 12.2 分片数据路由

MDB 支持自动数据路由：
- **Insert**：根据分片键自动路由到正确的分片
- **Find**：定向查询（分片键匹配）或广播查询（分片键不匹配）
- **Update/Delete**：定向更新/删除对应分片的文档

```javascript
// 插入时自动路由
db.users.insertOne({ userId: 12345, name: "张三" })

// 查询时自动路由
db.users.find({ userId: 12345 })  // 定向查询
db.users.find({ name: "张三" })    // 广播查询
```

### 12.3 分片管理

```javascript
// 查看分片状态
sh.status()

// 查看所有分片
sh.listShards()

// 手动迁移 chunk（包含数据迁移）
sh.moveChunk("mydb.users", { userId: 1000 }, "shard1")

// 分割 chunk
sh.splitAt("mydb.users", { userId: 5000 })

// 均衡器控制
sh.startBalancer()
sh.stopBalancer()
sh.getBalancerStatus()
```

### 12.4 Chunk 迁移

MDB 支持完整的 Chunk 迁移：
1. **迁移锁**：迁移期间缓冲写操作
2. **数据复制**：将源分片的数据复制到目标分片
3. **配置更新**：更新 ConfigDb 中的 chunk 归属
4. **缓冲应用**：迁移完成后应用缓冲的写操作

---

## 13. Change Stream

### 12.1 监听变更

```python
# Python 示例
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["mydb"]

# 监听集合变更
with db.users.watch() as stream:
    for change in stream:
        print(f"操作类型: {change['operationType']}")
        print(f"文档键: {change['documentKey']}")
        if 'fullDocument' in change:
            print(f"完整文档: {change['fullDocument']}")
```

### 12.2 高级用法

```javascript
// 带管道过滤
const pipeline = [
  { $match: { "operationType": "insert" } },
  { $project: { fullDocument: 1, documentKey: 1 } }
];

const changeStream = db.watch(pipeline, {
  fullDocument: "updateLookup",  // 更新时返回完整文档
  resumeAfter: resumeToken,      // 从指定位置恢复
  maxAwaitTimeMS: 60000          // 等待超时
});

// 获取变更事件
while (changeStream.hasNext()) {
  const event = changeStream.next();
  console.log(event);
}
```

### 12.3 恢复令牌

```javascript
// 保存恢复令牌
const resumeToken = changeStream.resumeToken;

// 从恢复令牌恢复
const newStream = db.watch([], { resumeAfter: resumeToken });

// 或从操作时间恢复
const newStream = db.watch([], {
  startAtOperationTime: new Date("2026-01-01T00:00:00Z")
});
```

---

## 14. GridFS

### 13.1 上传文件

```python
# Python: 上传文件
from mdb_native import MdbClient
from mdb_native.gridfs import GridFSBucket

client = MdbClient("localhost", 27017)
db = client.database("mydb")
bucket = GridFSBucket(db)

# 上传字节数据
file_id = bucket.upload("document.pdf", file_bytes)

# 上传文件
with open("image.png", "rb") as f:
    file_id = bucket.upload("image.png", f.read())
```

### 13.2 下载文件

```python
# 下载文件
data = bucket.download(file_id)

# 保存到文件
with open("downloaded.pdf", "wb") as f:
    f.write(bucket.download(file_id))
```

### 13.3 管理文件

```python
# 列出文件
files = bucket.list_files()
for file in files:
    print(f"{file['filename']} - {file['length']} bytes")

# 删除文件
bucket.delete(file_id)

# 重命名文件
bucket.rename(file_id, "new_name.pdf")
```

---

## 15. 安全特性

### 15.1 TLS 1.2/1.3 (标准 TLS)

MDB 支持标准 TLS 1.2/1.3，使用 rustls 实现：

```bash
# 生成 TLS 证书
openssl req -x509 -newkey rsa:4096 \
  -keyout server.key -out server.crt \
  -days 365 -nodes \
  -subj "/CN=mdb-server"
```

```yaml
# 配置文件
net:
  tls:
    enabled: true
    certificateKeyFile: /etc/mdb/tls/server.key
    CAFile: /etc/mdb/tls/ca.crt
```

### 15.2 TLCP 国密加密

MDB 同时支持 TLCP 国密协议（SM4-GCM/SM3）：

```yaml
# 启用 TLCP 国密加密
net:
  tls:
    mode: requireTLS
    certificateKeyFile: /etc/mdb/tls/server.pem
    CAFile: /etc/mdb/tls/ca.pem
    useGmssl: true
```

**生成国密证书**

```bash
# 生成 SM2 密钥对
gm-cert-gen -key sm2 -out server.key
gm-cert-gen -sign -key server.key -out server.crt \
  -subject "CN=mdb-server"

# 生成 CA 证书
gm-cert-gen -key sm2 -out ca.key
gm-cert-gen -sign -key ca.key -out ca.crt \
  -subject "CN=MDB-CA"

# 合并为 PEM
cat server.key server.crt > server.pem
```

### 15.3 TDE 透明加密

```javascript
// 启用集合加密
db.runCommand({
  encryptCollection: "users",
  algorithm: "SM4-GCM"
})

// 查看加密状态
db.runCommand({ keyStatus: "users" })

// 轮换加密密钥
db.runCommand({ rotateKey: "users" })

// 禁用加密
db.runCommand({ decryptCollection: "users" })
```

### 15.4 审计日志

```javascript
// 查看审计日志
db.adminCommand({ auditLog: {} })

// 按类型过滤
db.adminCommand({
  auditLog: { eventType: "Authentication" }
})

// 查看审计状态
db.adminCommand({ auditStatus: {} })

// 配置审计过滤器
db.adminCommand({
  auditFilter: {
    authEvents: true,
    dataEvents: true,
    adminEvents: true,
    securityEvents: true
  }
})
```

### 15.5 入侵检测

```javascript
// 查看安全告警
db.adminCommand({ securityAlerts: {} })

// 查看 IDS 状态
db.adminCommand({ idsStatus: {} })

// 手动封锁 IP
db.adminCommand({ blockIp: "192.168.1.100" })

// 解封 IP
db.adminCommand({ unblockIp: "192.168.1.100" })
```

---

## 16. 备份与恢复

### 15.1 全量备份

```javascript
// 触发备份
db.adminCommand({ backup: 1 })

// 查看备份列表
db.adminCommand({ listBackups: {} })
```

### 15.2 时间点恢复 (PITR)

```javascript
// 恢复到指定时间
db.adminCommand({
  pitr: { restoreTime: ISODate("2026-09-30T12:00:00Z") }
})

// 恢复到指定日志序列号
db.adminCommand({
  pitr: { restoreToLsn: 123456 }
})

// 查看可恢复点
db.adminCommand({ pitr: { listPoints: true } })
```

### 15.3 WAL 归档

```yaml
# 配置 WAL 归档
storage:
  mdb:
    walArchivePath: /var/lib/mdb/archive
    walArchiveRetentionDays: 7
```

---

## 17. 监控与运维

### 17.1 Prometheus 指标

MDB 暴露 Prometheus 格式指标：

```bash
# 访问指标端点
curl http://localhost:9216/metrics
```

**主要指标**

| 指标 | 类型 | 说明 |
|------|------|------|
| `mdb_connections_open` | Gauge | 当前打开的连接数 |
| `mdb_commands_total` | Counter | 命令总数 |
| `mdb_inserts_total` | Counter | 插入操作总数 |
| `mdb_finds_total` | Counter | 查询操作总数 |
| `mdb_updates_total` | Counter | 更新操作总数 |
| `mdb_deletes_total` | Counter | 删除操作总数 |
| `mdb_storage_data_size_bytes` | Gauge | 数据大小 |
| `mdb_storage_index_size_bytes` | Gauge | 索引大小 |
| `mdb_transactions_committed` | Counter | 已提交事务数 |
| `mdb_transactions_aborted` | Counter | 已中止事务数 |
| `mdb_audit_events_total` | Counter | 审计事件总数 |
| `mdb_auth_failures_total` | Counter | 认证失败总数 |

### 17.2 服务器状态

```javascript
// 查看服务器状态
db.adminCommand({ serverStatus: 1 })

// 查看连接状态
db.adminCommand({ connectionStatus: 1 })

// 查看当前操作
db.adminCommand({ currentOp: 1 })

// 终止操作
db.adminCommand({ killOp: <opid> })

// 查看主机信息
db.adminCommand({ hostInfo: 1 })

// 查看启动参数
db.adminCommand({ getCmdLineOpts: 1 })

// 查看操作统计
db.adminCommand({ top: 1 })
```

### 17.3 查询分析

```javascript
// 查看查询计划
db.users.explain().find({ name: "张三" })

// 启用慢查询分析
db.adminCommand({ profile: 1, slowms: 100 })

// 查看慢查询
db.system.profile.find().sort({ ts: -1 }).limit(10)

// 分析集合
db.adminCommand({ analyze: "users" })
```

### 17.4 数据验证

```javascript
// 验证集合数据完整性
db.adminCommand({ validate: "users" })
// 返回: { valid: true, errors: [], warnings: [], validity: {...} }

// 数据校验和
db.adminCommand({ dbHash: 1 })
// 返回: { collections: [{ collection, hash, size }] }

// 压缩集合
db.adminCommand({ compact: "users" })

// 索引统计
db.users.indexStatistics()
```

### 17.5 运行时参数

```javascript
// 设置参数
db.adminCommand({ setParameter: 1, profile: 2, slowms: 50 })

// 获取参数
db.adminCommand({ getParameter: 1, "*" })
db.adminCommand({ getParameter: 1, profile: 1, slowms: 1 })

// 修改集合
db.adminCommand({ collMod: "users", index: { name: "createdAt_1", expireAfterSeconds: 3600 } })
```

---

## 18. 客户端驱动

MDB 提供 5 种自研驱动，完全实现 BSON + Wire Protocol，不依赖任何 MongoDB 库。

### 17.1 Python 驱动

```bash
cd drivers/python/mdb-native
pip install -e .
```

```python
from mdb_native import MdbClient

client = MdbClient("localhost", 27017)
client.connect()

db = client.database("mydb")
coll = db.collection("users")

# CRUD
coll.insert_one({"name": "Alice"})
result = coll.find_one({"name": "Alice"})
coll.update_one({"name": "Alice"}, {"$set": {"age": 30}})
coll.delete_one({"name": "Alice"})

# 聚合
result = coll.aggregate([
    {"$match": {"age": {"$gte": 18}}},
    {"$group": {"_id": "$city", "count": {"$sum": 1}}}
])

# 游标
cursor = coll.find({"age": {"$gt": 20}}).sort("age").limit(10)
for doc in cursor:
    print(doc)
```

### 17.2 Java 驱动

```bash
cd drivers/java/mdb-native
mvn install
```

```java
import mdb.driver.MdbClient;
import mdb.driver.MdbCollection;

MdbClient client = new MdbClient("localhost", 27017);
client.connect();

MdbDatabase db = client.getDatabase("mydb");
MdbCollection coll = db.getCollection("users");

// 插入
Map<String, Object> doc = new HashMap<>();
doc.put("name", "Alice");
coll.insertOne(doc);

// 查询
FindCursor cursor = coll.find(Map.of("name", "Alice"));
while (cursor.hasNext()) {
    System.out.println(cursor.next());
}
```

### 17.3 Go 驱动

```bash
cd drivers/go/mdb-native
go mod tidy
```

```go
package main

import (
    "context"
    "fmt"
    mdb "github.com/mdb-official/mdb-go/mdb-native"
)

func main() {
    client, _ := mdb.NewClient("localhost", 27017)
    client.Connect()
    defer client.Close(context.Background())

    db := client.Database("mydb")
    coll := db.Collection("users")

    // 插入
    coll.InsertOne(context.Background(), map[string]interface{}{
        "name": "Alice",
        "age":  30,
    })

    // 查询
    cursor, _ := coll.Find(context.Background(), map[string]interface{}{})
    docs, _ := cursor.All()
    for _, doc := range docs {
        fmt.Println(doc)
    }
}
```

### 17.4 Node.js 驱动

```bash
cd drivers/nodejs/mdb-native
npm install
npm run build
```

```typescript
import { MdbClient } from 'mdb-native';

const client = new MdbClient('localhost', 27017);
await client.connect();

const db = client.database('mydb');
const coll = db.collection('users');

// 插入
await coll.insertOne({ name: 'Alice', age: 30 });

// 查询
const result = await coll.findOne({ name: 'Alice' });
console.log(result);

// 游标
const cursor = coll.find({ age: { $gt: 20 } }).sort({ age: 1 }).limit(10);
for await (const doc of cursor) {
    console.log(doc);
}
```

### 17.5 C/C++ 驱动

```bash
cd drivers/c/mdb-native
cmake -B build
cmake --build build
```

```c
#include <mdb_native/client.h>
#include <mdb_native/bson.h>

int main() {
    mdb_client_t* client = mdb_client_new("localhost", 27017);
    mdb_client_connect(client);

    mdb_database_t* db = mdb_client_database(client, "mydb");
    mdb_collection_t* coll = mdb_database_collection(db, "users");

    // 插入
    mdb_bson_t* doc = mdb_bson_new();
    mdb_bson_append_utf8(doc, "name", "Alice");
    mdb_bson_append_int32(doc, "age", 30);
    mdb_collection_insert_one(coll, doc, NULL, NULL);

    // 查询
    mdb_cursor_t* cursor = mdb_collection_find(coll, NULL);
    const mdb_bson_t* result;
    while (mdb_cursor_next(cursor, &result)) {
        // 处理结果
    }

    mdb_cursor_destroy(cursor);
    mdb_client_destroy(client);
    return 0;
}
```

---

## 19. 性能调优

### 18.1 存储引擎调优

```yaml
storage:
  mdb:
    # MemTable 大小：增大可减少写放大，但增加内存使用
    memtableSizeMB: 512

    # 块大小：增大可减少读放大，但增加内存使用
    blockSizeKB: 128

    # Bloom Filter：增大可减少误判率，但增加内存
    bloomBitsPerKey: 15

    # Compaction 线程数：根据 CPU 核数调整
    compactionThreads: 8

    # 压缩算法：none/lz4/snappy
    compression: lz4
```

### 18.2 索引优化

```javascript
// 1. 使用覆盖索引 (查询字段都在索引中)
db.users.createIndex({ name: 1, age: 1 })
db.users.find({ name: "张三" }, { name: 1, age: 1, _id: 0 })

// 2. 使用复合索引 (匹配查询条件顺序)
db.orders.createIndex({ userId: 1, createdAt: -1 })
db.orders.find({ userId: 123 }).sort({ createdAt: -1 })

// 3. 使用部分索引 (减少索引大小)
db.orders.createIndex(
  { status: 1 },
  { partialFilterExpression: { status: "active" } }
)

// 4. 查看索引使用情况
db.users.explain("executionStats").find({ name: "张三" })
```

### 18.3 查询优化

```javascript
// 1. 使用 $match 在管道最前
db.orders.aggregate([
  { $match: { status: "completed" } },  // 先筛选
  { $group: { _id: "$userId", total: { $sum: "$amount" } } }
])

// 2. 使用投影减少数据传输
db.users.find({}, { name: 1, email: 1, _id: 0 })

// 3. 使用 limit 限制结果集
db.users.find().sort({ age: -1 }).limit(100)

// 4. 避免全表扫描
db.users.createIndex({ email: 1 })
db.users.find({ email: "test@example.com" })
```

---

## 20. 常见问题

### Q1: 如何重置 root 密码？

```bash
# 1. 停止 MDB 服务
mdb-server --shutdown

# 2. 以无认证模式重启
mdb-server --noauth --port 27017

# 3. 连接并重置密码
mdbsh --port 27017
> use admin
> db.updateUser("admin", { pwd: "new_password" })

# 4. 重启 MDB 服务 (启用认证)
mdb-server --config /etc/mdb/mdb.conf
```

### Q2: 如何查看存储空间使用情况？

```javascript
// 数据库级别
db.stats()

// 集合级别
db.users.stats()

// 磁盘使用
db.adminCommand({ serverStatus: 1 }).storage
```

### Q3: 如何进行在线备份？

```javascript
// 触发热备
db.adminCommand({ backup: 1 })

// 查看备份状态
db.adminCommand({ listBackups: {} })
```

### Q4: 如何配置国密加密？

```yaml
# 1. 生成国密证书
# gm-cert-gen -key sm2 -out server.pem

# 2. 配置 MDB
net:
  tls:
    mode: requireTLS
    certificateKeyFile: /etc/mdb/tls/server.pem
    CAFile: /etc/mdb/tls/ca.pem
    useGmssl: true

# 3. 启用集合加密
# db.runCommand({ encryptCollection: "users", algorithm: "SM4-GCM" })
```

### Q5: 如何监控 MDB 性能？

```bash
# 1. 启用 Prometheus 指标 (默认端口 9216)
curl http://localhost:9216/metrics

# 2. 启用慢查询分析
db.adminCommand({ profile: 1, slowms: 100 })

# 3. 查看服务器状态
db.adminCommand({ serverStatus: 1 })
```

---

## 21. 附录

### 20.1 错误码

| 错误码 | 名称 | 说明 |
|--------|------|------|
| 2 | BadValue | 参数错误 |
| 13 | Unauthorized | 未授权 |
| 26 | NamespaceNotFound | 命名空间不存在 |
| 27 | IndexNotFound | 索引不存在 |
| 43 | CursorNotFound | 游标不存在 |
| 50 | MaxTimeMSExpired | 超时 |
| 59 | CommandNotImplemented | 命令未实现 |
| 112 | WriteConflict | 写冲突 |
| 11000 | DuplicateKey | 重复键 |
| 13000 | BadShardKey | 分片键错误 |

### 20.2 支持的 BSON 类型

| 类型 | 字节码 | 说明 |
|------|--------|------|
| Double | 0x01 | 双精度浮点 |
| String | 0x02 | UTF-8 字符串 |
| Document | 0x03 | 子文档 |
| Array | 0x04 | 数组 |
| Binary | 0x05 | 二进制数据 |
| ObjectId | 0x07 | 对象ID |
| Boolean | 0x08 | 布尔值 |
| DateTime | 0x09 | UTC 时间戳 |
| Null | 0x0A | 空值 |
| Regex | 0x0B | 正则表达式 |
| Int32 | 0x10 | 32位整数 |
| Timestamp | 0x11 | 时间戳 |
| Int64 | 0x12 | 64位整数 |
| Decimal128 | 0x13 | 128位十进制 |

### 20.3 端口使用

| 端口 | 用途 |
|------|------|
| 27017 | MDB 数据库服务 |
| 9216 | Prometheus 指标端点 |

### 20.4 文件路径

| 路径 | 用途 |
|------|------|
| `/etc/mdb/mdb.conf` | 配置文件 |
| `/var/lib/mdb/` | 数据目录 |
| `/var/log/mdb/` | 日志目录 |
| `/etc/mdb/tls/` | TLS 证书目录 |

### 20.5 相关文档

| 文档 | 说明 |
|------|------|
| [Wire Protocol 兼容性矩阵](01-wire-protocol-compat-matrix.md) | 协议兼容性详情 |
| [存储引擎设计](02-storage-engine-design.md) | LSM-Tree 架构设计 |
| [信创合规分析](03-xinchuang-mlps-compliance.md) | 等保三级合规分析 |
| [全系统架构](04-full-system-architecture.md) | 系统架构设计 |
| [等保三级自评](05-mlps-level3-self-assessment.md) | 等保自评报告 |
| [安全加固指南](06-security-hardening-guide.md) | 安全部署建议 |

---

**MDB — 信创自主可控文档数据库**
