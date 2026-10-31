# MDB 功能使用示例

本文档提供 MDB 每个功能的使用示例，包括 Python、JavaScript 和命令行三种方式。

---

## 目录

1. [CRUD 操作](#1-crud-操作)
2. [查询运算符](#2-查询运算符)
3. [索引管理](#3-索引管理)
4. [聚合管道](#4-聚合管道)
5. [事务](#5-事务)
6. [副本集](#6-副本集)
7. [分片](#7-分片)
8. [Change Stream](#8-change-stream)
9. [GridFS](#9-gridfs)
10. [安全特性](#10-安全特性)
11. [运维命令](#11-运维命令)

---

## 1. CRUD 操作

### 1.1 插入文档

```python
# Python (pymongo)
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["mydb"]
coll = db["users"]

# 插入单个文档
coll.insert_one({
    "name": "张三",
    "age": 30,
    "email": "zhangsan@example.com",
    "address": {
        "city": "北京",
        "district": "海淀区"
    },
    "tags": ["VIP", "活跃"]
})

# 插入多个文档
coll.insert_many([
    {"name": "李四", "age": 25},
    {"name": "王五", "age": 35},
    {"name": "赵六", "age": 28}
])

# 批量插入 (有序)
coll.insert_many([
    {"name": "用户1"},
    {"name": "用户2"}
], ordered=True)

# 批量插入 (无序，遇到错误继续)
coll.insert_many([
    {"name": "用户3"},
    {"name": "用户4"}
], ordered=False)
```

```javascript
// JavaScript (mongodb driver)
const { MongoClient } = require("mongodb");
const client = new MongoClient("mongodb://localhost:27017");
await client.connect();
const db = client.db("mydb");
const coll = db.collection("users");

// 插入单个文档
await coll.insertOne({
    name: "张三",
    age: 30,
    email: "zhangsan@example.com"
});

// 插入多个文档
await coll.insertMany([
    { name: "李四", age: 25 },
    { name: "王五", age: 35 }
]);
```

### 1.2 查询文档

```python
# Python
# 查询所有文档
for doc in coll.find():
    print(doc)

# 条件查询
for doc in coll.find({"age": {"$gt": 25}}):
    print(doc)

# 精确查询
doc = coll.find_one({"name": "张三"})

# OR 查询
for doc in coll.find({"$or": [{"age": {"$lt": 25}}, {"age": {"$gt": 35}}]}):
    print(doc)

# 投影 (只返回指定字段)
for doc in coll.find({}, {"name": 1, "age": 1, "_id": 0}):
    print(doc)

# 排序
for doc in coll.find().sort("age", -1):  # 降序
    print(doc)

# 分页
for doc in coll.find().skip(10).limit(20):
    print(doc)

# 统计数量
count = coll.count_documents({"age": {"$gte": 18}})

# 查询唯一值
cities = coll.distinct("city")
```

### 1.3 更新文档

```python
# Python
# 更新单个文档
coll.update_one(
    {"name": "张三"},
    {"$set": {"age": 31}}
)

# 更新多个文档
coll.update_many(
    {"age": {"$lt": 30}},
    {"$inc": {"age": 1}}
)

# upsert (不存在则插入)
coll.update_one(
    {"name": "新用户"},
    {"$set": {"age": 25}},
    upsert=True
)

# 替换文档
coll.replace_one(
    {"name": "张三"},
    {"name": "张三丰", "age": 100}
)

# 数组操作
coll.update_one(
    {"name": "张三"},
    {"$push": {"tags": "新标签"}}
)

# 删除数组元素
coll.update_one(
    {"name": "张三"},
    {"$pull": {"tags": "旧标签"}}
)
```

### 1.4 删除文档

```python
# Python
# 删除单个文档
coll.delete_one({"name": "李四"})

# 删除多个文档
coll.delete_many({"age": {"$lt": 25}})

# 删除所有文档
coll.delete_many({})
```

---

## 2. 查询运算符

### 2.1 比较运算符

```python
# Python
# 等于
coll.find({"age": 30})
coll.find({"age": {"$eq": 30}})

# 不等于
coll.find({"age": {"$ne": 30}})

# 大于/大于等于
coll.find({"age": {"$gt": 25}})
coll.find({"age": {"$gte": 25}})

# 小于/小于等于
coll.find({"age": {"$lt": 30}})
coll.find({"age": {"$lte": 30}})

# 在数组中
coll.find({"city": {"$in": ["北京", "上海"]}})

# 不在数组中
coll.find({"city": {"$nin": ["北京", "上海"]}})
```

### 2.2 逻辑运算符

```python
# Python
# AND
coll.find({"$and": [{"age": {"$gte": 25}}, {"city": "北京"}]})

# OR
coll.find({"$or": [{"age": {"$lt": 25}}, {"age": {"$gt": 35}}]})

# NOR (都不满足)
coll.find({"$nor": [{"age": {"$lt": 25}}, {"age": {"$gt": 35}}]})
```

### 2.3 元素运算符

```python
# Python
# 字段存在
coll.find({"email": {"$exists": True}})

# 字段类型
coll.find({"age": {"$type": "int"}})
coll.find({"name": {"$type": "string"}})

# 数组长度
coll.find({"tags": {"$size": 3}})

# 数组包含所有元素
coll.find({"tags": {"$all": ["VIP", "活跃"]}})

# 数组元素匹配
coll.find({"tags": {"$elemMatch": {"$gte": "A", "$lt": "M"}}})
```

### 2.4 位运算符

```python
# Python
# 所有位都为 0
coll.find({"flags": {"$bitsAllClear": 5}})

# 所有位都为 1
coll.find({"flags": {"$bitsAllSet": 5}})

# 至少一位为 0
coll.find({"flags": {"$bitsAnyClear": 5}})

# 至少一位为 1
coll.find({"flags": {"$bitsAnySet": 5}})
```

### 2.5 其他运算符

```python
# Python
# 取模
coll.find({"value": {"$mod": [3, 0]}})  # 能被 3 整除

# 正则表达式
coll.find({"name": {"$regex": "^张"}})
coll.find({"name": {"$regex": "^张", "$options": "i"}})  # 忽略大小写

# $where (JavaScript 表达式)
coll.find({"$where": "this.age > 25 && this.city == '北京'"})
coll.find({"$where": "this.name.startsWith('张')"})

# $expr (聚合表达式)
coll.find({"$expr": {"$gt": ["$price", "$cost"]}})
coll.find({"$expr": {"$eq": ["$status", "completed"]}})
```

### 2.6 点号路径查询

```python
# Python
# 查询嵌套字段
coll.find({"address.city": "北京"})
coll.find({"address.district": "海淀区"})

# 更新嵌套字段
coll.update_one({"_id": 1}, {"$set": {"address.city": "上海"}})
```

---

## 3. 索引管理

### 3.1 创建索引

```python
# Python
# 单字段索引
coll.create_index([("name", 1)])

# 复合索引
coll.create_index([("name", 1), ("age", -1)])

# 唯一索引
coll.create_index([("email", 1)], unique=True)

# TTL 索引 (文档在 expireAfterSeconds 后自动删除)
db.logs.create_index([("createdAt", 1)], expireAfterSeconds=86400)

# 文本索引
db.articles.create_index([("title", "text"), ("content", "text")])

# 地理空间索引
db.places.create_index([("location", "2dsphere")])

# Hashed 索引
db.users.create_index([("email", "hashed")])

# Multikey 索引 (数组字段索引)
db.users.create_index([("tags", "multikey")])

# Partial 索引 (只索引满足条件的文档)
db.users.create_index(
    [("age", 1)],
    partialFilterExpression={"age": {"$gt": 0}}
)
```

### 3.2 索引统计

```python
# Python
# 获取索引统计信息
stats = db.command({"indexStatistics": "users"})
print(stats)
# {'ok': 1.0, 'ns': 'mydb.users', 'stats': [{name, type, fields, entries, unique}, ...]}
```

### 3.3 索引恢复

索引定义持久化到 Catalog，重启后自动恢复。无需手动重建索引。

---

## 4. 聚合管道

### 4.1 基本聚合

```python
# Python
# 分组统计
pipeline = [
    {"$group": {
        "_id": "$category",
        "totalAmount": {"$sum": "$amount"},
        "avgAmount": {"$avg": "$amount"},
        "count": {"$sum": 1}
    }}
]
result = list(coll.aggregate(pipeline))

# 排序 + 限制
pipeline = [
    {"$group": {"_id": "$userId", "total": {"$sum": "$amount"}}},
    {"$sort": {"total": -1}},
    {"$limit": 10}
]
result = list(coll.aggregate(pipeline))

# 筛选 + 投影
pipeline = [
    {"$match": {"age": {"$gte": 18}}},
    {"$project": {"name": 1, "age": 1, "adult": {"$gte": ["$age", 18]}}}
]
result = list(coll.aggregate(pipeline))
```

### 4.2 关联查询 ($lookup)

```python
# Python
pipeline = [
    {"$lookup": {
        "from": "users",
        "localField": "userId",
        "foreignField": "_id",
        "as": "user"
    }},
    {"$unwind": "$user"}
]
result = list(coll.aggregate(pipeline))
```

### 4.3 图遍历 ($graphLookup)

```python
# Python
pipeline = [
    {"$graphLookup": {
        "from": "employees",
        "startWith": "$managerId",
        "connectFromField": "managerId",
        "connectToField": "_id",
        "as": "managers",
        "maxDepth": 5
    }}
]
result = list(coll.aggregate(pipeline))
```

### 4.4 窗口函数 ($setWindowFields)

```python
# Python
pipeline = [
    {"$setWindowFields": {
        "partitionBy": "$region",
        "sortBy": {"amount": -1},
        "output": {
            "rank": {"$rank": {}}
        }
    }}
]
result = list(coll.aggregate(pipeline))
```

### 4.5 其他聚合阶段

```python
# Python
# $bucket 分桶
pipeline = [
    {"$bucket": {
        "groupBy": "$age",
        "boundaries": [0, 18, 30, 50, 100],
        "default": "其他",
        "output": {"count": {"$sum": 1}}
    }}
]

# $sample 随机采样
pipeline = [{"$sample": {"size": 10}}]

# $facet 并行管道
pipeline = [
    {"$facet": {
        "total": [{"$group": {"_id": None, "sum": {"$sum": "$amount"}}}],
        "recent": [{"$limit": 5}]
    }}
]

# $redact 字段脱敏
pipeline = [
    {"$redact": {
        "$cond": {
            "if": {"$eq": ["$visibility", "secret"]},
            "then": "$$DROP",
            "else": "$$KEEP"
        }
    }}
]
```

---

## 5. 事务

### 5.1 基本事务

```python
# Python
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

### 5.2 Write Conflict 检测

```python
# Python
# 当两个事务同时修改相同的文档时，会检测到冲突
import pymongo
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")

# 会话 1
session1 = client.start_session()
session1.start_transaction()
db.accounts.update_one(
    {"name": "Alice"},
    {"$inc": {"balance": -100}},
    session=session1
)

# 会话 2 (同时)
session2 = client.start_session()
session2.start_transaction()
try:
    db.accounts.update_one(
        {"name": "Alice"},  # 修改相同的文档
        {"$inc": {"balance": 50}},
        session=session2
    )
    session2.commit_transaction()
except pymongo.errors.OperationFailure as e:
    if "WriteConflict" in str(e):
        print("检测到 WriteConflict，事务已回滚")
        session2.abort_transaction()
```

### 5.3 因果一致性

```python
# Python
# MDB 支持因果一致性，确保：
# - 读-己之写（Read-Your-Writes）
# - 单调读（Monotonic Reads）
# - 单调写（Monotonic Writes）

with client.start_session() as session:
    with session.start_transaction():
        db.users.update_one({"name": "Alice"}, {"$set": {"age": 31}}, session=session)
        # 事务内可以读到自己写入的数据
        doc = db.users.find_one({"name": "Alice"}, session=session)
        print(doc["age"])  # 31
```

---

## 6. 副本集

### 6.1 配置副本集

```python
# Python
from pymongo import MongoClient

# 连接到副本集
client = MongoClient([
    "localhost:27017",
    "localhost:27018",
    "localhost:27019"
], replicaSet="mdb-rs")

# 查看副本集状态
status = client.admin.command("replSetGetStatus")
print(status)

# 查看副本集配置
config = client.admin.command("replSetGetConfig")
print(config)
```

### 6.2 管理副本集

```python
# Python
# 初始化副本集
client.admin.command({
    "replSetInitiate": {
        "_id": "mdb-rs",
        "members": [
            {"_id": 0, "host": "localhost:27017"},
            {"_id": 1, "host": "localhost:27018"},
            {"_id": 2, "host": "localhost:27019"}
        ]
    }
})

# 重新配置
client.admin.command({
    "replSetReconfig": {
        "members": [
            {"_id": 0, "host": "localhost:27017"},
            {"_id": 1, "host": "localhost:27018"}
        ]
    }
})
```

---

## 7. 分片

### 7.1 配置分片

```python
# Python
from pymongo import MongoClient

client = MongoClient("localhost:27017")
admin = client.admin

# 添加分片
admin.command("addShard", "shard0:27017")
admin.command("addShard", "shard1:27018")

# 启用数据库分片
admin.command("enableSharding", "mydb", primary="shard0")

# 对集合分片 (哈希分片)
admin.command("shardCollection", "mydb.users", key={"userId": "hashed"})

# 对集合分片 (范围分片)
admin.command("shardCollection", "mydb.orders", key={"orderId": 1})
```

### 7.2 分片数据路由

```python
# Python
# 插入时自动路由
db.users.insertOne({"userId": 12345, "name": "张三"})

# 查询时自动路由
db.users.find({"userId": 12345})  # 定向查询
db.users.find({"name": "张三"})    # 广播查询
```

### 7.3 分片管理

```python
# Python
# 查看分片状态
admin.command("listShards")

# 手动迁移 chunk
admin.command("moveChunk", "mydb.users", {"userId": 1000}, toShard="shard1")

# 分割 chunk
admin.command("splitAt", "mydb.users", {"userId": 5000})

# 均衡器控制
admin.command("balancerStart")
admin.command("balancerStop")
admin.command("balancerStatus")
```

---

## 8. Change Stream

### 8.1 监听变更

```python
# Python
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

### 8.2 高级用法

```python
# Python
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["mydb"]

# 带管道过滤
pipeline = [
    {"$match": {"operationType": "insert"}},
    {"$project": {"fullDocument": 1, "documentKey": 1}}
]

with db.users.watch(pipeline, fullDocument="updateLookup") as stream:
    for change in stream:
        print(change)

# 使用恢复令牌
with db.users.watch(resumeAfter=resume_token) as stream:
    for change in stream:
        print(change)
```

---

## 9. GridFS

### 9.1 上传文件

```python
# Python
from pymongo import MongoClient
from gridfs import GridFS

client = MongoClient("mongodb://localhost:27017")
db = client["mydb"]
fs = GridFS(db)

# 上传文件
with open("example.txt", "rb") as f:
    file_id = fs.put(f, filename="example.txt")

print(f"文件 ID: {file_id}")

# 上传大文件
with open("large.bin", "rb") as f:
    file_id = fs.put(f, filename="large.bin", chunk_size=255*1024)
```

### 9.2 下载文件

```python
# Python
# 下载文件
with fs.get(file_id) as f:
    content = f.read()
    print(f"文件内容: {content}")

# 按文件名查找
file_doc = fs.find_one({"filename": "example.txt"})
if file_doc:
    with fs.get(file_doc._id) as f:
        content = f.read()
```

### 9.3 管理文件

```python
# Python
# 列出文件
for file_doc in fs.find():
    print(f"文件名: {file_doc.filename}, 大小: {file_doc.length}")

# 重命名文件
fs.put(data, filename="new_name.txt", _id=file_id)

# 删除文件
fs.delete(file_id)
```

---

## 10. 安全特性

### 10.1 TLS 加密

```python
# Python
from pymongo import MongoClient

# 使用 TLS 连接
client = MongoClient(
    "mongodb://localhost:27017",
    tls=True,
    tlsCertificateKeyFile="/path/to/client.pem",
    tlsCAFile="/path/to/ca.pem"
)
```

### 10.2 TDE 透明加密

```python
# Python
# 启用集合加密
client.admin.command({
    "encryptCollection": "users",
    "algorithm": "SM4-GCM"
})

# 查看加密状态
client.admin.command({"keyStatus": "users"})

# 轮换加密密钥
client.admin.command({"rotateKey": "users"})

# 禁用加密
client.admin.command({"decryptCollection": "users"})
```

### 10.3 RBAC 权限

```python
# Python
from pymongo import MongoClient
from pymongo.auth import AuthenticationMechanism

client = MongoClient("mongodb://localhost:27017")
db = client["admin"]

# 创建用户
db.command({
    "createUser": "app_user",
    "pwd": "secure_password",
    "roles": [
        {"role": "readWrite", "db": "mydb"},
        {"role": "dbAdmin", "db": "mydb"}
    ]
})

# 使用认证连接
client = MongoClient(
    "mongodb://app_user:secure_password@localhost:27017/mydb",
    authMechanism="SCRAM-SHA-256"
)
```

---

## 11. 运维命令

### 11.1 服务器状态

```python
# Python
client = MongoClient("mongodb://localhost:27017")

# 查看服务器状态
status = client.admin.command("serverStatus")
print(status["version"])
print(status["connections"])

# 查看当前操作
ops = client.admin.command("currentOp")
print(ops)

# 查看主机信息
host_info = client.admin.command("hostInfo")
print(host_info["system"]["numCores"])

# 查看启动参数
opts = client.admin.command("getCmdLineOpts")
print(opts["parsed"])
```

### 11.2 查询分析

```python
# Python
# 查看查询计划
explain = db.users.find({"name": "张三"}).explain()
print(explain["queryPlanner"]["winningPlan"])

# 启用慢查询分析
client.admin.command({"profile": 1, "slowms": 100})

# 查看慢查询
for doc in db.system.profile.find().sort("ts", -1).limit(10):
    print(doc)

# 分析集合
result = client.admin.command({"analyze": "users"})
print(result)
```

### 11.3 数据验证

```python
# Python
# 验证集合数据完整性
result = client.admin.command({"validate": "users"})
print(f"有效: {result['valid']}")
print(f"文档数: {result['validity']['count']}")

# 数据校验和
result = client.admin.command({"dbHash": 1})
for coll in result["collections"]:
    print(f"{coll['collection']}: {coll['hash']}")

# 压缩集合
result = client.admin.command({"compact": "users"})
print(result)

# 索引统计
stats = db.command({"indexStatistics": "users"})
print(stats)
```

### 11.4 运行时参数

```python
# Python
# 设置参数
client.admin.command({
    "setParameter": 1,
    "profile": 2,
    "slowms": 50
})

# 获取参数
params = client.admin.command({"getParameter": 1, "*"})
print(params["profile"])
print(params["slowms"])

# 修改集合
client.admin.command({
    "collMod": "users",
    "index": {
        "name": "createdAt_1",
        "expireAfterSeconds": 3600
    }
})
```

---

## 命令行客户端示例

```bash
# 连接到本地服务器
cargo run -p mdb-cli -- 127.0.0.1 27018

# 使用连接字符串
cargo run -p mdb-cli -- "mongodb://127.0.0.1:27018"

# 交互式命令
mdb> help
mdb> exit
```

---

## 性能调优建议

1. **索引优化**：为常用查询字段创建索引
2. **批量操作**：使用 insertMany/bulkWrite 替代多次 insertOne
3. **投影优化**：只返回需要的字段
4. **分页优化**：使用 skip+limit 或基于游标的分页
5. **压缩**：根据数据特征选择 LZ4/Snappy/zstd
6. **BlockCache**：调整缓存大小以适应工作负载
