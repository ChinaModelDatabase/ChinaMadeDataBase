# MDB Java 示例项目

基于自研驱动 `mdb-native` 的完整 Java 示例，演示 MDB 的全部核心能力：

| 示例类 | 功能 |
|--------|------|
| `ConnectionExample` | 连接、SCRAM 认证、Ping、Server Status、列出数据库、优雅断开 |
| `CrudExample` | insert/find/update/delete、过滤与更新操作符、findAndModify、游标、distinct、count |
| `IndexExample` | 单字段/复合/唯一/TTL/文本索引、listIndexes、dropIndex、collStats、explain |
| `AggregationExample` | $match / $group / $sort / $limit / $skip / $project / $unwind / $count、管道串联 |
| `TransactionExample` | 事务提交与回滚、WriteConcern、ReadConcern、可重试写入 (lsid + txnNumber) |
| `GridFSExample` | 文件上传/下载/列表/删除、大文件分块存储 |
| `ChangeStreamExample` | 打开 Change Stream、监听 insert/update/delete、resume token、管道过滤 |
| `AdminExample` | createUser/dropUser/grantRolesToUser/listUsers、collStats/dbStats/serverStatus、explain、validate |

**不依赖 MongoDB 官方驱动**，仅使用本地自研的 `mdb.driver` 包。

## 环境要求

- JDK 11+
- Maven 3.6+
- 运行示例需要可连接的 MDB 服务器（默认 `localhost:27017`）

## 目录结构

```
examples/java/mdb-example/
├── pom.xml
├── README.md
└── src/main/java/mdb/example/
    ├── Main.java               # 主入口，按顺序运行全部示例
    ├── ConnectionExample.java  # 连接示例
    ├── CrudExample.java        # CRUD 操作
    ├── IndexExample.java       # 索引管理
    ├── AggregationExample.java # 聚合管道
    ├── TransactionExample.java # 事务
    ├── GridFSExample.java      # GridFS 文件存储
    ├── ChangeStreamExample.java# Change Stream
    └── AdminExample.java       # 管理操作
```

## 构建步骤

### 1. 安装自研驱动到本地 Maven 仓库

示例依赖 `mdb:mdb-native:1.0.0`，请先在驱动目录执行安装：

```bash
# 在仓库根目录
mvn install -f drivers/java/mdb-native/pom.xml
```

### 2. 编译示例

```bash
cd examples/java/mdb-example
mvn compile
```

## 运行方式

### 运行全部示例

```bash
cd examples/java/mdb-example
mvn compile exec:java
```

### 指定服务器地址

```bash
mvn compile exec:java -Dexec.args="127.0.0.1 27017"
```

### 只运行某一个示例

```bash
# 参数顺序: host port 示例名
mvn compile exec:java -Dexec.args="localhost 27017 crud"
```

可用示例名：`connection` / `crud` / `index` / `aggregation` / `transaction` / `gridfs` / `changestream` / `admin`

### 单独运行某个示例类

每个示例类都有独立的 `main` 方法：

```bash
mvn compile exec:java -Dexec.mainClass=mdb.example.CrudExample -Dexec.args="localhost 27017"
```

或使用编译后的 classpath：

```bash
mvn dependency:build-classpath -Dmdep.outputFile=cp.txt
java -cp "target/classes;$(cat cp.txt)" mdb.example.CrudExample localhost 27017
```

## 示例说明

- 每个示例会自建演示用的数据库/集合（主要为 `mdb_example_db`），运行结束前会清理数据。
- 输出中的中文注释逐步解释每一步在做什么，适合对照学习驱动 API。
- 部分高级特性（TTL/文本索引、事务隔离、Change Stream）依赖服务器侧实现；若服务器尚未支持，示例会打印错误信息并继续演示协议用法，不影响其他示例运行。

## 驱动 API 速查

```java
import mdb.driver.MdbClient;
import mdb.driver.MdbDatabase;
import mdb.driver.MdbCollection;
import mdb.driver.FindCursor;
import mdb.driver.GridFS;

// 连接
MdbClient client = new MdbClient("localhost", 27017);
client.connect();

// 库 / 集合
MdbDatabase db = client.getDatabase("mydb");
MdbCollection coll = db.getCollection("users");

// CRUD
coll.insertOne(map("name", "Alice"));
FindCursor cursor = coll.find(map("age", map("$gte", 18))).sort(map("age", 1)).limit(10);
while (cursor.hasNext()) { System.out.println(cursor.next()); }
cursor.close();

// 聚合
Map<String, Object> result = coll.aggregate(pipeline);

// GridFS
GridFS fs = new GridFS(db, "files");
String id = fs.upload("a.txt", data);
byte[] out = fs.download(id);

client.close();
```

更多细节请直接阅读 `src/main/java/mdb/example/` 下各示例类的注释。
