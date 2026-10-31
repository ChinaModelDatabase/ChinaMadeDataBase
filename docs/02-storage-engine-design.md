# MDB 存储引擎设计

| 项 | 值 |
|---|---|
| 文档版本 | 0.1.0（草稿） |
| 对应 MDB 版本 | 0.1.0 |
| 实现位置 | `crates/mdb-storage`（当前 19 行骨架，无实现） |
| 相关文档 | `docs/01-wire-protocol-compat-matrix.md` |
| 编写日期 | 2026-09-29 |

---

## 0. 文档状态与前置约束

**本文档是设计，不是实现描述。`mdb-storage` 尚未落地，全文没有任何一行生产代码。**

设计必须同时满足以下既有约束，缺一不可：

| 约束 | 来源 | 对设计的强制影响 |
|---|---|---|
| 零第三方运行时依赖 | workspace `Cargo.lock` 仅含 14 个内部 crate | 无现成 LSM/B+树/压缩/位图库，全部自研 |
| `#![forbid(unsafe_code)]` | 全部 14 个 crate | **禁止 mmap**，只能走 `std::fs` 的 `&File` 定位读写 |
| 信创合规 | `docs/03` 规划 | TDE 用 SM4-GCM，校验和需 SM3 选项 |
| 与 MongoDB 行为兼容 | `docs/01` | 需对齐写关注（writconcern）与可见性语义 |

### 0.1 架构已由既有代码锁定

`mdb_common::config::StorageConfig` 已存在的字段事实上**排除了 B+树路线**，本设计不重新讨论选型：

| 字段 | 默认值 | 含义 |
|---|---|---|
| `wal_dir` | `None` | WAL 目录，空则落在 `dbpath` 下 |
| `wal_group_commit_us` | 200 | 组提交窗口（微秒） |
| `memtable_size_mb` | 256 | MemTable 刷盘阈值 |
| `max_memtable_count` | 3 | 同时存在的冻结 memtable 上限 |
| `l0_compaction_trigger` | 4 | L0 文件数达到该值触发压实 |
| `max_files_per_level` | 10 | L1 及以下每层文件数上限 |
| `compaction_threads` | 4 | 压实线程数 |
| `compaction_bandwidth_mb` | 0 | 压实 IO 限速，0 为不限 |
| `block_size_kb` | 64 | SSTable 块大小 |
| `bloom_bits_per_key` | 10 | Bloom filter 每键位数 |
| `checksum` | `Read` | `Off` / `Read` / `ReadBackground` |

**结论：MDB 采用 LSM 树（memtable + L0 + 分层压实），与 MongoDB 的 WiredTiger B+树 是不同架构。** 这一差异会影响读放大、点查延迟分布与 compaction 行为，必须在迁移评估中告知用户，不能默认"兼容即同构"。

---

## 1. 目标与非目标

### 1.1 目标

1. 崩溃后**已确认写入的数据绝不丢失**，且**绝不返回未确认的数据**。
2. 键值读取的可见性可由快照时间戳精确判定，支持多版本并发读。
3. 数据完整性可校验（块级校验和），静默位翻转必须被发现。
4. 静态数据加密（TDE）后仍能完成 compaction，不在用户态长期持有明文数据密钥。
5. 所有失败路径返回错误，**不允许 panic，不允许静默丢数据**。

### 1.2 非目标（本期明确不做）

- 不做块层/文件层压缩（无依赖即无 zstd/zlib，详见 §10.1）
- 不做副本集内部复制协议（属 `mdb-repl`）
- 不做分片路由（属 `mdb-router`）
- 不做 Direct I/O / `io_uring`（需 `unsafe`）
- 不做列式统计信息优化

---

## 2. 总体架构

```text
        命令层 (mdb-query)
               │
        ┌──────┴───────┐
        │  StorageEngine│  对外：get / scan / write / apply_txn
        └──────┬───────┘
               │
    ┌──────────┼────────────┬──────────────┐
    │          │            │              │
 MemTable   WAL 组提交    ColumnFamily     快照管理
 (mutable)  (durability)   (每集合一个)   (可见性判定)
    │          │            │
    └────┬─────┘            │
         │ 冻结/刷盘         │
         ▼                  ▼
      L0 SST  ◄── 压实 ──►  L1..Ln SST
                              │
                    Bloom filter + 索引块 + 数据块
```

### 2.1 ColumnFamily 映射

| MongoDB 概念 | MDB 映射 | 说明 |
|---|---|---|
| 集合 (collection) | 一个 ColumnFamily | 独立 SST 目录、独立的 compaction 状态 |
| 索引 (secondary index) | 同一 CF 内的独立索引列 | 见 `mdb-index`，本文不展开 |
| `_id` 主键 | 主索引列 | 键即 BSON 值，序为 BSON 类型序 |
| oplog | 独立 CF | 属 `mdb-repl` |

**集合名到目录名必须做转义**：集合名允许 `.`、`$`、空格与 Unicode，直接拼路径会产生穿越与非法文件名。映射需为 `encode(collection_name) -> dir_name` 的**双射且可逆**，实现前必须补往返测试。

---

## 3. 磁盘布局

```text
<dbpath>/
  CURRENT                     # 指向 MANIFEST-<N>，原子替换
  MANIFEST-<N>                # 当前文件清单与版本号
  IDENTITY
  journal/
    wal-<seq>.log             # WAL 段
  colcf/
    <encoded_name>/
      000014.sst              # SST 文件
      000015.sst
    _id_index/
      ...
  encryption/
    keyring.meta              # 密钥元数据（不含明文密钥）
```

### 3.1 原子性基础

`CURRENT` 指向 `MANIFEST-<N>`。更新流程必须是：

```text
1. 写 MANIFEST-<N+1>（完整新清单）
2. fsync(MANIFEST-<N+1>)
3. 写临时 CURRENT.tmp，写入 "MANIFEST-<N+1>\n"
4. fsync(CURRENT.tmp)
5. rename(CURRENT.tmp, CURRENT)     // 同目录 rename 在多数文件系统上是原子的
6. fsync(<dbpath> 目录)
```

**第 6 步不可省略。** 缺少目录 fsync 时，崩溃后 `rename` 可能已生效而目录项未落盘，导致回退到更旧的 MANIFEST，进而丢失已确认的 SST 引用。

### 3.2 命名约束

- 文件名为**零填充十进制**且单调递增（`000014.sst`），保证字典序 = 时间序
- 任何时刻同一 CF 内不得出现重复文件名
- 临时文件一律用 `.tmp` 后缀；恢复时**先删除全部 `.tmp`** 再读 MANIFEST

---

## 4. SSTable 格式

```text
┌──────────────┬──────────────┬───────────┬──────────────┬────────┬────────┐
│ Data Block 0 │ Data Block 1 │  ...      │ Index Block  │ Filter │ Footer │
└──────────────┴──────────────┴───────────┴──────────────┴────────┴────────┘
                                                               └─ 定长 ─┘
```

### 4.1 块通用结构

```text
offset  size  field
0       1     block_type   (0=data 1=index 2=meta 3=filter)
1       2     restart_count
3       4     uncompressed_size   (预留，当前恒等于压缩后大小)
7       8     sequence_range_hi    (该块内最大 seqno，便于裁剪)
15      4     crc32c               (覆盖本块除本字段外的全部字节)
19      2     num_entries
21      2     restart_offset_count
23      4     restart_array_offset
27      var   entries...
              restart array (uint16 偏移列表)
```

- `block_size_kb` 默认 64 KiB。**块大小是读放大的主要调节旋钮**：块大则顺序读与压缩更优，块小则随机读放大更小。
- `crc32c` 覆盖块内其余全部字节，**包括尾部填充字节**，否则尾部损坏不可检测。

### 4.2 记录编码（重启点前缀压缩）

```text
entry := shared_len(uvarint) | unshared_len(uvarint) | value_len(uvarint)
       | shared_bytes | unshared_bytes | value_bytes
```

- 每 `restart_interval`（建议 16）条记录设一个重启点，此时 `shared_len = 0`
- 重启点之间可引用前一条记录的公共前缀
- **重启点必须解码为完整键**；索引块只存重启点对应的键，节省索引空间

前缀压缩对本项目收益显著：BSON 键的相邻性高（如 `a.b.c` 系列），且相邻记录通常共享长前缀。

### 4.3 内部键（Internal Key）

MVCC 要求同一用户键的所有版本**相邻且新版本在前**，否则每层扫描都要去重多版本。

```text
internal_key := key_len(u32, big-endian)
              | user_key (key_len 字节, BSON 序编码的用户键)
              | 0x00
              | inverted_seqno(u64, big-endian)   // = u64::MAX - seqno
```

- 用 `u32` 长度前缀而非 `0x00` 分隔：用户键是 BSON 值而非 cstring，**可以合法包含 `0x00`**，用分隔符会歧义
- `inverted_seqno` 使**字节序升序 = 同键内版本新到旧**
- 扫描时"第一条命中即为可见版本"，配合 `key_len` 前缀可安全地跳过同键其余版本

### 4.4 SST 尾部

```text
footer (定长 56 字节):
  magic          u64   = 0x4D444253544C3031 ("MDBS TL01")
  format_version u32
  index_offset   u64
  index_length   u32
  filter_offset  u64   (0 表示无 Bloom)
  filter_length  u32
  seqno_min      u64
  seqno_max      u64
  crc32c         u32
```

**从 Footer 末尾反向读取**定位索引与 filter，避免在文件头写易损的偏移量。

### 4.5 Bloom filter

- 参数 `bloom_bits_per_key` 默认 10，取最优散列数 k = 7 时误判率约 **0.8%**（`0.5^6.93`）
- **按 CF 内不同的用户键计数**，不按记录数；否则重复键多时 filter 退化
- 哈希取双哈希（k1·h1 + k2·h2）从 64 位摘要拆分
- 摘要算法：默认 CRC64 或 SM3 截断（见 §8.2）
- filter 全为 0（长度 0）表示该 SST 无 filter，读取路径必须能处理

### 4.6 空 SST 的处理

全为删除标记的 SST 仍必须保留 Bloom 的"不存在"信息（避免无谓的磁盘读），但可以跳过整文件——**前提是能判定其覆盖区间与查询区间无交集**。索引块必须记录首尾内部键以支持该判定。

---

## 5. 写入路径

```text
write(key, value)
  │
  ├─ 1. 分配全局递增 seqno
  ├─ 2. 追加记录到 WAL（含 seqno、key、value、长度、校验和）
  ├─ 3. 纳入组提交窗口（wal_group_commit_us）
  ├─ 4. 窗口到期或达到提交阈值 → fsync(WAL)  ← 写确认点
  ├─ 5. 写入 MemTable
  └─ 6. 若 MemTable 超 memtable_size_mb → 冻结并异步刷盘
```

### 5.1 顺序很重要

**必须先写 WAL 再改 MemTable，且 MemTable 的写入必须在 `fsync` 返回之后对读者可见。** 若先改 MemTable 再写 WAL，崩溃后内存态与磁盘态不一致，且已回包的写可能丢失。

### 5.2 组提交

`wal_group_commit_us`（默认 200µs）内合并多个写请求为一次 `fsync`：

```text
写入线程: 追加 WAL → 入等待队列 → 等待 fsync 完成 → 回包
提交者:   第一个等待者成为 leader，负责 fsync，完成后唤醒全部等待者
```

**约束**：leader 必须在真正完成 `fsync` 之后才唤醒等待者，且唤醒需保证内存序（`Mutex`/`Condvar` 即可，`Relaxed` 不足）。

### 5.3 MemTable

- 结构：有序映射 `user_key -> (seqno, value)`，实现见 §5.4
- 支持按 `key` 点查、按 `start..end` 范围遍历、内部键序
- 冻结后不可变，直接顺序刷成 L0 SST，无需再排序

### 5.4 MemTable 实现选型

| 方案 | 优点 | 缺点 | 结论 |
|---|---|---|---|
| `std::collections::BTreeMap<Vec<u8>, ...>` | 零成本、已测试、天然有序 | 每键一次堆分配，内存放大显著 | **首版采用** |
| 自研紧凑 B+树（arena） | 内存紧凑 | 需自行实现分裂/合并，复杂 | 触发条件见下 |
| 自研跳表 | 实现较简单，内存适中 | 需 `Cell` 手工管理，`forbid(unsafe)` 下要靠 `Mutex` 包裹 | 备选 |

**首版采用 `BTreeMap` 是有意识的取舍**：先把正确性、恢复、可见性做对，再优化内存。

**升级触发条件**（达到任一即重估）：默认 256 MiB memtable 的**实际堆占用超过配置值的 1.5 倍**，或 §11 压测显示写吞吐受内存分配限制。届时以 arena B+树替换，接口边界（`get` / `range` / `freeze`）保持不变。

### 5.5 刷盘

```text
冻结 MemTable
  → 写 SST（数据块 → 索引块 → Bloom → Footer）
  → fsync(SST)
  → 原子更新 MANIFEST（新增该 SST，记录最小可保留 seqno）
  → fsync(MANIFEST) + fsync(目录)
  → 才可释放冻结的 MemTable
```

**在 MANIFEST 落盘前不得释放 MemTable**，否则崩溃后数据既不在 WAL 也不在 SST——永久丢失。

`max_memtable_count`（默认 3）限制同时存在的冻结 memtable 数。**若刷盘速度跟不上写入速度且该值已达上限，写入必须阻塞或拒绝，而不是无限堆积内存**。此处需明确定义背压策略（见 §12 未决问题）。

---

## 6. 读取路径与可见性

### 6.1 快照

```text
Snapshot { seqno: u64, 可见性集合: 活跃快照的最小 seqno }
```

- 读事务开始时取当前 `seqno` 作为快照点
- 记录 `active_snapshots` 的最小值 `oldest_active`；`seqno < oldest_active` 的旧版本可被压实丢弃

### 6.2 点查

```text
get(key, snapshot):
  1. 查 MemTable：命中且 seqno <= snapshot → 返回（最新可见版本）
  2. 查 L0 各 SST（按 seqno 降序）：Bloom 先筛，再查索引块 → 定位数据块
  3. 查 L1..Ln（层内按文件区间剪枝）
  4. 未命中 → 读 Bloom 排除的即为不存在 → 返回 None
```

**关键正确性**：必须按 `inverted_seqno` 从新到旧扫，**首个 `seqno <= snapshot` 的版本即为答案**；若首个版本 `seqno > snapshot`，说明该 key 在快照之后才写入，应继续往旧层找。

### 6.3 范围扫描

按内部键升序遍历。同一用户键的多个版本连续出现，需**按 `key_len` 只输出第一个可见版本并跳过其余**。

### 6.4 可见性判定表

| 记录 seqno vs 快照 | 动作 |
|---|---|
| `seqno <= snapshot` | 可见，若为删除标记则返回"不存在" |
| `seqno > snapshot` | 跳过，继续找更旧版本 |
| 删除标记且可见 | 返回"不存在"，**不可**回退到更旧版本 |

最后一行是常见错误来源：删除必须"遮蔽"旧值，一旦允许穿透会复活已删数据。

---

## 7. 压缩（Compaction）

### 7.1 触发

| 条件 | 动作 |
|---|---|
| L0 文件数 ≥ `l0_compaction_trigger` | L0 → L1 压实 |
| Ln 文件数 > `max_files_per_level` | Ln → L(n+1) 压实 |

### 7.2 L0 → L1 特殊性

L0 的 SST 之间**区间可能重叠**，因此：

- L0 → L1 是**多路归并**（所有触发的 L0 文件 + L1 目标文件）
- 归并时同一用户键的多个版本必须按 `inverted_seqno` 保序输出
- **不得丢弃**除最旧快照可见范围之外以外的版本

### 7.3 压实中的版本裁剪

```text
对同一用户键的版本序列（已按新→旧排序）:
  保留第一个 <= oldest_active 的版本作为"基线"
  丢弃它之前（即比它更新）的、但对所有活跃快照都不可见的版本
  基线之前的更旧版本可直接丢弃（已被遮蔽且超出快照范围）
```

**裁剪的三个前提必须同时满足**，缺一即数据丢失：

1. 存在活跃快照时，只裁剪 `seqno < oldest_active` 的版本
2. 删除标记必须保留到"没有活跃快照能看到它之前的数据"为止
3. 压实输出后，**MANIFEST 记录的新 `min_retain_seqno` 必须等于实际丢弃的最小 seqno**

### 7.4 限速

`compaction_bandwidth_mb > 0` 时按令牌桶限速。**限速必须同时约束读带宽与写带宽**，否则压实会饿死前台读。默认 0（不限速）优先保证启动与恢复速度。

### 7.5 崩溃安全

压实是**纯新增**：输出写到新文件名，完成并 fsync 后才在 MANIFEST 中替换引用。**被替换的旧 SST 在 MANIFEST 落盘且所有活跃读者退出后才能删除。** 崩溃后 MANIFEST 要么指向旧文件（重做压实），要么指向新文件（压实已完成），两者都可恢复。

---

## 8. 完整性校验

### 8.1 三种模式（对应 `checksum`）

| 模式 | 行为 | 适用 |
|---|---|---|
| `Off` | 不写不校 | 仅基准测试 |
| `Read`（默认） | 写入时计算，读取时校验，失败即报错 | 默认 |
| `ReadBackground` | 校验移入后台线程，读路径不阻塞 | 大数据量巡检 |

### 8.2 CRC32C（已实现，位于 `mdb-crypto`）

CRC32C（Castagnoli，多项式 `0x1EDC6F41`，反射形式 `0x82F63B78`）是纯查表实现，**不构成依赖**。

**实现归属修正**：初稿曾设想 CRC32C 由 `mdb-storage` 提供（依据 `sm4.rs` 注释的表述），但 `OP_MSG` 的 `checksumPresent` 同样需要它，而 `mdb-wire` 位于存储层之下。**两份实现必然漂移**，故改为在 `mdb-crypto` 实现一次（`crates/mdb-crypto/src/crc32c.rs`，含流式 `Crc32c` 与一次性 `crc32c()`），由 `mdb-wire` 与 `mdb-storage` 共同使用。`sm4.rs` 注释中的"上层校验和层"应理解为"上层调用方"。

选它而非 CRC32/CRC64 的理由：MongoDB 的 `OP_MSG` 校验和即为 CRC32C，行为可比，便于差分测试；硬件指令支持广泛（未来解除 `unsafe` 限制后可直接走 SSE4.2）。

**测试向量已锁定**（编译期常量表 + 单元测试）：

```text
CRC32C("")           = 0x00000000
CRC32C("a")          = 0xC1D04330
CRC32C("123456789")  = 0xE3069283
```

**注意：CRC32C 不是密码算法。** 它只能发现偶发位翻转，不能发现恶意篡改（无密钥，可被构造），**不得**替代 AEAD 标签或 MAC（见 `docs/03` §7.1）。

### 8.3 信创模式

国密场景下校验和改用 SM3 截断前 8 字节（`mdb-crypto` 已有实现）。代价是每字节开销高于 CRC32C，需要在 `docs/03` 中给出取舍说明。

### 8.4 校验失败的处理

**一律报错，绝不返回数据、绝不静默跳过。** 静默跳过等于静默丢数据。错误需携带文件名、块偏移、期望与实际校验值，便于定位坏盘扇区。

---

## 9. 静态加密（TDE）

### 9.1 层次

```text
主密钥 (MEK, SM4)
   └─ 存于 SDF/HSM 或受控密钥文件
   └─ 永不写入日志、MANIFEST 或任何明文文件

数据密钥 (DEK) —— 加密数据块
   └─ 每 CF 定期轮换
   └─ 经 MEK 加密后存于 keyring.meta
```

`sm4.rs` 已确立的原则：软件 CTR 不得替代硬件 GCM 通过密评；软件路径需由 SDF/HSM 承担完整性保护。

### 9.2 加密粒度

选择**块级**而非页/文件级：

- 块级允许只解密查询涉及的块，是 compaction 能进行的前提
- 块 64 KiB，加密开销占比可接受
- 每块独立 nonce 与认证标签，互不牵连

### 9.3 重放保护

`sm4.rs` 明确要求"**绝不复用**的 counter"。实现要求：

- 每个块的 counter = `block_offset`（单调且唯一），**跨文件轮换也不得复用**
- 块内位置不单独计 counter
- 密文布局：`nonce | ciphertext | tag`

**若 DEK 轮换时 counter 从 0 重新开始，即构成 nonce 重用**，属密码学灾难。counter 必须由 MEK 派生的文件唯一标识参与构造。

### 9.4 无硬件路径时的默认行为

`hw.rs` 已有 `allow_software_fallback` 且默认关闭。存储层必须**默认拒绝在软件回退下创建新数据密钥**，即：回退模式下只能读取既有数据，不能新建加密集合。

---

## 10. 恢复流程

```text
启动
 ├─ 1. 读取 CURRENT → MANIFEST-<N>
 ├─ 2. 删除全部 *.tmp 文件
 ├─ 3. 加载 MANIFEST：现存 SST 集合、每个 CF 的 seqno 上界
 ├─ 4. 扫描 WAL 段，从 min_retain_seqno 之后重放
 │     - 校验每条记录的长度与 CRC32c
 │     - 尾部不完整/校验失败的记录直接丢弃（后续记录视为不可信）
 │     - 重建 MemTable
 └─ 5. 将超出 seqno 上界的 WAL 段封存或删除
```

### 10.1 崩溃安全不变量

以下每条都应有对应的不变量测试：

| # | 不变量 |
|---|---|
| I1 | 任何时刻 `fsync(WAL)` 返回的写，在崩溃后必然可见 |
| I2 | 崩溃后不会出现部分写入的记录（记录有长度+校验和，可检测并丢弃） |
| I3 | MANIFEST 引用与磁盘上实际 SST 集合一致（无悬空引用） |
| I4 | 同一用户键最多一个版本对任意快照可见 |
| I5 | 删除标记生效后，不会通过回退读到更旧版本 |
| I6 | 压实后不丢失任何活跃快照可见的版本 |

**I1 与 I3 最容易被违反**，因为它们依赖 `fsync` 目录项这一容易被省略的步骤。

### 10.2 崩溃注入测试

必须实现：随机时刻 kill 进程 → 重启 → 校验不变量 + 校验"所有已确认写入都在"。

具体做法：写测试驱动在随机 N 次 `fsync` 后 `_exit(137)`，重启后比对"崩溃前已回 ack 的键集合"与"重启后可见的键集合"。**只测"能启动"是不够的**，要测 acked 集合的子集关系。

---

## 11. 复杂度与读放大

以下为量级分析，**不含任何实测数字**（当前无基准设施，编造数字比不给更糟）。

### 11.1 复杂度

| 操作 | 复杂度 |
|---|---|
| 写（到 ack） | 一次顺序追加 + 一次 `fsync` |
| 点查（未命中 MemTable） | O(L0 文件数 × log(块内记录数)) + O(log) 索引定位 |
| 范围扫描 | 触及的块数线性 + 解压/前缀解压开销 |
| 压实 | 读 O(输入总量)，写 O(输出总量) |

### 11.2 写放大

分层次数每加一层，写放大近似乘以 `T`（默认 10）。这是 LSM 的固有代价，与 WiredTiger 的原地更新有本质差异。**必须在压测中给出 T 的实测值，再据此决定默认 `max_files_per_level`。**

### 11.3 需要建立的基准

`mdb-test` 需提供：顺序写、随机写、点查命中率、范围扫描、压实后台压力、崩溃恢复耗时。

当前 `mdb-test` 已有 4 个真实测试，但都不是基准，而是**工作区结构守卫**：
全 crate `forbid(unsafe_code)`、全仓库无 `unsafe` 关键字、无 `assert!(true)` 占位断言、
依赖无外部 crate。选它做守卫而非塞占位测试，是因为占位测试会让 `cargo test`
报"通过"却零覆盖，给出已覆盖的错觉。上述基准尚未实现。

---

## 12. 未决问题

必须在实现前定夺，否则会在实现中途被迫大改：

| # | 问题 | 影响 | 建议 |
|---|---|---|---|
| Q1 | 冻结 MemTable 达到 `max_memtable_count` 时的背压策略 | 决定写路径是否可能 OOM | 阻塞写入（返回 Busy）优于无限堆积内存 |
| Q2 | 是否支持跨 CF 事务原子性 | 决定 WAL 记录格式 | 建议**首版不支持**，先做单 CF |
| Q3 | 集合名 → 目录名的转义方案 | 决定迁移期文件名稳定性 | 必须可逆，且需往返测试 |
| Q4 | 删除标记的保留策略（`tombstone` vs 无标记 GC） | 影响读路径与压实复杂度 | 无标记 GC 要求上层确知"该 key 永不再复活" |
| Q5 | `checksum` 用 CRC32C 还是 SM3 作默认 | 影响信创合规声明 | 取决于 `docs/03` 的结论 |
| Q6 | 集合删除后文件回收时机 | 影响磁盘回收 | 需与压实、活跃读者协调 |
| Q7 | 是否需要 `Send`/`Sync` 边界上的锁粒度 | 影响并发设计 | 待 `mdb-runtime` 线程模型确定 |

---

## 13. 分阶段实施

| 阶段 | 内容 | 完成判据 |
|---|---|---|
| S0 | CRC32C + 单表 SST 读写 | 往返测试 + CRC32C 三个 Castagnoli 检查值 |
| S1 | 基础键值 CF，无 WAL | 崩溃后已刷盘数据不丢 |
| S2 | WAL + 组提交 | 不变量 I1、I2 成立 |
| S3 | 恢复流程 | 崩溃注入测试通过 |
| S4 | 压实（L0→L1） | 不变量 I3、I4、I6 成立 |
| S5 | Bloom + 多层 | 点查不命中率下降可测 |
| S6 | TDE | counter 不复用；软件回退默认拒绝建密钥 |
| S7 | 基准设施 | §11.3 四类基准可运行 |

**每阶段的完成判据都是可执行测试，不是"代码写完了"。**

---

## 14. 与 MongoDB 行为差异声明

| 项 | MongoDB (WiredTiger) | MDB (LSM) | 用户可感知影响 |
|---|---|---|---|
| 索引结构 | B+树 | LSM 分层 | 点查尾延迟分布不同 |
| 写放大 | 低（原地更新） | 高（分层压实） | 持续写入下磁盘 IO 显著更高 |
| 压缩 | 支持多算法 | 无（依赖约束） | 磁盘占用更大 |
| 统计信息 | 精确 | 近似 | 查询计划可能与 MongoDB 不同 |
| 缓存 | 内部数据结构常驻 | 依赖 OS 页缓存 | 内存规划方式不同 |

**这份差异必须写进迁移手册**，不能因为"线协议兼容"就让用户以为存储行为也等价。

---

## 15. 相关文档

| 文档 | 状态 |
|---|---|
| `docs/01-wire-protocol-compat-matrix.md` | 已编写（含 7.2 真实驱动握手验证结果） |
| `docs/02-storage-engine-design.md` | 本文档 |
| `docs/03-xinchuang-mlps-compliance.md` | 已编写 |
| `docs/cases/wire-compat-cases.yaml` | **未编写**（下一步） |
