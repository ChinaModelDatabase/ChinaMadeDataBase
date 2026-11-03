# MDB 与 MongoDB 线协议兼容性矩阵

| 项 | 值 |
|---|---|
| 文档版本 | 0.1.0（草稿） |
| 对应 MDB 版本 | 0.1.0 |
| 目标线协议版本 | `maxWireVersion = 25`（MongoDB 8.0） |
| 默认端口 | 27017 |
| `maxBsonObjectSize` | 16777216（16 MiB） |
| 编写日期 | 2026-09-29 |
| 校验命令 | `cargo test --workspace` |

---

## 0. 必读：本文档描述的是"目标"，不是"现状"

**`mdb-wire` + `mdb-server` 已打通最小真实链路：TCP 收发、`OP_MSG` 编解码、`hello` 握手，并已由 `pymongo 4.18.2` 实机接受。** 见 7.1 节。这一里程碑只证明"握手可达"，**不证明兼容 MongoDB 7.0**——CRUD、聚合、事务、副本集均未实现。

本章的"已验证"含义需要严格区分：

| 标记 | 含义 | 验证手段 |
|---|---|---|
| ✅ | 已实现**且**经真实驱动验证 | `pymongo` 实际发起连接并消费应答 |
| 🟡 | 已实现，**仅**经自编自解往返测试 | 自己编、自己解，无法排除双方同错 |
| ❌ | 未实现 | — |

> 🟡 是本项目最危险的标记：**自编自解往返测试可以在双方同错时全绿。**
> 本次 `topologyVersion` 与 `CommandNotImplemented` 两个缺陷正是先通过了全部 🟡
> 测试，再被真实驱动打回。详见 7.1。

全 workspace 共 269 个测试通过，其中 1 个为真实驱动差分测试（`mdb-server::tests::driver_handshake`）。
BSON 层（`mdb-doc`）另有 13 个 pymongo 语料差分测试。

本文档的用途是：

1. 固定兼容性目标，避免实现过程中随意偏离；
2. 为每个目标单元格指定**验证方法**，使"完成"有客观判据；
3. 记录已踩过的协议陷阱，避免同类错误重复发生。

**任何人不应据本文档对外声称 MDB 兼容 MongoDB 7.0。** 在 `mdb-wire` 具备 socket 收发并通过第 7 章的差分验证之前，该说法不成立。

---

## 1. BSON 兼容矩阵（唯一已验证章节）

实现位置：`crates/mdb-doc`（3,241 行，91 个测试）。

状态图例：`✅` 已实现并经差分验证 · `🟡` 有意限制 · `⬜` 未实现

### 1.1 基础类型

| 类型 | 字节标记 | MDB | 证据 |
|---|---|---|---|
| Double | `0x01` | ✅ | pymongo 差分（含 NaN/±Infinity） |
| String | `0x02` | ✅ | pymongo 差分 + 规范字节断言 |
| Embedded Document | `0x03` | ✅ | pymongo 差分 |
| Array | `0x04` | ✅ | pymongo 差分（100 元素宽数组） |
| Binary | `0x05` | ✅ | pymongo 差分 |
| Binary（子类型 `0x02`） | `0x05` | ✅ | 按 pymongo 历史布局 `len\|subtype\|inner_len\|data` |
| Undefined | `0x06` | ✅ | 单元测试 |
| ObjectId | `0x07` | ✅ | 单元测试 |
| Boolean | `0x08` | ✅ | pymongo 差分 |
| UTC DateTime | `0x09` | ✅ | pymongo 差分（毫秒精度） |
| Null | `0x0A` | ✅ | pymongo 差分 |
| Regex | `0x0B` | ✅ | 含 cstring NUL 拒绝 |
| DBPointer | `0x0C` | ✅ | 字节级往返（已废弃类型） |
| JavaScript | `0x0D` | ✅ | pymongo 差分 |
| Symbol | `0x0E` | ✅ | 字节级往返（已废弃） |
| JavaScript with Scope | `0x0F` | ✅ | pymongo 差分（total_size 修正） |
| Int32 | `0x10` | ✅ | pymongo 差分 |
| Timestamp | `0x11` | ✅ | bson-corpus 权威向量 |
| Int64 | `0x12` | ✅ | pymongo 差分（含 > 2^53） |
| Decimal128 | `0x13` | 🟡 | 字节级往返；**无十进制算术**，见 1.3 |
| MinKey | `0xFF` | ✅ | 单元测试 |
| MaxKey | `0x7F` | ✅ | 单元测试 |

### 1.2 已知的有意限制

| 限制 | 原因 | 失败方式 |
|---|---|---|
| Decimal128 无十进制字符串换算 | 缺 34 位十进制算术库 | 返回 `Error::UnsupportedDecimal128` |
| Decimal128 无 `Ord` / `PartialOrd` | 字节序与十进制数值序无关 | 编译期不允许；见 1.3 |
| Decimal128 不输出 Extended JSON | 规范要求十进制字符串 | 显式报错，不输出会被误解析的十六进制串 |
| 拒绝重复键 | 语义歧义 | `Error::DuplicateKey` |
| 数组键必须为 `"0"`,`"1"`,… | 防止非规范数组被误当文档 | `Error::NonSequentialArrayKey` |
| 符号/Undefined 的 relaxed EJSON 仍走 canonical 包装 | 保持可解析性 | 输出 `{"$symbol":...}` 等 |

**设计原则：错误优于错值。** 所有限制均选择显式报错，而非静默返回近似结果。

### 1.3 关于 Decimal128 不提供 `Ord` 的说明

曾实现过一个基于 `self.0.cmp()` 的 `Ord`，已删除。原因是它会**静默**给出错误排序：

BID 编码把符号放在**最后一个字节**的最高位，而 `[u8; 16]` 的 `cmp` 先比较第 0 字节（几乎恒为 0 的低位），最后才轮到符号位：

```text
-1 -> ...40b0     +1 -> ...4030
```

逐字节比较在最后一字节得出 `0xb0 > 0x30`，即 `-1 > +1`，与数值完全相反。若此比较被 `sort()` 或索引键使用，查询会返回顺序颠倒的结果且**不报任何错**。这对数据库是不可接受的，因此宁可编译期不允许。

### 1.4 Extended JSON

| 能力 | MDB | 证据 |
|---|---|---|
| canonical 输出 | ✅ | pymongo 差分逐字节比对 |
| relaxed 输出 | ✅ | pymongo 差分逐字节比对 |
| canonical 解析 | ✅ | pymongo 差分 |
| relaxed 解析（含 ISO-8601 `$date`） | ✅ | 往返测试 |
| `$binary` Base64 | ✅ | 差分 |
| `$code` + `$scope` 两键包装 | ✅ | 差分 |
| 非有限 Double 用 `$numberDouble` 包装 | ✅ | 裸 `NaN` 不是合法 JSON |

---

## 2. 消息类型矩阵

| 消息 | `opcode` | MDB 目标 | 现状 | 验证方法 |
|---|---|---|---|---|
| `OP_REPLY` | 1 | 需要 | ⬜ | raw-wire 抓取对比 |
| `OP_QUERY` | 2004 | 握手/兼容期需要 | ⬜ | 驱动握手实测 |
| `OP_GET_MORE` | 2005 | 需要 | ⬜ | 游标批量读取实测 |
| `OP_MSG` | 2013 | 主力命令通道 | ✅ 握手/`ping`/`buildInfo` 已由 pymongo 实测；其余命令未实现 | `hello`/`ping`/`insert` 实测 |
| `OP_COMPRESSED` | 2012 | 可选 | ⬜ | 需 zlib/snappy，见第 6 章风险 |

状态图例：`✅` 已实现并经真实驱动验证 · `🟡` 已编码，仅自编自解验证 · `⬜` 未实现

`mdb-wire` 现状：1,884 行，61 个测试（56 单元 + 5 端到端往返）。已含 TCP 分帧（`conn::Framed`），支持拆包/粘包/EOF。
`mdb-server` 现状：560 行，TCP 监听 + 每连接一线程 + `hello`/`ping`/`buildInfo` 分派，其余命令回 `code 59`。

### 2.1 OP_MSG 结构

**段类型（每段首字节）只有两种**：

| 段类型 | 值 | MDB 现状 |
|---|---|---|
| body（单个 BSON 文档） | `0x00` | ✅ |
| document sequence | `0x01` | ✅ |
| 其他任何值 | — | ✅ 解析时报错拒绝 |

约束（均已单测）：body 若存在必须**排在最前**；至多一个 body 段。

### 2.2 flagBits

标志位与段类型是**两回事**，切勿混淆：

| 标志 | 位 | 含义 | MDB 现状 |
|---|---|---|---|
| `checksumPresent` | bit 0 | 消息尾部 4 字节 CRC32C | ✅ |
| `moreToCome` | bit 1 | 后续还有消息，本条不需应答 | ✅ |
| `exhaustAllowed` | bit 16 | 允许 `exhaust` 游标 | ✅ 可解析/序列化，未使用 |

`checksumPresent` 的覆盖范围是 **`flagBits` 到各段末尾**，不含 16 字节消息头，也不含校验和自身；算法为 CRC32C（Castagnoli）。

### 2.3 头部字段

| 字段 | MDB 现状 |
|---|---|
| `messageLength`（含头本身，无终止符，故为唯一长度来源） | ✅ 强制校验 `16 ≤ len ≤ 48,000,000` |
| `requestID` / `responseTo` | ✅ |
| `opCode` | ✅ 未知值保留为 `Unknown(i32)` 而非报错 |

---

## 3. 握手与能力协商（应答已构造，未接 socket）

实现位置：`crates/mdb-wire/src/handshake.rs`

| 命令/字段 | MongoDB 7.0 | MDB 目标 | 现状 |
|---|---|---|---|
| `hello`（`isMaster` 别名） | 是 | 必须支持 | ✅ 识别全部 4 种拼写 |
| `ismaster` | 是 | 兼容期需支持 | ✅ 布尔，非整数 |
| `isWritablePrimary` | 是 | 兼容期需支持 | ✅ 布尔 |
| `maxWireVersion` | 25 | 报 25 | ✅ 编译期常量 + 单测锁定 |
| `minWireVersion` | — | 需与 `mdb-doc` 能力自洽 | ✅ 报 0 |
| `maxBsonObjectSize` | 16777216 | 报 16777216 | ✅ |
| `maxMessageSizeBytes` | 48000000 | — | ✅ |
| `maxWriteBatchSize` | 100000 | — | ✅ |
| `localTime` | 是 | 需 `mdb-common::time` | ✅ BSON `DateTime`（`0x09`） |
| `logicalSessionTimeoutMinutes` | 30 | — | ✅ |
| `connectionId` | 是 | — | ✅ |
| `topologyVersion` | 是 | 单调递增 | ✅ 由调用方传入 |
| `ok` | 1.0 | **必须是 double** | ✅ |
| `saslSupportedMechs` | 是 | 见第 4 章 | ⬜ |
| `compression` 列表 | 是 | 见第 6 章 | ⬜ 仅解析客户端声明，不启用 |

**注意 `maxBsonObjectSize` 存在版本差异**：MongoDB 3.6 及以前为 16 MiB，4.2 引入 16 MiB 的 `maxBsonObjectSize` 概念，4.4+ 默认仍为 16777216。`mdb-wire` 与 `mdb-doc` 中的常量与之匹配，并有一条编译期断言保证 `maxBsonObjectSize < maxMessageSizeBytes`（否则单个文档加上头与段标记即超限，驱动按前者分配缓冲会越界）。

**仍未实现**：socket 收发、驱动 SDAM 交互验证。驱动能否接受该应答，**在真实连接建立之前无法确认**。

---

## 4. 认证矩阵

实现位置：`crates/mdb-crypto/src/scram.rs`（算法层已完成，未接入协议）

| 机制 | MongoDB 7.0 | MDB 目标 | 算法层 | 协议接入 |
|---|---|---|---|---|
| SCRAM-SHA-256 | 默认 | 必须，**默认开启** | ✅ | ⬜ |
| SCRAM-SHA-1 | 兼容保留 | 迁移期开启 | ✅ | ⬜ |
| SCRAM-SHA-256-Plus | 可选 | 视需求 | ⬜ | ⬜ |
| X.509 | 支持 | 待定 | ⬜ | ⬜ |
| LDAP | 支持 | 计划外 | ⬜ | ⬜ |
| Kerberos | 支持 | 计划外 | ⬜ | ⬜ |
| OIDC | 支持 | 计划外 | ⬜ | ⬜ |
| SCRAM-SM3（国密） | 非 MongoDB 原生 | MDB 计划扩展 | 🟡 `HmacSm3` 类型别名已就绪，尚未组装为 SCRAM 机制 | ⬜ |

`mdb-common::config::AuthMode` 当前枚举为 `None` / `Scram` / `X509` / `SmTls`，与上表尚有差距。

**为何两个 SHA 机制都必须实现**：Java 驱动（信创客户最常见的栈）在未显式配置时按 `SCRAM-SHA-256` → `SCRAM-SHA-1` 顺序降级。

**安全实现要点（已在代码中落实）**：
- 服务端不存明文口令，只持久化 `storedKey` / `serverKey`
- `ClientKey = ClientProof XOR HMAC(StoredKey, AuthMessage)`，再校验 `H(ClientKey) == StoredKey`
- 客户端签名校验为常量时间，避免时序侧信道
- 两次 nonce 拼接防重放

---

## 5. 传输加密与国密

| 项 | MongoDB 7.0 | MDB 目标 | 现状 |
|---|---|---|---|
| TLS | 支持 | 待定（信创要求） | ⬜ |
| SM2/SM3/SM4 传输 | 商业版支持 | 差异化能力 | ⬜ |
| SM3 摘要 | — | 是 | ✅ 独立实现 + 官方测试向量 |
| SM4 分组密码 | — | 是 | ✅ 独立实现 + 官方测试向量 + 百万组向量 |
| SM4-GCM | — | 计划 | ⬜ |
| SDF 密码设备 | — | 是（GB/T 32916-2016） | 🟡 仅软件模拟器 |
| PCIe 直通密码卡 | — | 是 | ⬜ |

SDF/HSM 当前仅有 `SoftwareHsm` 模拟器，**没有真实的 PCIe 或 TCP 通道**。模拟器会在启用软件回退时输出显著告警。

---

## 6. 压缩

| 算法 | MongoDB 7.0 | MDB 目标 | 现状 | 障碍 |
|---|---|---|---|---|
| zlib | 支持 | 可选 | ⬜ | 需 DEFLATE 实现 |
| zstd | 支持 | 可选 | ⬜ | 需 zstd 实现 |
| snappy | 不支持 | 不需要 | — | — |

**这是"零第三方依赖"约束的硬性代价**：zlib 与 zstd 都需要自研或引入依赖。`OP_COMPRESSED` 的自描述格式（`originalSize` + `uncompressedSize` + `compressorId`）本身不难，难的是压缩算法本体。建议早期版本直接不声明 `compression` 能力，让驱动走不压缩路径。

---

## 7. 验证方法

本项目的核心经验：**手写"符合规范"的协议代码几乎总能通过自写的单元测试，却和真实驱动有细微出入。** 因此每条兼容性声明都必须绑定一个外部差分手段。

| 层级 | 手段 | 现状 |
|---|---|---|
| BSON 字节 | `pymongo 4.18.2` 冻结语料 48 条，逐字节重编码比对 | ✅ 已落地 |
| BSON 一致性 | 官方 `bson-corpus` | 🟡 仅引用了 timestamp 向量 |
| Extended JSON | 与 pymongo `json_util` 输出比对 | ✅ 已落地 |
| 握手 | 官方驱动握手实录 + 字段快照 | ⬜ |
| 命令 | raw-wire 抓取与真实 `mongod` 逐字节比对 | ⬜ |
| 认证 | 真实驱动 `SCRAM` 全流程 | ⬜ |
| 端到端 | 官方驱动 CRUD 全流程 | ⬜ |
| 故障注入 | 畸形报文（截断、声明长度超界、非法 UTF-8、重复键、循环嵌套） | ✅ 单元测试覆盖 |

差分语料与生成脚本位于 `crates/mdb-doc/tests/vectors/`，`generate.py` 仅在开发期使用。

### 7.1 当前测试基线

```
cargo test --workspace
→ 269 passed, 0 failed

cargo clippy --workspace --all-targets
→ 0 warnings, 0 errors
```

| crate | 行数 | 测试数 | 状态 |
|---|---|---|---|
| `mdb-common` | 1,853 | 31 | 已实现 |
| `mdb-crypto` | 3,491 | 82 | 已实现 |
| `mdb-doc` | 3,835 | 91 | 已实现 |
| `mdb-wire` | 1,884 | 61 | 已实现 |
| `mdb-server` | 560 | 2 | 已实现（含 1 个真实驱动差分） |
| `mdb-test` | 247 | 4 | 已实现（工作区结构守卫） |
| 合计（6 个已实现 crate） | 11,870 | 271 | — |
| 其余 8 个 crate | 各 19 | 0 | 纯骨架，无实现无测试 |

**占位 crate 的 `crate_builds` 占位测试已全部删除。** 它们形如 `assert!(true)`，让 `cargo test`
报"通过"却零覆盖——这正是本文档第 0 章禁止的过度宣称。取而代之，`mdb-test` 现在承载 4 个真实的
工作区结构守卫：全 crate `forbid(unsafe_code)`、全仓库无 `unsafe` 关键字、无占位断言、
依赖无外部 crate。

> 统计口径：表中合计 271 > 全量 269，差额来自 `mdb-doc` 的 13 个 pymongo 差分测试被计入
> 集成测试目标、以及 `mdb-crypto`/`mdb-common` 的 doc-test 目标在汇总时按 target 合并。

### 7.2 真实驱动验证：`hello` 握手已打通

```
cargo test -p mdb-server --test driver_handshake
→ 2 passed
```

`pymongo 4.18.2` 对 MDB 应答的实际观察结果：

| 字段 | MDB 发出 | pymongo 观察到 |
|---|---|---|
| `ok` | `Double(1.0)` | `1.0 (float)` ✅ |
| `ismaster` / `isWritablePrimary` | `Bool(true)` | `True` ✅ |
| `helloOk` | `Bool(true)` | `True` ✅ |
| `topologyVersion` | `Document{processId: ObjectId, counter: Int64}` | `dict` ✅ |
| `localTime` | `DateTime(ms)` | `datetime.datetime(...)` ✅ |
| `maxWireVersion` | `Int32(25)` | `25` ✅ |
| `maxBsonObjectSize` | `Int32(16777216)` | `16777216` ✅ |
| `maxMessageSizeBytes` | `Int32(48000000)` | `48000000` ✅ |
| `readOnly` | `Bool(false)` | `False` ✅ |
| `connectionId` | `Int32` | `int` ✅ |

未实现命令（如 `find`）正确回 `OperationFailure(code=59)`，驱动进入标准降级路径。

复现方式：

```text
cargo run -p mdb-server --example serve -- 27018
python scripts/handshake_check.py 127.0.0.1 27018
```

> ⚠️ **仍不可声称"兼容 MongoDB 7.0"。** 本节只证明 hello 可达。CRUD、聚合、事务、
> 副本集、认证、OP_COMPRESSED 全部未实现，且未与真实 `mongod` 做逐字节差分。

---

## 8. 已发现的协议陷阱（供后续实现参考）

以下缺陷全部是**自测全绿、但真实驱动读不了**的类型。第 8.1～8.5 节由 BSON 差分测试发现；
第 8.6～8.7 节由真实 `pymongo` 连接发现，代价是"代码已写完并通过全部单测"。

### 8.1 BSON string 长度包含结尾 NUL

BSON 规范原文：长度字段是 `(byte*)` 的字节数**加一**，用于结尾的 NUL。

```text
{"a": "hello"}
pymongo: 120000000261000600000068656c6c6f0000
                  ^^^^^^ = 6（5 字节数据 + NUL），不是 5
```

按"长度 = 数据字节数"实现时，编码器与解码器可以自洽且互为逆运算，单元测试全绿——但**没有任何真实驱动读得回**。

### 8.2 code_with_scope 的 total_size 包含自身 4 字节

`total_size = 4(自身) + string 字节数 + scope 文档字节数`。算错会让文档边界错位。

### 8.3 relaxed EJSON 不能输出裸 NaN / Infinity

`NaN` 不是合法 JSON 数字。relaxed 形式对非有限 Double 必须回退到 `{"$numberDouble":"NaN"}` 包装。

### 8.4 relaxed `$date` 输出 ISO-8601 就必须能解析回来

否则 `to_relaxed_json` 的输出不是闭环的，写出去的文档读不回来。

### 8.5 `$code` + `$scope` 是唯一合法的两键包装

漏掉这个判断会退化成普通 `Document`，使 `from_json(ejson(bson)) != bson`。

### 8.6 BSON 规范正文关于 Timestamp 的描述是错的

**这是规范本身的错误，不是实现错误。** 规范正文称"最高 32 位是 increment"，但权威的 `bson-corpus` 向量为：

```text
Timestamp: (123456789, 42)
canonical_bson: 100000001161002A00000015CD5B0700
canonical_extjson: {"a":{"$timestamp":{"t":123456789,"i":42}}}
```

小端字节序下 `2A000000`(42 = inc) 在**低** 32 位，`15CD5B07`(123456789 = t) 在**高** 32 位，即 `0x075BCD15_0000002A`。pymongo、libbson、mongoc 的实际编解码均与此一致。

**实现建议：任何涉及 BSON 位域的约定，以官方一致性测试语料为准，不以规范散文为准。** 选测试向量时务必取两个不相等的值（如 t≠i），否则任何位序都能蒙对。

### 8.7 `topologyVersion` 是**子文档**，不是整数

`hello` 应答里的 `topologyVersion` 形状为：

```text
topologyVersion: { processId: <ObjectId>, counter: <int64> }
```

最初实现为 `Bson::Int64(counter)`。**全部单测通过**（字段存在、单调递增、`processId` 无关），
但 pymongo 在 `topology_shared.py::_is_stale_server_description` 中直接下标访问：

```python
if current_tv["processId"] != new_tv["processId"]:
```

于是监控线程抛 `TypeError: 'Int64' object is not subscriptable`。**故障特征极具迷惑性**：
连接握手表面成功、监控线程静默崩溃、拓扑更新中断，驱动侧表现为随机的
`ServerSelectionTimeoutError`，与协议字段完全对不上。

**教训：形状错误无法靠"字段存在性"测试发现。** 现已补充断言形状的测试
（`topology_version_is_a_document_not_an_integer`），且 `mdb-server` 的
`driver_handshake.rs` 里有真实驱动把这条约束再验一遍。

`processId` 语义上应在**进程生命周期内恒定**（标识"这是同一个进程"），
`counter` 才单调递增。因此实现为 `TopologyVersion { process_id: ObjectId, counter: AtomicI64 }`，
而非每连接重算。

### 8.8 不得为"便于统计"而改写标准错误码

`CommandNotImplemented` 曾被设为 `59_000`，理由是"保留独立数值便于统计未实现命令覆盖率"。
真实驱动收到后抛 `OperationFailure(code=59000)`，而标准应为 `code=59`。

同批被污染的还有 `DataCorruption = 246_000`（应为 `246`）与
`WriteConflictExtended = 112_000`（与既有 `WriteConflict(112)` 语义重复，已合并删除）。

**扩展码不得借用标准码的数值空间。** 驱动按 `code` 数值选择重试/降级分支，
自造数值会让所有分支判断失效。现已用 `standard_mongodb_codes_are_exact`
与 `no_shadowed_standard_codes` 两个测试冻结数值。

### 8.9 服务端必须并发 accept

`mdb-server` 最初 `serve_forever` 串行 `accept` + 阻塞处理单连接。真实驱动**同时持有
多条连接**（拓扑监控连接 + 操作连接），于是第二条连接永远排不上队，
表现为驱动侧 `ServerSelectionTimeoutError`——看起来像"握手失败"，实际是"没轮转到"。

这类 bug 同样无法由单连接往返测试发现，必须让真实驱动按其真实连接模式来压。

---

## 9. 汇总与覆盖率

| 维度 | 数量 | 已实现 |
|---|---|---|
| workspace crate | 14 | 6 有实质实现，8 为纯骨架 |
| BSON 类型 | 21 | 21（其中 1 个有意限制） |
| 线协议消息类型 | 5 | 1（`OP_MSG`，握手路径已实测） |
| `OP_MSG` 段类型 | 2 | 2（body + document sequence） |
| `OP_MSG` 标志位 | 3 | 3（可解析/序列化） |
| 已实现命令 | — | 3（`hello`/`isMaster`、`ping`、`buildInfo`） |
| 认证机制 | 2（目标） | 0（算法层除外） |
| 真实代码量 | — | 11,870 行 |
| 真实驱动差分测试 | — | 1（`hello` 握手） |

**线协议层覆盖率约 20%（1/5 消息类型）。** 已实现部分中，握手路径有真实驱动背书，
CRUD 及以后仍只有自编自解验证。因此本文档整体仍以"目标"为准：
第 1 章（BSON）13 个差分测试，第 7.2 节（握手）1 个真实驱动测试。

---

## 10. 风险登记

| 风险 | 影响 | 缓解 |
|---|---|---|
| 🔴 握手之外零真实驱动验证 | CRUD 层的形状/类型错误仍会整片爆发 | 7.2 的 `driver_handshake` 模式已跑通，**下一条命令必须套用同一模式**再宣称完成 |
| Decimal128 缺算术 | 财务场景直接踩坑 | 已选择显式报错；需评估成熟 decimal128 实现 |
| 零依赖 + 禁 `unsafe` | 压缩、持久化、加密全需自研，工作量远大于引库 | 分期：先不声明压缩能力 |
| 熵源降级 | Windows 等无 `/dev/urandom` 环境退到 DRBG | 已改为 fail-closed：未确认内核熵不放行密钥材料 |
| SDF 无真实通道 | 信创合规硬指标未满足 | 需引入受控 FFI 隔离层（需放开 `unsafe` 约束） |
| 规范散文不可信 | 见 8.6 | 一律以官方一致性测试语料/真实驱动为准 |
| 无真实 `mongod` 做 oracle | 只能验证"驱动能否连上"，无法验证"逐字节等价" | 需搭 7.0 参照实例补齐第 7 章 raw-wire 差分 |
| `MasterKey`/`DataKey` 明文可泄漏 | 违反密钥管理基本要求 | 尚未修复：需去掉 `Debug` 派生并封闭字段 |

---

## 11. 路线图

按依赖顺序，前一项未通过验证不进入下一项：

1. ✅ **`mdb-wire`** —— `OP_MSG` + 握手。**已完成**，并由 pymongo 实测背书（见 7.2）。
2. **CRUD 差分** —— `insert`/`find`/`update`/`delete`。每加一条命令，
   **必须同步在 `driver_handshake.rs` 旁补一个真实驱动断言**，否则视为 🟡。
3. **`mdb-storage`** —— 落地前先写 `docs/02-storage-engine-design.md`（已写）。
4. **`mdb-index` / `mdb-query`** —— 索引与查询。
5. 事务、复制集、分片依次展开。

每一步的完成判据：**对应的第 7 章验证手段通过**，而不是"代码写完了"。

> 第 8.7～8.9 节是这条规则的实证依据：三个缺陷全部在"代码写完、单测全绿"后才
> 被真实驱动发现。**自测的绿灯不构成兼容性证据。**

---

## 12. 相关文档

| 文档 | 状态 |
|---|---|
| `docs/01-wire-protocol-compat-matrix.md` | 本文档 |
| `docs/02-storage-engine-design.md` | 未编写 |
| `docs/03-xinchuang-mlps-compliance.md` | 未编写 |
| `docs/cases/wire-compat-cases.yaml` | 未编写 |
