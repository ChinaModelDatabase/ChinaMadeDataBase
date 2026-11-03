//! `hello` / `isMaster` 握手。
//!
//! # 为什么这是最关键的一条命令
//!
//! 驱动连上 MDB 后的**第一个**命令就是 `hello`（旧驱动发 `isMaster` /
//! `ismaster`）。驱动的 SDAM（拓扑发现）完全依赖应答中的：
//!
//! | 字段 | 驱动据此决定 |
//! |------|--------------|
//! | `maxWireVersion` | 是否降级到兼容模式；**不满足则直接断开** |
//! | `minWireVersion` | 同上 |
//! | `helloOk` | 是否可以用 `hello` 取代 `isMaster` |
//! | `isWritablePrimary` | 能否写入 |
//! | `topologyVersion` | 后续变更推送的游标基准 |
//!
//! # 版本号不是随手写的
//!
//! 本模块的 [`MAX_WIRE_VERSION`] 取 **25**，即 **MongoDB 8.0**
//! （依据 MongoDB 官方 wire version / feature list 与 DRIVERS-2595，
//! 已在 `docs/01` §3 核实）。**不能**凭记忆写成 4.4 或 7.0：
//!
//! - 报低 → 驱动进入兼容模式，功能与性能受限；
//! - 报高 → 驱动启用我们尚未实现的特性，行为不可预期。
//!
//! # 兼容性陷阱：驼峰与下划线
//!
//! MongoDB 历史上对 `isWritablePrimary` / `ismaster` / `isMaster` 三个
//! 键名混用过，且**按 BSON 类型严格取值**：
//!
//! - 布尔必须是真的 `bool`，不能是 `1`（`docs/01` §1.3 记录的驱动坑）；
//! - `ismaster` 兼容键缺失时，旧驱动会判定副本集模式不匹配。
//!
//! 因此应答中**同时**给出新旧键名。

use mdb_common::time;
use mdb_doc::{Bson, Document, ObjectId};

use crate::op_msg::OpMsg;

/// 目标最高线协议版本：MongoDB 8.0。
pub const MAX_WIRE_VERSION: i32 = 25;

/// 目标最低线协议版本（MongoDB 2.6 起）。
pub const MIN_WIRE_VERSION: i32 = 0;

/// `maxBsonObjectSize`（16 MiB）。
pub const MAX_BSON_OBJECT_SIZE: i32 = 16 * 1024 * 1024;

/// `maxMessageSizeBytes`（48 MB）。
pub const MAX_MESSAGE_SIZE_BYTES: i32 = 48_000_000;

/// `maxWriteBatchSize`。
pub const MAX_WRITE_BATCH_SIZE: i32 = 100_000;

/// `logicalSessionTimeoutMinutes`。
pub const LOGICAL_SESSION_TIMEOUT_MINUTES: i32 = 30;

/// 握手命令名（大小写敏感，含历史拼写变体）。
const HELLO_NAMES: [&str; 4] = ["hello", "isMaster", "ismaster", "isWritablePrimary"];

/// 该 body 是否是握手命令。
pub fn is_handshake(body: &Document) -> bool {
    HELLO_NAMES.iter().any(|n| body.contains_key(n))
}

/// 从握手命令中取出客户端是否接受压缩。
///
/// 返回 `true` 表示客户端在 `$db.admin` 或 `$cmd` 上声明了 `compression`
/// 数组。**本模块不据此启用压缩**，仅供上层决策。
pub fn client_compressors(body: &Document) -> Vec<String> {
    body.get("compression")
        .and_then(Bson::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|b| b.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// 取握手命令上的 `$db`，缺省 `admin`。
pub fn handshake_db(body: &Document) -> String {
    body.get("$db")
        .and_then(Bson::as_str)
        .unwrap_or("admin")
        .to_owned()
}

/// 拓扑版本号。
///
/// # 形状必须是文档，不能是整数
///
/// MongoDB 的 `topologyVersion` 是**子文档**：
///
/// ```text
/// topologyVersion: { processId: <ObjectId>, counter: <int64> }
/// ```
///
/// pymongo 在 `topology_shared.py::_is_stale_server_description` 中直接
/// 对它做**下标访问** `tv["processId"]`。若误发为整数，驱动会抛
/// `TypeError: 'Int64' object is not subscriptable`，且异常发生在驱动的
/// 监控线程里——连接看似建立，拓扑更新却已崩。
///
/// 这是纯单测**无法**发现的缺陷：字段存在、单调递增都能通过，
/// 只有真实驱动按文档形状访问时才暴露。
#[derive(Debug)]
pub struct TopologyVersion {
    process_id: ObjectId,
    counter: std::sync::atomic::AtomicI64,
}

impl TopologyVersion {
    /// 新建。`process_id` 缺省取本进程首个 `ObjectId`（含随机机器段与计数器）。
    pub fn new() -> Self {
        Self {
            process_id: ObjectId::new().unwrap_or_else(|| ObjectId::from_bytes([0u8; 12])),
            counter: std::sync::atomic::AtomicI64::new(0),
        }
    }

    /// 取下一个版本号并递增。
    pub fn next(&self) -> TopologyVersionSnapshot {
        let counter = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        TopologyVersionSnapshot {
            process_id: self.process_id,
            counter,
        }
    }
}

impl Default for TopologyVersion {
    fn default() -> Self {
        Self::new()
    }
}

/// 某一时刻的拓扑版本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopologyVersionSnapshot {
    /// 进程标识，整个进程生命周期内不变。
    pub process_id: ObjectId,
    /// 单调递增计数。
    pub counter: i64,
}

impl TopologyVersionSnapshot {
    /// 转为 wire 文档。
    pub fn to_document(self) -> Document {
        let mut d = Document::new();
        d.insert("processId", self.process_id);
        d.insert("counter", self.counter);
        d
    }
}

/// 构造 `hello` 应答。
///
/// # 参数
///
/// - `connection_id` 本连接 id，驱动用它做日志关联
/// - `topology_version` 拓扑版本（**子文档**形状，见 [`TopologyVersion`]）
/// - `read_only` 是否以只读模式连接
///
/// # 关于 `localTime`
///
/// 值为 BSON `DateTime`（`int64` 毫秒），**不是** Extended JSON 字符串。
/// 驱动按 `DateTime` 直接解析；发字符串会让拓扑同步逻辑走错分支。
pub fn hello_reply(
    connection_id: i32,
    topology_version: TopologyVersionSnapshot,
    read_only: bool,
) -> Document {
    let mut d = Document::new();

    // --- 兼容键名：不同驱动世代认不同的名字 ---
    d.insert("ismaster", true);
    d.insert("isWritablePrimary", !read_only);

    d.insert("helloOk", true);
    d.insert("topologyVersion", topology_version.to_document());
    d.insert("connectionId", connection_id);

    // --- 能力协商：驱动据此判断能否建立会话、能否批量写 ---
    d.insert("maxBsonObjectSize", MAX_BSON_OBJECT_SIZE);
    d.insert("maxMessageSizeBytes", MAX_MESSAGE_SIZE_BYTES);
    d.insert("maxWriteBatchSize", MAX_WRITE_BATCH_SIZE);
    d.insert(
        "logicalSessionTimeoutMinutes",
        LOGICAL_SESSION_TIMEOUT_MINUTES,
    );
    d.insert("minWireVersion", MIN_WIRE_VERSION);
    d.insert("maxWireVersion", MAX_WIRE_VERSION);

    d.insert("readOnly", read_only);
    // 必须是 BSON DateTime（0x09），不能是 Int64（0x12）：
    // `unix_millis()` 返回 i64，`From<i64>` 会编码成 Int64，驱动读不到
    // 日期语义。此处显式构造 DateTime，与本模块 `ok` 必须是 double 同理。
    d.insert("localTime", Bson::DateTime(time::unix_millis()));

    // `ok: 1.0` 必须是 double：驱动用 BSON 类型判定命令是否成功，
    // 整数 1 会被判为失败。
    d.insert("ok", 1.0f64);

    d
}

/// 便捷方法：把 `hello_reply` 包成一条 `OP_MSG` 应答。
pub fn hello_reply_msg(
    response_to: i32,
    connection_id: i32,
    topology_version: TopologyVersionSnapshot,
) -> OpMsg {
    OpMsg::reply(response_to, hello_reply(connection_id, topology_version, false))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(counter: i64) -> TopologyVersionSnapshot {
        TopologyVersionSnapshot {
            process_id: ObjectId::from_bytes([7u8; 12]),
            counter,
        }
    }

    fn reply() -> Document {
        hello_reply(1, snap(0), false)
    }

    #[test]
    fn recognises_every_historical_spelling() {
        for name in ["hello", "isMaster", "ismaster", "isWritablePrimary"] {
            let mut d = Document::new();
            d.insert(name, 1i32);
            assert!(is_handshake(&d), "{name} was not recognised");
        }
    }

    #[test]
    fn ordinary_command_is_not_a_handshake() {
        let mut d = Document::new();
        d.insert("find", "coll");
        assert!(!is_handshake(&d));
    }

    #[test]
    fn is_handshake_ignores_the_value_type() {
        // 命令值为任意类型都算握手：驱动可能发 {hello: 1, ...} 之外的形态
        let mut d = Document::new();
        d.insert("hello", Bson::Null);
        assert!(is_handshake(&d));
    }

    #[test]
    fn advertises_wire_version_21() {
        let d = hello_reply(1, snap(0), false);
        assert_eq!(d.get("maxWireVersion"), Some(&Bson::Int32(25)));
        assert_eq!(d.get("minWireVersion"), Some(&Bson::Int32(0)));
    }

    #[test]
    fn ok_is_double_one() {
        let d = hello_reply(1, snap(0), false);
        // 整数 1 会被驱动判为失败，必须是 double
        assert_eq!(d.get("ok"), Some(&Bson::Double(1.0)));
        assert!(matches!(d.get("ok"), Some(Bson::Double(_))));
    }

    #[test]
    fn is_master_keys_are_booleans() {
        let d = hello_reply(1, snap(0), false);
        assert_eq!(d.get("ismaster"), Some(&Bson::Boolean(true)));
        assert_eq!(d.get("isWritablePrimary"), Some(&Bson::Boolean(true)));
    }

    #[test]
    fn read_only_flips_primary_flag() {
        let d = hello_reply(1, snap(0), true);
        assert_eq!(d.get("readOnly"), Some(&Bson::Boolean(true)));
        assert_eq!(d.get("isWritablePrimary"), Some(&Bson::Boolean(false)));
    }

    #[test]
    fn local_time_is_datetime_not_string() {
        let d = hello_reply(1, snap(0), false);
        match d.get("localTime") {
            Some(Bson::DateTime(ms)) => assert!(*ms > 0),
            other => panic!("localTime must be BSON DateTime, got {other:?}"),
        }
    }

    #[test]
    fn all_capability_fields_present() {
        let d = hello_reply(7, snap(3), false);
        assert_eq!(d.get("maxBsonObjectSize"), Some(&Bson::Int32(16_777_216)));
        assert_eq!(d.get("maxMessageSizeBytes"), Some(&Bson::Int32(48_000_000)));
        assert_eq!(d.get("maxWriteBatchSize"), Some(&Bson::Int32(100_000)));
        assert_eq!(d.get("connectionId"), Some(&Bson::Int32(7)));
    }

    #[test]
    fn topology_version_is_a_document_not_an_integer() {
        // pymongo 会执行 `tv["processId"]`；发整数会抛
        // TypeError: 'Int64' object is not subscriptable
        let d = reply();
        let tv = d
            .get("topologyVersion")
            .expect("topologyVersion present");
        let doc = match tv {
            Bson::Document(doc) => doc,
            other => panic!("topologyVersion must be a Document, got {other:?}"),
        };
        assert!(matches!(doc.get("processId"), Some(Bson::ObjectId(_))));
        assert_eq!(doc.get("counter"), Some(&Bson::Int64(0)));
    }

    #[test]
    fn topology_version_counter_increments_and_process_id_is_stable() {
        let tv = TopologyVersion::new();
        let first = tv.next();
        let second = tv.next();
        let third = tv.next();
        assert_eq!(first.counter, 0);
        assert_eq!(second.counter, 1);
        assert_eq!(third.counter, 2);
        assert_eq!(
            first.process_id, second.process_id,
            "processId must not change within one process"
        );
        assert_eq!(second.process_id, third.process_id);
    }

    #[test]
    fn two_servers_get_distinct_process_ids() {
        // 驱动靠 processId 区分"换了进程"与"同一进程内版本前进"
        assert_ne!(TopologyVersion::new().next().process_id, TopologyVersion::new().next().process_id);
    }

    #[test]
    fn parses_client_compressors() {
        let mut d = Document::new();
        d.insert(
            "compression",
            Bson::Array(vec![
                Bson::String("snappy".into()),
                Bson::String("zlib".into()),
            ]),
        );
        assert_eq!(client_compressors(&d), vec!["snappy", "zlib"]);
    }

    #[test]
    fn missing_compression_yields_empty_list() {
        assert!(client_compressors(&Document::new()).is_empty());
    }

    #[test]
    fn handshake_db_defaults_to_admin() {
        assert_eq!(handshake_db(&Document::new()), "admin");
        let mut d = Document::new();
        d.insert("$db", "admin");
        assert_eq!(handshake_db(&d), "admin");
        let mut d = Document::new();
        d.insert("$db", "config");
        assert_eq!(handshake_db(&d), "config");
    }

    #[test]
    fn hello_reply_msg_responds_to_request() {
        let msg = hello_reply_msg(99, 1, snap(0));
        assert_eq!(msg.response_to, 99);
        assert_eq!(msg.body.unwrap().get("maxWireVersion"), Some(&Bson::Int32(25)));
    }

    #[test]
    fn reply_survives_a_wire_roundtrip() {
        // 驱动看到的是编码后的字节，必须仍然可解析
        let msg = hello_reply_msg(1, 1, snap(0));
        let bytes = msg.to_bytes().unwrap();
        let back = OpMsg::parse(&bytes).unwrap();
        let body = back.body.unwrap();
        assert_eq!(body.get("maxWireVersion"), Some(&Bson::Int32(25)));
        assert_eq!(body.get("ok"), Some(&Bson::Double(1.0)));
        assert!(is_handshake(&body), "reply should be recognisable");
    }
}
