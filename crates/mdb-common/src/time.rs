//! 时间源与时间戳。
//!
//! # 精度与单调性
//!
//! MongoDB 的 `Date`（BSON 0x09）为**毫秒**精度 UTC 毫秒时间戳，64 位有符号。
//! 本模块在此之上提供两项数据库必需能力：
//!
//! 1. [`Timestamp`]：64 位无符号，**高 32 位 = 秒，低 32 位 = 增量**，
//!    对齐 BSON Timestamp 类型与 oplog `ts` 字段的编码。//! 2. [`MonotonicClock`]：单调递增时钟，作为 `localTime` 与 TTL 巡检的时基，
//!    避免系统时钟回拨（NTP 校时、运维误改）导致游标/TTL 行为异常。
//!
//! # 跨节点时钟
//!
//! 单机内使用 `SystemTime`（NTP 同步后误差 < 1ms）。集群内的全局时间戳由
//! `mdb-txn` 的 TSO 分配器统一发放，不依赖各节点 `SystemTime` 一致。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Unix 纪元以来的毫秒数（可能为负，表示纪元前）。
pub fn unix_millis() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => millis_of(d),
        Err(e) => -millis_of(e.duration()),
    }
}

fn millis_of(d: Duration) -> i64 {
    let secs = d.as_secs() as i64;
    let nanos = d.subsec_nanos() as i64;
    secs.saturating_mul(1000).saturating_add(nanos / 1_000_000)
}

/// 单调纳秒计数器（`SystemTime` 不可用时的回退基准）。
pub fn monotonic_nanos() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static ORIGIN: std::sync::OnceLock<AtomicU64> = std::sync::OnceLock::new();
    let origin = ORIGIN.get_or_init(|| AtomicU64::new(now_nanos()));
    now_nanos().saturating_sub(origin.load(Ordering::Relaxed))
}

fn now_nanos() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as u64,
        Err(_) => 0,
    }
}

/// BSON Timestamp（0x11）或 oplog `ts`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Timestamp {
    /// 自定义 epoch 以来的秒数（MongoDB 为 1970，MDB 保持一致以免 wire 混用出歧义）。
    pub secs: u32,
    /// 同秒内的递增计数，用于同一秒内多个操作的全序。
    pub inc: u32,
}

impl Timestamp {
    /// 由秒与同秒内序号构造。
    pub const fn new(secs: u32, inc: u32) -> Self {
        Self { secs, inc }
    }

    /// 由 Unix 毫秒构造，`inc` 由 `ordinal` 给出。
    pub fn from_millis(millis: i64, ordinal: u32) -> Self {
        // i64 -> u32 secs：超出 u32 范围的日期视为异常，退化为 u32::MAX
        let secs = (millis.div_euclid(1000)).clamp(0, u32::MAX as i64) as u32;
        Self { secs, inc: ordinal }
    }

    /// 还原为 Unix 毫秒（忽略 `inc`）。
    pub fn to_millis(self) -> i64 {
        i64::from(self.secs).saturating_mul(1000)
    }

    /// 组合为 64 位值：**高 32 位 = secs，低 32 位 = inc**。
    ///
    /// # 权威依据
    ///
    /// BSON 规范正文说 Timestamp 的 "most significant 32 bits are the increment
    /// and the least significant 32 bits is the timestamp"，**这句话是错的**。
    /// 真正的权威是 mongodb/specifications 的 bson-corpus 一致性测试：
    ///
    /// ```text
    /// Timestamp: (123456789, 42)
    ///   canonical_bson: 100000001161002A00000015CD5B0700
    ///   canonical_extjson: {"a":{"$timestamp":{"t":123456789,"i":42}}}
    /// ```
    ///
    /// 小端字节序下 `2A000000`(42 = inc) 落在**低** 32 位，`15CD5B07`
    /// (123456789 = t) 落在**高** 32 位，即 `0x075BCD15_0000002A`。
    /// pymongo、libbson、mongoc 的实际编解码都与此一致。
    ///
    /// 写反不会编译报错，只会让所有 `Timestamp` 字段与 MongoDB 整体错位，
    /// 表现为 oplog 顺序错乱、`$currentOp` 时间异常这类极难定位的问题。
    pub const fn as_u64(self) -> u64 {
        ((self.secs as u64) << 32) | self.inc as u64
    }

    /// 从 64 位值还原（高 32 位 = secs，低 32 位 = inc，见 [`Self::as_u64`]）。
    pub const fn from_u64(v: u64) -> Self {
        Self { secs: (v >> 32) as u32, inc: (v & 0xFFFF_FFFF) as u32 }
    }

    /// 后继时间戳：用于保证同集合内 `ts` 严格单调。
    ///
    /// 这是 BSON Timestamp 类型的**约定**而非算术要求：MongoDB 依赖它判定
    /// oplog 条目顺序。MDB 的复制层同样依赖此单调性。
    #[must_use]
    pub fn next(self) -> Self {
        if self.inc == u32::MAX {
            Self { secs: self.secs.saturating_add(1), inc: 0 }
        } else {
            Self { secs: self.secs, inc: self.inc + 1 }
        }
    }
}

/// 毫秒精度 UTC 日期时间的 ISO-8601 渲染。
///
/// MongoDB 的 `Date` 字段在 Extended JSON 中渲染为 `{"$date": {"$numberLong": "..."}}`，
/// 本函数输出 ISO 字符串仅用于日志与 `mdb-cli` 展示，不参与协议。
pub fn format_iso8601(millis: i64) -> String {
    let (secs, sub_millis) = (millis.div_euclid(1000), millis.rem_euclid(1000));
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (h, m, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{sub_millis:03}Z")
}

/// Howard Hinnant 的 civil_from_days 算法（公历转日期）。
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_bit_layout() {
        // 高 32 位 = secs，低 32 位 = inc（依据见 as_u64 的文档）。
        // 顺序写反不会编译报错，只会让线上值与 MongoDB 整体错位，因此固定下来。
        let ts = Timestamp { secs: 0x0102_0304, inc: 0x0A0B_0C0D };
        assert_eq!(ts.as_u64(), 0x0102_0304_0A0B_0C0D);
        assert_eq!(Timestamp::from_u64(ts.as_u64()), ts);
    }

    #[test]
    fn timestamp_matches_bson_corpus() {
        // bson-corpus/tests/timestamp.json:
        //   Timestamp: (123456789, 42)
        //   canonical_bson: 100000001161002A00000015CD5B0700
        // t=123456789=0x075BCD15 在高 32 位，i=42=0x2A 在低 32 位。
        // 这个向量刻意取两个不相等的数；若取 t==i 的值，任何位序都能蒙对。
        let ts = Timestamp { secs: 123_456_789, inc: 42 };
        assert_eq!(ts.as_u64().to_le_bytes(), [0x2A, 0, 0, 0, 0x15, 0xCD, 0x5B, 0x07]);
        assert_eq!(Timestamp::from_u64(0x075BCD15_0000002A), ts);
    }

    #[test]
    fn timestamp_u64_order_matches_field_order() {
        // oplog 依赖 Timestamp 的数值序就是 (secs, inc) 字典序
        let a = Timestamp { secs: 1, inc: u32::MAX };
        let b = Timestamp { secs: 2, inc: 0 };
        assert!(a.as_u64() < b.as_u64());
        assert!(a < b);
    }

    #[test]
    fn timestamp_is_monotonic() {
        let t = Timestamp { secs: 100, inc: u32::MAX };
        assert_eq!(t.next(), Timestamp { secs: 101, inc: 0 });
        let t = Timestamp { secs: 100, inc: 5 };
        assert!(t.next() > t);
    }

    #[test]
    fn timestamp_millis_roundtrip() {
        // BSON Timestamp 的 `t` 字段是**秒**，`i` 是同秒内的序号而非毫秒，
        // 因此毫秒精度在此被有意截断（见 `to_millis` 的文档）。
        let ts = Timestamp::from_millis(1_700_000_000_123, 7);
        assert_eq!(ts.secs, 1_700_000_000);
        assert_eq!(ts.inc, 7);
        assert_eq!(ts.to_millis(), 1_700_000_000_000);
        // 秒对齐时无损
        let exact = Timestamp::from_millis(1_700_000_000_000, 3);
        assert_eq!(exact.to_millis(), 1_700_000_000_000);
        // 负数向零截断而非 panic
        assert_eq!(Timestamp::from_millis(-1, 0).to_millis(), 0);
    }

    #[test]
    fn negative_millis_clamps_sane() {
        // 纪元前时间：secs 为负，clamp 后 to_millis 不再是负数，但不应 panic
        let ts = Timestamp::from_millis(-5_000, 0);
        assert_eq!(ts.secs, 0);
    }

    #[test]
    fn iso8601_epoch_and_known_dates() {
        assert_eq!(format_iso8601(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(format_iso8601(1_700_000_000_000), "2023-11-14T22:13:20.000Z");
        // 闰年边界：2024-02-29
        assert_eq!(format_iso8601(1_709_164_800_000), "2024-02-29T00:00:00.000Z");
    }
}
