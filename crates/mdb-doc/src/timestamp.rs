//! BSON Timestamp（`0x11`）在 `mdb-doc` 侧的入口。
//!
//! 实现在 `mdb_common::time`（复制层与 oplog 也要用同一套位布局，
//! 不能各写一份），这里只做重导出，避免调用方到处写全路径。

pub use mdb_common::time::Timestamp;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bson_bit_order_is_stable() {
        // 高 32 位 = secs，低 32 位 = inc，依据见 mdb_common::time::Timestamp::as_u64。
        // BSON 规范正文把这句话写反了，bson-corpus 的权威向量是 (123456789, 42)
        // => 0x075BCD15_0000002A。
        let ts = Timestamp::new(123_456_789, 42);
        assert_eq!(ts.as_u64().to_le_bytes(), [0x2A, 0, 0, 0, 0x15, 0xCD, 0x5B, 0x07]);
        assert_eq!(Timestamp::from_u64(ts.as_u64()), ts);
    }

    #[test]
    fn seconds_drive_ordering_not_the_increment() {
        // oplog 顺序按秒推进；同秒内由 inc 打破平局。
        assert!(Timestamp::new(10, 0) < Timestamp::new(11, 0));
        assert!(Timestamp::new(10, 0) < Timestamp::new(10, 1));
    }
}
