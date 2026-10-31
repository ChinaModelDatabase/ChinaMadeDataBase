//! BSON 文档模型：自研编解码、保序文档、Extended JSON。
//!
//! ## 依赖方向（单向，禁止反向依赖）
//!
//! ```text
//! mdb-common <- mdb-crypto <- mdb-doc <- mdb-wire <- mdb-server
//!                     \                     \             \
//!                      \                     \             +- mdb-cli
//!                       \- mdb-storage <- mdb-index <- mdb-query
//!                                            \            \        /
//!                                             \- mdb-txn --+- mdb-repl
//!                                                          \- mdb-router
//! ```
//!
//! ## 本 crate 的定位
//!
//! 这是唯一知道"字节怎么排"的地方。`mdb-wire` 只负责 framing，
//! 存储层只负责字节与键的比较规则——都不得重复实现 BSON 布局，
//! 否则会出现两份互相不一致的编解码器，产生极难定位的字节级差异。
//!
//! ## 已知缺口
//!
//! - [`error::Error::UnsupportedDecimal128`]：Decimal128 暂不支持输出到
//!   Extended JSON 的 `$numberDecimal` 十进制字符串。宁可报错也不输出会被
//!   下游误解析的值，详见 [`decimal128`]。

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod binary;
pub mod codec;
pub mod decimal128;
pub mod error;
pub mod json;
pub mod oid;
pub mod timestamp;
pub mod value;

pub use binary::{Binary, BinarySubtype};
pub use codec::{
    from_bson, from_bytes, from_bytes_prefix, to_bson, to_bytes, MAX_BSON_OBJECT_SIZE, MAX_DEPTH,
};
pub use decimal128::Decimal128;
pub use error::{Error, Result};
pub use oid::ObjectId;
pub use timestamp::Timestamp;
pub use value::{Bson, DbPointer, Document, Regex};

/// BSON 类型字节常量，供 `mdb-query` 做 `$type` 判定时复用。
pub mod type_byte {
    pub const DOUBLE: u8 = 0x01;
    pub const STRING: u8 = 0x02;
    pub const DOCUMENT: u8 = 0x03;
    pub const ARRAY: u8 = 0x04;
    pub const BINARY: u8 = 0x05;
    pub const UNDEFINED: u8 = 0x06;
    pub const OBJECT_ID: u8 = 0x07;
    pub const BOOLEAN: u8 = 0x08;
    pub const DATE_TIME: u8 = 0x09;
    pub const NULL: u8 = 0x0a;
    pub const REGEX: u8 = 0x0b;
    pub const DB_POINTER: u8 = 0x0c;
    pub const JAVASCRIPT: u8 = 0x0d;
    pub const SYMBOL: u8 = 0x0e;
    pub const JAVASCRIPT_WITH_SCOPE: u8 = 0x0f;
    pub const INT32: u8 = 0x10;
    pub const TIMESTAMP: u8 = 0x11;
    pub const INT64: u8 = 0x12;
    pub const DECIMAL128: u8 = 0x13;
    pub const MAX_KEY: u8 = 0x7f;
    pub const MIN_KEY: u8 = 0xff;
}

#[cfg(test)]
mod integration {
    use super::*;

    #[test]
    fn public_api_roundtrips_a_realistic_document() {
        let mut meta = Document::new();
        meta.insert("firmware", "2.1");
        let mut doc = Document::new();
        doc.insert("_id", ObjectId::new().expect("counter not exhausted"))
            .insert("name", "sensor-42")
            .insert("celsius", 21.5f64)
            .insert("count", 1024i64)
            .insert("ok", true)
            .insert("ts", Bson::DateTime(1_700_000_000_000))
            .insert("tags", Bson::Array(vec![Bson::String("a".into()), Bson::String("b".into())]))
            .insert("meta", Bson::Document(meta));

        let bytes = to_bytes(&doc).expect("encode");
        assert!(bytes.len() < MAX_BSON_OBJECT_SIZE);
        let back = from_bytes(&bytes).expect("decode");
        assert_eq!(back, doc);
    }

    #[test]
    fn type_bytes_match_the_spec() {
        assert_eq!(Bson::Int32(0).type_byte(), type_byte::INT32);
        assert_eq!(Bson::Int64(0).type_byte(), type_byte::INT64);
        assert_eq!(Bson::MinKey.type_byte(), type_byte::MIN_KEY);
        assert_eq!(Bson::MaxKey.type_byte(), type_byte::MAX_KEY);
    }
}
