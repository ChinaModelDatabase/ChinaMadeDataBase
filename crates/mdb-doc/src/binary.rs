//! BSON Binary（`0x05`）。
//!
//! BSON 布局：
//!
//! ```text
//! int32  长度
//! byte   子类型（见 BinarySubtype）
//! byte*  数据
//! ```
//!
//! # subtype 0x02 的历史包袱
//!
//! BSON 规范中 binary subtype 2（"BinaryOld"）的定义是
//! `int32 内层长度 | byte 内层子类型 | byte* 数据`，即多一个长度与子类型
//! 前缀。历史上不同驱动对该类型的处理不一致（有的把内层子类型当作外层
//! 的一部分）。本实现按规范原样**解析并保留**这层结构，编码时原样写回，
//! 以保证对旧数据的字节级往返；不试图"修正"它。

use std::fmt;

/// 二进制子类型。
///
/// 未知值以 [`BinarySubtype::Unknown`] **原样保留**，不折叠成固定值：
/// 折叠会让"读出再写出"悄悄改掉数据字节，新客户端因此再也读不回原值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinarySubtype {
    Generic,
    Function,
    BinaryOld,
    UuidOld,
    Uuid,
    Md5,
    Encrypted,
    Column,
    Sensitive,
    Vector,
    /// 规范未定义的子类型，携带原始字节。
    Unknown(u8),
}

impl BinarySubtype {
    /// 由原始字节解析；未定义值原样保留。
    pub const fn from_u8(b: u8) -> Self {
        match b {
            0x00 => BinarySubtype::Generic,
            0x01 => BinarySubtype::Function,
            0x02 => BinarySubtype::BinaryOld,
            0x03 => BinarySubtype::UuidOld,
            0x04 => BinarySubtype::Uuid,
            0x05 => BinarySubtype::Md5,
            0x06 => BinarySubtype::Encrypted,
            0x07 => BinarySubtype::Column,
            0x08 => BinarySubtype::Sensitive,
            0x09 => BinarySubtype::Vector,
            other => BinarySubtype::Unknown(other),
        }
    }

    /// 写回 BSON 的单个子类型字节。
    pub const fn code(self) -> u8 {
        match self {
            BinarySubtype::Generic => 0x00,
            BinarySubtype::Function => 0x01,
            BinarySubtype::BinaryOld => 0x02,
            BinarySubtype::UuidOld => 0x03,
            BinarySubtype::Uuid => 0x04,
            BinarySubtype::Md5 => 0x05,
            BinarySubtype::Encrypted => 0x06,
            BinarySubtype::Column => 0x07,
            BinarySubtype::Sensitive => 0x08,
            BinarySubtype::Vector => 0x09,
            BinarySubtype::Unknown(b) => b,
        }
    }

    /// 是否为规范已定义的子类型。
    pub const fn is_known(self) -> bool {
        !matches!(self, BinarySubtype::Unknown(_))
    }
}

/// BSON Binary 值。
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Binary {
    pub subtype: BinarySubtype,
    pub bytes: Vec<u8>,
}

impl Binary {
    pub fn new(subtype: BinarySubtype, bytes: Vec<u8>) -> Self {
        Self { subtype, bytes }
    }

    pub fn generic(bytes: Vec<u8>) -> Self {
        Self::new(BinarySubtype::Generic, bytes)
    }
}

impl fmt::Debug for Binary {
    /// 只打印长度与前若干字节：Binary 常含口令散列/密文，全量打印会
    /// 把机密写进日志。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const PREVIEW: usize = 8;
        write!(f, "Binary({:?}, len={}", self.subtype, self.bytes.len())?;
        if !self.bytes.is_empty() {
            f.write_str(", [")?;
            for b in &self.bytes[..self.bytes.len().min(PREVIEW)] {
                write!(f, "{b:02x}")?;
            }
            if self.bytes.len() > PREVIEW {
                write!(f, "..{} more", self.bytes.len() - PREVIEW)?;
            }
            f.write_str("]")?;
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_does_not_leak_full_content() {
        let b = Binary::generic(vec![0xaa; 64]);
        let s = format!("{b:?}");
        assert!(s.contains("len=64"), "{s}");
        assert!(!s.contains(&"aa".repeat(20)), "must not dump the whole payload");
    }

    #[test]
    fn unknown_subtype_roundtrips_losslessly() {
        // 折叠未知值会让"读出再写出"改掉数据字节，因此必须原样保留。
        for raw in [0x0a_u8, 0x42, 0x7f, 0x80, 0xff] {
            let s = BinarySubtype::from_u8(raw);
            assert!(!s.is_known());
            assert_eq!(s.code(), raw, "raw byte must survive a roundtrip");
        }
    }

    #[test]
    fn known_subtypes_have_stable_codes() {
        for (s, raw) in [
            (BinarySubtype::Generic, 0x00),
            (BinarySubtype::Function, 0x01),
            (BinarySubtype::BinaryOld, 0x02),
            (BinarySubtype::Uuid, 0x04),
            (BinarySubtype::Vector, 0x09),
        ] {
            assert_eq!(s.code(), raw);
            assert_eq!(BinarySubtype::from_u8(raw), s);
        }
    }
}
