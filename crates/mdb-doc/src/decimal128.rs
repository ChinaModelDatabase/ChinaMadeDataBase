//! BSON Decimal128（`0x13`）。
//!
//! # 范围声明（重要）
//!
//! 本类型**不透明保存** 16 字节 IEEE 754-2008 decimal128，并提供：
//!
//! - 原样字节存取与十六进制往返（`from_bytes`/`bytes`/`from_hex`/`to_hex`）
//! - 特殊值识别（`NaN` / `±Infinity`）与有限性判断
//!
//! 刻意**不**提供 `Ord`/`PartialOrd`：字节序与十进制数值序无关，据此比较会
//! 静默产出错误的排序结果。理由见测试 `no_total_order`。
//!
//! # 为什么不实现十进制换算
//!
//! decimal128 的 BID 编码把十进制有效数字按 BCD 打包进组合位与高位，
//! 指数并不是一个可直接读出的连续位域，因此"字节 <-> 十进制字符串"的换算
//! 需要完整的 34 位十进制算术。本项目当前没有该算术。
//!
//! 这里刻意**不**提供经 `f64` 中转的换算：财务金额经二进制浮点会静默
//! 丢精度，产出"看起来对但差几分钱"的结果。这类数据损坏比直接不支持更难
//! 发现与回滚。同理，[`Decimal128::to_decimal_string`] 不会猜测任何位模式。
//!
//! 需要十进制语义时的正确做法：接入成熟的 decimal128 实现，或由上层
//! 明确选择用 `i64` 分（分为单位）表示金额——这在账务场景往往更稳妥。
//!
//! # 与 Extended JSON
//!
//! Extended JSON 的规范形式要求 `$numberDecimal` 为十进制字符串。由于本
//! 类型不提供该字符串，序列化时会返回 [`crate::Error::UnsupportedDecimal128`]
//! 而不是输出一个会被下游误解析的十六进制串——**宁可报错，不可给出错值**。

use std::fmt;

/// BSON Decimal128 值（原始 16 字节，小端字节序）。
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal128([u8; 16]);

impl Decimal128 {
    /// BSON 中的固定字节数。
    pub const LEN: usize = 16;

    /// 由 12 个原始字节构造。
    pub const fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }

    /// 由任意长度的切片构造，长度必须为 16。
    pub fn from_slice(b: &[u8]) -> Option<Self> {
        let arr: [u8; 16] = b.try_into().ok()?;
        Some(Self(arr))
    }

    /// 返回底层 16 字节。
    pub const fn bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// 小写十六进制（32 字符），用于诊断与无损序列化。
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(32);
        for b in self.0 {
            s.push(char::from_digit((b >> 4) as u32, 16).expect("nibble < 16"));
            s.push(char::from_digit((b & 0x0f) as u32, 16).expect("nibble < 16"));
        }
        s
    }

    /// 解析 32 字符十六进制字符串。
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 32 {
            return None;
        }
        let mut out = [0u8; 16];
        let b = s.as_bytes();
        for (i, slot) in out.iter_mut().enumerate() {
            *slot = (hex_val(b[i * 2])? << 4) | hex_val(b[i * 2 + 1])?;
        }
        Some(Self(out))
    }

    /// 是否为 NaN。
    pub fn is_nan(&self) -> bool {
        // 组合位 11110 且非有限值标志位清零 => NaN
        self.0[15] & 0x7c == 0x7c
    }

    /// 是否为正无穷。
    pub fn is_positive_infinity(&self) -> bool {
        self.0[15] == 0x78 && self.0[8..15].iter().all(|&b| b == 0)
    }

    /// 是否为负无穷。
    pub fn is_negative_infinity(&self) -> bool {
        self.0[15] == 0xf8 && self.0[8..15].iter().all(|&b| b == 0)
    }

    /// 是否为有限值（非 NaN、非无穷）。
    pub fn is_finite(&self) -> bool {
        !self.is_nan() && !self.is_positive_infinity() && !self.is_negative_infinity()
    }
}

// 刻意**不**实现 `Ord` / `PartialOrd`。见 `no_total_order` 测试的说明：
// 字节序与十进制数值序完全无关，错误的比较会静默产出错误的排序键与索引顺序。
// 宁可让调用方显式失败，也不要给出一个看似合理的结果。

impl fmt::Debug for Decimal128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_nan() {
            f.write_str("Decimal128(NaN)")
        } else if self.is_positive_infinity() {
            f.write_str("Decimal128(Infinity)")
        } else if self.is_negative_infinity() {
            f.write_str("Decimal128(-Infinity)")
        } else {
            write!(f, "Decimal128({})", self.to_hex())
        }
    }
}

impl fmt::Display for Decimal128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip_is_lossless() {
        // 来自 pymongo Decimal128 的真实编码
        for hex in [
            "00000000000000000000000000004030", // 0
            "01000000000000000000000000004030", // 1
            "010000000000000000000000000040b0", // -1
            "01000000000000000000000000003a30", // 0.001
            "15cd5b07000000000000000000004030", // 123456789
            "0000000000000000000000000000007c", // NaN
            "00000000000000000000000000000078", // Infinity
            "000000000000000000000000000000f8", // -Infinity
        ] {
            let d = Decimal128::from_hex(hex).expect("valid hex");
            assert_eq!(d.to_hex(), hex);
        }
    }

    #[test]
    fn rejects_malformed_hex() {
        assert!(Decimal128::from_hex("").is_none());
        assert!(Decimal128::from_hex("0102").is_none());
        assert!(Decimal128::from_hex(&"zz".repeat(16)).is_none());
    }

    #[test]
    fn slice_length_is_enforced() {
        assert!(Decimal128::from_slice(&[0u8; 16]).is_some());
        assert!(Decimal128::from_slice(&[0u8; 15]).is_none());
    }

    #[test]
    fn special_values_are_recognised() {
        assert!(Decimal128::from_hex("0000000000000000000000000000007c").unwrap().is_nan());
        assert!(Decimal128::from_hex("00000000000000000000000000000078")
            .unwrap()
            .is_positive_infinity());
        assert!(Decimal128::from_hex("000000000000000000000000000000f8")
            .unwrap()
            .is_negative_infinity());
        let one = Decimal128::from_hex("01000000000000000000000000004030").unwrap();
        assert!(one.is_finite() && !one.is_nan());
    }

    #[test]
    fn no_total_order() {
        // 为什么不给 Decimal128 实现 Ord：字节序与十进制数值序毫无关系。
        //
        // 决定性反例是符号位。BID 编码把符号放在**最后一个字节**的最高位，
        // 而 `[u8; 16]` 的 `cmp` 先比第 0 字节——也就是先比几乎恒为 0 的低位，
        // 最后才轮到符号位：
        //
        //   -1 -> ...40b0     +1 -> ...4030
        //
        // 逐字节比较会在最后一字节得出 `0xb0 > 0x30`，即 `-1 > +1`，
        // 与数值完全相反。曾经存在一个基于 `self.0.cmp()` 的 `Ord` 实现
        // （外加"NaN 最大"的特判），一旦被 `sort()` 或索引键比较用到，
        // 查询就会返回顺序颠倒的结果，而且**不会报任何错**。
        //
        // 另一个反例是 `-0` 与 `+0`（末字节 0xb0 / 0x30）：IEEE 754 规定两者
        // 相等，字节序必然判出不等。
        let neg_one = Decimal128::from_hex("010000000000000000000000000040b0").unwrap();
        let one = Decimal128::from_hex("01000000000000000000000000004030").unwrap();
        assert_eq!(neg_one.bytes()[..15], one.bytes()[..15], "sign is the only difference");
        assert!(
            neg_one.bytes().cmp(one.bytes()).is_gt(),
            "byte order says -1 > +1, which is backwards; hence no Ord"
        );

        let neg_zero = Decimal128::from_hex("000000000000000000000000000000b0").unwrap();
        let pos_zero = Decimal128::from_hex("00000000000000000000000000000030").unwrap();
        assert_ne!(neg_zero, pos_zero, "bytes differ, so derived PartialEq says unequal");
    }
}
