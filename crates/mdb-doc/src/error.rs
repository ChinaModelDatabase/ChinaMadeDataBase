//! `mdb-doc` 错误类型。
//!
//! # 为什么单独定义而不是直接用 `mdb_common::MdbError`
//!
//! 编解码失败需要携带**精确的畸形特征**（少几个字节、哪个类型字节不认识、
//! 第几层嵌套超限）。把它们压成一个 `FailedToParse` 字符串后，运维在生产
//! 环境看到的只是一行 `bad BSON`，无法判断是客户端 bug 还是被篡改的流量。
//! 本模块保留结构化细节，再通过 [`From`] 转成 wire 层需要的 `MdbError`。

use std::fmt;

use mdb_common::{ErrorCode, MdbError};

/// `mdb-doc` 结果别名。
pub type Result<T> = std::result::Result<T, Error>;

/// BSON 处理错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// 缓冲区在读取完成前结束。
    Truncated { need: usize, have: usize },
    /// 文档长度字段为负或小于 BSON 最小文档长度。
    NegativeLength(&'static str),
    /// 声明长度超过实际可用字节。
    LengthExceedsBuffer { declared: usize, have: usize },
    /// cstring 未以 NUL 结束。
    UnterminatedCstring,
    /// string/document 末尾缺少 NUL 终止符。
    MissingNul(&'static str),
    /// 非 UTF-8 字节出现在必须是 UTF-8 的位置。
    InvalidUtf8(&'static str),
    /// 未定义的 BSON 类型字节。
    UnknownType(u8),
    /// bool 只能是 0 或 1。
    InvalidBoolean(u8),
    /// 同一文档内出现重复键。
    DuplicateKey(String),
    /// 嵌套深度超限。
    DepthLimit { max: usize },
    /// 超过 `maxBsonObjectSize`。
    TooLarge { len: usize, max: usize },
    /// 文档结束后仍有未消费的字节。
    TrailingBytes { consumed: usize, len: usize },
    /// BinaryOld 的内外层长度/子类型不一致。
    BinaryOldMismatch { outer: u32, inner: u32 },
    /// 数组元素的键不是 `"0"`,`"1"`,… 形式。
    NonSequentialArrayKey { index: usize, key: String },
    /// 键名内嵌 NUL，无法用 cstring 表示。
    KeyContainsNul(String),
    /// cstring 类型的**值**（如正则的 pattern/options）内嵌 NUL。
    ValueContainsNul(&'static str),
    /// 编码侧不支持的取值（当前仅 Decimal128 的 Extended JSON 输出）。
    UnsupportedDecimal128,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Truncated { need, have } => {
                write!(f, "BSON truncated: need {need} more bytes, only {have} left")
            }
            Error::NegativeLength(what) => write!(f, "negative or too-small length for {what}"),
            Error::LengthExceedsBuffer { declared, have } => {
                write!(f, "declared length {declared} exceeds available {have} bytes")
            }
            Error::UnterminatedCstring => f.write_str("cstring is not NUL-terminated"),
            Error::MissingNul(what) => write!(f, "{what} is missing its NUL terminator"),
            Error::InvalidUtf8(what) => write!(f, "{what} contains invalid UTF-8"),
            Error::UnknownType(t) => write!(f, "unknown BSON type byte 0x{t:02x}"),
            Error::InvalidBoolean(v) => write!(f, "boolean must be 0 or 1, got {v}"),
            Error::DuplicateKey(k) => write!(f, "duplicate key {k:?} in one document"),
            Error::DepthLimit { max } => write!(f, "nesting deeper than {max} levels"),
            Error::TooLarge { len, max } => write!(f, "document of {len} bytes exceeds {max}"),
            Error::TrailingBytes { consumed, len } => {
                write!(f, "{consumed} bytes consumed but {len} supplied (trailing data)")
            }
            Error::BinaryOldMismatch { outer, inner } => {
                write!(f, "BinaryOld inner length {inner} != outer length {outer}")
            }
            Error::NonSequentialArrayKey { index, key } => {
                write!(f, "array element {index} has key {key:?}, expected {index}")
            }
            Error::KeyContainsNul(k) => write!(f, "key {k:?} contains a NUL byte"),
            Error::ValueContainsNul(what) => {
                write!(f, "{what} contains a NUL byte and cannot be a BSON cstring")
            }
            Error::UnsupportedDecimal128 => f.write_str(
                "Decimal128 -> Extended JSON is not implemented; \
                 refusing to emit a value that downstream parsers would misread",
            ),
        }
    }
}

impl std::error::Error for Error {}

/// 映射到 wire 层错误码。
///
/// 大多数畸形输入属于 `FailedToParse`；超限单独归入 `InvalidLength`，
/// 因为那是资源边界问题而非格式问题，两者对客户端的处置不同。
impl From<Error> for MdbError {
    fn from(e: Error) -> Self {
        let code = match e {
            Error::Truncated { .. }
            | Error::NegativeLength(_)
            | Error::LengthExceedsBuffer { .. }
            | Error::UnterminatedCstring
            | Error::MissingNul(_)
            | Error::InvalidUtf8(_)
            | Error::UnknownType(_)
            | Error::InvalidBoolean(_)
            | Error::DuplicateKey(_)
            | Error::BinaryOldMismatch { .. }
            | Error::NonSequentialArrayKey { .. }
            | Error::KeyContainsNul(_)
            | Error::ValueContainsNul(_) => ErrorCode::FailedToParse,
            Error::DepthLimit { .. } | Error::TooLarge { .. } | Error::TrailingBytes { .. } => {
                ErrorCode::InvalidLength
            }
            // 无法表示的取值：报 BadValue 而非 Unimplemented，让调用方
            // 明确知道"这个值在此处不可表达"，而不是"命令不存在"。
            Error::UnsupportedDecimal128 => ErrorCode::BadValue,
        };
        MdbError::new(code, e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_specific_enough_to_triage() {
        let e = Error::UnknownType(0x99);
        assert!(e.to_string().contains("0x99"), "{e}");
        let e = Error::Truncated { need: 12, have: 3 };
        assert!(e.to_string().contains("need 12") && e.to_string().contains("only 3"), "{e}");
    }

    #[test]
    fn maps_to_distinct_wire_codes() {
        let parse: MdbError = Error::UnknownType(1).into();
        assert_eq!(parse.code(), ErrorCode::FailedToParse);
        let len: MdbError = Error::TooLarge { len: 99, max: 1 }.into();
        assert_eq!(len.code(), ErrorCode::InvalidLength);
        let unsup: MdbError = Error::UnsupportedDecimal128.into();
        assert_eq!(unsup.code(), ErrorCode::BadValue);
    }
}
