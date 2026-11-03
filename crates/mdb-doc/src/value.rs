//! BSON 值类型与文档模型。
//!
//! # 为什么不直接用 `HashMap`
//!
//! BSON 是**有序**格式：字段顺序参与字节级比较，也决定
//! `OP_QUERY`/`OP_MSG` 中命令文档的识别方式（首字段为命令名）。
//! 另外 `_id` 必须是文档的第一个字段才能被 MongoDB 正确处理。
//! 因此本模块使用保序的 `Vec<(String, Bson)>`，而不是 `HashMap`。

use std::fmt;

use crate::binary::Binary;
use crate::decimal128::Decimal128;
use crate::oid::ObjectId;

pub use crate::binary::BinarySubtype;
pub use crate::timestamp::Timestamp;

/// BSON 正则表达式。
///
/// BSON 用两个 **cstring** 存储 pattern 与 options，顺序选项按字母序排列。
/// 这里保持原样，不尝试重排（重排会改变字节表示）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Regex {
    pub pattern: String,
    pub options: String,
}

impl Regex {
    pub fn new(pattern: impl Into<String>, options: impl Into<String>) -> Self {
        Self { pattern: pattern.into(), options: options.into() }
    }
}

/// BSON DBPointer（已废弃，保留仅为字节级往返）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DbPointer {
    pub namespace: String,
    pub id: ObjectId,
}

/// BSON 值。
#[derive(Debug, Clone, PartialEq)]
pub enum Bson {
    /// `0x01` IEEE-754 double。
    Double(f64),
    /// `0x02` UTF-8 字符串。
    String(String),
    /// `0x03` 子文档。
    Document(Document),
    /// `0x04` 数组（编码形式为文档，键为 `"0"`,`"1"`,…）。
    Array(Vec<Bson>),
    /// `0x05` 二进制。
    Binary(Binary),
    /// `0x06` undefined（已废弃）。
    Undefined,
    /// `0x07` ObjectId。
    ObjectId(ObjectId),
    /// `0x08` 布尔。
    Boolean(bool),
    /// `0x09` UTC 毫秒时间戳。
    DateTime(i64),
    /// `0x0A` null。
    Null,
    /// `0x0B` 正则。
    Regex(Regex),
    /// `0x0C` DBPointer（已废弃）。
    DbPointer(DbPointer),
    /// `0x0D` JavaScript 代码。
    JavaScript(String),
    /// `0x0E` Symbol（已废弃）。
    Symbol(String),
    /// `0x0F` 带作用域的 JavaScript。
    JavaScriptWithScope { code: String, scope: Document },
    /// `0x10` 32 位有符号整数。
    Int32(i32),
    /// `0x11` BSON 时间戳（内部排序用）。
    Timestamp(Timestamp),
    /// `0x12` 64 位有符号整数。
    Int64(i64),
    /// `0x13` Decimal128。
    Decimal128(Decimal128),
    /// `0xFF` MinKey（小于任何其他值）。
    MinKey,
    /// `0x7F` MaxKey（大于任何其他值）。
    MaxKey,
}

impl Bson {
    /// BSON 类型字节。
    pub const fn type_byte(&self) -> u8 {
        match self {
            Bson::Double(_) => 0x01,
            Bson::String(_) => 0x02,
            Bson::Document(_) => 0x03,
            Bson::Array(_) => 0x04,
            Bson::Binary(_) => 0x05,
            Bson::Undefined => 0x06,
            Bson::ObjectId(_) => 0x07,
            Bson::Boolean(_) => 0x08,
            Bson::DateTime(_) => 0x09,
            Bson::Null => 0x0a,
            Bson::Regex(_) => 0x0b,
            Bson::DbPointer(_) => 0x0c,
            Bson::JavaScript(_) => 0x0d,
            Bson::Symbol(_) => 0x0e,
            Bson::JavaScriptWithScope { .. } => 0x0f,
            Bson::Int32(_) => 0x10,
            Bson::Timestamp(_) => 0x11,
            Bson::Int64(_) => 0x12,
            Bson::Decimal128(_) => 0x13,
            Bson::MaxKey => 0x7f,
            Bson::MinKey => 0xff,
        }
    }

    /// 该值是否为数字（`Int32`/`Int64`/`Double`/`Decimal128`）。
    pub const fn is_number(&self) -> bool {
        matches!(
            self,
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
        )
    }

    /// 尽力提取有符号整数。
    ///
    /// `Double` 仅在**恰好**为整数且处于 `i64` 范围内时转换；
    /// 截断 `2.5` 为 `2` 会在查询匹配中产生极难排查的错误。
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Bson::Int32(v) => Some(i64::from(*v)),
            Bson::Int64(v) => Some(*v),
            Bson::Double(v) => {
                if v.is_finite() && v.fract() == 0.0 && *v >= -(2f64.powi(63)) && *v < 2f64.powi(63)
                {
                    Some(*v as i64)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// 尽力提取 `f64`。整数会被提升为 `Double`。
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Bson::Int32(v) => Some(f64::from(*v)),
            Bson::Int64(v) => Some(*v as f64),
            Bson::Double(v) => Some(*v),
            _ => None,
        }
    }

    /// 提取字符串切片。
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Bson::String(s) | Bson::JavaScript(s) | Bson::Symbol(s) => Some(s),
            _ => None,
        }
    }

    /// 提取布尔。
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Bson::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// 提取文档（不含数组）。
    pub const fn as_document(&self) -> Option<&Document> {
        match self {
            Bson::Document(d) => Some(d),
            _ => None,
        }
    }

    /// 提取数组。
    pub fn as_array(&self) -> Option<&[Bson]> {
        match self {
            Bson::Array(a) => Some(a),
            _ => None,
        }
    }

    /// 提取 ObjectId。
    pub const fn as_object_id(&self) -> Option<ObjectId> {
        match self {
            Bson::ObjectId(o) => Some(*o),
            _ => None,
        }
    }

    /// 提取 UTC 毫秒时间戳。
    pub const fn as_datetime(&self) -> Option<i64> {
        match self {
            Bson::DateTime(t) => Some(*t),
            _ => None,
        }
    }

    /// 是否为 `null` 或 `undefined`。
    ///
    /// 查询匹配按 MongoDB 语义把两者都视为"无值"。
    pub const fn is_nullish(&self) -> bool {
        matches!(self, Bson::Null | Bson::Undefined)
    }

    /// 该值的类型名（用于错误信息）。
    pub fn type_name(&self) -> &'static str {
        match self {
            Bson::Double(_) => "double",
            Bson::String(_) => "string",
            Bson::Document(_) => "object",
            Bson::Array(_) => "array",
            Bson::Binary(_) => "binData",
            Bson::Undefined => "undefined",
            Bson::ObjectId(_) => "objectId",
            Bson::Boolean(_) => "bool",
            Bson::DateTime(_) => "date",
            Bson::Null => "null",
            Bson::Regex(_) => "regex",
            Bson::DbPointer(_) => "dbPointer",
            Bson::JavaScript(_) => "javascript",
            Bson::Symbol(_) => "symbol",
            Bson::JavaScriptWithScope { .. } => "javascriptWithScope",
            Bson::Int32(_) => "int",
            Bson::Timestamp(_) => "timestamp",
            Bson::Int64(_) => "long",
            Bson::Decimal128(_) => "decimal",
            Bson::MinKey => "minKey",
            Bson::MaxKey => "maxKey",
        }
    }
}

/// 从多种整数类型构造 [`Bson`]。
///
/// 显式区分 `i32`/`i64` 很重要：BSON 有独立的 Int32/Int64 类型码，
/// 与 MongoDB 的数值比较、索引统计都相关，自动提升会改变类型码。
impl fmt::Display for Bson {
    /// 紧凑的单行形式，供日志与错误信息使用。
    ///
    /// 刻意不输出二进制载荷与 Decimal128 原始字节：这两者常含密钥材料或
    /// 需要专用工具解释的编码，整段打印既淹没日志又可能泄露机密。
    /// 需要无损表示时用 [`crate::json`]。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Bson::String(s) | Bson::JavaScript(s) | Bson::Symbol(s) => {
                f.write_str(&crate::json::quote(s))
            }
            Bson::Int32(v) => write!(f, "{v}"),
            Bson::Int64(v) => write!(f, "{v}"),
            Bson::Double(v) => write!(f, "{v}"),
            Bson::Boolean(b) => write!(f, "{b}"),
            Bson::DateTime(ms) => write!(f, "Date({ms})"),
            Bson::Null => f.write_str("null"),
            Bson::Undefined => f.write_str("undefined"),
            Bson::ObjectId(o) => write!(f, "ObjectId({o})"),
            Bson::Timestamp(t) => write!(f, "Timestamp({}, {})", t.secs, t.inc),
            Bson::Binary(b) => write!(f, "Binary({:?}, {} bytes)", b.subtype, b.bytes.len()),
            Bson::Decimal128(d) => write!(f, "Decimal128({})", d),
            Bson::MinKey => f.write_str("MinKey"),
            Bson::MaxKey => f.write_str("MaxKey"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl From<i32> for Bson {
    fn from(v: i32) -> Self {
        Bson::Int32(v)
    }
}

impl From<i64> for Bson {
    fn from(v: i64) -> Self {
        Bson::Int64(v)
    }
}

impl From<f64> for Bson {
    fn from(v: f64) -> Self {
        Bson::Double(v)
    }
}

impl From<bool> for Bson {
    fn from(v: bool) -> Self {
        Bson::Boolean(v)
    }
}

impl From<&str> for Bson {
    fn from(v: &str) -> Self {
        Bson::String(v.to_owned())
    }
}

impl From<String> for Bson {
    fn from(v: String) -> Self {
        Bson::String(v)
    }
}

impl From<ObjectId> for Bson {
    fn from(v: ObjectId) -> Self {
        Bson::ObjectId(v)
    }
}

impl From<Document> for Bson {
    fn from(v: Document) -> Self {
        Bson::Document(v)
    }
}

impl<T: Into<Bson>> From<Vec<T>> for Bson {
    fn from(v: Vec<T>) -> Self {
        Bson::Array(v.into_iter().map(Into::into).collect())
    }
}

/// BSON 文档：保序的键值序列。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Document {
    entries: Vec<(String, Bson)>,
}

impl Document {
    /// 空文档。
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// 预分配容量。
    pub fn with_capacity(n: usize) -> Self {
        Self { entries: Vec::with_capacity(n) }
    }

    /// 写入字段（保持插入顺序）。
    ///
    /// 键已存在时**覆盖**：BSON 文档不允许重复键，追加语义会悄悄产出非法
    /// 报文（我们自己的解析器会报 `duplicate key`，驱动虽然容忍但读到的值
    /// 取决于实现）。已经踩过两次：`cursor.id` 被写了两遍、`ok` 被写了两遍。
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Bson>) -> &mut Self {
        let key = key.into();
        let value = value.into();
        match self.entries.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => self.entries.push((key, value)),
        }
        self
    }

    /// 覆盖同名字段的值，原地保持其位置。
    ///
    /// 语义与 `HashMap::insert` 一致：键已存在时**不追加**到末尾。
    /// MongoDB 的 `$set` 依赖这一行为避免字段顺序漂移。
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<Bson>) -> &mut Self {
        let key = key.into();
        let value = value.into();
        match self.entries.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => self.entries.push((key, value)),
        }
        self
    }

    /// 查找字段。
    pub fn get(&self, key: &str) -> Option<&Bson> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// 变更字段；键不存在时返回 `None`。
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Bson> {
        self.entries.iter_mut().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// 移除字段并返回其值。
    pub fn remove(&mut self, key: &str) -> Option<Bson> {
        let idx = self.entries.iter().position(|(k, _)| k == key)?;
        Some(self.entries.remove(idx).1)
    }

    /// 是否含有该键。
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.iter().any(|(k, _)| k == key)
    }

    /// 字段数量。
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 是否为空文档。
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 按顺序迭代。
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Bson)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// 按顺序可变迭代。
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&str, &mut Bson)> {
        self.entries.iter_mut().map(|(k, v)| (k.as_str(), v))
    }

    /// 键列表（保持顺序）。
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|(k, _)| k.as_str())
    }

    /// 按点分路径取值，例如 `"a.b.c"`。
    pub fn get_path(&self, path: &str) -> Option<&Bson> {
        let mut cur: Option<&Bson> = None;
        for (i, seg) in path.split('.').enumerate() {
            cur = if i == 0 {
                self.get(seg)
            } else {
                match cur {
                    Some(Bson::Document(d)) => d.get(seg),
                    _ => return None,
                }
            };
            cur.as_ref()?;
        }
        cur
    }

    /// 便捷构造：字符串字段。
    pub fn str_field(&mut self, key: &str, value: &str) -> &mut Self {
        self.insert(key, Bson::String(value.to_owned()))
    }

    /// 便捷构造：整数字段。
    pub fn int_field(&mut self, key: &str, value: i64) -> &mut Self {
        self.insert(key, Bson::Int64(value))
    }

    /// 便捷构造：布尔字段。
    pub fn bool_field(&mut self, key: &str, value: bool) -> &mut Self {
        self.insert(key, Bson::Boolean(value))
    }

    /// 便捷构造：双精度字段。
    pub fn double_field(&mut self, key: &str, value: f64) -> &mut Self {
        self.insert(key, Bson::Double(value))
    }

    /// 便捷构造：UTC 毫秒时间戳字段。
    pub fn datetime_field(&mut self, key: &str, millis: i64) -> &mut Self {
        self.insert(key, Bson::DateTime(millis))
    }

    /// 便捷构造：数组字段。
    pub fn array_field(&mut self, key: &str, values: Vec<Bson>) -> &mut Self {
        self.insert(key, Bson::Array(values))
    }

    /// 便捷构造：子文档字段。
    pub fn doc_field(&mut self, key: &str, value: Document) -> &mut Self {
        self.insert(key, Bson::Document(value))
    }
}

impl fmt::Display for Document {
    /// 输出 `{key: value, ...}` 的紧凑形式，便于日志速览。
    /// 需要可被解析的表示时用 [`crate::json`]。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for (i, (k, v)) in self.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{k}: {v}")?;
        }
        f.write_str("}")
    }
}

impl<'a> IntoIterator for &'a Document {
    type Item = (&'a str, &'a Bson);
    type IntoIter = Box<dyn Iterator<Item = (&'a str, &'a Bson)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

impl FromIterator<(String, Bson)> for Document {
    fn from_iter<T: IntoIterator<Item = (String, Bson)>>(iter: T) -> Self {
        Self { entries: iter.into_iter().collect() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_insertion_order() {
        let mut d = Document::new();
        d.insert("z", 1i32).insert("a", 2i32).insert("m", 3i32);
        assert_eq!(d.keys().collect::<Vec<_>>(), ["z", "a", "m"]);
    }

    #[test]
    fn set_overwrites_in_place() {
        let mut d = Document::new();
        d.insert("a", 1i32).insert("b", 2i32).insert("c", 3i32);
        d.set("a", 9i32);
        assert_eq!(d.keys().collect::<Vec<_>>(), ["a", "b", "c"]);
        assert_eq!(d.get("a").and_then(Bson::as_i64), Some(9));
    }

    #[test]
    fn get_path_walks_nested_documents() {
        let mut inner = Document::new();
        inner.insert("c", 7i32);
        let mut mid = Document::new();
        mid.insert("b", Bson::Document(inner));
        let mut root = Document::new();
        root.insert("a", Bson::Document(mid));
        assert_eq!(root.get_path("a.b.c").and_then(Bson::as_i64), Some(7));
        assert!(root.get_path("a.b.missing").is_none());
        assert!(root.get_path("a.b.c.d").is_none(), "cannot descend into a scalar");
    }

    #[test]
    fn as_i64_refuses_to_truncate_floats() {
        assert_eq!(Bson::Double(2.0).as_i64(), Some(2));
        assert_eq!(Bson::Double(2.5).as_i64(), None, "must not silently truncate");
        assert_eq!(Bson::Double(f64::NAN).as_i64(), None);
        assert_eq!(Bson::Double(1e30).as_i64(), None, "out of i64 range");
        assert_eq!(Bson::Int32(5).as_i64(), Some(5));
    }

    #[test]
    fn integer_types_stay_distinct() {
        assert_eq!(Bson::from(1i32).type_byte(), 0x10);
        assert_eq!(Bson::from(1i64).type_byte(), 0x12);
    }
}
