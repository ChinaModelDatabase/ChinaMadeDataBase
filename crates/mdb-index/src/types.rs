//! 索引核心类型定义。

use std::cmp::Ordering;

use mdb_doc::value::Bson;

/// 文档记录标识符。
pub type RecordId = u64;

/// 索引排序方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// 索引类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexType {
    BTree,
    Unique,
    TTL,
    Multikey,
    Text,
    TwoDSphere,
    Hashed,
    Partial,
}

/// 索引键规范：字段路径 + 排序方向。
#[derive(Debug, Clone)]
pub struct IndexKeySpec {
    pub fields: Vec<(String, SortOrder)>,
}

impl IndexKeySpec {
    pub fn single(field: impl Into<String>, order: SortOrder) -> Self {
        Self { fields: vec![(field.into(), order)] }
    }

    pub fn multi(fields: Vec<(String, SortOrder)>) -> Self {
        Self { fields }
    }

    /// 索引名称，默认为 `field1_dir_field2_dir_...`。
    pub fn default_name(&self) -> String {
        self.fields
            .iter()
            .map(|(f, o)| {
                let dir = match o {
                    SortOrder::Asc => "1",
                    SortOrder::Desc => "-1",
                };
                    format!("{f}_{dir}")
            })
            .collect::<Vec<_>>()
            .join("_")
    }
}

/// 索引创建选项。
#[derive(Debug, Clone, Default)]
pub struct IndexOptions {
    pub unique: bool,
    pub sparse: bool,
    pub expire_after_seconds: Option<i64>,
    pub partial_filter_expression: Option<Bson>,
    pub name: Option<String>,
}

impl IndexOptions {
    pub fn name_or_default(&self, spec: &IndexKeySpec) -> String {
        self.name.clone().unwrap_or_else(|| spec.default_name())
    }
}

/// 索引条目：键 + 记录标识符。
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub key: Bson,
    pub record_id: RecordId,
}

// ---------------------------------------------------------------------------
// Bson 比较：遵循 MongoDB comparison order
// ---------------------------------------------------------------------------

/// 类型排名（值越小排序越靠前）。
fn type_rank(b: &Bson) -> u8 {
    match b {
        Bson::MinKey => 0,
        Bson::Null | Bson::Undefined => 1,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => 2,
        Bson::Symbol(_) | Bson::String(_) | Bson::JavaScript(_) => 3,
        Bson::Document(_) => 4,
        Bson::Array(_) => 5,
        Bson::Binary(_) => 6,
        Bson::ObjectId(_) => 7,
        Bson::Boolean(_) => 8,
        Bson::DateTime(_) => 9,
        Bson::Timestamp(_) => 10,
        Bson::Regex(_) => 11,
        Bson::DbPointer(_) => 12,
        Bson::JavaScriptWithScope { .. } => 13,
        Bson::MaxKey => 14,
    }
}

/// BinarySubtype 转换为可比较的 u8 排名。
fn binary_subtype_rank(subtype: &mdb_doc::BinarySubtype) -> u8 {
    use mdb_doc::BinarySubtype;
    match subtype {
        BinarySubtype::Generic => 0,
        BinarySubtype::Function => 1,
        BinarySubtype::BinaryOld => 2,
        BinarySubtype::UuidOld => 3,
        BinarySubtype::Uuid => 4,
        BinarySubtype::Md5 => 5,
        BinarySubtype::Encrypted => 6,
        BinarySubtype::Column => 7,
        BinarySubtype::Sensitive => 8,
        BinarySubtype::Vector => 9,
        BinarySubtype::Unknown(b) => 100 + b,
    }
}

/// 同类型值的比较。
fn value_cmp(a: &Bson, b: &Bson) -> Ordering {
    match (a, b) {
        (Bson::Null, Bson::Null) | (Bson::Undefined, Bson::Undefined) => Ordering::Equal,
        (Bson::Null, Bson::Undefined) | (Bson::Undefined, Bson::Null) => Ordering::Equal,

        (Bson::Boolean(a), Bson::Boolean(b)) => a.cmp(b),

        (Bson::Int32(a), Bson::Int32(b)) => a.cmp(b),
        (Bson::Int64(a), Bson::Int64(b)) => a.cmp(b),
        (Bson::Double(a), Bson::Double(b)) => a.total_cmp(b),

        (Bson::Int32(a), Bson::Int64(b)) => (*a as i64).cmp(b),
        (Bson::Int64(a), Bson::Int32(b)) => a.cmp(&i64::from(*b)),
        (Bson::Int32(a), Bson::Double(b)) => (*a as f64).total_cmp(b),
        (Bson::Double(a), Bson::Int32(b)) => a.total_cmp(&f64::from(*b)),
        (Bson::Int64(a), Bson::Double(b)) => (*a as f64).total_cmp(b),
        (Bson::Double(a), Bson::Int64(b)) => a.total_cmp(&(*b as f64)),

        (Bson::String(a), Bson::String(b)) => a.cmp(b),
        (Bson::JavaScript(a), Bson::JavaScript(b))
        | (Bson::Symbol(a), Bson::Symbol(b)) => a.cmp(b),

        (Bson::ObjectId(a), Bson::ObjectId(b)) => a.cmp(b),

        (Bson::DateTime(a), Bson::DateTime(b)) => a.cmp(b),

        (Bson::Timestamp(a), Bson::Timestamp(b)) => {
            a.secs.cmp(&b.secs).then_with(|| a.inc.cmp(&b.inc))
        }

        (Bson::Array(a), Bson::Array(b)) => {
            for (ae, be) in a.iter().zip(b.iter()) {
                match bson_cmp(ae, be) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            a.len().cmp(&b.len())
        }

        (Bson::Document(a), Bson::Document(b)) => {
            for ((ak, av), (bk, bv)) in a.iter().zip(b.iter()) {
                match ak.cmp(bk) {
                    Ordering::Equal => match bson_cmp(av, bv) {
                        Ordering::Equal => continue,
                        other => return other,
                    },
                    other => return other,
                }
            }
            a.len().cmp(&b.len())
        }

        (Bson::Binary(a), Bson::Binary(b)) => {
            binary_subtype_rank(&a.subtype)
                .cmp(&binary_subtype_rank(&b.subtype))
                .then(a.bytes.cmp(&b.bytes))
        }

        (Bson::Regex(a), Bson::Regex(b)) => a.pattern.cmp(&b.pattern).then(a.options.cmp(&b.options)),

        _ => Ordering::Equal,
    }
}

/// Bson 全序比较，遵循 MongoDB comparison rules。
pub fn bson_cmp(a: &Bson, b: &Bson) -> Ordering {
    let ra = type_rank(a);
    let rb = type_rank(b);
    match ra.cmp(&rb) {
        Ordering::Equal => value_cmp(a, b),
        other => other,
    }
}

/// 从 `Bson` 值中提取用于索引比较的字段值，支持点分路径。
pub fn extract_field<'a>(doc: &'a mdb_doc::value::Document, path: &str) -> Option<&'a Bson> {
    doc.get_path(path)
}

/// 从 `Bson` 值提取字符串表示（用于文本索引）。
pub fn bson_to_text(value: &Bson) -> Option<&str> {
    match value {
        Bson::String(s) => Some(s),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bson_cmp_null_vs_number() {
        assert!(bson_cmp(&Bson::Null, &Bson::Int32(1)) == Ordering::Less);
    }

    #[test]
    fn bson_cmp_numbers_cross_type() {
        assert_eq!(bson_cmp(&Bson::Int32(5), &Bson::Int64(5)), Ordering::Equal);
        assert_eq!(bson_cmp(&Bson::Int32(5), &Bson::Double(5.0)), Ordering::Equal);
        assert_eq!(bson_cmp(&Bson::Int64(5), &Bson::Double(5.0)), Ordering::Equal);
        assert!(bson_cmp(&Bson::Int32(3), &Bson::Double(5.0)) == Ordering::Less);
    }

    #[test]
    fn bson_cmp_strings() {
        assert_eq!(
            bson_cmp(&Bson::String("abc".into()), &Bson::String("abc".into())),
            Ordering::Equal
        );
        assert!(
            bson_cmp(&Bson::String("abc".into()), &Bson::String("abd".into())) == Ordering::Less
        );
    }

    #[test]
    fn bson_cmp_different_types() {
        assert!(bson_cmp(&Bson::Int32(1), &Bson::String("a".into())) == Ordering::Less);
        assert!(bson_cmp(&Bson::Boolean(false), &Bson::Int32(1)) == Ordering::Greater);
    }

    #[test]
    fn bson_cmp_bool() {
        assert_eq!(
            bson_cmp(&Bson::Boolean(false), &Bson::Boolean(false)),
            Ordering::Equal
        );
        assert!(
            bson_cmp(&Bson::Boolean(false), &Bson::Boolean(true)) == Ordering::Less
        );
    }

    #[test]
    fn bson_cmp_timestamp() {
        let a = Bson::Timestamp(mdb_doc::Timestamp { secs: 100, inc: 1 });
        let b = Bson::Timestamp(mdb_doc::Timestamp { secs: 100, inc: 2 });
        assert!(bson_cmp(&a, &b) == Ordering::Less);
    }

    #[test]
    fn index_key_spec_default_name() {
        let spec =
            IndexKeySpec::multi(vec![("x".into(), SortOrder::Asc), ("y".into(), SortOrder::Desc)]);
        assert_eq!(spec.default_name(), "x_1_y_-1");
    }
}
