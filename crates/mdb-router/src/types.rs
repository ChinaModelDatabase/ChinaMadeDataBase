use std::fmt;

use mdb_doc::Bson;

/// Shard identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardId(pub String);

impl ShardId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ShardId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Chunk identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChunkId(pub String);

impl ChunkId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ChunkId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Shard key specification for a collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardKey {
    /// Hashed sharding: documents are distributed by hash of the field value.
    Hashed { field: String },
    /// Range sharding: documents are distributed by range of the field value.
    Range { field: String },
    /// Compound sharding: multiple fields form the shard key.
    Compound { fields: Vec<String> },
}

impl ShardKey {
    /// Returns the field path(s) used as the shard key.
    pub fn fields(&self) -> Vec<&str> {
        match self {
            ShardKey::Hashed { field } => vec![field.as_str()],
            ShardKey::Range { field } => vec![field.as_str()],
            ShardKey::Compound { fields } => fields.iter().map(String::as_str).collect(),
        }
    }

    /// Returns true if this is a hashed shard key.
    pub fn is_hashed(&self) -> bool {
        matches!(self, ShardKey::Hashed { .. })
    }
}

/// A key range: [min, max).
#[derive(Debug, Clone, PartialEq)]
pub struct KeyRange {
    pub min: Bson,
    pub max: Bson,
}

impl KeyRange {
    pub fn new(min: Bson, max: Bson) -> Self {
        Self { min, max }
    }

    /// Returns true if the given key falls within this range.
    pub fn contains(&self, key: &Bson) -> bool {
        bson_compare(key, &self.min) != std::cmp::Ordering::Less
            && bson_compare(key, &self.max) == std::cmp::Ordering::Less
    }

    /// Returns true if this range overlaps with another.
    pub fn overlaps(&self, other: &KeyRange) -> bool {
        bson_compare(&self.min, &other.max) == std::cmp::Ordering::Less
            && bson_compare(&other.min, &self.max) == std::cmp::Ordering::Less
    }
}

/// A chunk: a contiguous range of shard key values assigned to a shard.
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub id: ChunkId,
    pub ns: String,
    pub range: KeyRange,
    pub shard_id: ShardId,
}

/// Shard state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShardState {
    /// Shard is online and accepting reads/writes.
    Active,
    /// Shard is draining (no new chunks should be assigned).
    Draining,
    /// Shard is offline.
    Offline,
}

impl ShardState {
    pub fn as_str(self) -> &'static str {
        match self {
            ShardState::Active => "active",
            ShardState::Draining => "draining",
            ShardState::Offline => "offline",
        }
    }
}

/// A shard in the cluster.
#[derive(Debug, Clone, PartialEq)]
pub struct Shard {
    pub id: ShardId,
    pub host: String,
    pub state: ShardState,
}

/// BSON comparison following MongoDB canonical ordering.
///
/// The ordering is:
/// MinKey < null < number types (compared numerically) < string < document < array < binary <
/// ObjectId < bool < DateTime < Regex < DBPointer < JavaScript < Symbol < JavaScriptWithScope <
/// Timestamp < Int32 < Int64 < Decimal128 < MaxKey
pub fn bson_compare(a: &Bson, b: &Bson) -> std::cmp::Ordering {
    type_rank(a).cmp(&type_rank(b)).then_with(|| value_compare(a, b))
}

fn type_rank(b: &Bson) -> u8 {
    match b {
        Bson::MinKey => 0,
        Bson::Null | Bson::Undefined => 1,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => 2,
        Bson::String(_) | Bson::Symbol(_) | Bson::JavaScript(_) => 3,
        Bson::Document(_) => 4,
        Bson::Array(_) => 5,
        Bson::Binary(_) => 6,
        Bson::ObjectId(_) => 7,
        Bson::Boolean(_) => 8,
        Bson::DateTime(_) => 9,
        Bson::Regex(_) => 10,
        Bson::DbPointer(_) => 11,
        Bson::JavaScriptWithScope { .. } => 12,
        Bson::Timestamp(_) => 13,
        Bson::MaxKey => 14,
    }
}

fn value_compare(a: &Bson, b: &Bson) -> std::cmp::Ordering {
    match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => x.cmp(y),
        (Bson::Int64(x), Bson::Int64(y)) => x.cmp(y),
        (Bson::Double(x), Bson::Double(y)) => x
            .partial_cmp(y)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Bson::Int32(x), Bson::Int64(y)) => (i64::from(*x)).cmp(y),
        (Bson::Int32(x), Bson::Double(y)) => {
            f64::from(*x).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Bson::Int64(x), Bson::Int32(y)) => x.cmp(&i64::from(*y)),
        (Bson::Int64(x), Bson::Double(y)) => {
            (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Bson::Double(x), Bson::Int32(y)) => {
            x.partial_cmp(&f64::from(*y)).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Bson::Double(x), Bson::Int64(y)) => {
            x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Bson::String(x), Bson::String(y)) => x.cmp(y),
        (Bson::Boolean(x), Bson::Boolean(y)) => x.cmp(y),
        (Bson::ObjectId(x), Bson::ObjectId(y)) => x.cmp(y),
        (Bson::DateTime(x), Bson::DateTime(y)) => x.cmp(y),
        (Bson::Timestamp(x), Bson::Timestamp(y)) => x.secs.cmp(&y.secs).then(x.inc.cmp(&y.inc)),
        (Bson::Document(x), Bson::Document(y)) => compare_documents(x, y),
        (Bson::Array(x), Bson::Array(y)) => {
            for (a, b) in x.iter().zip(y.iter()) {
                let ord = bson_compare(a, b);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            x.len().cmp(&y.len())
        }
        (Bson::Binary(x), Bson::Binary(y)) => x
            .subtype
            .code()
            .cmp(&y.subtype.code())
            .then(x.bytes.len().cmp(&y.bytes.len()))
            .then(x.bytes.cmp(&y.bytes)),
        (Bson::Regex(x), Bson::Regex(y)) => {
            x.pattern.cmp(&y.pattern).then(x.options.cmp(&y.options))
        }
        _ => std::cmp::Ordering::Equal,
    }
}

fn compare_documents(a: &mdb_doc::Document, b: &mdb_doc::Document) -> std::cmp::Ordering {
    let a_keys: Vec<&str> = a.keys().collect();
    let b_keys: Vec<&str> = b.keys().collect();
    for (ak, bk) in a_keys.iter().zip(b_keys.iter()) {
        let ord = ak.cmp(bk);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        let av = a.get(ak).unwrap();
        let bv = b.get(bk).unwrap();
        let ord = bson_compare(av, bv);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    a_keys.len().cmp(&b_keys.len())
}

/// Proposal for moving a chunk from one shard to another.
#[derive(Debug, Clone)]
pub struct MoveProposal {
    pub chunk_id: ChunkId,
    pub from_shard: ShardId,
    pub to_shard: ShardId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_id_display() {
        let id = ShardId::new("s0");
        assert_eq!(id.to_string(), "s0");
        assert_eq!(id.as_str(), "s0");
    }

    #[test]
    fn chunk_id_display() {
        let id = ChunkId::new("chunk1");
        assert_eq!(id.to_string(), "chunk1");
    }

    #[test]
    fn shard_key_fields() {
        let hk = ShardKey::Hashed {
            field: "user_id".into(),
        };
        assert_eq!(hk.fields(), vec!["user_id"]);
        assert!(hk.is_hashed());

        let rk = ShardKey::Range {
            field: "created_at".into(),
        };
        assert_eq!(rk.fields(), vec!["created_at"]);
        assert!(!rk.is_hashed());

        let ck = ShardKey::Compound {
            fields: vec!["a".into(), "b".into()],
        };
        assert_eq!(ck.fields(), vec!["a", "b"]);
        assert!(!ck.is_hashed());
    }

    #[test]
    fn key_range_contains() {
        let r = KeyRange::new(Bson::Int32(0), Bson::Int32(100));
        assert!(r.contains(&Bson::Int32(0)));
        assert!(r.contains(&Bson::Int32(50)));
        assert!(!r.contains(&Bson::Int32(100)));
        assert!(!r.contains(&Bson::Int32(-1)));
    }

    #[test]
    fn key_range_overlaps() {
        let a = KeyRange::new(Bson::Int32(0), Bson::Int32(50));
        let b = KeyRange::new(Bson::Int32(30), Bson::Int32(80));
        assert!(a.overlaps(&b));

        let c = KeyRange::new(Bson::Int32(60), Bson::Int32(90));
        assert!(!a.overlaps(&c));
    }

    #[test]
    fn shard_state_str() {
        assert_eq!(ShardState::Active.as_str(), "active");
        assert_eq!(ShardState::Draining.as_str(), "draining");
        assert_eq!(ShardState::Offline.as_str(), "offline");
    }

    #[test]
    fn bson_compare_same_type_int() {
        assert_eq!(
            bson_compare(&Bson::Int32(1), &Bson::Int32(2)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            bson_compare(&Bson::Int32(2), &Bson::Int32(2)),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            bson_compare(&Bson::Int32(3), &Bson::Int32(2)),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn bson_compare_cross_numeric() {
        assert_eq!(
            bson_compare(&Bson::Int32(1), &Bson::Int64(2)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            bson_compare(&Bson::Int64(1), &Bson::Double(2.0)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            bson_compare(&Bson::Double(1.5), &Bson::Int32(2)),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn bson_compare_type_ordering() {
        assert_eq!(
            bson_compare(&Bson::Null, &Bson::Int32(0)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            bson_compare(&Bson::Int32(0), &Bson::String("a".into())),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            bson_compare(&Bson::String("z".into()), &Bson::Boolean(true)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            bson_compare(&Bson::Boolean(false), &Bson::MinKey),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            bson_compare(&Bson::MinKey, &Bson::MaxKey),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn bson_compare_string() {
        assert_eq!(
            bson_compare(&Bson::String("a".into()), &Bson::String("b".into())),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            bson_compare(&Bson::String("ab".into()), &Bson::String("a".into())),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn bson_compare_array() {
        let a = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
        let b = Bson::Array(vec![Bson::Int32(1), Bson::Int32(3)]);
        assert_eq!(bson_compare(&a, &b), std::cmp::Ordering::Less);

        let c = Bson::Array(vec![Bson::Int32(1)]);
        assert_eq!(bson_compare(&a, &c), std::cmp::Ordering::Greater);
    }

    #[test]
    fn bson_compare_document() {
        let mut a = mdb_doc::Document::new();
        a.insert("x", Bson::Int32(1));
        let mut b = mdb_doc::Document::new();
        b.insert("x", Bson::Int32(2));
        assert_eq!(
            bson_compare(&Bson::Document(a), &Bson::Document(b)),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn bson_compare_double_nan() {
        assert_eq!(
            bson_compare(&Bson::Double(f64::NAN), &Bson::Double(1.0)),
            std::cmp::Ordering::Equal
        );
    }
}
