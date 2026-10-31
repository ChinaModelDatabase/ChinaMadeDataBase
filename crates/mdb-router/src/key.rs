use std::cmp::Ordering;

use mdb_doc::{Bson, Document};

use crate::types::{ShardKey, bson_compare};

/// Extract the shard key value from a document.
///
/// Returns `None` if the key field(s) are missing from the document.
/// For compound keys, returns an array of the key values.
pub fn extract_shard_key(doc: &Document, key_spec: &ShardKey) -> Option<Bson> {
    match key_spec {
        ShardKey::Hashed { field } | ShardKey::Range { field } => {
            doc.get_path(field).cloned()
        }
        ShardKey::Compound { fields } => {
            let values: Vec<Bson> = fields
                .iter()
                .map(|f| doc.get_path(f).cloned())
                .collect::<Option<Vec<_>>>()?;
            Some(Bson::Array(values))
        }
    }
}

/// Consistent hash function for shard key values.
///
/// Produces a uniform distribution over i64::MIN..i64::MAX.
/// Uses FNV-1a for speed and distribution quality.
pub fn hash_shard_key(key: &Bson) -> i64 {
    let mut h: u64 = 0xcbf29ce484222325; // FNV offset basis
    let bytes = bson_to_bytes(key);
    for &b in &bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3); // FNV prime
    }
    h as i64
}

/// Compare two shard key values.
///
/// Follows the same ordering as `bson_compare` in `types.rs`.
pub fn compare_shard_key(a: &Bson, b: &Bson) -> Ordering {
    bson_compare(a, b)
}

/// Validate a shard key specification.
///
/// Rules (following MongoDB semantics):
/// - The shard key field must not be an array type
/// - The key value extracted from an empty document must not fail for non-compound keys
/// - Compound keys must have at least one field
pub fn validate_shard_key(key_spec: &ShardKey) -> Result<(), &'static str> {
    match key_spec {
        ShardKey::Hashed { field } | ShardKey::Range { field } => {
            if field.is_empty() {
                return Err("shard key field must not be empty");
            }
            Ok(())
        }
        ShardKey::Compound { fields } => {
            if fields.is_empty() {
                return Err("compound shard key must have at least one field");
            }
            for f in fields {
                if f.is_empty() {
                    return Err("compound shard key field must not be empty");
                }
            }
            Ok(())
        }
    }
}

/// Check if the extracted shard key value is valid (not an array for non-compound keys).
pub fn is_valid_key_value(value: &Bson) -> bool {
    !matches!(value, Bson::Array(_))
}

/// Convert a BSON value to bytes for hashing.
///
/// Uses a deterministic byte representation.
fn bson_to_bytes(key: &Bson) -> Vec<u8> {
    match key {
        Bson::Double(v) => v.to_le_bytes().to_vec(),
        Bson::Int32(v) => v.to_le_bytes().to_vec(),
        Bson::Int64(v) => v.to_le_bytes().to_vec(),
        Bson::String(s) => s.as_bytes().to_vec(),
        Bson::Boolean(b) => vec![if *b { 1 } else { 0 }],
        Bson::ObjectId(oid) => {
            let s = oid.to_string();
            s.into_bytes()
        }
        Bson::DateTime(ms) => ms.to_le_bytes().to_vec(),
        Bson::Timestamp(ts) => {
            let mut bytes = ts.secs.to_le_bytes().to_vec();
            bytes.extend_from_slice(&ts.inc.to_le_bytes());
            bytes
        }
        Bson::Null => vec![0],
        Bson::Undefined => vec![0],
        Bson::MinKey => vec![0xFF],
        Bson::MaxKey => vec![0x7F],
        Bson::Binary(b) => b.bytes.clone(),
        Bson::Document(d) => {
            // Hash each key-value pair
            let mut bytes = Vec::new();
            for (k, v) in d.iter() {
                bytes.extend_from_slice(k.as_bytes());
                bytes.push(0); // separator
                bytes.extend_from_slice(&bson_to_bytes(v));
                bytes.push(0); // value terminator
            }
            bytes
        }
        Bson::Array(arr) => {
            let mut bytes = Vec::new();
            for item in arr {
                bytes.extend_from_slice(&bson_to_bytes(item));
                bytes.push(0);
            }
            bytes
        }
        Bson::Regex(r) => {
            let mut bytes = r.pattern.as_bytes().to_vec();
            bytes.push(0);
            bytes.extend_from_slice(r.options.as_bytes());
            bytes
        }
        Bson::JavaScript(s) | Bson::Symbol(s) => s.as_bytes().to_vec(),
        Bson::JavaScriptWithScope { code, .. } => code.as_bytes().to_vec(),
        Bson::Decimal128(_) => {
            // Use display form for hashing
            let s = key.to_string();
            s.into_bytes()
        }
        Bson::DbPointer(dp) => {
            let mut bytes = dp.namespace.as_bytes().to_vec();
            bytes.push(0);
            let s = dp.id.to_string();
            bytes.extend_from_slice(s.as_bytes());
            bytes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_shard_key_hashed() {
        let mut doc = Document::new();
        doc.insert("user_id", Bson::Int32(42));

        let key = ShardKey::Hashed {
            field: "user_id".into(),
        };
        assert_eq!(
            extract_shard_key(&doc, &key),
            Some(Bson::Int32(42))
        );
    }

    #[test]
    fn extract_shard_key_nested() {
        let mut inner = Document::new();
        inner.insert("zip", Bson::String("10001".into()));
        let mut doc = Document::new();
        doc.insert("address", Bson::Document(inner));

        let key = ShardKey::Range {
            field: "address.zip".into(),
        };
        assert_eq!(
            extract_shard_key(&doc, &key),
            Some(Bson::String("10001".into()))
        );
    }

    #[test]
    fn extract_shard_key_missing() {
        let doc = Document::new();
        let key = ShardKey::Hashed {
            field: "user_id".into(),
        };
        assert_eq!(extract_shard_key(&doc, &key), None);
    }

    #[test]
    fn extract_shard_key_compound() {
        let mut doc = Document::new();
        doc.insert("a", Bson::Int32(1));
        doc.insert("b", Bson::String("x".into()));

        let key = ShardKey::Compound {
            fields: vec!["a".into(), "b".into()],
        };
        assert_eq!(
            extract_shard_key(&doc, &key),
            Some(Bson::Array(vec![Bson::Int32(1), Bson::String("x".into())]))
        );
    }

    #[test]
    fn extract_shard_key_compound_missing() {
        let mut doc = Document::new();
        doc.insert("a", Bson::Int32(1));

        let key = ShardKey::Compound {
            fields: vec!["a".into(), "b".into()],
        };
        assert_eq!(extract_shard_key(&doc, &key), None);
    }

    #[test]
    fn hash_shard_key_deterministic() {
        let k1 = Bson::Int32(42);
        let k2 = Bson::Int32(42);
        assert_eq!(hash_shard_key(&k1), hash_shard_key(&k2));
    }

    #[test]
    fn hash_shard_key_distribution() {
        let mut seen = std::collections::HashSet::new();
        for i in 0..1000 {
            let k = Bson::Int32(i);
            seen.insert(hash_shard_key(&k));
        }
        // With 1000 distinct keys, we expect many distinct hashes
        assert!(seen.len() > 900, "hash distribution too poor: {} distinct", seen.len());
    }

    #[test]
    fn hash_shard_key_types() {
        // Different types with "similar" values should hash differently
        let h_int = hash_shard_key(&Bson::Int32(1));
        let h_str = hash_shard_key(&Bson::String("1".into()));
        let h_double = hash_shard_key(&Bson::Double(1.0));
        assert_ne!(h_int, h_str);
        assert_ne!(h_int, h_double);
    }

    #[test]
    fn compare_shard_key_ordering() {
        assert_eq!(
            compare_shard_key(&Bson::Int32(1), &Bson::Int32(2)),
            Ordering::Less
        );
        assert_eq!(
            compare_shard_key(&Bson::String("a".into()), &Bson::String("b".into())),
            Ordering::Less
        );
    }

    #[test]
    fn validate_shard_key_ok() {
        assert!(validate_shard_key(&ShardKey::Hashed {
            field: "x".into()
        })
        .is_ok());
        assert!(validate_shard_key(&ShardKey::Range {
            field: "x".into()
        })
        .is_ok());
        assert!(validate_shard_key(&ShardKey::Compound {
            fields: vec!["a".into(), "b".into()]
        })
        .is_ok());
    }

    #[test]
    fn validate_shard_key_empty_field() {
        assert!(validate_shard_key(&ShardKey::Hashed {
            field: "".into()
        })
        .is_err());
    }

    #[test]
    fn validate_shard_key_empty_compound() {
        assert!(validate_shard_key(&ShardKey::Compound {
            fields: vec![]
        })
        .is_err());
    }

    #[test]
    fn validate_shard_key_empty_compound_field() {
        assert!(validate_shard_key(&ShardKey::Compound {
            fields: vec!["a".into(), "".into()]
        })
        .is_err());
    }

    #[test]
    fn is_valid_key_value_rejects_array() {
        assert!(!is_valid_key_value(&Bson::Array(vec![Bson::Int32(1)])));
        assert!(is_valid_key_value(&Bson::Int32(1)));
        assert!(is_valid_key_value(&Bson::String("x".into())));
    }

    #[test]
    fn bson_to_bytes_deterministic() {
        let a = bson_to_bytes(&Bson::Int32(42));
        let b = bson_to_bytes(&Bson::Int32(42));
        assert_eq!(a, b);
    }
}
