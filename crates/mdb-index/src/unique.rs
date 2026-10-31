//! 唯一索引：在 BTree 基础上强制 key 唯一性。
//!
//! 插入重复 key 时返回 `ErrorCode::DuplicateKey`。

use mdb_common::{MdbError, MdbResult};
use mdb_doc::value::Bson;

use crate::btree::BTree;
use crate::types::RecordId;

/// 唯一索引。
#[derive(Debug)]
pub struct UniqueIndex {
    btree: BTree,
    name: String,
}

impl UniqueIndex {
    pub fn new(name: impl Into<String>, page_size: usize) -> Self {
        Self { btree: BTree::new(page_size), name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn len(&self) -> usize {
        self.btree.len()
    }

    pub fn is_empty(&self) -> bool {
        self.btree.is_empty()
    }

    /// 插入唯一键。key 已存在时返回 DuplicateKey 错误。
    pub fn insert(&mut self, key: Bson, record_id: RecordId) -> MdbResult<()> {
        if !self.btree.point_lookup(&key).is_empty() {
            return Err(MdbError::new(
                mdb_common::ErrorCode::DuplicateKey,
                format!("duplicate key on unique index '{}': {key}", self.name),
            ));
        }
        self.btree.insert(key, record_id);
        Ok(())
    }

    /// 移除指定的 (key, record_id)。
    pub fn remove(&mut self, key: &Bson, record_id: RecordId) -> bool {
        self.btree.remove(key, record_id)
    }

    /// 点查询。
    pub fn point_lookup(&self, key: &Bson) -> Option<RecordId> {
        let rids = self.btree.point_lookup(key);
        rids.into_iter().next()
    }

    /// 范围扫描。
    pub fn range_scan(&self, start: &Bson, end: &Bson) -> Vec<RecordId> {
        self.btree.range_scan(start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_unique_keys() {
        let mut idx = UniqueIndex::new("test_unique", 4);
        assert!(idx.insert(Bson::Int32(1), 100).is_ok());
        assert!(idx.insert(Bson::Int32(2), 200).is_ok());
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn duplicate_key_rejected() {
        let mut idx = UniqueIndex::new("test_unique", 4);
        assert!(idx.insert(Bson::Int32(1), 100).is_ok());
        let err = idx.insert(Bson::Int32(1), 200).unwrap_err();
        assert_eq!(err.code(), mdb_common::ErrorCode::DuplicateKey);
    }

    #[test]
    fn remove_and_reinsert() {
        let mut idx = UniqueIndex::new("test_unique", 4);
        idx.insert(Bson::Int32(1), 100).unwrap();
        assert!(idx.remove(&Bson::Int32(1), 100));
        // 现在可以重新插入
        assert!(idx.insert(Bson::Int32(1), 200).is_ok());
        assert_eq!(idx.point_lookup(&Bson::Int32(1)), Some(200));
    }

    #[test]
    fn point_lookup_none() {
        let idx = UniqueIndex::new("test_unique", 4);
        assert_eq!(idx.point_lookup(&Bson::Int32(1)), None);
    }

    #[test]
    fn range_scan_unique() {
        let mut idx = UniqueIndex::new("test_unique", 4);
        for i in 0..10 {
            idx.insert(Bson::Int32(i), i as u64).unwrap();
        }
        let result = idx.range_scan(&Bson::Int32(3), &Bson::Int32(7));
        assert_eq!(result, vec![3, 4, 5, 6, 7]);
    }
}
