//! 查询类型定义：查询计划、排序方向、查询结果。

use mdb_doc::{Bson, Document};

/// 查询计划：描述如何执行一个查询。
#[derive(Debug, Clone, PartialEq)]
pub enum QueryPlan {
    /// 全集合扫描，逐文档匹配过滤条件。
    CollectionScan,
    /// 索引扫描，使用指定索引的键范围进行高效查找。
    IndexScan {
        /// 索引名称。
        index_name: String,
        /// 范围起始键（含）。
        range_start: Option<Document>,
        /// 范围结束键（含）。
        range_end: Option<Document>,
    },
    /// 覆盖索引扫描：索引本身包含查询所需全部字段，无需回表。
    CoveredIndexScan {
        /// 索引名称。
        index_name: String,
        /// 范围起始键（含）。
        range_start: Option<Document>,
        /// 范围结束键（含）。
        range_end: Option<Document>,
    },
}

/// 索引统计信息：用于查询计划优化。
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    /// 索引名称。
    pub name: String,
    /// 索引键字段。
    pub keys: Vec<String>,
    /// 索引中的文档数。
    pub count: u64,
    /// 平均每个键指向的文档数。
    pub avg_keys_per_doc: f64,
}

/// 可用索引信息：描述集合上可用的索引。
#[derive(Debug, Clone, Default)]
pub struct CollectionIndexes {
    /// 所有可用索引。
    pub indexes: Vec<IndexStats>,
}

impl CollectionIndexes {
    /// 查找匹配查询前缀的索引。
    pub fn find_matching_index(&self, filter_fields: &[String]) -> Option<&IndexStats> {
        if filter_fields.is_empty() {
            return None;
        }
        // 查找第一个字段匹配的索引
        self.indexes.iter().find(|idx| {
            idx.keys.iter().any(|k| filter_fields.contains(k))
        })
    }
}

/// 排序方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortDirection {
    /// 升序（1）。
    Ascending,
    /// 降序（-1）。
    Descending,
}

impl SortDirection {
    /// 从 MongoDB 排序值构造：`1` → Ascending，`-1` → Descending。
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            1 => Some(Self::Ascending),
            -1 => Some(Self::Descending),
            _ => None,
        }
    }
}

/// 查询结果。
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// 匹配的文档列表。
    pub documents: Vec<Document>,
    /// 游标 ID；`0` 表示结果已全部返回（无游标）。
    pub cursor_id: i64,
    /// 是否还有更多结果（游标未耗尽）。
    pub has_more: bool,
}

impl QueryResult {
    /// 构造无游标的一次性结果。
    pub fn batch(documents: Vec<Document>) -> Self {
        Self { documents, cursor_id: 0, has_more: false }
    }

    /// 构造带游标的结果。
    pub fn with_cursor(documents: Vec<Document>, cursor_id: i64, has_more: bool) -> Self {
        Self { documents, cursor_id, has_more }
    }

    /// 空结果集。
    pub fn empty() -> Self {
        Self::batch(Vec::new())
    }
}

/// 从 Document 提取排序规格，返回字段名与方向的有序列表。
pub fn parse_sort_spec(doc: &Document) -> Vec<(String, SortDirection)> {
    doc.iter()
        .filter_map(|(k, v)| {
            let dir = match v {
                Bson::Int32(n) => SortDirection::from_i32(*n)?,
                Bson::Int64(n) => SortDirection::from_i32(*n as i32)?,
                Bson::Double(n) => SortDirection::from_i32(*n as i32)?,
                _ => return None,
            };
            Some((k.to_owned(), dir))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_direction_from_i32() {
        assert_eq!(SortDirection::from_i32(1), Some(SortDirection::Ascending));
        assert_eq!(SortDirection::from_i32(-1), Some(SortDirection::Descending));
        assert_eq!(SortDirection::from_i32(0), None);
        assert_eq!(SortDirection::from_i32(2), None);
    }

    #[test]
    fn query_result_batch() {
        let r = QueryResult::batch(vec![]);
        assert_eq!(r.cursor_id, 0);
        assert!(!r.has_more);
    }

    #[test]
    fn query_result_with_cursor() {
        let r = QueryResult::with_cursor(vec![], 42, true);
        assert_eq!(r.cursor_id, 42);
        assert!(r.has_more);
    }

    #[test]
    fn parse_sort_spec_basic() {
        let mut doc = Document::new();
        doc.insert("age", Bson::Int32(1));
        doc.insert("name", Bson::Int32(-1));
        let spec = parse_sort_spec(&doc);
        assert_eq!(spec.len(), 2);
        assert_eq!(spec[0], ("age".to_owned(), SortDirection::Ascending));
        assert_eq!(spec[1], ("name".to_owned(), SortDirection::Descending));
    }

    #[test]
    fn parse_sort_spec_ignores_invalid() {
        let mut doc = Document::new();
        doc.insert("a", Bson::Int32(1));
        doc.insert("b", Bson::Int32(0));
        doc.insert("c", Bson::String("asc".into()));
        let spec = parse_sort_spec(&doc);
        assert_eq!(spec.len(), 1);
    }

    #[test]
    fn query_plan_variants() {
        let plan = QueryPlan::CollectionScan;
        assert_eq!(plan, QueryPlan::CollectionScan);

        let plan = QueryPlan::IndexScan {
            index_name: "idx_age".into(),
            range_start: None,
            range_end: None,
        };
        assert!(matches!(plan, QueryPlan::IndexScan { .. }));

        let plan = QueryPlan::CoveredIndexScan {
            index_name: "idx_name".into(),
            range_start: None,
            range_end: None,
        };
        assert!(matches!(plan, QueryPlan::CoveredIndexScan { .. }));
    }
}
