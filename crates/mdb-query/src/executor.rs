//! 查询执行器：过滤求值、投影应用、执行计划选择。

use mdb_doc::{Bson, Document};

use crate::explain::ExplainPlan;
use crate::operators::{self, QueryOperator};
use crate::parser::{parse_filter, ComparisonOp, Filter, LogicalOp};
use crate::types::{parse_sort_spec, QueryPlan, QueryResult, SortDirection};

/// 查询执行器 trait。
pub trait QueryExecutor {
    /// 执行查询，返回结果集。
    fn execute(
        &self,
        filter: &Document,
        sort: Option<&Document>,
        projection: Option<&Document>,
        limit: Option<i64>,
        skip: Option<i64>,
    ) -> QueryResult;

    /// 获取查询执行计划（explain）。
    ///
    /// 返回查询将如何执行的详细信息，包括扫描类型、
    /// 使用的索引、预计扫描和返回的文档数。
    fn explain(
        &self,
        filter: &Document,
        sort: Option<&Document>,
        projection: Option<&Document>,
    ) -> ExplainPlan;
}

/// 内存中的文档集合，用于执行查询。
#[derive(Debug)]
pub struct InMemoryCollection {
    /// 集合名称。
    pub name: String,
    /// 集合中的所有文档。
    pub documents: Vec<Document>,
}

impl InMemoryCollection {
    /// 创建新的内存集合。
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), documents: Vec::new() }
    }

    /// 插入文档。
    pub fn insert(&mut self, doc: Document) {
        self.documents.push(doc);
    }

    /// 批量插入文档。
    pub fn insert_many(&mut self, docs: Vec<Document>) {
        self.documents.extend(docs);
    }
}

/// 全集合扫描执行器。
#[derive(Debug)]
pub struct CollectionScanExecutor<'a> {
    pub collection: &'a InMemoryCollection,
}

impl<'a> QueryExecutor for CollectionScanExecutor<'a> {
    fn execute(
        &self,
        filter: &Document,
        sort: Option<&Document>,
        projection: Option<&Document>,
        limit: Option<i64>,
        skip: Option<i64>,
    ) -> QueryResult {
        let evaluator = FilterEvaluator::new(filter);
        let mut results: Vec<Document> = self
            .collection
            .documents
            .iter()
            .filter(|doc| evaluator.evaluate(doc))
            .cloned()
            .collect();

        // 排序
        if let Some(sort_doc) = sort {
            let spec = parse_sort_spec(sort_doc);
            if !spec.is_empty() {
                sort_documents(&mut results, &spec);
            }
        }

        // 跳过
        let skip_n = skip.unwrap_or(0).max(0) as usize;
        if skip_n > 0 && skip_n < results.len() {
            results = results.split_off(skip_n);
        } else if skip_n >= results.len() {
            results.clear();
        }

        // 限制
        if let Some(limit_n) = limit {
            let limit_n = limit_n.max(0) as usize;
            results.truncate(limit_n);
        }

        // 投影
        if let Some(proj) = projection {
            let applier = ProjectionApplier::new(proj);
            results = results.into_iter().map(|doc| applier.apply(&doc)).collect();
        }

        QueryResult::batch(results)
    }

    fn explain(
        &self,
        filter: &Document,
        sort: Option<&Document>,
        _projection: Option<&Document>,
    ) -> ExplainPlan {
        let total_docs = self.collection.documents.len() as u64;
        let evaluator = FilterEvaluator::new(filter);
        let n_returned = self
            .collection
            .documents
            .iter()
            .filter(|doc| evaluator.evaluate(doc))
            .count() as u64;

        let mut plan = ExplainPlan::collscan(n_returned, total_docs);

        if sort_doc_exists(sort) {
            plan = plan.with_sort(n_returned);
        }

        plan
    }
}

/// 索引扫描执行器（模拟）。
///
/// 在实际实现中，此执行器会使用 B-Tree 索引进行高效查找。
/// 当前为简化实现，仅作为接口占位。
#[derive(Debug)]
pub struct IndexScanExecutor<'a> {
    pub collection: &'a InMemoryCollection,
    pub index_name: String,
}

impl<'a> QueryExecutor for IndexScanExecutor<'a> {
    fn execute(
        &self,
        filter: &Document,
        sort: Option<&Document>,
        projection: Option<&Document>,
        limit: Option<i64>,
        skip: Option<i64>,
    ) -> QueryResult {
        // 简化实现：回退到全集合扫描
        let delegate = CollectionScanExecutor { collection: self.collection };
        delegate.execute(filter, sort, projection, limit, skip)
    }

    fn explain(
        &self,
        filter: &Document,
        sort: Option<&Document>,
        _projection: Option<&Document>,
    ) -> ExplainPlan {
        // 当前为模拟实现：实际应使用 B-Tree 索引
        let total_docs = self.collection.documents.len() as u64;
        let evaluator = FilterEvaluator::new(filter);
        let n_returned = self
            .collection
            .documents
            .iter()
            .filter(|doc| evaluator.evaluate(doc))
            .count() as u64;

        let mut plan = ExplainPlan::index_scan(
            self.index_name.clone(),
            n_returned,
            n_returned, // 简化：n_scanned == n_returned
            total_docs,
        );

        if sort_doc_exists(sort) {
            plan = plan.with_sort(n_returned);
        }

        plan
    }
}

/// 查询计划选择器：根据查询条件选择最优执行计划。
#[derive(Debug)]
pub struct QueryPlanner;

impl QueryPlanner {
    /// 选择最优查询计划。
    ///
    /// 当前实现始终选择全集合扫描。在实际系统中，此函数会分析
    /// 查询条件、可用索引和统计信息来选择最优计划。
    pub fn choose_plan(
        _filter: &Document,
        _sort: Option<&Document>,
    ) -> QueryPlan {
        // TODO: 基于索引统计信息选择最优计划
        QueryPlan::CollectionScan
    }
}

/// 过滤器求值器：将 Filter AST 应用于文档。
#[derive(Debug)]
pub struct FilterEvaluator {
    filter: Filter,
}

impl FilterEvaluator {
    /// 从查询文档构造求值器。
    pub fn new(filter_doc: &Document) -> Self {
        let filter = parse_filter(filter_doc).unwrap_or(Filter::And(vec![]));
        Self { filter }
    }

    /// 从 Filter AST 直接构造。
    pub fn from_filter(filter: Filter) -> Self {
        Self { filter }
    }

    /// 求值文档是否匹配过滤器。
    pub fn evaluate(&self, doc: &Document) -> bool {
        eval_filter(&self.filter, doc)
    }
}

/// 递归求值过滤器。
fn eval_filter(filter: &Filter, doc: &Document) -> bool {
    match filter {
        Filter::Comparison { field, op, value } => eval_comparison(field, op, value, doc),
        Filter::Logical { op, children } => match op {
            LogicalOp::And => children.iter().all(|c| eval_filter(c, doc)),
            LogicalOp::Or => children.iter().any(|c| eval_filter(c, doc)),
            LogicalOp::Not => !children.iter().any(|c| eval_filter(c, doc)),
            LogicalOp::Nor => children.iter().all(|c| !eval_filter(c, doc)),
        },
        Filter::Exists { field, negated } => {
            let exists = doc.get(field).is_some();
            if *negated { !exists } else { exists }
        }
        Filter::Regex { field, pattern, options } => {
            let val = match doc.get(field) {
                Some(v) => v,
                None => return false,
            };
            let op = operators::RegexOp { pattern: pattern.clone(), options: options.clone() };
            op.matches(val)
        }
        Filter::In { field, values } => {
            let val = match doc.get(field) {
                Some(v) => v,
                None => return false,
            };
            values.iter().any(|v| operators::bson_eq(val, v))
        }
        Filter::Nor(children) => children.iter().all(|c| !eval_filter(c, doc)),
        Filter::And(children) => children.iter().all(|c| eval_filter(c, doc)),
        Filter::Or(children) => children.iter().any(|c| eval_filter(c, doc)),
    }
}

/// 求值比较过滤器。
fn eval_comparison(field: &str, op: &ComparisonOp, target: &Bson, doc: &Document) -> bool {
    // 处理 $mod 特殊情况
    if let Bson::Array(ref arr) = target {
        if arr.len() == 2 {
            if let (Some(divisor), Some(remainder)) = (arr[0].as_i64(), arr[1].as_i64()) {
                let val = match doc.get(field) {
                    Some(v) => v,
                    None => return false,
                };
                let n = match val.as_i64() {
                    Some(n) => n,
                    None => return false,
                };
                if divisor == 0 {
                    return false;
                }
                return n % divisor == remainder;
            }
        }
    }

    let val = match doc.get(field) {
        Some(v) => v,
        None => {
            return match op {
                ComparisonOp::Eq => operators::bson_eq(&Bson::Null, target),
                ComparisonOp::Ne => !operators::bson_eq(&Bson::Null, target),
                _ => false,
            };
        }
    };

    match op {
        ComparisonOp::Eq => operators::bson_eq(val, target),
        ComparisonOp::Ne => !operators::bson_eq(val, target),
        ComparisonOp::Gt => {
            operators::bson_cmp(val, target) == Some(std::cmp::Ordering::Greater)
        }
        ComparisonOp::Gte => matches!(
            operators::bson_cmp(val, target),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
        ComparisonOp::Lt => {
            operators::bson_cmp(val, target) == Some(std::cmp::Ordering::Less)
        }
        ComparisonOp::Lte => matches!(
            operators::bson_cmp(val, target),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
        ComparisonOp::Nin => !operators::bson_eq(val, target),
    }
}

/// 投影应用器。
#[derive(Debug)]
pub struct ProjectionApplier {
    inclusion: Option<Vec<String>>,
    exclusion: Option<Vec<String>>,
}

impl ProjectionApplier {
    /// 从投影文档构造。
    ///
    /// MongoDB 投影规则：
    /// - `{ _id: 1, name: 1 }`：包含模式（只返回指定字段）
    /// - `{ _id: 0, password: 0 }`：排除模式（返回所有字段除指定字段）
    /// - 混合 0/1 是错误的（`_id` 除外）
    pub fn new(projection: &Document) -> Self {
        let mut inclusion = Vec::new();
        let mut exclusion = Vec::new();

        let has_inclusion = projection.iter().any(|(_, v)| matches!(v, Bson::Int32(1) | Bson::Int64(1)));
        let has_exclusion = projection.iter().any(|(_, v)| matches!(v, Bson::Int32(0) | Bson::Int64(0)));

        for (key, val) in projection.iter() {
            let is_one = matches!(val, Bson::Int32(1) | Bson::Int64(1));
            let is_zero = matches!(val, Bson::Int32(0) | Bson::Int64(0));

            if key == "_id" {
                if is_zero {
                    exclusion.push(key.to_owned());
                }
                // _id 默认包含，显式 1 也包含
                continue;
            }

            if has_inclusion && is_one {
                inclusion.push(key.to_owned());
            } else if has_exclusion && is_zero {
                exclusion.push(key.to_owned());
            }
        }

        Self {
            inclusion: if inclusion.is_empty() { None } else { Some(inclusion) },
            exclusion: if exclusion.is_empty() { None } else { Some(exclusion) },
        }
    }

    /// 对文档应用投影。
    pub fn apply(&self, doc: &Document) -> Document {
        match (&self.inclusion, &self.exclusion) {
            (Some(inc), _) => {
                // 包含模式：只返回指定字段（始终包含 _id）
                let mut result = Document::new();
                for key in inc {
                    if let Some(val) = doc.get(key) {
                        result.insert(key.clone(), val.clone());
                    }
                }
                // _id 默认包含
                if !inc.contains(&"_id".to_owned()) {
                    if let Some(val) = doc.get("_id") {
                        result.insert("_id", val.clone());
                    }
                }
                result
            }
            (_, Some(exc)) => {
                // 排除模式：返回所有字段（除排除的字段）
                let mut result = Document::new();
                for (key, val) in doc.iter() {
                    if !exc.contains(&key.to_owned()) {
                        result.insert(key.to_owned(), val.clone());
                    }
                }
                result
            }
            _ => doc.clone(),
        }
    }
}

/// 检查排序文档是否包含有效排序字段。
fn sort_doc_exists(sort: Option<&Document>) -> bool {
    sort.map(|s| !parse_sort_spec(s).is_empty()).unwrap_or(false)
}

/// 内存排序函数：按多键排序文档集合。
pub fn sort_documents(docs: &mut Vec<Document>, spec: &[(String, SortDirection)]) {
    docs.sort_by(|a, b| {
        for (field, dir) in spec {
            let va = a.get(field);
            let vb = b.get(field);
            let ord = match (va, vb) {
                (Some(a), Some(b)) => operators::bson_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal),
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (None, None) => std::cmp::Ordering::Equal,
            };
            match dir {
                SortDirection::Ascending => {
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                SortDirection::Descending => {
                    if ord != std::cmp::Ordering::Equal {
                        return ord.reverse();
                    }
                }
            }
        }
        std::cmp::Ordering::Equal
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_doc(name: &str, age: i32) -> Document {
        let mut doc = Document::new();
        doc.insert("name", Bson::String(name.into()));
        doc.insert("age", Bson::Int32(age));
        doc
    }

    fn make_collection() -> InMemoryCollection {
        let mut col = InMemoryCollection::new("test");
        col.insert(make_doc("Alice", 30));
        col.insert(make_doc("Bob", 25));
        col.insert(make_doc("Charlie", 35));
        col.insert(make_doc("Diana", 25));
        col
    }

    #[test]
    fn collection_scan_equality() {
        let col = make_collection();
        let executor = CollectionScanExecutor { collection: &col };
        let mut filter = Document::new();
        filter.insert("age", Bson::Int32(25));
        let result = executor.execute(&filter, None, None, None, None);
        assert_eq!(result.documents.len(), 2);
    }

    #[test]
    fn collection_scan_with_limit() {
        let col = make_collection();
        let executor = CollectionScanExecutor { collection: &col };
        let mut filter = Document::new();
        filter.insert("age", Bson::Int32(25));
        let result = executor.execute(&filter, None, None, Some(1), None);
        assert_eq!(result.documents.len(), 1);
    }

    #[test]
    fn collection_scan_with_skip() {
        let col = make_collection();
        let executor = CollectionScanExecutor { collection: &col };
        let filter = Document::new();
        let result = executor.execute(&filter, None, None, None, Some(2));
        assert_eq!(result.documents.len(), 2); // 4 - 2 = 2
    }

    #[test]
    fn collection_scan_with_sort() {
        let col = make_collection();
        let executor = CollectionScanExecutor { collection: &col };
        let filter = Document::new();
        let mut sort = Document::new();
        sort.insert("age", Bson::Int32(1)); // ascending
        let result = executor.execute(&filter, Some(&sort), None, None, None);
        assert_eq!(result.documents.len(), 4);
        // Bob(25) or Diana(25) first
        let first_age = result.documents[0].get("age").and_then(Bson::as_i64).unwrap();
        assert_eq!(first_age, 25);
    }

    #[test]
    fn collection_scan_with_projection() {
        let col = make_collection();
        let executor = CollectionScanExecutor { collection: &col };
        let filter = Document::new();
        let mut proj = Document::new();
        proj.insert("name", Bson::Int32(1));
        let result = executor.execute(&filter, None, Some(&proj), None, None);
        assert_eq!(result.documents.len(), 4);
        // Each doc should only have _id and name
        for doc in &result.documents {
            assert!(doc.get("name").is_some());
            assert!(doc.get("age").is_none());
        }
    }

    #[test]
    fn filter_evaluator_equality() {
        let mut filter_doc = Document::new();
        filter_doc.insert("age", Bson::Int32(25));
        let evaluator = FilterEvaluator::new(&filter_doc);
        let doc = make_doc("Bob", 25);
        assert!(evaluator.evaluate(&doc));
        let doc = make_doc("Alice", 30);
        assert!(!evaluator.evaluate(&doc));
    }

    #[test]
    fn filter_evaluator_range() {
        let mut filter_doc = Document::new();
        let mut inner = Document::new();
        inner.insert("$gte", Bson::Int32(25));
        inner.insert("$lte", Bson::Int32(30));
        filter_doc.insert("age", Bson::Document(inner));
        let evaluator = FilterEvaluator::new(&filter_doc);

        assert!(evaluator.evaluate(&make_doc("Bob", 25)));
        assert!(evaluator.evaluate(&make_doc("Alice", 30)));
        assert!(!evaluator.evaluate(&make_doc("Charlie", 35)));
    }

    #[test]
    fn filter_evaluator_and() {
        let mut filter_doc = Document::new();
        filter_doc.insert("age", Bson::Int32(25));
        filter_doc.insert("name", Bson::String("Bob".into()));
        let evaluator = FilterEvaluator::new(&filter_doc);

        assert!(evaluator.evaluate(&make_doc("Bob", 25)));
        assert!(!evaluator.evaluate(&make_doc("Alice", 25)));
        assert!(!evaluator.evaluate(&make_doc("Bob", 30)));
    }

    #[test]
    fn projection_inclusion() {
        let mut proj = Document::new();
        proj.insert("name", Bson::Int32(1));
        let applier = ProjectionApplier::new(&proj);

        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("name", Bson::String("test".into()));
        doc.insert("age", Bson::Int32(25));

        let result = applier.apply(&doc);
        assert!(result.get("_id").is_some());
        assert!(result.get("name").is_some());
        assert!(result.get("age").is_none());
    }

    #[test]
    fn projection_exclusion() {
        let mut proj = Document::new();
        proj.insert("age", Bson::Int32(0));
        let applier = ProjectionApplier::new(&proj);

        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("name", Bson::String("test".into()));
        doc.insert("age", Bson::Int32(25));

        let result = applier.apply(&doc);
        assert!(result.get("_id").is_some());
        assert!(result.get("name").is_some());
        assert!(result.get("age").is_none());
    }

    #[test]
    fn projection_exclude_id() {
        let mut proj = Document::new();
        proj.insert("_id", Bson::Int32(0));
        proj.insert("age", Bson::Int32(0));
        let applier = ProjectionApplier::new(&proj);

        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("name", Bson::String("test".into()));
        doc.insert("age", Bson::Int32(25));

        let result = applier.apply(&doc);
        assert!(result.get("_id").is_none());
        assert!(result.get("name").is_some());
        assert!(result.get("age").is_none());
    }

    #[test]
    fn sort_documents_ascending() {
        let mut docs = vec![
            make_doc("Charlie", 35),
            make_doc("Alice", 30),
            make_doc("Bob", 25),
        ];
        let spec = vec![("age".to_owned(), SortDirection::Ascending)];
        sort_documents(&mut docs, &spec);
        assert_eq!(docs[0].get("name").unwrap().as_str(), Some("Bob"));
        assert_eq!(docs[1].get("name").unwrap().as_str(), Some("Alice"));
        assert_eq!(docs[2].get("name").unwrap().as_str(), Some("Charlie"));
    }

    #[test]
    fn sort_documents_descending() {
        let mut docs = vec![
            make_doc("Charlie", 35),
            make_doc("Alice", 30),
            make_doc("Bob", 25),
        ];
        let spec = vec![("age".to_owned(), SortDirection::Descending)];
        sort_documents(&mut docs, &spec);
        assert_eq!(docs[0].get("name").unwrap().as_str(), Some("Charlie"));
        assert_eq!(docs[1].get("name").unwrap().as_str(), Some("Alice"));
        assert_eq!(docs[2].get("name").unwrap().as_str(), Some("Bob"));
    }

    #[test]
    fn sort_documents_multi_key() {
        let mut doc1 = Document::new();
        doc1.insert("a", Bson::Int32(1));
        doc1.insert("b", Bson::Int32(2));
        let mut doc2 = Document::new();
        doc2.insert("a", Bson::Int32(1));
        doc2.insert("b", Bson::Int32(1));
        let mut doc3 = Document::new();
        doc3.insert("a", Bson::Int32(0));
        doc3.insert("b", Bson::Int32(3));
        let mut docs = vec![doc1, doc2, doc3];
        let spec = vec![
            ("a".to_owned(), SortDirection::Ascending),
            ("b".to_owned(), SortDirection::Ascending),
        ];
        sort_documents(&mut docs, &spec);
        assert_eq!(docs[0].get("a").and_then(Bson::as_i64), Some(0));
        assert_eq!(docs[1].get("b").and_then(Bson::as_i64), Some(1));
        assert_eq!(docs[2].get("b").and_then(Bson::as_i64), Some(2));
    }

    #[test]
    fn query_plan_choose() {
        let filter = Document::new();
        let plan = QueryPlanner::choose_plan(&filter, None);
        assert_eq!(plan, QueryPlan::CollectionScan);
    }

    #[test]
    fn collection_scan_empty_filter_matches_all() {
        let col = make_collection();
        let executor = CollectionScanExecutor { collection: &col };
        let filter = Document::new();
        let result = executor.execute(&filter, None, None, None, None);
        assert_eq!(result.documents.len(), 4);
    }

    #[test]
    fn collection_scan_exists_filter() {
        let mut col = InMemoryCollection::new("test");
        let mut d1 = Document::new();
        d1.insert("email", Bson::String("a@b.com".into()));
        let mut d2 = Document::new();
        d2.insert("name", Bson::String("no-email".into()));
        col.insert(d1);
        col.insert(d2);

        let executor = CollectionScanExecutor { collection: &col };
        let mut filter = Document::new();
        let mut inner = Document::new();
        inner.insert("$exists", Bson::Boolean(true));
        filter.insert("email", Bson::Document(inner));
        let result = executor.execute(&filter, None, None, None, None);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(
            result.documents[0].get("email").and_then(Bson::as_str),
            Some("a@b.com")
        );
    }

    #[test]
    fn explain_collscan_empty_filter() {
        let col = make_collection();
        let executor = CollectionScanExecutor { collection: &col };
        let filter = Document::new();
        let plan = executor.explain(&filter, None, None);
        assert_eq!(plan.query_plan, "COLLSCAN");
        assert!(plan.index_name.is_none());
        assert_eq!(plan.n_returned, 4);
        assert_eq!(plan.n_scanned_objects, 4);
        assert_eq!(plan.stage_details.len(), 1);
        assert_eq!(plan.stage_details[0].stage, "COLLSCAN");
    }

    #[test]
    fn explain_collscan_with_filter() {
        let col = make_collection();
        let executor = CollectionScanExecutor { collection: &col };
        let mut filter = Document::new();
        filter.insert("age", Bson::Int32(25));
        let plan = executor.explain(&filter, None, None);
        assert_eq!(plan.query_plan, "COLLSCAN");
        assert_eq!(plan.n_returned, 2);
        assert_eq!(plan.n_scanned_objects, 4);
    }

    #[test]
    fn explain_collscan_with_sort() {
        let col = make_collection();
        let executor = CollectionScanExecutor { collection: &col };
        let filter = Document::new();
        let mut sort = Document::new();
        sort.insert("age", Bson::Int32(1));
        let plan = executor.explain(&filter, Some(&sort), None);
        assert_eq!(plan.query_plan, "COLLSCAN");
        assert_eq!(plan.stage_details.len(), 2);
        assert_eq!(plan.stage_details[0].stage, "COLLSCAN");
        assert_eq!(plan.stage_details[1].stage, "SORT");
    }

    #[test]
    fn explain_collscan_without_sort() {
        let col = make_collection();
        let executor = CollectionScanExecutor { collection: &col };
        let filter = Document::new();
        let mut sort = Document::new();
        sort.insert("nonexistent", Bson::Int32(1));
        let plan = executor.explain(&filter, Some(&sort), None);
        // parse_sort_spec returns valid sorts regardless of field existence
        // so SORT stage is added when spec is non-empty
        assert_eq!(plan.stage_details.len(), 2);
        assert_eq!(plan.stage_details[0].stage, "COLLSCAN");
        assert_eq!(plan.stage_details[1].stage, "SORT");
    }

    #[test]
    fn explain_index_scan() {
        let col = make_collection();
        let executor =
            IndexScanExecutor { collection: &col, index_name: "idx_age".into() };
        let mut filter = Document::new();
        filter.insert("age", Bson::Int32(25));
        let plan = executor.explain(&filter, None, None);
        assert!(plan.query_plan.contains("IXSCAN"));
        assert!(plan.query_plan.contains("idx_age"));
        assert_eq!(plan.index_name.as_deref(), Some("idx_age"));
        assert_eq!(plan.n_returned, 2);
        assert_eq!(plan.stage_details[0].stage, "IXSCAN");
        assert_eq!(plan.stage_details[1].stage, "FETCH");
    }

    #[test]
    fn explain_index_scan_with_sort() {
        let col = make_collection();
        let executor =
            IndexScanExecutor { collection: &col, index_name: "idx_name".into() };
        let filter = Document::new();
        let mut sort = Document::new();
        sort.insert("name", Bson::Int32(1));
        let plan = executor.explain(&filter, Some(&sort), None);
        assert_eq!(plan.stage_details.len(), 3);
        assert_eq!(plan.stage_details[0].stage, "IXSCAN");
        assert_eq!(plan.stage_details[1].stage, "FETCH");
        assert_eq!(plan.stage_details[2].stage, "SORT");
    }

    #[test]
    fn explain_matches_execute_results() {
        let col = make_collection();
        let executor = CollectionScanExecutor { collection: &col };
        let mut filter = Document::new();
        filter.insert("age", Bson::Int32(25));
        let result = executor.execute(&filter, None, None, None, None);
        let plan = executor.explain(&filter, None, None);
        assert_eq!(plan.n_returned, result.documents.len() as u64);
    }
}
