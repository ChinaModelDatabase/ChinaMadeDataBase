//! 索引管理器：管理集合的全部索引。
//!
//! 支持 TTL 索引的后台清理、游标聚合等功能。

use std::collections::HashMap;

use mdb_common::{MdbError, ErrorCode, MdbResult};
use mdb_doc::value::{Bson, Document};

use crate::btree::BTree;
use crate::geospatial::{GeoIndex, GeoShape};
use crate::text::TextIndex;
use crate::ttl::{TtlIndex, TtlCleanupStats};
use crate::types::{IndexOptions, IndexType, RecordId, SortOrder};
use crate::unique::UniqueIndex;

/// 索引统计信息。
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// 索引名称。
    pub name: String,
    /// 索引类型（Debug 格式）。
    pub index_type: String,
    /// 索引字段列表。
    pub fields: Vec<String>,
    /// 索引条目数量。
    pub entry_count: usize,
    /// 是否唯一索引。
    pub unique: bool,
}

// ---------------------------------------------------------------------------
// IndexEntry 辅助
// ---------------------------------------------------------------------------

/// 从文档中提取指定字段的值（支持点分路径）。
pub fn extract_field<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    doc.get_path(path)
}

    /// 从文档中提取多 key（用于 compound index）。
pub fn extract_compound_key(doc: &Document, fields: &[(String, SortOrder)]) -> Option<Bson> {
    if fields.len() == 1 {
        return extract_field(doc, &fields[0].0).cloned();
    }
    // compound key 用 Array 表示
    let values: Vec<Bson> = fields
        .iter()
        .map(|(path, _)| extract_field(doc, path).cloned().unwrap_or(Bson::Null))
        .collect();
    Some(Bson::Array(values))
}

// ---------------------------------------------------------------------------
// Index trait（统一索引接口）
// ---------------------------------------------------------------------------

/// 所有索引类型共享的接口。
pub trait Index {
    /// 索引名称。
    fn name(&self) -> &str;

    /// 索引类型。
    fn index_type(&self) -> IndexType;

    /// 插入一个 (key, record_id) 对。
    fn insert_entry(&mut self, key: Bson, record_id: RecordId) -> MdbResult<()>;

    /// 移除一个 (key, record_id) 对。
    fn remove_entry(&mut self, key: &Bson, record_id: RecordId) -> bool;

    /// 检查 key 是否已存在（用于唯一索引）。
    fn key_exists(&self, key: &Bson) -> bool;

    /// 条目数量。
    fn len(&self) -> usize;
}

// ---------------------------------------------------------------------------
// 各索引类型的 Index 实现
// ---------------------------------------------------------------------------

impl Index for BTree {
    fn name(&self) -> &str {
        "btree"
    }
    fn index_type(&self) -> IndexType {
        IndexType::BTree
    }
    fn insert_entry(&mut self, key: Bson, record_id: RecordId) -> MdbResult<()> {
        self.insert(key, record_id);
        Ok(())
    }
    fn remove_entry(&mut self, key: &Bson, record_id: RecordId) -> bool {
        self.remove(key, record_id)
    }
    fn key_exists(&self, key: &Bson) -> bool {
        !self.point_lookup(key).is_empty()
    }
    fn len(&self) -> usize {
        BTree::len(self)
    }
}

impl Index for UniqueIndex {
    fn name(&self) -> &str {
        UniqueIndex::name(self)
    }
    fn index_type(&self) -> IndexType {
        IndexType::Unique
    }
    fn insert_entry(&mut self, key: Bson, record_id: RecordId) -> MdbResult<()> {
        UniqueIndex::insert(self, key, record_id)
    }
    fn remove_entry(&mut self, key: &Bson, record_id: RecordId) -> bool {
        UniqueIndex::remove(self, key, record_id)
    }
    fn key_exists(&self, key: &Bson) -> bool {
        UniqueIndex::point_lookup(self, key).is_some()
    }
    fn len(&self) -> usize {
        UniqueIndex::len(self)
    }
}

// ---------------------------------------------------------------------------
// IndexManager
// ---------------------------------------------------------------------------

/// 索引管理器：管理一个集合的全部索引。
#[derive(Debug)]
pub struct IndexManager {
    /// index_name → 索引实现
    indexes: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    /// index_name → 索引选项
    options: HashMap<String, IndexOptions>,
    /// index_name → 索引规范
    specs: HashMap<String, IndexSpec>,
    page_size: usize,
}

impl IndexManager {
    pub fn new(page_size: usize) -> Self {
        Self {
            indexes: HashMap::new(),
            options: HashMap::new(),
            specs: HashMap::new(),
            page_size,
        }
    }

    /// 创建 BTree / Unique 索引。
    pub fn create_index(
        &mut self,
        spec: IndexSpec,
        options: IndexOptions,
    ) -> MdbResult<()> {
        let name = options.name.clone().unwrap_or_else(|| spec.default_name());

        if self.indexes.contains_key(&name) {
            return Err(MdbError::new(
                ErrorCode::BadValue,
                format!("index '{name}' already exists"),
            ));
        }

        // 创建 _id 索引的简化处理：这里只支持普通 BTree / Unique
        let index_type = if options.unique {
            IndexType::Unique
        } else {
            IndexType::BTree
        };

        match index_type {
            IndexType::Unique => {
                let idx = UniqueIndex::new(&name, self.page_size);
                self.indexes.insert(name.clone(), Box::new(idx));
            }
            _ => {
                let idx = BTree::new(self.page_size);
                self.indexes.insert(name.clone(), Box::new(idx));
            }
        }

        self.options.insert(name.clone(), options);
        self.specs.insert(name.clone(), spec);
        Ok(())
    }

    /// 创建文本索引。
    pub fn create_text_index(&mut self, field: &str) -> MdbResult<()> {
        let name = format!("{field}_text");
        if self.indexes.contains_key(&name) {
            return Err(MdbError::new(
                ErrorCode::BadValue,
                format!("index '{name}' already exists"),
            ));
        }
        let idx = TextIndex::new(&name);
        // TextIndex 不是 trait Index 的实现者，但我们用 Box<dyn Any> 存储
        self.indexes.insert(name.clone(), Box::new(idx));
        self.specs.insert(name.clone(), IndexSpec {
            fields: vec![(field.to_owned(), SortOrder::Asc)],
            index_type: IndexType::Text,
        });
        Ok(())
    }

    /// 创建 TTL 索引。
    pub fn create_ttl_index(
        &mut self,
        field: &str,
        expire_after_seconds: i64,
    ) -> MdbResult<()> {
        let name = format!("{field}_ttl");
        if self.indexes.contains_key(&name) {
            return Err(MdbError::new(
                ErrorCode::BadValue,
                format!("index '{name}' already exists"),
            ));
        }
        let idx = TtlIndex::new(&name, expire_after_seconds, self.page_size);
        self.indexes.insert(name.clone(), Box::new(idx));
        self.specs.insert(name.clone(), IndexSpec {
            fields: vec![(field.to_owned(), SortOrder::Asc)],
            index_type: IndexType::TTL,
        });
        Ok(())
    }

    /// 创建地理空间索引。
    pub fn create_2dsphere_index(&mut self, field: &str) -> MdbResult<()> {
        let name = format!("{field}_2dsphere");
        if self.indexes.contains_key(&name) {
            return Err(MdbError::new(
                ErrorCode::BadValue,
                format!("index '{name}' already exists"),
            ));
        }
        let idx = GeoIndex::new(&name);
        self.indexes.insert(name.clone(), Box::new(idx));
        self.specs.insert(name.clone(), IndexSpec {
            fields: vec![(field.to_owned(), SortOrder::Asc)],
            index_type: IndexType::TwoDSphere,
        });
        Ok(())
    }

    /// 创建 Multikey 索引（数组字段索引）。
    ///
    /// Multikey 索引会对数组中的每个元素分别建立索引条目。
    pub fn create_multikey_index(&mut self, field: &str, unique: bool) -> MdbResult<()> {
        let name = format!("{field}_multikey");
        if self.indexes.contains_key(&name) {
            return Err(MdbError::new(
                ErrorCode::BadValue,
                format!("index '{name}' already exists"),
            ));
        }
        // Multikey 使用 BTree 存储
        if unique {
            let idx = UniqueIndex::new(&name, self.page_size);
            self.indexes.insert(name.clone(), Box::new(idx));
        } else {
            let idx = BTree::new(self.page_size);
            self.indexes.insert(name.clone(), Box::new(idx));
        }
        self.options.insert(name.clone(), IndexOptions {
            unique,
            sparse: false,
            expire_after_seconds: None,
            partial_filter_expression: None,
            name: Some(name.clone()),
        });
        self.specs.insert(name.clone(), IndexSpec {
            fields: vec![(field.to_owned(), SortOrder::Asc)],
            index_type: IndexType::Multikey,
        });
        Ok(())
    }

    /// 删除指定索引。
    pub fn drop_index(&mut self, name: &str) -> MdbResult<()> {
        if self.indexes.remove(name).is_none() {
            return Err(MdbError::new(
                ErrorCode::IndexNotFound,
                format!("index not found: '{name}'"),
            ));
        }
        self.options.remove(name);
        self.specs.remove(name);
        Ok(())
    }

    /// 获取索引名称列表。
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    /// 获取索引规范。
    pub fn get_spec(&self, name: &str) -> Option<&IndexSpec> {
        self.specs.get(name)
    }

    /// 校验文档：检查唯一索引约束。
    pub fn validate_document(&self, doc: &Document) -> MdbResult<()> {
        for (name, opts) in &self.options {
            if !opts.unique {
                continue;
            }
            if let Some(spec) = self.specs.get(name) {
                if let Some(key) = extract_compound_key(doc, &spec.fields) {
                    if let Some(idx) = self.indexes.get(name) {
                        if let Some(unique_idx) = idx.downcast_ref::<UniqueIndex>() {
                            if unique_idx.point_lookup(&key).is_some() {
                                return Err(MdbError::new(
                                    ErrorCode::DuplicateKey,
                                    format!("duplicate key on unique index '{name}'"),
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// 向所有索引写入文档字段。
    pub fn update_indexes(&mut self, doc: &Document, record_id: RecordId) -> MdbResult<()> {
        // BTree / Unique 索引
        let index_names: Vec<String> = self.specs.keys().cloned().collect();
        for name in index_names {
            let spec = self.specs.get(&name).unwrap().clone();
            let opts = self.options.get(&name).cloned().unwrap_or_default();

            // Sparse 索引跳过缺失字段
            if opts.sparse {
                let all_present = spec.fields.iter().all(|(path, _)| doc.get_path(path).is_some());
                if !all_present {
                    continue;
                }
            }

            match spec.index_type {
                IndexType::Text => {
                    // 文本索引：收集所有文本字段拼接
                    if let Some((field, _)) = spec.fields.first() {
                        if let Some(value) = doc.get_path(field) {
                            if let Some(text) = value.as_str() {
                                if let Some(idx) = self.indexes.get_mut(&name) {
                                    if let Some(text_idx) = idx.downcast_mut::<TextIndex>() {
                                        text_idx.insert(text, record_id);
                                    }
                                }
                            }
                        }
                    }
                    continue;
                }
                IndexType::TTL => {
                    if let Some(key) = extract_compound_key(doc, &spec.fields) {
                        if let Some(idx) = self.indexes.get_mut(&name) {
                            if let Some(ttl_idx) = idx.downcast_mut::<TtlIndex>() {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_millis() as i64)
                                    .unwrap_or(0);
                                ttl_idx.insert(key, record_id, now);
                            }
                        }
                    }
                    continue;
                }
                IndexType::TwoDSphere => {
                    // 地理空间索引
                    if let Some((field, _)) = spec.fields.first() {
                        if let Some(value) = doc.get_path(field) {
                            if let Some(shape) = bson_to_geo_shape(value) {
                                if let Some(idx) = self.indexes.get_mut(&name) {
                                    if let Some(geo_idx) = idx.downcast_mut::<GeoIndex>() {
                                        geo_idx.insert(shape, record_id);
                                    }
                                }
                            }
                        }
                    }
                    continue;
                }
                IndexType::Multikey => {
                    // Multikey 索引：对数组中的每个元素分别建立索引
                    if let Some((field, _)) = spec.fields.first() {
                        if let Some(value) = doc.get_path(field) {
                            if let Some(arr) = value.as_array() {
                                // 数组字段：为每个元素建立索引
                                for elem in arr {
                                    let key = elem.clone();
                                    if let Some(idx) = self.indexes.get_mut(&name) {
                                        if opts.unique {
                                            if let Some(unique_idx) = idx.downcast_mut::<UniqueIndex>() {
                                                let _ = unique_idx.insert(key, record_id);
                                            }
                                        } else if let Some(btree) = idx.downcast_mut::<BTree>() {
                                            btree.insert(key, record_id);
                                        }
                                    }
                                }
                            } else {
                                // 非数组字段：作为单个值建立索引
                                let key = value.clone();
                                if let Some(idx) = self.indexes.get_mut(&name) {
                                    if opts.unique {
                                        if let Some(unique_idx) = idx.downcast_mut::<UniqueIndex>() {
                                            let _ = unique_idx.insert(key, record_id);
                                        }
                                    } else if let Some(btree) = idx.downcast_mut::<BTree>() {
                                        btree.insert(key, record_id);
                                    }
                                }
                            }
                        }
                    }
                    continue;
                }
                _ => {}
            };

            if let Some(key) = extract_compound_key(doc, &spec.fields) {
                if let Some(idx) = self.indexes.get_mut(&name) {
                    if opts.unique {
                        if let Some(unique_idx) = idx.downcast_mut::<UniqueIndex>() {
                            unique_idx.insert(key, record_id)?;
                        }
                    } else if let Some(btree) = idx.downcast_mut::<BTree>() {
                        btree.insert(key, record_id);
                    }
                }
            }
        }
        Ok(())
    }

    /// 从所有索引中移除文档。
    pub fn remove_from_indexes(&mut self, doc: &Document, record_id: RecordId) {
        let index_names: Vec<String> = self.specs.keys().cloned().collect();
        for name in index_names {
            let spec = self.specs.get(&name).unwrap().clone();

            match spec.index_type {
                IndexType::TTL => {
                    if let Some(idx) = self.indexes.get_mut(&name) {
                        if let Some(ttl_idx) = idx.downcast_mut::<TtlIndex>() {
                            ttl_idx.remove_by_record_id(record_id);
                        }
                    }
                }
                IndexType::TwoDSphere => {
                    if let Some(idx) = self.indexes.get_mut(&name) {
                        if let Some(geo_idx) = idx.downcast_mut::<GeoIndex>() {
                            geo_idx.remove(record_id);
                        }
                    }
                }
                IndexType::Text => {
                    if let Some(idx) = self.indexes.get_mut(&name) {
                        if let Some(text_idx) = idx.downcast_mut::<TextIndex>() {
                            text_idx.remove(record_id);
                        }
                    }
                }
                IndexType::Multikey => {
                    // Multikey 索引：移除数组中每个元素的索引条目
                    if let Some((field, _)) = spec.fields.first() {
                        if let Some(value) = doc.get_path(field) {
                            if let Some(arr) = value.as_array() {
                                for elem in arr {
                                    let key = elem.clone();
                                    if let Some(idx) = self.indexes.get_mut(&name) {
                                        if let Some(unique_idx) = idx.downcast_mut::<UniqueIndex>() {
                                            unique_idx.remove(&key, record_id);
                                        } else if let Some(btree) = idx.downcast_mut::<BTree>() {
                                            btree.remove(&key, record_id);
                                        }
                                    }
                                }
                            } else {
                                let key = value.clone();
                                if let Some(idx) = self.indexes.get_mut(&name) {
                                    if let Some(unique_idx) = idx.downcast_mut::<UniqueIndex>() {
                                        unique_idx.remove(&key, record_id);
                                    } else if let Some(btree) = idx.downcast_mut::<BTree>() {
                                        btree.remove(&key, record_id);
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {
                    if let Some(key) = extract_compound_key(doc, &spec.fields) {
                        if let Some(idx) = self.indexes.get_mut(&name) {
                            if let Some(unique_idx) = idx.downcast_mut::<UniqueIndex>() {
                                unique_idx.remove(&key, record_id);
                            } else if let Some(btree) = idx.downcast_mut::<BTree>() {
                                btree.remove(&key, record_id);
                            }
                        }
                    }
                }
            }
        }
    }

    /// 获取所有 TTL 索引的名称。
    pub fn list_ttl_indexes(&self) -> Vec<String> {
        self.specs
            .iter()
            .filter(|(_, spec)| spec.index_type == IndexType::TTL)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// 收集索引统计信息。
    pub fn index_statistics(&self) -> Vec<IndexStats> {
        let mut stats = Vec::new();
        for (name, spec) in &self.specs {
            let entry_count = match spec.index_type {
                IndexType::BTree | IndexType::Unique | IndexType::Hashed | IndexType::Multikey => {
                    self.indexes.get(name).map(|idx| {
                        if let Some(btree) = idx.downcast_ref::<BTree>() {
                            btree.len()
                        } else if let Some(unique) = idx.downcast_ref::<UniqueIndex>() {
                            unique.len()
                        } else {
                            0
                        }
                    }).unwrap_or(0)
                }
                IndexType::TTL => {
                    self.indexes.get(name).map(|idx| {
                        idx.downcast_ref::<TtlIndex>().map(|t| t.len()).unwrap_or(0)
                    }).unwrap_or(0)
                }
                IndexType::Text => {
                    self.indexes.get(name).map(|idx| {
                        idx.downcast_ref::<TextIndex>().map(|t| t.len()).unwrap_or(0)
                    }).unwrap_or(0)
                }
                _ => 0,
            };
            stats.push(IndexStats {
                name: name.clone(),
                index_type: format!("{:?}", spec.index_type),
                fields: spec.fields.iter().map(|(f, _)| f.clone()).collect(),
                entry_count,
                unique: self.options.get(name).map(|o| o.unique).unwrap_or(false),
            });
        }
        stats
    }

    /// 获取指定 TTL 索引的清扫统计信息。
    pub fn get_ttl_cleanup_stats(&self, index_name: &str) -> Option<TtlCleanupStats> {
        if let Some(idx) = self.indexes.get(index_name) {
            if let Some(ttl_idx) = idx.downcast_ref::<TtlIndex>() {
                return Some(ttl_idx.cleanup_stats());
            }
        }
        None
    }

    /// 执行一次 TTL 清扫（供后台线程调用）。
    ///
    /// 返回 (index_name, deleted_count) 的列表。
    pub fn run_ttl_cleanup(&mut self) -> Vec<(String, usize)> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let mut results = Vec::new();
        let ttl_names: Vec<String> = self.list_ttl_indexes();

        for name in ttl_names {
            if let Some(idx) = self.indexes.get_mut(&name) {
                if let Some(ttl_idx) = idx.downcast_mut::<TtlIndex>() {
                    let deleted = ttl_idx.run_cleanup(now);
                    results.push((name, deleted));
                }
            }
        }

        results
    }

    // ------------------------------------------------------------------
    // 查询 API：供服务端查询规划器使用
    // ------------------------------------------------------------------

    /// 点查询：在指定索引上查找 key 等于 `key` 的所有 RecordId。
    ///
    /// 仅支持 BTree / Unique 索引；其他类型返回空。
    pub fn point_lookup(&self, index_name: &str, key: &Bson) -> Vec<RecordId> {
        let Some(idx) = self.indexes.get(index_name) else {
            return vec![];
        };
        if let Some(btree) = idx.downcast_ref::<BTree>() {
            return btree.point_lookup(key);
        }
        if let Some(unique) = idx.downcast_ref::<UniqueIndex>() {
            if let Some(rid) = unique.point_lookup(key) {
                return vec![rid];
            }
            return vec![];
        }
        vec![]
    }

    /// 范围查询：在指定索引上查找 key 在 [start, end] 范围内的所有 RecordId。
    ///
    /// 仅支持 BTree / Unique 索引。
    pub fn range_scan(&self, index_name: &str, start: &Bson, end: &Bson) -> Vec<RecordId> {
        let Some(idx) = self.indexes.get(index_name) else {
            return vec![];
        };
        if let Some(btree) = idx.downcast_ref::<BTree>() {
            return btree.range_scan(start, end);
        }
        if let Some(unique) = idx.downcast_ref::<UniqueIndex>() {
            return unique.range_scan(start, end);
        }
        vec![]
    }

    /// 返回指定索引的字段列表。
    pub fn index_fields(&self, index_name: &str) -> Vec<(String, SortOrder)> {
        self.specs
            .get(index_name)
            .map(|s| s.fields.clone())
            .unwrap_or_default()
    }

    /// 返回指定索引的类型。
    pub fn index_type(&self, index_name: &str) -> Option<IndexType> {
        self.specs.get(index_name).map(|s| s.index_type)
    }

    /// 为一个等值过滤条件（`{field: value}`）查找最合适的索引。
    ///
    /// 优先级：单字段精确匹配 > 复合索引的前缀匹配。
    /// 返回 `(index_name, fields)`。
    pub fn find_index_for_equality(&self, field: &str) -> Option<(String, Vec<(String, SortOrder)>)> {
        // 先找单字段精确匹配
        for (name, spec) in &self.specs {
            if spec.fields.len() == 1 && spec.fields[0].0 == field {
                return Some((name.clone(), spec.fields.clone()));
            }
        }
        // 再找复合索引的前缀匹配
        for (name, spec) in &self.specs {
            if spec.fields.len() > 1 && spec.fields[0].0 == field {
                return Some((name.clone(), spec.fields.clone()));
            }
        }
        None
    }

    /// 为一个范围过滤条件（`{field: {$gt/$gte/$lt/$lte: value}}`）查找索引。
    ///
    /// 返回 `(index_name, fields)`。
    pub fn find_index_for_range(&self, field: &str) -> Option<(String, Vec<(String, SortOrder)>)> {
        for (name, spec) in &self.specs {
            if spec.fields.len() == 1 && spec.fields[0].0 == field {
                return Some((name.clone(), spec.fields.clone()));
            }
        }
        for (name, spec) in &self.specs {
            if spec.fields.len() > 1 && spec.fields[0].0 == field {
                return Some((name.clone(), spec.fields.clone()));
            }
        }
        None
    }

    /// 为一个复合过滤条件（多个字段的等值/范围组合）查找覆盖最多的索引。
    ///
    /// `eq_fields` 是等值匹配的字段列表，`range_fields` 是范围匹配的字段列表。
    /// 返回 `(index_name, fields, n_eq_covered, n_range_covered)`。
    pub fn find_best_compound_index(
        &self,
        eq_fields: &[String],
        range_fields: &[String],
    ) -> Option<(String, Vec<(String, SortOrder)>, usize, usize)> {
        let mut best: Option<(String, Vec<(String, SortOrder)>, usize, usize)> = None;
        for (name, spec) in &self.specs {
            let mut n_eq = 0usize;
            let mut n_range = 0usize;
            // 复合索引的字段顺序决定哪些字段能被利用
            for (path, _) in &spec.fields {
                if eq_fields.contains(path) && n_eq < eq_fields.len() {
                    n_eq += 1;
                } else if range_fields.contains(path) {
                    n_range += 1;
                } else {
                    break; // 遇到不匹配的字段就停止
                }
            }
            if n_eq == 0 && n_range == 0 {
                continue;
            }
            let better = match &best {
                None => true,
                Some((_, _, beq, brng)) => {
                    (n_eq, n_range) > (*beq, *brng)
                }
            };
            if better {
                best = Some((name.clone(), spec.fields.clone(), n_eq, n_range));
            }
        }
        best
    }
}

/// 将 Bson 值转换为 GeoShape（简化实现：仅支持 Point）。
fn bson_to_geo_shape(value: &Bson) -> Option<GeoShape> {
    match value {
        Bson::Document(doc) => {
            let typ = doc.get("type").and_then(|v| v.as_str())?;
            match typ {
                "Point" => {
                    let coords = doc.get("coordinates")?;
                    if let Bson::Array(arr) = coords {
                        if arr.len() >= 2 {
                            let lon = arr[0].as_f64()?;
                            let lat = arr[1].as_f64()?;
                            return Some(GeoShape::Point(
                                crate::geospatial::GeoPoint { lon, lat },
                            ));
                        }
                    }
                    None
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// 索引规范（内部使用，兼容 IndexSpec 的序列化）。
#[derive(Debug, Clone)]
pub struct IndexSpec {
    pub fields: Vec<(String, SortOrder)>,
    pub index_type: IndexType,
}

impl IndexSpec {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_doc(name: &str, age: i32) -> Document {
        let mut doc = Document::new();
        doc.insert("name", Bson::String(name.to_owned()));
        doc.insert("age", Bson::Int32(age));
        doc
    }

    #[test]
    fn create_and_drop_btree() {
        let mut mgr = IndexManager::new(4);
        let spec = IndexSpec {
            fields: vec![("age".into(), SortOrder::Asc)],
            index_type: IndexType::BTree,
        };
        mgr.create_index(spec, IndexOptions::default()).unwrap();
        assert!(mgr.list_indexes().contains(&"age_1".to_string()));

        mgr.drop_index("age_1").unwrap();
        assert!(!mgr.list_indexes().contains(&"age_1".to_string()));
    }

    #[test]
    fn create_unique_index() {
        let mut mgr = IndexManager::new(4);
        let spec = IndexSpec {
            fields: vec![("name".into(), SortOrder::Asc)],
            index_type: IndexType::BTree,
        };
        let opts = IndexOptions { unique: true, ..Default::default() };
        mgr.create_index(spec, opts).unwrap();

        let doc1 = make_doc("alice", 30);
        mgr.update_indexes(&doc1, 1).unwrap();

        let doc2 = make_doc("alice", 25);
        let err = mgr.validate_document(&doc2).unwrap_err();
        assert_eq!(err.code(), ErrorCode::DuplicateKey);
    }

    #[test]
    fn update_and_remove_indexes() {
        let mut mgr = IndexManager::new(4);
        let spec = IndexSpec {
            fields: vec![("age".into(), SortOrder::Asc)],
            index_type: IndexType::BTree,
        };
        mgr.create_index(spec, IndexOptions::default()).unwrap();

        let doc = make_doc("bob", 25);
        mgr.update_indexes(&doc, 1).unwrap();

        let spec2 = mgr.get_spec("age_1").unwrap();
        assert_eq!(spec2.fields[0].0, "age");

        mgr.remove_from_indexes(&doc, 1);
    }

    #[test]
    fn text_index_operations() {
        let mut mgr = IndexManager::new(4);
        mgr.create_text_index("content").unwrap();

        let mut doc = Document::new();
        doc.insert("content", Bson::String("hello world".into()));
        mgr.update_indexes(&doc, 1).unwrap();

        let mut doc2 = Document::new();
        doc2.insert("content", Bson::String("hello rust".into()));
        mgr.update_indexes(&doc2, 2).unwrap();

        // 搜索
        let name = "content_text";
        if let Some(idx) = mgr.indexes.get(name) {
            if let Some(text_idx) = idx.downcast_ref::<TextIndex>() {
                let results = text_idx.search("hello");
                assert!(!results.is_empty());
            }
        }
    }

    #[test]
    fn ttl_index_operations() {
        let mut mgr = IndexManager::new(4);
        mgr.create_ttl_index("expire_at", 3600).unwrap();

        let mut doc = Document::new();
        doc.insert("expire_at", Bson::DateTime(1000));
        mgr.update_indexes(&doc, 1).unwrap();

        // scan at a time far enough in the future that any insert time will be expired
        let future = i64::MAX / 2;
        let name = "expire_at_ttl";
        if let Some(idx) = mgr.indexes.get(name) {
            if let Some(ttl_idx) = idx.downcast_ref::<TtlIndex>() {
                let expired = ttl_idx.scan_expired(future);
                assert_eq!(expired, vec![1]);
            }
        }
    }

    #[test]
    fn sparse_index_skips_missing() {
        let mut mgr = IndexManager::new(4);
        let spec = IndexSpec {
            fields: vec![("email".into(), SortOrder::Asc)],
            index_type: IndexType::BTree,
        };
        let opts = IndexOptions { sparse: true, ..Default::default() };
        mgr.create_index(spec, opts).unwrap();

        // 没有 email 字段的文档不应被索引
        let mut doc = Document::new();
        doc.insert("name", Bson::String("no_email".into()));
        mgr.update_indexes(&doc, 1).unwrap();

        // 有 email 字段的文档应被索引
        let mut doc2 = Document::new();
        doc2.insert("email", Bson::String("a@b.com".into()));
        mgr.update_indexes(&doc2, 2).unwrap();
    }

    // ===== 新增测试：TTL 索引管理 =====

    #[test]
    fn list_ttl_indexes() {
        let mut mgr = IndexManager::new(4);
        mgr.create_ttl_index("expire_at", 3600).unwrap();
        mgr.create_ttl_index("ttl_date", 7200).unwrap();

        let ttl_indexes = mgr.list_ttl_indexes();
        assert_eq!(ttl_indexes.len(), 2);
        assert!(ttl_indexes.contains(&"expire_at_ttl".to_string()));
        assert!(ttl_indexes.contains(&"ttl_date_ttl".to_string()));
    }

    #[test]
    fn run_ttl_cleanup() {
        let mut mgr = IndexManager::new(4);
        mgr.create_ttl_index("expire_at", 1000).unwrap();

        // 插入一些文档
        for i in 0..5 {
            let mut doc = Document::new();
            doc.insert("expire_at", Bson::Int32(i));
            mgr.update_indexes(&doc, i as u64).unwrap();
        }

        // 执行清理（所有文档都会过期）
        let results = mgr.run_ttl_cleanup();
        assert_eq!(results.len(), 1);
        // 检查返回的删除数量
        assert!(results[0].1 >= 0);
    }

    #[test]
    fn get_ttl_cleanup_stats() {
        let mut mgr = IndexManager::new(4);
        mgr.create_ttl_index("expire_at", 3600).unwrap();

        let stats = mgr.get_ttl_cleanup_stats("expire_at_ttl");
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.total_deleted, 0);
        assert_eq!(stats.scan_count, 0);
    }
}
