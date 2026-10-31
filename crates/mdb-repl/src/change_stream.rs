//! Change Stream 实现。
//!
//! 基于 oplog 的变更流，支持 resume token、聚合管道过滤、fullDocument/fullDocumentBeforeChange
//! 选项、数据库/集群级流、事件排序保证与内存管理。

use std::collections::BTreeMap;

use crate::oplog::Oplog;
use crate::types::{ChangeStreamEvent, OperationType, OplogEntry, OpType, ResumeToken};
use mdb_doc::{Bson, Document, Timestamp};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Change Stream 错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeStreamError {
    /// fullDocument=required 时预映像不可用。
    FullDocumentRequiredUnavailable,
    /// 聚合管道阶段不支持。
    UnsupportedStage(String),
    /// startAtOperationTime 早于 oplog 可用范围。
    StartTimeTooOld,
    /// 聚合管道处理失败。
    PipelineError(String),
}

impl std::fmt::Display for ChangeStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FullDocumentRequiredUnavailable => {
                write!(f, "fullDocument=required but pre-image not available")
            }
            Self::UnsupportedStage(s) => write!(f, "unsupported pipeline stage: {s}"),
            Self::StartTimeTooOld => {
                write!(f, "startAtOperationTime is before earliest oplog entry")
            }
            Self::PipelineError(msg) => write!(f, "pipeline error: {msg}"),
        }
    }
}

impl std::error::Error for ChangeStreamError {}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// fullDocument 选项：控制 update 事件是否返回完整文档。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FullDocumentMode {
    /// 默认：update 事件不返回 fullDocument。
    #[default]
    Default,
    /// 从集合中查找完整文档（仅 update）。
    UpdateLookup,
    /// 当预映像可用时返回，否则跳过。
    WhenAvailable,
    /// 预映像必须可用，否则报错。
    Required,
}

impl FullDocumentMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::UpdateLookup => "updateLookup",
            Self::WhenAvailable => "whenAvailable",
            Self::Required => "required",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "default" => Some(Self::Default),
            "updateLookup" => Some(Self::UpdateLookup),
            "whenAvailable" => Some(Self::WhenAvailable),
            "required" => Some(Self::Required),
            _ => None,
        }
    }
}

/// fullDocumentBeforeChange 选项（MongoDB 6.0+）：控制 pre-image 返回。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FullDocumentBeforeChangeMode {
    /// 默认：不返回 pre-image。
    #[default]
    Off,
    /// 当预映像可用时返回，否则跳过。
    WhenAvailable,
    /// 预映像必须可用，否则报错。
    Required,
}

impl FullDocumentBeforeChangeMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::WhenAvailable => "whenAvailable",
            Self::Required => "required",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "off" => Some(Self::Off),
            "whenAvailable" => Some(Self::WhenAvailable),
            "required" => Some(Self::Required),
            _ => None,
        }
    }
}

/// Resume 起始位置。
#[derive(Debug, Clone)]
pub enum ResumeStartPoint {
    /// 从上一个 resume token 之后继续。
    ResumeAfter(ResumeToken),
    /// 从某个 token 之后开始（与 resumeAfter 类似但用于 invalidate 场景）。
    StartAfter(ResumeToken),
    /// 从指定操作时间戳开始。
    StartAtOperationTime(Timestamp),
}

/// 聚合管道阶段（Change Stream 支持的子集）。
#[derive(Debug, Clone)]
pub enum PipelineStage {
    /// `{"$match": {<filter>}}` — 过滤事件字段。
    Match(Document),
    /// `{"$project": {<spec>}}` — 投影字段。
    Project(Document),
    /// `{"$addFields": {<spec>}}` — 添加/覆盖字段。
    AddFields(Document),
    /// `{"$unset": [<field>...]}` — 移除字段。
    Unset(Vec<String>),
    /// `{"$replaceRoot": {"newRoot": <doc>}}` — 替换文档根。
    ReplaceRoot(Document),
    /// `{"$redact": {<spec>}}` — 基于字段值保留/丢弃事件。
    Redact(Document),
}

/// 流级别：单集合 / 数据库 / 集群。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamLevel {
    /// db.collection — 单集合。
    Collection,
    /// db — 数据库级，匹配 `db.*` 命名空间。
    Database,
    /// admin / cluster — 全集群。
    Cluster,
}

/// Change Stream 配置。
#[derive(Debug, Clone)]
pub struct ChangeStreamConfig {
    /// 目标命名空间（`db.collection`），空字符串表示全库。
    pub target_ns: String,
    /// 流级别。
    pub level: StreamLevel,
    /// 最大等待事件数。
    pub max_batch_size: usize,
    /// 等待新事件的最大超时（毫秒）。
    pub max_await_time_ms: u64,
    /// fullDocument 选项。
    pub full_document: FullDocumentMode,
    /// fullDocumentBeforeChange 选项。
    pub full_document_before_change: FullDocumentBeforeChangeMode,
    /// 聚合管道阶段。
    pub pipeline: Vec<PipelineStage>,
    /// Resume 起始位置。
    pub resume_start: Option<ResumeStartPoint>,
    /// seen_timestamps 滑动窗口最大容量。
    pub seen_window_capacity: usize,
}

impl Default for ChangeStreamConfig {
    fn default() -> Self {
        Self {
            target_ns: String::new(),
            level: StreamLevel::Collection,
            max_batch_size: 100,
            max_await_time_ms: 60_000,
            full_document: FullDocumentMode::Default,
            full_document_before_change: FullDocumentBeforeChangeMode::Off,
            pipeline: Vec::new(),
            resume_start: None,
            seen_window_capacity: 4096,
        }
    }
}

// ---------------------------------------------------------------------------
// Seen-timestamp sliding window
// ---------------------------------------------------------------------------

/// 有界滑动窗口：保留最近 `capacity` 个时间戳，按插入顺序淘汰。
#[derive(Debug)]
struct SeenWindow {
    capacity: usize,
    /// BTreeMap 保持按时间戳排序，便于区间淘汰。
    inner: BTreeMap<u64, ()>,
}

impl SeenWindow {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: BTreeMap::new(),
        }
    }

    fn contains(&self, ts: &u64) -> bool {
        self.inner.contains_key(ts)
    }

    fn insert(&mut self, ts: u64) {
        if self.inner.insert(ts, ()).is_some() {
            return;
        }
        // 驱逐超出容量的最旧条目
        while self.inner.len() > self.capacity {
            let oldest = *self.inner.keys().next().unwrap();
            self.inner.remove(&oldest);
        }
    }

    /// 驱逐早于 `cutoff_ts` 的所有条目（5 分钟窗口）。
    fn evict_before(&mut self, cutoff_ts: u64) {
        let keys_to_remove: Vec<u64> = self.inner.keys().take_while(|&&k| k < cutoff_ts).copied().collect();
        for k in keys_to_remove {
            self.inner.remove(&k);
        }
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

// ---------------------------------------------------------------------------
// Change Stream
// ---------------------------------------------------------------------------

/// Change Stream：基于 oplog 的变更流。
#[derive(Debug)]
pub struct ChangeStream {
    /// 恢复令牌：从该位置继续消费。
    resume_token: Option<ResumeToken>,
    /// 目标命名空间。
    target_ns: String,
    /// 流级别。
    level: StreamLevel,
    /// 最大批大小。
    max_batch_size: usize,
    /// 等待新事件的最大超时（毫秒）。
    max_await_time_ms: u64,
    /// fullDocument 选项。
    full_document: FullDocumentMode,
    /// fullDocumentBeforeChange 选项。
    full_document_before_change: FullDocumentBeforeChangeMode,
    /// 聚合管道阶段。
    pipeline: Vec<PipelineStage>,
    /// 已处理的最后时间戳。
    last_cluster_time: Option<Timestamp>,
    /// 已看到的 oplog 条目时间戳集合（有界滑动窗口，去重用）。
    seen_timestamps: SeenWindow,
    /// startAtOperationTime（可选）。
    start_at_operation_time: Option<Timestamp>,
}

impl ChangeStream {
    /// 创建新的 Change Stream（集合级）。
    pub fn new(target_ns: impl Into<String>) -> Self {
        Self {
            resume_token: None,
            target_ns: target_ns.into(),
            level: StreamLevel::Collection,
            max_batch_size: 100,
            max_await_time_ms: 60_000,
            full_document: FullDocumentMode::default(),
            full_document_before_change: FullDocumentBeforeChangeMode::default(),
            pipeline: Vec::new(),
            last_cluster_time: None,
            seen_timestamps: SeenWindow::new(4096),
            start_at_operation_time: None,
        }
    }

    /// 带配置创建。
    pub fn with_config(config: ChangeStreamConfig) -> Self {
        let mut cs = Self {
            resume_token: None,
            target_ns: config.target_ns,
            level: config.level,
            max_batch_size: config.max_batch_size,
            max_await_time_ms: config.max_await_time_ms,
            full_document: config.full_document,
            full_document_before_change: config.full_document_before_change,
            pipeline: config.pipeline,
            last_cluster_time: None,
            seen_timestamps: SeenWindow::new(config.seen_window_capacity),
            start_at_operation_time: None,
        };

        if let Some(start) = config.resume_start {
            match start {
                ResumeStartPoint::ResumeAfter(token) => {
                    cs.resume_token = Some(token);
                    cs.resolve_resume_timestamp();
                }
                ResumeStartPoint::StartAfter(token) => {
                    cs.resume_token = Some(token);
                    cs.resolve_resume_timestamp();
                }
                ResumeStartPoint::StartAtOperationTime(ts) => {
                    cs.start_at_operation_time = Some(ts);
                    cs.last_cluster_time = Some(ts);
                }
            }
        }

        cs
    }

    // -- Builder methods ---------------------------------------------------

    /// 创建数据库级 Change Stream（匹配该数据库所有集合）。
    pub fn database(db_name: impl Into<String>) -> Self {
        let db = db_name.into();
        Self {
            target_ns: format!("{db}."),
            level: StreamLevel::Database,
            ..Self::new(db)
        }
    }

    /// 创建集群级 Change Stream（匹配所有命名空间）。
    pub fn cluster() -> Self {
        Self {
            target_ns: String::new(),
            level: StreamLevel::Cluster,
            ..Self::new(String::new())
        }
    }

    /// 添加聚合管道阶段。
    pub fn pipeline_stage(mut self, stage: PipelineStage) -> Self {
        self.pipeline.push(stage);
        self
    }

    /// 设置 fullDocument 模式。
    pub fn full_document(mut self, mode: FullDocumentMode) -> Self {
        self.full_document = mode;
        self
    }

    /// 设置 fullDocumentBeforeChange 模式。
    pub fn full_document_before_change(mut self, mode: FullDocumentBeforeChangeMode) -> Self {
        self.full_document_before_change = mode;
        self
    }

    /// 设置 maxAwaitTimeMS。
    pub fn max_await_time_ms(mut self, ms: u64) -> Self {
        self.max_await_time_ms = ms;
        self
    }

    /// 设置最大批大小。
    pub fn max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    /// 设置 startAtOperationTime。
    pub fn start_at_operation_time(mut self, ts: Timestamp) -> Self {
        self.start_at_operation_time = Some(ts);
        self.last_cluster_time = Some(ts);
        self
    }

    /// 设置 resumeAfter。
    pub fn resume_after(mut self, token: ResumeToken) -> Self {
        self.resume_token = Some(token);
        self.resolve_resume_timestamp();
        self
    }

    /// 设置 startAfter。
    pub fn start_after(mut self, token: ResumeToken) -> Self {
        self.resume_token = Some(token);
        self.resolve_resume_timestamp();
        self
    }

    // -- Resume token ------------------------------------------------------

    /// 获取当前恢复令牌。
    pub fn get_resume_token(&self) -> Option<&ResumeToken> {
        self.resume_token.as_ref()
    }

    /// 从 resume token 中提取时间戳并设置 last_cluster_time。
    fn resolve_resume_timestamp(&mut self) {
        if let Some(ref token) = self.resume_token {
            if let Some(Bson::String(data)) = token.data().get("_data") {
                if let Some((secs_str, inc_str)) = data.split_once(':') {
                    if let (Ok(secs), Ok(inc)) =
                        (secs_str.parse::<u32>(), inc_str.parse::<u32>())
                    {
                        self.last_cluster_time = Some(Timestamp::new(secs, inc));
                    }
                }
            }
        }
    }

    /// 创建一个带 { _data, _tokenType } 结构的 resume token。
    #[cfg(test)]
    fn make_token(data_str: &str, token_type: &str) -> ResumeToken {
        let mut doc = Document::new();
        doc.insert("_data", Bson::String(data_str.to_owned()));
        doc.insert("_tokenType", Bson::String(token_type.to_owned()));
        ResumeToken::new(doc)
    }

    // -- Namespace matching ------------------------------------------------

    /// 检查条目的命名空间是否匹配目标。
    fn matches_namespace(&self, ns: &str) -> bool {
        match self.level {
            StreamLevel::Cluster => true,
            StreamLevel::Database => {
                // target_ns 格式为 "dbname."，匹配所有以 "dbname." 开头的命名空间
                ns.starts_with(&self.target_ns)
            }
            StreamLevel::Collection => ns == self.target_ns,
        }
    }

    // -- Event processing --------------------------------------------------

    /// 轮询 oplog 获取下一个匹配事件。
    pub fn next_event(&mut self, oplog: &Oplog) -> Option<ChangeStreamEvent> {
        let start_ts = self
            .last_cluster_time
            .map(|t| t.next())
            .unwrap_or_else(|| {
                self.start_at_operation_time
                    .unwrap_or_else(|| Timestamp::new(0, 0))
            });

        let entries = oplog.scan(start_ts);

        for entry in entries {
            if !self.matches_namespace(&entry.ns) {
                continue;
            }

            let ts_key = entry.ts.as_u64();
            if self.seen_timestamps.contains(&ts_key) {
                continue;
            }

            let event = match self.process_entry(&entry) {
                Ok(event) => event,
                Err(ChangeStreamError::FullDocumentRequiredUnavailable) => {
                    continue;
                }
                Err(_) => continue,
            };

            self.seen_timestamps.insert(ts_key);
            self.last_cluster_time = Some(entry.ts);
            self.resume_token = Some(ResumeToken::from_event(&event));

            return Some(event);
        }

        None
    }

    /// 批量获取事件。
    pub fn next_batch(&mut self, oplog: &Oplog) -> Vec<ChangeStreamEvent> {
        let mut batch = Vec::with_capacity(self.max_batch_size);
        while batch.len() < self.max_batch_size {
            match self.next_event(oplog) {
                Some(event) => batch.push(event),
                None => break,
            }
        }
        batch
    }

    /// 驱逐 seen_timestamps 中早于 cutoff（当前时间 - 5 分钟）的条目。
    pub fn evict_stale_timestamps(&mut self, now_secs: u32) {
        let cutoff = now_secs.saturating_sub(300);
        // 用 cutoff 的 inc=0 作为比较键
        self.seen_timestamps.evict_before(Timestamp::new(cutoff, 0).as_u64());
    }

    // -- Pipeline processing -----------------------------------------------

    /// 将 OplogEntry 转换为 ChangeStreamEvent 并执行聚合管道。
    fn process_entry(
        &self,
        entry: &OplogEntry,
    ) -> Result<ChangeStreamEvent, ChangeStreamError> {
        let mut event = self.entry_to_event(entry);

        self.apply_full_document_options(&mut event, entry)?;
        self.apply_full_document_before_change_options(&mut event)?;

        // 依次执行管道阶段
        for stage in &self.pipeline {
            event = self.apply_pipeline_stage(event, stage)?;
        }

        Ok(event)
    }

    /// 将 OplogEntry 转换为 ChangeStreamEvent（基础转换）。
    fn entry_to_event(&self, entry: &OplogEntry) -> ChangeStreamEvent {
        let operation_type = match entry.op {
            OpType::Insert => OperationType::Insert,
            OpType::Update => OperationType::Update,
            OpType::Delete => OperationType::Delete,
            OpType::Command => OperationType::Drop,
        };

        // fullDocument:
        // - Insert: always the inserted document
        // - Update/Replace: None by default (requires fullDocument=updateLookup)
        // - Delete: None
        let full_document = if matches!(entry.op, OpType::Insert) {
            Some(entry.o.clone())
        } else {
            None
        };

        let document_key = entry.o2.clone().unwrap_or_else(|| {
            let mut dk = Document::new();
            if let Some(id) = entry.o.get("_id") {
                dk.insert("_id", id.clone());
            }
            dk
        });

        let data_str = format!("{}:{}", entry.ts.secs, entry.ts.inc);

        // full_document_before_change:
        // - Insert: None (null)
        // - Delete: entry.o is the deletion criteria; use it as pre-image proxy
        // - Update: None (would require collection lookup in production)
        let full_document_before_change = match entry.op {
            OpType::Delete => Some(entry.o.clone()),
            _ => None,
        };

        ChangeStreamEvent {
            _id: {
                let mut id_doc = Document::new();
                id_doc.insert("_data", Bson::String(data_str));
                id_doc.insert("_tokenType", Bson::String("resume_token".into()));
                id_doc
            },
            operation_type,
            ns: entry.ns.clone(),
            full_document,
            full_document_before_change,
            document_key,
            cluster_time: entry.ts,
            txn_number: None,
            update_description: None,
        }
    }

    // -- fullDocument / fullDocumentBeforeChange ---------------------------

    /// 根据 fullDocument 选项填充 fullDocument 字段。
    fn apply_full_document_options(
        &self,
        event: &mut ChangeStreamEvent,
        _entry: &OplogEntry,
    ) -> Result<(), ChangeStreamError> {
        match self.full_document {
            FullDocumentMode::Default => {
                // Insert/Replace 默认有 fullDocument，Update 默认没有
            }
            FullDocumentMode::UpdateLookup => {
                // Update 事件：如果 fullDocument 为 None，尝试查找（此处简化为从 oplog 获取）
                if event.operation_type == OperationType::Update && event.full_document.is_none() {
                    // 在实际实现中应从集合中查询完整文档
                    // 此处保留 None 表示查找未实现
                }
            }
            FullDocumentMode::WhenAvailable => {
                // 当预映像不可用时不报错，保持现状
            }
            FullDocumentMode::Required => {
                if matches!(
                    event.operation_type,
                    OperationType::Update | OperationType::Replace
                ) && event.full_document.is_none()
                {
                    return Err(ChangeStreamError::FullDocumentRequiredUnavailable);
                }
            }
        }
        Ok(())
    }

    /// 根据 fullDocumentBeforeChange 选项填充 pre-image。
    fn apply_full_document_before_change_options(
        &self,
        event: &mut ChangeStreamEvent,
    ) -> Result<(), ChangeStreamError> {
        match self.full_document_before_change {
            FullDocumentBeforeChangeMode::Off => {}
            FullDocumentBeforeChangeMode::WhenAvailable => {
                // 当 pre-image 不可用时保持 None（不报错）
            }
            FullDocumentBeforeChangeMode::Required => {
                // Insert 事件的 pre-image 为 null 是正常的
                if event.operation_type == OperationType::Insert {
                    event.full_document_before_change = Some(Document::new());
                } else if event.full_document_before_change.is_none() {
                    return Err(ChangeStreamError::FullDocumentRequiredUnavailable);
                }
            }
        }
        Ok(())
    }

    // -- Pipeline stages ---------------------------------------------------

    /// 对单个事件执行聚合管道阶段。
    fn apply_pipeline_stage(
        &self,
        event: ChangeStreamEvent,
        stage: &PipelineStage,
    ) -> Result<ChangeStreamEvent, ChangeStreamError> {
        match stage {
            PipelineStage::Match(filter) => {
                if self.document_matches_filter(&event_to_doc(&event), filter) {
                    Ok(event)
                } else {
                    Err(ChangeStreamError::PipelineError(
                        "event filtered out by $match".into(),
                    ))
                }
            }
            PipelineStage::Project(spec) => {
                let doc = event_to_doc(&event);
                let projected = self.apply_projection(&doc, spec);
                Ok(doc_to_event(projected, &event))
            }
            PipelineStage::AddFields(spec) => {
                let doc = event_to_doc(&event);
                let mut result = doc;
                for (key, value) in spec.iter() {
                    result.set(key.to_owned(), value.clone());
                }
                Ok(doc_to_event(result, &event))
            }
            PipelineStage::Unset(fields) => {
                let doc = event_to_doc(&event);
                let mut result = doc;
                for field in fields {
                    result.remove(field);
                }
                Ok(doc_to_event(result, &event))
            }
            PipelineStage::ReplaceRoot(new_root) => {
                if let Some(Bson::Document(root_doc)) = new_root.get("newRoot") {
                    Ok(doc_to_event(root_doc.clone(), &event))
                } else {
                    Err(ChangeStreamError::PipelineError(
                        "$replaceRoot requires 'newRoot' field".into(),
                    ))
                }
            }
            PipelineStage::Redact(spec) => {
                let doc = event_to_doc(&event);
                if self.evaluate_redact(&doc, spec) {
                    Ok(event)
                } else {
                    Err(ChangeStreamError::PipelineError(
                        "event redacted by $redact".into(),
                    ))
                }
            }
        }
    }

    /// 简化的文档匹配：检查 filter 中的每个字段是否与文档匹配。
    fn document_matches_filter(&self, doc: &Document, filter: &Document) -> bool {
        for (key, value) in filter.iter() {
            match doc.get(key) {
                Some(doc_val) => {
                    if !bson_values_equal(doc_val, value) {
                        return false;
                    }
                }
                None => return false,
            }
        }
        true
    }

    /// 简化的投影：spec 中值为 1 的字段保留，为 0 的移除。
    /// 如果混合了 include/exclude，仅 include 生效。
    fn apply_projection(&self, doc: &Document, spec: &Document) -> Document {
        let has_include = spec.iter().any(|(_, v)| matches!(v, Bson::Int32(1)));
        let mut result = Document::new();

        if has_include {
            for (key, value) in doc.iter() {
                if spec.get(key).is_some() {
                    result.insert(key.to_owned(), value.clone());
                }
            }
        } else {
            for (key, value) in doc.iter() {
                if !spec.contains_key(key) {
                    result.insert(key.to_owned(), value.clone());
                }
            }
        }
        result
    }

    /// 简化的 $redact 求值：检查 $$DESCEND / $$PRUNE / $$KEEP。
    fn evaluate_redact(&self, _doc: &Document, _spec: &Document) -> bool {
        // 简化实现：默认保留事件
        true
    }
}

impl Default for ChangeStream {
    fn default() -> Self {
        Self::new(String::new())
    }
}

// ---------------------------------------------------------------------------
// Helpers for pipeline processing
// ---------------------------------------------------------------------------

/// 将 ChangeStreamEvent 转为 Document（供管道阶段使用）。
fn event_to_doc(event: &ChangeStreamEvent) -> Document {
    let mut doc = Document::new();
    // _id
    doc.insert("_id", Bson::Document(event._id.clone()));
    // operationType
    doc.insert(
        "operationType",
        Bson::String(event.operation_type.as_str().to_owned()),
    );
    // ns
    doc.insert("ns", Bson::String(event.ns.clone()));
    // fullDocument
    if let Some(ref fd) = event.full_document {
        doc.insert("fullDocument", Bson::Document(fd.clone()));
    }
    // fullDocumentBeforeChange
    if let Some(ref fdbc) = event.full_document_before_change {
        doc.insert("fullDocumentBeforeChange", Bson::Document(fdbc.clone()));
    }
    // documentKey
    doc.insert("documentKey", Bson::Document(event.document_key.clone()));
    // clusterTime
    doc.insert("clusterTime", Bson::Timestamp(event.cluster_time));
    // txnNumber
    if let Some(txn) = event.txn_number {
        doc.insert("txnNumber", Bson::Int64(txn));
    }
    // updateDescription
    if let Some(ref ud) = event.update_description {
        doc.insert("updateDescription", Bson::Document(ud.clone()));
    }
    doc
}

/// 将 Document 转回 ChangeStreamEvent（保留原始事件元数据）。
fn doc_to_event(doc: Document, original: &ChangeStreamEvent) -> ChangeStreamEvent {
    ChangeStreamEvent {
        _id: original._id.clone(),
        operation_type: original.operation_type,
        ns: original.ns.clone(),
        full_document: doc
            .get("fullDocument")
            .and_then(|v| v.as_document().cloned()),
        full_document_before_change: doc
            .get("fullDocumentBeforeChange")
            .and_then(|v| v.as_document().cloned()),
        document_key: doc
            .get("documentKey")
            .and_then(|v| v.as_document().cloned())
            .unwrap_or_else(|| original.document_key.clone()),
        cluster_time: original.cluster_time,
        txn_number: original.txn_number,
        update_description: doc
            .get("updateDescription")
            .and_then(|v| v.as_document().cloned()),
    }
}

/// 深度比较两个 Bson 值。
fn bson_values_equal(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Int32(a), Bson::Int32(b)) => a == b,
        (Bson::Int64(a), Bson::Int64(b)) => a == b,
        (Bson::Double(a), Bson::Double(b)) => a == b,
        (Bson::String(a), Bson::String(b)) => a == b,
        (Bson::Boolean(a), Bson::Boolean(b)) => a == b,
        (Bson::Null, Bson::Null) => true,
        (Bson::Document(a), Bson::Document(b)) => a == b,
        (Bson::ObjectId(a), Bson::ObjectId(b)) => a == b,
        (Bson::Timestamp(a), Bson::Timestamp(b)) => a == b,
        (Bson::DateTime(a), Bson::DateTime(b)) => a == b,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oplog::{Oplog, OplogConfig};
    use crate::types::{OpType, OplogEntry};
    use mdb_doc::{Document, ObjectId, Timestamp};

    fn make_entry(ts_secs: u32, ts_inc: u32, op: OpType, ns: &str) -> OplogEntry {
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(ts_inc as i32));
        doc.insert("name", Bson::String("test".into()));
        OplogEntry {
            ts: Timestamp::new(ts_secs, ts_inc),
            op,
            ns: ns.to_owned(),
            o: doc,
            o2: None,
            ui: ObjectId::from_bytes([0; 12]),
        }
    }

    fn make_update_entry(ts_secs: u32, ts_inc: u32, ns: &str) -> OplogEntry {
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(ts_inc as i32));
        doc.insert("$set", Bson::Document({
            let mut set = Document::new();
            set.insert("name", Bson::String("updated".into()));
            set
        }));
        OplogEntry {
            ts: Timestamp::new(ts_secs, ts_inc),
            op: OpType::Update,
            ns: ns.to_owned(),
            o: doc,
            o2: Some({
                let mut o2 = Document::new();
                o2.insert("_id", Bson::Int32(ts_inc as i32));
                o2
            }),
            ui: ObjectId::from_bytes([0; 12]),
        }
    }

    fn make_delete_entry(ts_secs: u32, ts_inc: u32, ns: &str) -> OplogEntry {
        OplogEntry {
            ts: Timestamp::new(ts_secs, ts_inc),
            op: OpType::Delete,
            ns: ns.to_owned(),
            o: Document::new(),
            o2: Some({
                let mut o2 = Document::new();
                o2.insert("_id", Bson::Int32(ts_inc as i32));
                o2
            }),
            ui: ObjectId::from_bytes([0; 12]),
        }
    }

    // -- Basic tests -------------------------------------------------------

    #[test]
    fn new_change_stream() {
        let cs = ChangeStream::new("test.users");
        assert_eq!(cs.target_ns, "test.users");
        assert!(cs.get_resume_token().is_none());
    }

    #[test]
    fn next_event_filters_by_namespace() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));
        oplog.append(make_entry(100, 2, OpType::Insert, "test.orders"));
        oplog.append(make_entry(100, 3, OpType::Insert, "test.users"));

        let mut cs = ChangeStream::new("test.users");
        let event1 = cs.next_event(&oplog).unwrap();
        assert_eq!(event1.ns, "test.users");

        let event2 = cs.next_event(&oplog).unwrap();
        assert_eq!(event2.ns, "test.users");
        assert_eq!(event2.document_key.get("_id").unwrap(), &Bson::Int32(3));
    }

    #[test]
    fn next_event_no_match() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.orders"));

        let mut cs = ChangeStream::new("test.users");
        let event = cs.next_event(&oplog);
        assert!(event.is_none());
    }

    #[test]
    fn next_batch() {
        let mut oplog = Oplog::new(OplogConfig::default());
        for i in 1..=5 {
            oplog.append(make_entry(100, i, OpType::Insert, "test.users"));
        }

        let mut cs = ChangeStream::new("test.users").max_batch_size(3);
        let batch = cs.next_batch(&oplog);
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn resume_token_persists() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));

        let mut cs = ChangeStream::new("test.users");
        assert!(cs.get_resume_token().is_none());

        cs.next_event(&oplog);
        assert!(cs.get_resume_token().is_some());
    }

    #[test]
    fn resume_from_token() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));
        oplog.append(make_entry(100, 2, OpType::Insert, "test.users"));
        oplog.append(make_entry(100, 3, OpType::Insert, "test.users"));

        let mut cs1 = ChangeStream::new("test.users");
        let _event1 = cs1.next_event(&oplog).unwrap();
        let token = cs1.get_resume_token().unwrap().clone();

        let mut cs2 = ChangeStream::new("test.users").resume_after(token);
        let event2 = cs2.next_event(&oplog).unwrap();
        assert_eq!(event2.cluster_time, Timestamp::new(100, 2));
    }

    #[test]
    fn resume_after_and_start_after_both_work() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));
        oplog.append(make_entry(100, 2, OpType::Insert, "test.users"));

        let mut cs1 = ChangeStream::new("test.users");
        let _event = cs1.next_event(&oplog).unwrap();
        let token = cs1.get_resume_token().unwrap().clone();

        let mut cs2 = ChangeStream::new("test.users").start_after(token);
        let event2 = cs2.next_event(&oplog).unwrap();
        assert_eq!(event2.cluster_time, Timestamp::new(100, 2));
    }

    #[test]
    fn start_at_operation_time() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));
        oplog.append(make_entry(200, 1, OpType::Insert, "test.users"));

        let mut cs = ChangeStream::new("test.users")
            .start_at_operation_time(Timestamp::new(150, 0));
        let event = cs.next_event(&oplog).unwrap();
        assert_eq!(event.cluster_time, Timestamp::new(200, 1));
    }

    #[test]
    fn resume_token_has_token_type() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));

        let mut cs = ChangeStream::new("test.users");
        let _event = cs.next_event(&oplog).unwrap();
        let token = cs.get_resume_token().unwrap();
        assert_eq!(
            token.data().get("_tokenType").and_then(Bson::as_str),
            Some("resume_token")
        );
    }

    // -- Database / Cluster level ------------------------------------------

    #[test]
    fn database_level_stream() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "mydb.users"));
        oplog.append(make_entry(100, 2, OpType::Insert, "mydb.orders"));
        oplog.append(make_entry(100, 3, OpType::Insert, "otherdb.things"));

        let mut cs = ChangeStream::database("mydb");
        let event1 = cs.next_event(&oplog).unwrap();
        assert_eq!(event1.ns, "mydb.users");

        let event2 = cs.next_event(&oplog).unwrap();
        assert_eq!(event2.ns, "mydb.orders");

        // otherdb should not appear
        assert!(cs.next_event(&oplog).is_none());
    }

    #[test]
    fn cluster_level_stream() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "db1.a"));
        oplog.append(make_entry(100, 2, OpType::Insert, "db2.b"));
        oplog.append(make_entry(100, 3, OpType::Insert, "db3.c"));

        let mut cs = ChangeStream::cluster();
        assert_eq!(cs.next_event(&oplog).unwrap().ns, "db1.a");
        assert_eq!(cs.next_event(&oplog).unwrap().ns, "db2.b");
        assert_eq!(cs.next_event(&oplog).unwrap().ns, "db3.c");
    }

    #[test]
    fn database_level_with_prefix() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.coll1"));
        oplog.append(make_entry(100, 2, OpType::Insert, "test.coll2"));
        // "test." prefix should not match "test_extra.coll"
        oplog.append(make_entry(100, 3, OpType::Insert, "test_extra.coll"));

        let mut cs = ChangeStream::database("test");
        assert_eq!(cs.next_event(&oplog).unwrap().ns, "test.coll1");
        assert_eq!(cs.next_event(&oplog).unwrap().ns, "test.coll2");
        assert!(cs.next_event(&oplog).is_none());
    }

    // -- fullDocument option ------------------------------------------------

    #[test]
    fn full_document_on_insert() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));

        let mut cs = ChangeStream::new("test.users");
        let event = cs.next_event(&oplog).unwrap();
        assert!(event.full_document.is_some());
    }

    #[test]
    fn no_full_document_on_delete() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Delete, "test.users"));

        let mut cs = ChangeStream::new("test.users");
        let event = cs.next_event(&oplog).unwrap();
        assert!(event.full_document.is_none());
    }

    #[test]
    fn full_document_mode_update_lookup() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));

        let cs = ChangeStream::new("test.users").full_document(FullDocumentMode::UpdateLookup);
        assert_eq!(cs.full_document, FullDocumentMode::UpdateLookup);
    }

    #[test]
    fn full_document_mode_from_str() {
        assert_eq!(FullDocumentMode::from_str("updateLookup"), Some(FullDocumentMode::UpdateLookup));
        assert_eq!(FullDocumentMode::from_str("whenAvailable"), Some(FullDocumentMode::WhenAvailable));
        assert_eq!(FullDocumentMode::from_str("required"), Some(FullDocumentMode::Required));
        assert_eq!(FullDocumentMode::from_str("default"), Some(FullDocumentMode::Default));
        assert!(FullDocumentMode::from_str("invalid").is_none());
    }

    // -- fullDocumentBeforeChange ------------------------------------------

    #[test]
    fn full_document_before_change_insert_is_null() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));

        let mut cs = ChangeStream::new("test.users")
            .full_document_before_change(FullDocumentBeforeChangeMode::WhenAvailable);
        let event = cs.next_event(&oplog).unwrap();
        // Insert events have no pre-image (should be null/empty)
        assert!(event.full_document_before_change.is_none());
    }

    #[test]
    fn full_document_before_change_delete_has_doc() {
        let mut oplog = Oplog::new(OplogConfig::default());
        let mut entry = make_delete_entry(100, 1, "test.users");
        entry.o = {
            let mut doc = Document::new();
            doc.insert("_id", Bson::Int32(1));
            doc.insert("name", Bson::String("before_delete".into()));
            doc
        };

        oplog.append(entry);

        let mut cs = ChangeStream::new("test.users")
            .full_document_before_change(FullDocumentBeforeChangeMode::WhenAvailable);
        let event = cs.next_event(&oplog).unwrap();
        assert!(event.full_document_before_change.is_some());
    }

    #[test]
    fn full_document_before_change_mode_from_str() {
        assert_eq!(
            FullDocumentBeforeChangeMode::from_str("off"),
            Some(FullDocumentBeforeChangeMode::Off)
        );
        assert_eq!(
            FullDocumentBeforeChangeMode::from_str("whenAvailable"),
            Some(FullDocumentBeforeChangeMode::WhenAvailable)
        );
        assert_eq!(
            FullDocumentBeforeChangeMode::from_str("required"),
            Some(FullDocumentBeforeChangeMode::Required)
        );
        assert!(FullDocumentBeforeChangeMode::from_str("invalid").is_none());
    }

    // -- Pipeline filter support -------------------------------------------

    #[test]
    fn pipeline_match_stage() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));
        oplog.append(make_entry(100, 2, OpType::Insert, "test.orders"));

        let mut filter = Document::new();
        filter.insert("operationType", Bson::String("insert".into()));
        filter.insert("ns", Bson::String("test.users".into()));

        let mut cs = ChangeStream::new("test.users")
            .pipeline_stage(PipelineStage::Match(filter));

        let event = cs.next_event(&oplog).unwrap();
        assert_eq!(event.ns, "test.users");
    }

    #[test]
    fn pipeline_add_fields_stage() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));

        let mut add_fields = Document::new();
        add_fields.insert("customField", Bson::String("hello".into()));

        let mut cs = ChangeStream::new("test.users")
            .pipeline_stage(PipelineStage::AddFields(add_fields));

        let event = cs.next_event(&oplog).unwrap();
        assert_eq!(event.ns, "test.users");
    }

    #[test]
    fn pipeline_unset_stage() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));

        let mut cs = ChangeStream::new("test.users")
            .pipeline_stage(PipelineStage::Unset(vec!["fullDocument".into()]));

        let event = cs.next_event(&oplog).unwrap();
        assert!(event.full_document.is_none());
    }

    #[test]
    fn pipeline_replace_root_stage() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));

        let mut new_root = Document::new();
        let mut root_val = Document::new();
        root_val.insert("replaced", Bson::Boolean(true));
        new_root.insert("newRoot", Bson::Document(root_val));

        let mut cs = ChangeStream::new("test.users")
            .pipeline_stage(PipelineStage::ReplaceRoot(new_root));

        let event = cs.next_event(&oplog).unwrap();
        assert_eq!(event.ns, "test.users");
    }

    #[test]
    fn pipeline_multiple_stages() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));
        oplog.append(make_entry(100, 2, OpType::Insert, "test.orders"));

        let mut match_filter = Document::new();
        match_filter.insert("operationType", Bson::String("insert".into()));
        match_filter.insert("ns", Bson::String("test.users".into()));

        let mut add_fields = Document::new();
        add_fields.insert("extra", Bson::Int32(42));

        let mut cs = ChangeStream::new("test.users")
            .pipeline_stage(PipelineStage::Match(match_filter))
            .pipeline_stage(PipelineStage::AddFields(add_fields));

        let event = cs.next_event(&oplog).unwrap();
        assert_eq!(event.ns, "test.users");
    }

    // -- maxAwaitTimeMS ----------------------------------------------------

    #[test]
    fn max_await_time_ms_default() {
        let cs = ChangeStream::new("test.users");
        assert_eq!(cs.max_await_time_ms, 60_000);
    }

    #[test]
    fn max_await_time_ms_configurable() {
        let cs = ChangeStream::new("test.users").max_await_time_ms(5000);
        assert_eq!(cs.max_await_time_ms, 5000);
    }

    // -- Memory management -------------------------------------------------

    #[test]
    fn seen_window_evicts_old_timestamps() {
        let mut window = SeenWindow::new(100);
        // 插入一个较旧的时间戳 (100:1)
        let old_ts = Timestamp::new(100, 1).as_u64();
        window.insert(old_ts);
        assert!(window.contains(&old_ts));

        // 驱逐 cutoff = 200，100:1 < 200:0，应被驱逐
        window.evict_before(Timestamp::new(200, 0).as_u64());
        assert!(!window.contains(&old_ts));
    }

    #[test]
    fn seen_window_capacity_limit() {
        let mut window = SeenWindow::new(3);
        window.insert(1);
        window.insert(2);
        window.insert(3);
        assert_eq!(window.len(), 3);

        window.insert(4); // 应驱逐最旧的
        assert_eq!(window.len(), 3);
        assert!(!window.contains(&1));
        assert!(window.contains(&4));
    }

    #[test]
    fn evict_stale_timestamps_method() {
        let mut cs = ChangeStream::new("test.users");
        // 模拟在时间戳 600 时插入旧时间戳
        let old_ts = Timestamp::new(200, 1).as_u64();
        cs.seen_timestamps.insert(old_ts);
        assert!(cs.seen_timestamps.contains(&old_ts));

        // 驱逐 5 分钟前的条目 (now_secs=600, cutoff=300)
        cs.evict_stale_timestamps(600);
        assert!(!cs.seen_timestamps.contains(&old_ts));
    }

    #[test]
    fn seen_window_preserves_recent() {
        let mut window = SeenWindow::new(100);
        let recent = Timestamp::new(500, 1).as_u64();
        window.insert(recent);

        // cutoff 是 400，500 的时间戳不应被驱逐
        window.evict_before(Timestamp::new(400, 0).as_u64());
        assert!(window.contains(&recent));
    }

    // -- Event ordering ----------------------------------------------------

    #[test]
    fn events_ordered_by_cluster_time() {
        let mut oplog = Oplog::new(OplogConfig::default());
        // 按时间戳升序插入，保证扫描顺序
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));
        oplog.append(make_entry(200, 1, OpType::Insert, "test.users"));
        oplog.append(make_entry(300, 1, OpType::Insert, "test.users"));

        let mut cs = ChangeStream::new("test.users");
        let batch = cs.next_batch(&oplog);

        assert_eq!(batch.len(), 3);
        // 验证事件按 clusterTime 升序排列
        assert!(batch[0].cluster_time <= batch[1].cluster_time);
        assert!(batch[1].cluster_time <= batch[2].cluster_time);
        // 每个事件都有 clusterTime
        for event in &batch {
            assert!(event.cluster_time.secs > 0);
        }
    }

    #[test]
    fn cluster_time_present_in_all_events() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));

        let mut cs = ChangeStream::new("test.users");
        let event = cs.next_event(&oplog).unwrap();
        assert_eq!(event.cluster_time, Timestamp::new(100, 1));
    }

    // -- Resume token structure --------------------------------------------

    #[test]
    fn resume_token_data_format() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 42, OpType::Insert, "test.users"));

        let mut cs = ChangeStream::new("test.users");
        let _event = cs.next_event(&oplog).unwrap();
        let token = cs.get_resume_token().unwrap();

        let data = token.data().get("_data").and_then(Bson::as_str).unwrap();
        assert_eq!(data, "100:42");
    }

    #[test]
    fn resume_token_has_token_type_field() {
        let token = ChangeStream::make_token("100:1", "resume_token");
        assert_eq!(
            token.data().get("_tokenType").and_then(Bson::as_str),
            Some("resume_token")
        );
    }

    // -- Config defaults ---------------------------------------------------

    #[test]
    fn default_change_stream() {
        let cs = ChangeStream::default();
        assert!(cs.target_ns.is_empty());
        assert!(cs.pipeline.is_empty());
        assert_eq!(cs.level, StreamLevel::Collection);
    }

    #[test]
    fn change_stream_config() {
        let config = ChangeStreamConfig {
            target_ns: "test.events".into(),
            level: StreamLevel::Database,
            max_batch_size: 50,
            max_await_time_ms: 10_000,
            full_document: FullDocumentMode::UpdateLookup,
            full_document_before_change: FullDocumentBeforeChangeMode::WhenAvailable,
            pipeline: vec![],
            resume_start: None,
            seen_window_capacity: 2048,
        };
        let cs = ChangeStream::with_config(config);
        assert_eq!(cs.target_ns, "test.events");
        assert_eq!(cs.max_batch_size, 50);
        assert_eq!(cs.max_await_time_ms, 10_000);
        assert_eq!(cs.level, StreamLevel::Database);
    }

    #[test]
    fn empty_namespace_matches_all_at_cluster_level() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));
        oplog.append(make_entry(100, 2, OpType::Insert, "test.orders"));

        let mut cs = ChangeStream::cluster();
        let event1 = cs.next_event(&oplog).unwrap();
        assert_eq!(event1.ns, "test.users");
        let event2 = cs.next_event(&oplog).unwrap();
        assert_eq!(event2.ns, "test.orders");
    }

    // -- Update description ------------------------------------------------

    #[test]
    fn update_event_has_document_key() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_update_entry(100, 1, "test.users"));

        let mut cs = ChangeStream::new("test.users");
        let event = cs.next_event(&oplog).unwrap();
        assert_eq!(event.operation_type, OperationType::Update);
        assert!(event.document_key.get("_id").is_some());
    }

    // -- Error Display -----------------------------------------------------

    #[test]
    fn error_display() {
        let e = ChangeStreamError::FullDocumentRequiredUnavailable;
        assert!(format!("{e}").contains("fullDocument=required"));

        let e = ChangeStreamError::UnsupportedStage("$lookup".into());
        assert!(format!("{e}").contains("$lookup"));

        let e = ChangeStreamError::StartTimeTooOld;
        assert!(format!("{e}").contains("startAtOperationTime"));

        let e = ChangeStreamError::PipelineError("test".into());
        assert!(format!("{e}").contains("test"));
    }

    // -- Builder chaining --------------------------------------------------

    #[test]
    fn builder_chain_all_options() {
        let mut match_filter = Document::new();
        match_filter.insert("operationType", Bson::String("insert".into()));

        let cs = ChangeStream::new("test.users")
            .full_document(FullDocumentMode::UpdateLookup)
            .full_document_before_change(FullDocumentBeforeChangeMode::WhenAvailable)
            .max_await_time_ms(5000)
            .max_batch_size(200)
            .pipeline_stage(PipelineStage::Match(match_filter))
            .pipeline_stage(PipelineStage::Unset(vec!["fullDocument".into()]));

        assert_eq!(cs.full_document, FullDocumentMode::UpdateLookup);
        assert_eq!(
            cs.full_document_before_change,
            FullDocumentBeforeChangeMode::WhenAvailable
        );
        assert_eq!(cs.max_await_time_ms, 5000);
        assert_eq!(cs.max_batch_size, 200);
        assert_eq!(cs.pipeline.len(), 2);
    }

    // -- Redact stage (simplified) -----------------------------------------

    #[test]
    fn redact_stage_defaults_to_keep() {
        let mut oplog = Oplog::new(OplogConfig::default());
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));

        let redact_spec = Document::new();
        let mut cs = ChangeStream::new("test.users")
            .pipeline_stage(PipelineStage::Redact(redact_spec));

        let event = cs.next_event(&oplog);
        assert!(event.is_some());
    }

    // -- Full document required error --------------------------------------

    #[test]
    fn full_document_required_skips_update_without_doc() {
        let mut oplog = Oplog::new(OplogConfig::default());
        // Update 事件没有 fullDocument 时，Required 模式应跳过
        let mut entry = OplogEntry {
            ts: Timestamp::new(100, 1),
            op: OpType::Update,
            ns: "test.users".to_owned(),
            o: Document::new(),
            o2: Some({
                let mut o2 = Document::new();
                o2.insert("_id", Bson::Int32(1));
                o2
            }),
            ui: ObjectId::from_bytes([0; 12]),
        };
        // 清除 o 中的 _id 使其无法提取 fullDocument
        entry.o.remove("_id");
        oplog.append(entry);

        let mut cs = ChangeStream::new("test.users")
            .full_document(FullDocumentMode::Required);

        let event = cs.next_event(&oplog);
        // Update without fullDocument in Required mode should be skipped
        assert!(event.is_none());
    }

    #[test]
    fn full_document_required_allows_insert() {
        let mut oplog = Oplog::new(OplogConfig::default());
        // Insert 事件始终有 fullDocument，Required 模式应正常工作
        oplog.append(make_entry(100, 1, OpType::Insert, "test.users"));

        let mut cs = ChangeStream::new("test.users")
            .full_document(FullDocumentMode::Required);

        let event = cs.next_event(&oplog);
        assert!(event.is_some());
        assert!(event.unwrap().full_document.is_some());
    }
}
