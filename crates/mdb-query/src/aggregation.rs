//! 聚合管道：Pipeline 定义与各阶段处理逻辑。

use mdb_doc::{Bson, Document};

use crate::executor::sort_documents;
use crate::types::SortDirection;

/// 聚合管道。
#[derive(Debug, Clone)]
pub struct Pipeline {
    stages: Vec<PipelineStage>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    pub fn add_stage(&mut self, stage: PipelineStage) -> &mut Self {
        self.stages.push(stage);
        self
    }

    pub fn len(&self) -> usize {
        self.stages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    pub fn execute(&self, input: Vec<Document>) -> Vec<Document> {
        let mut docs = input;
        for stage in &self.stages {
            docs = stage.process(docs);
        }
        docs
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// 聚合管道阶段。
#[derive(Debug, Clone)]
pub enum PipelineStage {
    Match(Document),
    Group { id: GroupId, accumulators: Vec<(String, Accumulator)> },
    Sort(Vec<(String, SortDirection)>),
    Project(Document),
    Limit(i64),
    Skip(i64),
    Unwind { field: String, preserve_null_and_empty: bool, include_array_index: Option<String> },
    AddFields(Document),
    Count(String),
    Bucket { group_by: String, boundaries: Vec<Bson>, default: Option<Bson>, output: Option<Document> },
    Lookup { from: String, local_field: String, foreign_field: String, as_field: String },
    LookupPipeline { from: String, let_vars: Document, pipeline: Pipeline, as_field: String },
    Out(String),
    Merge { into_collection: String, when_matched: MergeWhenMatched, when_not_matched: MergeWhenNotMatched },
    GraphLookup { from: String, start_with: String, connect_from_field: String, connect_to_field: String, as_field: String, max_depth: Option<i32>, depth_field: Option<String> },
    SetWindowFields { partition_by: Option<String>, sort: Option<Vec<(String, SortDirection)>>, output: Vec<(String, WindowFunction)> },
    UnionWith { collection: String, pipeline: Option<Pipeline> },
    Sample { size: i64 },
    Densify { field: String, range: DensifyRange, partition_by: Option<String> },
    Fill { sort: Vec<(String, SortDirection)>, partition_by: Option<String>, output: Document },
    Redact(Document),
    SortByCount(String),
    Facet(Vec<(String, Pipeline)>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeWhenMatched { Replace, KeepExisting, Merge, Fail }

impl Default for MergeWhenMatched { fn default() -> Self { Self::Replace } }

#[derive(Debug, Clone, PartialEq)]
pub enum MergeWhenNotMatched { Insert, Discard, Fail }

impl Default for MergeWhenNotMatched { fn default() -> Self { Self::Insert } }

#[derive(Debug, Clone)]
pub enum DensifyRange {
    Step(Bson),
    Bounds { start: Bson, end: Bson, unit: Bson },
}

#[derive(Debug, Clone)]
pub enum WindowFunction {
    Rank,
    DenseRank,
    RowNumber,
    RunningTotal(String),
    ExpMovingAvg { input: String, alpha: Option<f64> },
    Sum(String),
    Push(String),
    Min(String),
    Max(String),
    Avg(String),
}

#[derive(Debug, Clone)]
pub enum GroupId {
    Field(String),
    Literal(Bson),
    Expression(Expression),
}

#[derive(Debug, Clone)]
pub enum Expression {
    Field(String),
    Year(String),
    Month(String),
    Day(String),
    Hour(String),
    Minute(String),
    Second(String),
    DayOfWeek(String),
    DayOfYear(String),
    Week(String),
}

impl GroupId {
    pub fn from_bson(val: &Bson) -> Self {
        match val {
            Bson::String(s) if s.starts_with('$') => GroupId::Field(s[1..].to_owned()),
            Bson::String(_) => GroupId::Literal(val.clone()),
            Bson::Document(doc) => {
                for (key, value) in doc.iter() {
                    if let Some(field) = value.as_str() {
                        let field_name = field.strip_prefix('$').unwrap_or(field).to_owned();
                        let expr = match key {
                            "$year" => Expression::Year(field_name),
                            "$month" => Expression::Month(field_name),
                            "$day" => Expression::Day(field_name),
                            "$hour" => Expression::Hour(field_name),
                            "$minute" => Expression::Minute(field_name),
                            "$second" => Expression::Second(field_name),
                            "$dayOfWeek" => Expression::DayOfWeek(field_name),
                            "$dayOfYear" => Expression::DayOfYear(field_name),
                            "$week" => Expression::Week(field_name),
                            _ => Expression::Field(field_name),
                        };
                        return GroupId::Expression(expr);
                    }
                }
                GroupId::Literal(val.clone())
            }
            _ => GroupId::Literal(val.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Accumulator {
    Sum(String),
    /// `{$sum: <number>}`：对每个文档累加常量（`count_documents` 的 `$group` 就靠它）。
    SumConst(f64),
    Avg(String),
    Min(String),
    Max(String),
    First(String),
    Last(String),
    Push(String),
    AddToSet(String),
    Count,
}

/// 集合写入 trait，用于 $out 和 $merge 阶段。
pub trait CollectionSink: Send + Sync {
    fn insert_many(&self, docs: Vec<Document>) -> Result<(), String>;
    fn replace_one(&self, filter: &Document, replacement: &Document, upsert: bool) -> Result<(), String>;
    fn delete_many(&self, filter: &Document) -> Result<(), String>;
}

// ─── Expression evaluation helpers ────────────────────────────────────────────

fn resolve_field(doc: &Document, field: &str) -> Option<Bson> {
    doc.get_path(field).cloned()
}

fn evaluate_expression(doc: &Document, expr: &Expression) -> Option<Bson> {
    match expr {
        Expression::Field(f) => resolve_field(doc, f),
        Expression::Year(f) => resolve_field(doc, f).and_then(|v| extract_year(&v)),
        Expression::Month(f) => resolve_field(doc, f).and_then(|v| extract_month(&v)),
        Expression::Day(f) => resolve_field(doc, f).and_then(|v| extract_day(&v)),
        Expression::Hour(f) => resolve_field(doc, f).and_then(|v| extract_hour(&v)),
        Expression::Minute(f) => resolve_field(doc, f).and_then(|v| extract_minute(&v)),
        Expression::Second(f) => resolve_field(doc, f).and_then(|v| extract_second(&v)),
        Expression::DayOfWeek(f) => resolve_field(doc, f).and_then(|v| extract_day_of_week(&v)),
        Expression::DayOfYear(f) => resolve_field(doc, f).and_then(|v| extract_day_of_year(&v)),
        Expression::Week(f) => resolve_field(doc, f).and_then(|v| extract_week(&v)),
    }
}

fn millis_to_secs(ms: i64) -> i64 { ms / 1000 }

fn extract_datetime_ms(val: &Bson) -> Option<i64> {
    match val {
        Bson::DateTime(ms) => Some(*ms),
        Bson::Int64(ms) => Some(*ms),
        Bson::Int32(ms) => Some(*ms as i64),
        _ => None,
    }
}

fn extract_year(val: &Bson) -> Option<Bson> {
    let ms = extract_datetime_ms(val)?;
    let days = millis_to_secs(ms) / 86400;
    let year = 1970 + (days as f64 / 365.25).floor() as i32;
    Some(Bson::Int32(year))
}

fn extract_month(val: &Bson) -> Option<Bson> {
    let ms = extract_datetime_ms(val)?;
    let days = millis_to_secs(ms) / 86400;
    let day_of_year = days % 365;
    let month = (day_of_year / 30 + 1).min(12);
    Some(Bson::Int32(month as i32))
}

fn extract_day(val: &Bson) -> Option<Bson> {
    let ms = extract_datetime_ms(val)?;
    let days = millis_to_secs(ms) / 86400;
    Some(Bson::Int32((days % 30 + 1) as i32))
}

fn extract_hour(val: &Bson) -> Option<Bson> {
    let ms = extract_datetime_ms(val)?;
    let secs = millis_to_secs(ms);
    Some(Bson::Int32(((secs % 86400) / 3600) as i32))
}

fn extract_minute(val: &Bson) -> Option<Bson> {
    let ms = extract_datetime_ms(val)?;
    let secs = millis_to_secs(ms);
    Some(Bson::Int32(((secs % 3600) / 60) as i32))
}

fn extract_second(val: &Bson) -> Option<Bson> {
    let ms = extract_datetime_ms(val)?;
    let secs = millis_to_secs(ms);
    Some(Bson::Int32((secs % 60) as i32))
}

fn extract_day_of_week(val: &Bson) -> Option<Bson> {
    let ms = extract_datetime_ms(val)?;
    let days = millis_to_secs(ms) / 86400;
    Some(Bson::Int32(((days + 4) % 7 + 1) as i32))
}

fn extract_day_of_year(val: &Bson) -> Option<Bson> {
    let ms = extract_datetime_ms(val)?;
    let days = millis_to_secs(ms) / 86400;
    Some(Bson::Int32((days % 365 + 1) as i32))
}

fn extract_week(val: &Bson) -> Option<Bson> {
    let ms = extract_datetime_ms(val)?;
    let days = millis_to_secs(ms) / 86400;
    Some(Bson::Int32((days / 7 + 1) as i32))
}

// ─── Project expression evaluation ────────────────────────────────────────────

fn evaluate_project_expr(doc: &Document, expr: &Bson) -> Bson {
    match expr {
        Bson::Document(op_doc) => {
            if let Some((op, args)) = op_doc.iter().next() {
                match op {
                    "$concat" => eval_concat(doc, args),
                    "$add" => eval_arithmetic(doc, args, ArithOp::Add),
                    "$multiply" => eval_arithmetic(doc, args, ArithOp::Multiply),
                    "$subtract" => eval_arithmetic(doc, args, ArithOp::Subtract),
                    "$divide" => eval_arithmetic(doc, args, ArithOp::Divide),
                    "$mod" => eval_arithmetic(doc, args, ArithOp::Mod),
                    "$cond" => eval_cond(doc, args),
                    "$ifNull" => eval_if_null(doc, args),
                    "$switch" => eval_switch(doc, args),
                    "$toLower" => eval_to_lower(doc, args),
                    "$toUpper" => eval_to_upper(doc, args),
                    "$substr" => eval_substr(doc, args),
                    "$strLenCP" => eval_str_len(doc, args),
                    "$eq" => eval_cmp_op(doc, args, CmpOp::Eq),
                    "$ne" => eval_cmp_op(doc, args, CmpOp::Ne),
                    "$gt" => eval_cmp_op(doc, args, CmpOp::Gt),
                    "$gte" => eval_cmp_op(doc, args, CmpOp::Gte),
                    "$lt" => eval_cmp_op(doc, args, CmpOp::Lt),
                    "$lte" => eval_cmp_op(doc, args, CmpOp::Lte),
                    "$and" => eval_logical_op(doc, args, true),
                    "$or" => eval_logical_op(doc, args, false),
                    "$not" => eval_logical_not(doc, args),
                    "$in" => eval_in_op(doc, args),
                    "$size" => eval_size(doc, args),
                    "$arrayElemAt" => eval_array_elem_at(doc, args),
                    _ => Bson::Null,
                }
            } else {
                Bson::Null
            }
        }
        Bson::String(s) if s.starts_with('$') => {
            doc.get_path(&s[1..]).cloned().unwrap_or(Bson::Null)
        }
        _ => expr.clone(),
    }
}

#[derive(Clone, Copy)]
enum ArithOp { Add, Multiply, Subtract, Divide, Mod }

#[derive(Clone, Copy)]
enum CmpOp { Eq, Ne, Gt, Gte, Lt, Lte }

fn resolve_value(doc: &Document, val: &Bson) -> Bson {
    match val {
        Bson::String(s) if s.starts_with('$') => doc.get_path(&s[1..]).cloned().unwrap_or(Bson::Null),
        Bson::Document(_) => evaluate_project_expr(doc, val),
        _ => val.clone(),
    }
}

fn to_f64(b: &Bson) -> Option<f64> {
    match b {
        Bson::Int32(v) => Some(*v as f64),
        Bson::Int64(v) => Some(*v as f64),
        Bson::Double(v) => Some(*v),
        _ => None,
    }
}

fn bson_truthy(val: &Bson) -> bool {
    match val {
        Bson::Boolean(b) => *b,
        Bson::Null | Bson::Undefined => false,
        Bson::Int32(0) | Bson::Int64(0) | Bson::Double(0.0) => false,
        Bson::String(s) if s.is_empty() => false,
        Bson::Array(a) if a.is_empty() => false,
        Bson::Document(d) if d.is_empty() => false,
        _ => true,
    }
}

fn eval_concat(doc: &Document, args: &Bson) -> Bson {
    if let Some(arr) = args.as_array() {
        let mut result = String::new();
        for item in arr {
            let val = resolve_value(doc, item);
            match val {
                Bson::String(s) => result.push_str(&s),
                other => result.push_str(&format!("{other}")),
            }
        }
        Bson::String(result)
    } else {
        Bson::Null
    }
}

fn eval_arithmetic(doc: &Document, args: &Bson, op: ArithOp) -> Bson {
    if let Some(arr) = args.as_array() {
        if arr.len() < 2 { return Bson::Null; }
        let a = resolve_value(doc, &arr[0]);
        let b = resolve_value(doc, &arr[1]);
        let fa = to_f64(&a).unwrap_or(0.0);
        let fb = to_f64(&b).unwrap_or(0.0);
        let result = match op {
            ArithOp::Add => fa + fb,
            ArithOp::Multiply => fa * fb,
            ArithOp::Subtract => fa - fb,
            ArithOp::Divide => { if fb == 0.0 { return Bson::Double(f64::INFINITY); } fa / fb }
            ArithOp::Mod => { if fb == 0.0 { return Bson::Null; } fa % fb }
        };
        if result.fract() == 0.0 && result.abs() < i64::MAX as f64 {
            Bson::Int64(result as i64)
        } else {
            Bson::Double(result)
        }
    } else {
        Bson::Null
    }
}

fn eval_cond(doc: &Document, args: &Bson) -> Bson {
    if let Some(arr) = args.as_array() {
        if arr.len() >= 3 {
            let cond_val = resolve_value(doc, &arr[0]);
            if bson_truthy(&cond_val) { resolve_value(doc, &arr[1]) } else { resolve_value(doc, &arr[2]) }
        } else { Bson::Null }
    } else { Bson::Null }
}

fn eval_if_null(doc: &Document, args: &Bson) -> Bson {
    if let Some(arr) = args.as_array() {
        if arr.len() >= 2 {
            let val = resolve_value(doc, &arr[0]);
            if val.is_nullish() { resolve_value(doc, &arr[1]) } else { val }
        } else { Bson::Null }
    } else { Bson::Null }
}

fn eval_switch(doc: &Document, args: &Bson) -> Bson {
    if let Some(branch_doc) = args.as_document() {
        if let Some(Bson::Array(branches)) = branch_doc.get("branches") {
            for branch in branches {
                if let Some(b) = branch.as_document() {
                    if let Some(case) = b.get("case") {
                        if bson_truthy(&resolve_value(doc, case)) {
                            return b.get("then").map(|v| resolve_value(doc, v)).unwrap_or(Bson::Null);
                        }
                    }
                }
            }
        }
        if let Some(default) = branch_doc.get("default") {
            return resolve_value(doc, default);
        }
    }
    Bson::Null
}

fn eval_to_lower(doc: &Document, args: &Bson) -> Bson {
    match resolve_value(doc, args) { Bson::String(s) => Bson::String(s.to_lowercase()), _ => Bson::Null }
}

fn eval_to_upper(doc: &Document, args: &Bson) -> Bson {
    match resolve_value(doc, args) { Bson::String(s) => Bson::String(s.to_uppercase()), _ => Bson::Null }
}

fn eval_substr(doc: &Document, args: &Bson) -> Bson {
    if let Some(arr) = args.as_array() {
        if arr.len() >= 3 {
            let s = resolve_value(doc, &arr[0]);
            let start = resolve_value(doc, &arr[1]).as_i64().unwrap_or(0) as usize;
            let len = resolve_value(doc, &arr[2]).as_i64().unwrap_or(0) as usize;
            if let Bson::String(s) = s {
                let start = start.min(s.len());
                let end = (start + len).min(s.len());
                return Bson::String(s[start..end].to_owned());
            }
        }
    }
    Bson::Null
}

fn eval_str_len(doc: &Document, args: &Bson) -> Bson {
    match resolve_value(doc, args) { Bson::String(s) => Bson::Int32(s.chars().count() as i32), _ => Bson::Null }
}

fn eval_cmp_op(doc: &Document, args: &Bson, op: CmpOp) -> Bson {
    if let Some(arr) = args.as_array() {
        if arr.len() >= 2 {
            let a = resolve_value(doc, &arr[0]);
            let b = resolve_value(doc, &arr[1]);
            let result = match op {
                CmpOp::Eq => crate::operators::bson_eq(&a, &b),
                CmpOp::Ne => !crate::operators::bson_eq(&a, &b),
                CmpOp::Gt => crate::operators::bson_cmp(&a, &b) == Some(std::cmp::Ordering::Greater),
                CmpOp::Gte => matches!(crate::operators::bson_cmp(&a, &b), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)),
                CmpOp::Lt => crate::operators::bson_cmp(&a, &b) == Some(std::cmp::Ordering::Less),
                CmpOp::Lte => matches!(crate::operators::bson_cmp(&a, &b), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)),
            };
            return Bson::Boolean(result);
        }
    }
    Bson::Null
}

fn eval_logical_op(doc: &Document, args: &Bson, is_and: bool) -> Bson {
    if let Some(arr) = args.as_array() {
        let results: Vec<bool> = arr.iter().map(|item| bson_truthy(&resolve_value(doc, item))).collect();
        let result = if is_and { results.iter().all(|&b| b) } else { results.iter().any(|&b| b) };
        return Bson::Boolean(result);
    }
    Bson::Null
}

fn eval_logical_not(doc: &Document, args: &Bson) -> Bson {
    Bson::Boolean(!bson_truthy(&resolve_value(doc, args)))
}

fn eval_in_op(doc: &Document, args: &Bson) -> Bson {
    if let Some(arr) = args.as_array() {
        if arr.len() >= 2 {
            let val = resolve_value(doc, &arr[0]);
            if let Bson::Array(options) = resolve_value(doc, &arr[1]) {
                return Bson::Boolean(options.iter().any(|opt| crate::operators::bson_eq(&val, opt)));
            }
        }
    }
    Bson::Null
}

fn eval_size(doc: &Document, args: &Bson) -> Bson {
    match resolve_value(doc, args) {
        Bson::Array(a) => Bson::Int32(a.len() as i32),
        Bson::String(s) => Bson::Int32(s.chars().count() as i32),
        _ => Bson::Null,
    }
}

fn eval_array_elem_at(doc: &Document, args: &Bson) -> Bson {
    if let Some(arr) = args.as_array() {
        if arr.len() >= 2 {
            let array_val = resolve_value(doc, &arr[0]);
            let idx = resolve_value(doc, &arr[1]).as_i64().unwrap_or(0);
            if let Bson::Array(a) = array_val {
                let actual = if idx < 0 { (a.len() as i64 + idx) as usize } else { idx as usize };
                if actual < a.len() { return a[actual].clone(); }
            }
        }
    }
    Bson::Null
}

// ─── Filter matching ──────────────────────────────────────────────────────────

fn document_matches_filter(doc: &Document, filter: &Document) -> bool {
    for (key, filter_val) in filter.iter() {
        match key {
            "$and" => {
                if let Some(arr) = filter_val.as_array() {
                    for item in arr {
                        if let Some(sub) = item.as_document() {
                            if !document_matches_filter(doc, sub) { return false; }
                        }
                    }
                }
            }
            "$or" => {
                if let Some(arr) = filter_val.as_array() {
                    if !arr.iter().any(|item| {
                        item.as_document().map_or(false, |sub| document_matches_filter(doc, sub))
                    }) { return false; }
                }
            }
            "$nor" => {
                if let Some(arr) = filter_val.as_array() {
                    for item in arr {
                        if let Some(sub) = item.as_document() {
                            if document_matches_filter(doc, sub) { return false; }
                        }
                    }
                }
            }
            _ => match filter_val {
                Bson::Document(ops) => { if !eval_field_ops(doc.get(key), ops) { return false; } }
                _ => {
                    let doc_val = doc.get(key);
                    match doc_val {
                        Some(v) => { if v != filter_val { return false; } }
                        None => return false,
                    }
                }
            },
        }
    }
    true
}

fn eval_field_ops(doc_val: Option<&Bson>, ops: &Document) -> bool {
    for (op, target) in ops.iter() {
        match op {
            "$eq" => { if !doc_val.map_or(false, |v| crate::operators::bson_eq(v, target)) { return false; } }
            "$ne" => { if doc_val.map_or(false, |v| crate::operators::bson_eq(v, target)) { return false; } }
            "$gt" => { if !doc_val.map_or(false, |v| crate::operators::bson_cmp(v, target) == Some(std::cmp::Ordering::Greater)) { return false; } }
            "$gte" => { if !doc_val.map_or(false, |v| matches!(crate::operators::bson_cmp(v, target), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))) { return false; } }
            "$lt" => { if !doc_val.map_or(false, |v| crate::operators::bson_cmp(v, target) == Some(std::cmp::Ordering::Less)) { return false; } }
            "$lte" => { if !doc_val.map_or(false, |v| matches!(crate::operators::bson_cmp(v, target), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))) { return false; } }
            "$in" => {
                if let Some(arr) = target.as_array() {
                    if !doc_val.map_or(false, |v| arr.iter().any(|t| crate::operators::bson_eq(v, t))) { return false; }
                }
            }
            "$exists" => { if doc_val.is_some() != target.as_bool().unwrap_or(true) { return false; } }
            _ => { return false; }
        }
    }
    true
}

// ─── Group ────────────────────────────────────────────────────────────────────

fn process_group(input: Vec<Document>, id: &GroupId, accumulators: &[(String, Accumulator)]) -> Vec<Document> {
    let mut groups: Vec<(Bson, Vec<Document>)> = Vec::new();
    for doc in &input {
        let key = match id {
            GroupId::Field(f) => doc.get(f).cloned().unwrap_or(Bson::Null),
            GroupId::Literal(v) => v.clone(),
            GroupId::Expression(expr) => evaluate_expression(doc, expr).unwrap_or(Bson::Null),
        };
        match groups.iter_mut().find(|(k, _)| crate::operators::bson_eq(k, &key)) {
            Some((_, docs)) => docs.push(doc.clone()),
            None => groups.push((key, vec![doc.clone()])),
        }
    }
    groups.into_iter().map(|(key, docs)| {
        let mut out = Document::new();
        out.insert("_id", key);
        for (name, acc) in accumulators {
            out.insert(name.clone(), compute_accumulator(acc, &docs));
        }
        out
    }).collect()
}

fn compute_accumulator(acc: &Accumulator, docs: &[Document]) -> Bson {
    match acc {
        Accumulator::Sum(field) => {
            let lookup = field.strip_prefix('$').unwrap_or(field);
            let sum: f64 = docs.iter().filter_map(|d| d.get(lookup)).filter_map(Bson::as_f64).sum();
            if sum.fract() == 0.0 && sum.abs() < i64::MAX as f64 { Bson::Int64(sum as i64) } else { Bson::Double(sum) }
        }
        Accumulator::SumConst(v) => {
            let sum = v * docs.len() as f64;
            if sum.fract() == 0.0 && sum.abs() < i64::MAX as f64 { Bson::Int64(sum as i64) } else { Bson::Double(sum) }
        }
        Accumulator::Avg(field) => {
            let lookup = field.strip_prefix('$').unwrap_or(field);
            let (sum, count): (f64, u64) = docs.iter().filter_map(|d| d.get(lookup)).filter_map(Bson::as_f64)
                .fold((0.0, 0), |(s, c), v| (s + v, c + 1));
            if count == 0 { Bson::Double(f64::NAN) } else { Bson::Double(sum / count as f64) }
        }
        Accumulator::Min(field) => {
            let lookup = field.strip_prefix('$').unwrap_or(field);
            docs.iter().filter_map(|d| d.get(lookup))
                .min_by(|a, b| crate::operators::bson_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal))
                .cloned().unwrap_or(Bson::Null)
        }
        Accumulator::Max(field) => {
            let lookup = field.strip_prefix('$').unwrap_or(field);
            docs.iter().filter_map(|d| d.get(lookup))
                .max_by(|a, b| crate::operators::bson_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal))
                .cloned().unwrap_or(Bson::Null)
        }
        Accumulator::First(field) => {
            let lookup = field.strip_prefix('$').unwrap_or(field);
            docs.first().and_then(|d| d.get(lookup)).cloned().unwrap_or(Bson::Null)
        }
        Accumulator::Last(field) => {
            let lookup = field.strip_prefix('$').unwrap_or(field);
            docs.last().and_then(|d| d.get(lookup)).cloned().unwrap_or(Bson::Null)
        }
        Accumulator::Push(field) => {
            let lookup = field.strip_prefix('$').unwrap_or(field);
            Bson::Array(docs.iter().filter_map(|d| d.get(lookup).cloned()).collect())
        }
        Accumulator::AddToSet(field) => {
            let lookup = field.strip_prefix('$').unwrap_or(field);
            let mut seen = Vec::new();
            for doc in docs {
                if let Some(val) = doc.get(lookup) {
                    if !seen.iter().any(|v| crate::operators::bson_eq(v, val)) { seen.push(val.clone()); }
                }
            }
            Bson::Array(seen)
        }
        Accumulator::Count => Bson::Int64(docs.len() as i64),
    }
}

// ─── Bucket ───────────────────────────────────────────────────────────────────

fn process_bucket(input: Vec<Document>, group_by: &str, boundaries: &[Bson], default: &Option<Bson>, output: Option<&Document>) -> Vec<Document> {
    let mut buckets: Vec<(Bson, Vec<Document>)> = Vec::new();
    for doc in input {
        let val = doc.get(group_by).cloned().unwrap_or(Bson::Null);
        let mut bucket_key = None;
        for boundary in boundaries {
            match crate::operators::bson_cmp(&val, boundary) {
                Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal) => { bucket_key = Some(boundary.clone()); }
                _ => break,
            }
        }
        if bucket_key.is_none() { if let Some(def) = default { bucket_key = Some(def.clone()); } }
        if let Some(key) = bucket_key {
            match buckets.iter_mut().find(|(k, _)| crate::operators::bson_eq(k, &key)) {
                Some((_, docs)) => docs.push(doc),
                None => buckets.push((key, vec![doc])),
            }
        }
    }
    buckets.into_iter().map(|(key, docs)| {
        let mut out = Document::new();
        out.insert("_id", key);
        if let Some(output_spec) = output {
            for (field_name, acc_val) in output_spec.iter() {
                if let Some(acc_doc) = acc_val.as_document() {
                    if let Some((acc_name, acc_field)) = acc_doc.iter().next() {
                        let accumulator = match acc_name {
                            "$sum" => acc_field.as_str().map(|f| Accumulator::Sum(f.to_owned())),
                            "$avg" => acc_field.as_str().map(|f| Accumulator::Avg(f.to_owned())),
                            "$min" => acc_field.as_str().map(|f| Accumulator::Min(f.to_owned())),
                            "$max" => acc_field.as_str().map(|f| Accumulator::Max(f.to_owned())),
                            "$push" => acc_field.as_str().map(|f| Accumulator::Push(f.to_owned())),
                            "$count" => Some(Accumulator::Count),
                            _ => None,
                        };
                        if let Some(acc) = accumulator { out.insert(field_name.clone(), compute_accumulator(&acc, &docs)); }
                    }
                }
            }
        } else {
            out.insert("count", Bson::Int64(docs.len() as i64));
        }
        out
    }).collect()
}

// ─── Lookup ───────────────────────────────────────────────────────────────────

fn process_lookup_basic(input: Vec<Document>, local_field: &str, _foreign_field: &str, as_field: &str) -> Vec<Document> {
    input.into_iter().map(|mut doc| {
        if doc.get(local_field).is_some() {
            doc.insert(as_field.to_owned(), Bson::Array(vec![]));
        }
        doc
    }).collect()
}

/// 简单的 BSON 值比较。
fn bson_equals(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => x == y,
        (Bson::Int64(x), Bson::Int64(y)) => x == y,
        (Bson::Int32(x), Bson::Int64(y)) => *x as i64 == *y,
        (Bson::Int64(x), Bson::Int32(y)) => *x == *y as i64,
        (Bson::Double(x), Bson::Double(y)) => x == y,
        (Bson::Int32(x), Bson::Double(y)) => *x as f64 == *y,
        (Bson::Double(x), Bson::Int32(y)) => *x == *y as f64,
        (Bson::String(x), Bson::String(y)) => x == y,
        (Bson::Boolean(x), Bson::Boolean(y)) => x == y,
        (Bson::Null, Bson::Null) => true,
        (Bson::ObjectId(x), Bson::ObjectId(y)) => x == y,
        _ => false,
    }
}

/// GraphLookup：递归图遍历。
///
/// 简化实现：从每个文档的 connect_from_field 字段值开始，
/// 在同一输入中查找 connect_to_field 匹配的文档，递归直到达到 max_depth。
fn process_graph_lookup(
    input: &[Document],
    as_field: &str,
    connect_from_field: &str,
    connect_to_field: &str,
) -> Vec<Document> {
    let max_depth = 10; // 简化的最大深度

    input.iter().map(|doc| {
        let mut doc = doc.clone();
        let mut result = Vec::new();
        let mut visited = std::collections::HashSet::new();

        if let Some(start_val) = doc.get(connect_from_field).cloned() {
            // BFS遍历
            let mut queue: Vec<(Bson, usize)> = vec![(start_val, 0)];
            while let Some((val, depth)) = queue.pop() {
                if depth >= max_depth { continue; }
                let key = format!("{val:?}");
                if visited.contains(&key) { continue; }
                visited.insert(key);

                for other in input {
                    if let Some(other_val) = other.get(connect_to_field) {
                        if bson_equals(other_val, &val) {
                            // 找到匹配，添加到结果
                            let mut linked = other.clone();
                            linked.insert("depth", Bson::Int32(depth as i32));
                            result.push(Bson::Document(linked));

                            // 继续递归
                            if let Some(next_val) = other.get(connect_from_field) {
                                queue.push((next_val.clone(), depth + 1));
                            }
                        }
                    }
                }
            }
        }

        doc.insert(as_field.to_owned(), Bson::Array(result));
        doc
    }).collect()
}

// ─── Unwind ───────────────────────────────────────────────────────────────────

fn process_unwind(input: Vec<Document>, field: &str, preserve_null_and_empty: bool, include_array_index: &Option<String>) -> Vec<Document> {
    let mut result = Vec::new();
    for mut doc in input {
        match doc.get(field).cloned() {
            Some(Bson::Array(arr)) => {
                if arr.is_empty() {
                    if preserve_null_and_empty {
                        doc.set(field.to_owned(), Bson::Null);
                        result.push(doc);
                    }
                    continue;
                }
                for (idx, item) in arr.into_iter().enumerate() {
                    let mut cloned = doc.clone();
                    cloned.insert(field.to_owned(), item);
                    if let Some(idx_field) = include_array_index { cloned.insert(idx_field.clone(), Bson::Int64(idx as i64)); }
                    result.push(cloned);
                }
            }
            Some(val) => {
                doc.insert(field.to_owned(), val);
                if let Some(idx_field) = include_array_index { doc.insert(idx_field.clone(), Bson::Int64(0)); }
                result.push(doc);
            }
            None => {
                if preserve_null_and_empty {
                    doc.set(field.to_owned(), Bson::Null);
                    if let Some(idx_field) = include_array_index { doc.insert(idx_field.clone(), Bson::Null); }
                    result.push(doc);
                }
            }
        }
    }
    result
}

// ─── Project ──────────────────────────────────────────────────────────────────

fn project_document(doc: &Document, projection: &Document) -> Document {
    let has_computed = projection.iter().any(|(_, v)| {
        matches!(v, Bson::Document(d) if d.keys().next().map(|k| k.starts_with('$')).unwrap_or(false))
    });
    if has_computed {
        let mut result = Document::new();
        if !matches!(projection.get("_id"), Some(Bson::Int32(0)) | Some(Bson::Int64(0))) {
            if let Some(id) = doc.get("_id") { result.insert("_id", id.clone()); }
        }
        for (key, val) in projection.iter() {
            match val {
                Bson::Int32(1) | Bson::Int64(1) => { if let Some(v) = doc.get(key) { result.insert(key, v.clone()); } }
                Bson::Int32(0) | Bson::Int64(0) => {}
                _ => { result.insert(key.clone(), evaluate_project_expr(doc, val)); }
            }
        }
        return result;
    }
    let include_mode = projection.iter().any(|(_, v)| matches!(v, Bson::Int32(1) | Bson::Int64(1)));
    if include_mode {
        let mut result = Document::new();
        for (key, val) in projection.iter() {
            if matches!(val, Bson::Int32(1) | Bson::Int64(1)) {
                if let Some(v) = doc.get(key) { result.insert(key, v.clone()); }
            }
        }
        if !projection.contains_key("_id") { if let Some(id) = doc.get("_id") { result.insert("_id", id.clone()); } }
        result
    } else {
        let excluded: Vec<String> = projection.iter().filter(|(_, v)| matches!(v, Bson::Int32(0) | Bson::Int64(0))).map(|(k, _)| k.to_owned()).collect();
        let mut result = Document::new();
        for (key, val) in doc.iter() { if !excluded.contains(&key.to_owned()) { result.insert(key, val.clone()); } }
        result
    }
}

// ─── SetWindowFields ──────────────────────────────────────────────────────────

fn partition_documents(input: &[Document], partition_by: Option<&String>) -> Vec<(Option<Bson>, Vec<usize>)> {
    if let Some(pf) = partition_by {
        let mut partitions: Vec<(Option<Bson>, Vec<usize>)> = Vec::new();
        for (idx, doc) in input.iter().enumerate() {
            let key = doc.get(pf).cloned();
            match partitions.iter_mut().find(|(k, _)| match (&key, k) {
                (Some(a), Some(b)) => crate::operators::bson_eq(a, b),
                (None, None) => true, _ => false,
            }) {
                Some((_, indices)) => indices.push(idx),
                None => partitions.push((key, vec![idx])),
            }
        }
        partitions
    } else {
        vec![(None, (0..input.len()).collect())]
    }
}

fn docs_equal(a: &Document, b: &Document) -> bool {
    a.iter().all(|(k, v)| b.get(k).map(|bv| crate::operators::bson_eq(v, bv)).unwrap_or(false))
        && b.iter().all(|(k, v)| a.get(k).map(|av| crate::operators::bson_eq(v, av)).unwrap_or(false))
}

fn sort_keys_equal(a: &Document, b: &Document, sort_keys: Option<&Vec<(String, SortDirection)>>) -> bool {
    match sort_keys {
        None => docs_equal(a, b),
        Some(keys) => {
            if keys.is_empty() { return docs_equal(a, b); }
            keys.iter().all(|(field, _)| {
                let va = a.get(field);
                let vb = b.get(field);
                match (va, vb) {
                    (Some(a), Some(b)) => crate::operators::bson_eq(a, b),
                    (None, None) => true,
                    _ => false,
                }
            })
        }
    }
}

fn process_set_window_fields(mut input: Vec<Document>, partition_by: Option<&String>, sort: Option<&Vec<(String, SortDirection)>>, output: &[(String, WindowFunction)]) -> Vec<Document> {
    if input.is_empty() { return input; }
    if let Some(sort_spec) = sort { sort_documents(&mut input, sort_spec); }
    let partitions = partition_documents(&input, partition_by);

    for (_pk, indices) in &partitions {
        for (field_name, wf) in output {
            match wf {
                WindowFunction::RowNumber => {
                    for (rn, &idx) in indices.iter().enumerate() {
                        input[idx].insert(field_name.clone(), Bson::Int64(rn as i64 + 1));
                    }
                }
                WindowFunction::Rank => {
                    let mut i = 0;
                    while i < indices.len() {
                        let mut j = i + 1;
                        while j < indices.len() && sort_keys_equal(&input[indices[i]], &input[indices[j]], sort) { j += 1; }
                        let rank = i as i64 + 1;
                        for k in i..j { input[indices[k]].insert(field_name.clone(), Bson::Int64(rank)); }
                        i = j;
                    }
                }
                WindowFunction::DenseRank => {
                    let mut i = 0; let mut cur_rank = 1i64;
                    while i < indices.len() {
                        let mut j = i + 1;
                        while j < indices.len() && sort_keys_equal(&input[indices[i]], &input[indices[j]], sort) { j += 1; }
                        for k in i..j { input[indices[k]].insert(field_name.clone(), Bson::Int64(cur_rank)); }
                        cur_rank += 1; i = j;
                    }
                }
                WindowFunction::RunningTotal(field) => {
                    let mut sum: f64 = 0.0;
                    for &idx in indices {
                        if let Some(v) = input[idx].get(field).and_then(Bson::as_f64) { sum += v; }
                        let r = if sum.fract() == 0.0 && sum.abs() < i64::MAX as f64 { Bson::Int64(sum as i64) } else { Bson::Double(sum) };
                        input[idx].insert(field_name.clone(), r);
                    }
                }
                WindowFunction::ExpMovingAvg { input: src, alpha } => {
                    let a = alpha.unwrap_or(2.0 / 3.0);
                    let mut avg: Option<f64> = None;
                    for &idx in indices {
                        if let Some(v) = input[idx].get(src).and_then(Bson::as_f64) {
                            avg = Some(match avg { None => v, Some(p) => a * v + (1.0 - a) * p });
                        }
                        input[idx].insert(field_name.clone(), avg.map(Bson::Double).unwrap_or(Bson::Null));
                    }
                }
                WindowFunction::Sum(field) => {
                    let mut sum: f64 = 0.0;
                    for &idx in indices {
                        if let Some(v) = input[idx].get(field).and_then(Bson::as_f64) { sum += v; }
                        let r = if sum.fract() == 0.0 && sum.abs() < i64::MAX as f64 { Bson::Int64(sum as i64) } else { Bson::Double(sum) };
                        input[idx].insert(field_name.clone(), r);
                    }
                }
                WindowFunction::Push(field) => {
                    let arr: Vec<Bson> = indices.iter().filter_map(|&i| input[i].get(field).cloned()).collect();
                    for &idx in indices { input[idx].insert(field_name.clone(), Bson::Array(arr.clone())); }
                }
                WindowFunction::Min(field) => {
                    let mv = indices.iter().filter_map(|&i| input[i].get(field))
                        .min_by(|a, b| crate::operators::bson_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal))
                        .cloned().unwrap_or(Bson::Null);
                    for &idx in indices { input[idx].insert(field_name.clone(), mv.clone()); }
                }
                WindowFunction::Max(field) => {
                    let mv = indices.iter().filter_map(|&i| input[i].get(field))
                        .max_by(|a, b| crate::operators::bson_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal))
                        .cloned().unwrap_or(Bson::Null);
                    for &idx in indices { input[idx].insert(field_name.clone(), mv.clone()); }
                }
                WindowFunction::Avg(field) => {
                    let (sum, count): (f64, u64) = indices.iter().filter_map(|&i| input[i].get(field))
                        .filter_map(Bson::as_f64).fold((0.0, 0), |(s, c), v| (s + v, c + 1));
                    let av = if count == 0 { Bson::Double(f64::NAN) } else { Bson::Double(sum / count as f64) };
                    for &idx in indices { input[idx].insert(field_name.clone(), av.clone()); }
                }
            }
        }
    }
    input
}

// ─── Sample ───────────────────────────────────────────────────────────────────

fn process_sample(input: Vec<Document>, size: i64) -> Vec<Document> {
    let n = size.max(0) as usize;
    if n >= input.len() { return input; }
    let mut indexed: Vec<(u64, Document)> = input.into_iter().enumerate()
        .map(|(i, d)| { let mut h = i as u64; h = h.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (h, d) }).collect();
    indexed.sort_by_key(|(h, _)| *h);
    indexed.into_iter().take(n).map(|(_, d)| d).collect()
}

// ─── Densify ──────────────────────────────────────────────────────────────────

fn partition_documents_by_key(input: &[Document], partition_by: Option<&String>) -> Vec<(Option<Bson>, Vec<Document>)> {
    if let Some(pf) = partition_by {
        let mut partitions: Vec<(Option<Bson>, Vec<Document>)> = Vec::new();
        for doc in input {
            let key = doc.get(pf).cloned();
            match partitions.iter_mut().find(|(k, _)| match (&key, k) {
                (Some(a), Some(b)) => crate::operators::bson_eq(a, b), (None, None) => true, _ => false,
            }) {
                Some((_, docs)) => docs.push(doc.clone()),
                None => partitions.push((key, vec![doc.clone()])),
            }
        }
        partitions
    } else { vec![(None, input.to_vec())] }
}

fn process_densify(input: Vec<Document>, field: &str, range: &DensifyRange, partition_by: Option<&String>) -> Vec<Document> {
    if input.is_empty() { return input; }
    match range {
        DensifyRange::Step(step) => {
            let step_val = step.as_f64().unwrap_or(1.0);
            let mut result = Vec::new();
            for (_pk, mut docs) in partition_documents_by_key(&input, partition_by) {
                docs.sort_by(|a, b| a.get(field).and_then(Bson::as_f64).unwrap_or(0.0)
                    .partial_cmp(&b.get(field).and_then(Bson::as_f64).unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal));
                if docs.is_empty() { continue; }
                let first = docs[0].get(field).and_then(Bson::as_f64).unwrap_or(0.0);
                let last = docs.last().unwrap().get(field).and_then(Bson::as_f64).unwrap_or(0.0);
                let mut cur = first;
                while cur <= last + step_val * 0.5 {
                    if let Some(doc) = docs.iter().find(|d| d.get(field).and_then(Bson::as_f64).map(|v| (v - cur).abs() < f64::EPSILON).unwrap_or(false)) {
                        result.push(doc.clone());
                    } else {
                        let mut new_doc = Document::new();
                        new_doc.insert(field.to_owned(), Bson::Double(cur));
                        if let Some(pf) = partition_by { if let Some(v) = docs[0].get(pf) { new_doc.insert(pf.clone(), v.clone()); } }
                        result.push(new_doc);
                    }
                    cur += step_val;
                }
            }
            result
        }
        DensifyRange::Bounds { .. } => input,
    }
}

// ─── Fill ─────────────────────────────────────────────────────────────────────

fn process_fill(input: Vec<Document>, sort: &[(String, SortDirection)], partition_by: Option<&String>, output: &Document) -> Vec<Document> {
    if input.is_empty() { return input; }
    let mut result = Vec::new();
    for (_pk, mut docs) in partition_documents_by_key(&input, partition_by) {
        sort_documents(&mut docs, sort);
        for (field_name, fill_spec) in output.iter() {
            let method = fill_spec.as_document().and_then(|d| {
                if d.contains_key("value") { Some("value") }
                else if d.contains_key("linear") { Some("linear") }
                else { Some("prev") }
            }).unwrap_or("prev");
            match method {
                "value" => {
                    let fv = fill_spec.as_document().and_then(|d| d.get("value")).cloned().unwrap_or(Bson::Null);
                    for doc in &mut docs {
                        if !doc.contains_key(field_name) || doc.get(field_name).map(|v| v.is_nullish()).unwrap_or(true) {
                            doc.insert(field_name.clone(), fv.clone());
                        }
                    }
                }
                "linear" => {
                    let len = docs.len(); let mut i = 0;
                    while i < len {
                        if docs[i].get(field_name).map(|v| v.is_nullish()).unwrap_or(true) {
                            let start = if i > 0 { docs[i-1].get(field_name).and_then(Bson::as_f64) } else { None };
                            let mut end_idx = i; let mut end_val: Option<f64> = None;
                            for j in (i+1)..len { if let Some(v) = docs[j].get(field_name).and_then(Bson::as_f64) { end_val = Some(v); end_idx = j; break; } }
                            if let (Some(sv), Some(ev)) = (start, end_val) {
                                let gap = (end_idx - i + 1) as f64;
                                for k in i..end_idx {
                                    let t = (k - i + 1) as f64 / gap;
                                    docs[k].insert(field_name.to_owned(), Bson::Double(sv + t * (ev - sv)));
                                }
                            }
                            i = end_idx + 1;
                        } else { i += 1; }
                    }
                }
                _ => {
                    let mut last_valid: Option<Bson> = None;
                    for doc in &mut docs {
                        match doc.get(field_name).cloned() {
                            Some(v) if !v.is_nullish() => { last_valid = Some(v); }
                            _ => { if let Some(ref v) = last_valid { doc.insert(field_name.clone(), v.clone()); } }
                        }
                    }
                }
            }
        }
        result.extend(docs);
    }
    result
}

// ─── Redact ───────────────────────────────────────────────────────────────────

fn process_redact(input: Vec<Document>, condition: &Document) -> Vec<Document> {
    input.into_iter().filter_map(|mut doc| {
        if evaluate_redact_condition(&doc, condition) { Some(doc) }
        else if should_descend_and_keep(&doc, condition) { redact_descend(&mut doc, condition); Some(doc) }
        else { None }
    }).collect()
}

fn evaluate_redact_condition(doc: &Document, condition: &Document) -> bool {
    for (key, val) in condition.iter() {
        match key {
            "$eq" => { if let Some(arr) = val.as_array() { if arr.len() == 2 { let l = evaluate_redact_value(doc, &arr[0]); let r = evaluate_redact_value(doc, &arr[1]); if l != r { return false; } } } }
            "$gt" => { if let Some(arr) = val.as_array() { if arr.len() == 2 { let l = evaluate_redact_value(doc, &arr[0]); let r = evaluate_redact_value(doc, &arr[1]); match (&l, &r) { (Some(lv), Some(rv)) => { if crate::operators::bson_cmp(lv, rv) != Some(std::cmp::Ordering::Greater) { return false; } } _ => { return false; } } } } }
            "$lt" => { if let Some(arr) = val.as_array() { if arr.len() == 2 { let l = evaluate_redact_value(doc, &arr[0]); let r = evaluate_redact_value(doc, &arr[1]); if let (Some(lv), Some(rv)) = (&l, &r) { if crate::operators::bson_cmp(lv, rv) != Some(std::cmp::Ordering::Less) { return false; } } } } }
            "$and" => { if let Some(arr) = val.as_array() { for sub in arr { if let Some(sd) = sub.as_document() { if !evaluate_redact_condition(doc, sd) { return false; } } } } }
            "$or" => { if let Some(arr) = val.as_array() { if !arr.iter().any(|s| s.as_document().map_or(false, |sd| evaluate_redact_condition(doc, sd))) { return false; } } }
            _ => {
                let dv = doc.get(key);
                match val {
                    Bson::Boolean(true) => { if dv.is_none() { return false; } }
                    Bson::Boolean(false) => { if dv.is_some() { return false; } }
                    Bson::String(s) => { if !dv.map_or(false, |v| crate::operators::bson_eq(v, &Bson::String(s.clone()))) { return false; } }
                    _ => {}
                }
            }
        }
    }
    true
}

fn evaluate_redact_value(doc: &Document, val: &Bson) -> Option<Bson> {
    match val { Bson::String(s) if s.starts_with('$') => doc.get(&s[1..]).cloned(), _ => Some(val.clone()) }
}

fn should_descend_and_keep(doc: &Document, condition: &Document) -> bool {
    doc.iter().any(|(_, v)| matches!(v, Bson::Document(sd) if evaluate_redact_condition(sd, condition)))
}

fn redact_descend(doc: &mut Document, condition: &Document) {
    let mut to_remove = Vec::new();
    let mut to_insert: Vec<(String, Bson)> = Vec::new();
    for (key, val) in doc.iter() {
        match val {
            Bson::Document(sd) => { if !evaluate_redact_condition(sd, condition) { to_remove.push(key.to_owned()); } }
            Bson::Array(arr) => {
                let filtered: Vec<Bson> = arr.iter().filter(|i| match i {
                    Bson::Document(d) => evaluate_redact_condition(d, condition), _ => true
                }).cloned().collect();
                to_insert.push((key.to_owned(), Bson::Array(filtered)));
            }
            _ => {}
        }
    }
    for key in to_remove { doc.remove(&key); }
    for (k, v) in to_insert { doc.insert(k, v); }
}

// ─── SortByCount ──────────────────────────────────────────────────────────────

fn process_sort_by_count(input: Vec<Document>, field: &str) -> Vec<Document> {
    let mut groups: Vec<(Bson, usize)> = Vec::new();
    for doc in &input {
        let key = doc.get(field).cloned().unwrap_or(Bson::Null);
        match groups.iter_mut().find(|(k, _)| crate::operators::bson_eq(k, &key)) {
            Some((_, c)) => *c += 1,
            None => groups.push((key, 1)),
        }
    }
    groups.sort_by(|a, b| b.1.cmp(&a.1));
    groups.into_iter().map(|(key, count)| {
        let mut doc = Document::new(); doc.insert("_id", key); doc.insert("count", Bson::Int64(count as i64)); doc
    }).collect()
}

// ─── Facet ────────────────────────────────────────────────────────────────────

fn process_facet(input: Vec<Document>, facets: &[(String, Pipeline)]) -> Vec<Document> {
    if input.is_empty() { return vec![Document::new()]; }
    let mut result = Document::new();
    for (name, pipeline) in facets {
        let output = pipeline.execute(input.clone());
        result.insert(name.clone(), Bson::Array(output.into_iter().map(Bson::Document).collect()));
    }
    vec![result]
}

// ─── Main process dispatch ────────────────────────────────────────────────────

impl PipelineStage {
    pub fn process(&self, input: Vec<Document>) -> Vec<Document> {
        match self {
            PipelineStage::Match(f) => input.into_iter().filter(|d| document_matches_filter(d, f)).collect(),
            PipelineStage::Group { id, accumulators } => process_group(input, id, accumulators),
            PipelineStage::Sort(spec) => { let mut d = input; sort_documents(&mut d, spec); d }
            PipelineStage::Project(proj) => input.into_iter().map(|d| project_document(&d, proj)).collect(),
            PipelineStage::Limit(n) => input.into_iter().take((*n).max(0) as usize).collect(),
            PipelineStage::Skip(n) => input.into_iter().skip((*n).max(0) as usize).collect(),
            PipelineStage::Unwind { field, preserve_null_and_empty, include_array_index } => process_unwind(input, field, *preserve_null_and_empty, include_array_index),
            PipelineStage::AddFields(fields) => { let mut r = Vec::new(); for mut d in input { for (k, v) in fields.iter() { d.insert(k.to_owned(), v.clone()); } r.push(d); } r }
            PipelineStage::Count(name) => { let mut doc = Document::new(); doc.insert(name.clone(), Bson::Int64(input.len() as i64)); vec![doc] }
            PipelineStage::Bucket { group_by, boundaries, default, output } => process_bucket(input, group_by, boundaries, default, output.as_ref()),
            PipelineStage::Lookup { from: _, local_field, foreign_field, as_field } => process_lookup_basic(input, local_field, foreign_field, as_field),
            PipelineStage::LookupPipeline { from: _, let_vars: _, pipeline: _, as_field } => {
                // 简化实现：插入空数组
                let mut r = Vec::new();
                for mut d in input {
                    d.insert(as_field.clone(), Bson::Array(vec![]));
                    r.push(d);
                }
                r
            }
            PipelineStage::Out(_) => {
                // $out：写入外部集合，在服务端处理
                input
            }
            PipelineStage::Merge { into_collection: _, when_matched: _, when_not_matched: _ } => {
                // $merge：合并到外部集合，在服务端处理
                input
            }
            PipelineStage::GraphLookup { from: _, start_with: _, connect_from_field, connect_to_field, as_field, max_depth: _, depth_field: _ } => {
                // 简化实现：递归图遍历
                process_graph_lookup(&input, as_field, connect_from_field, connect_to_field)
            }
            PipelineStage::UnionWith { collection: _, pipeline: _ } => {
                // $unionWith：合并另一个集合的数据，在服务端处理
                input
            }
            PipelineStage::SetWindowFields { partition_by, sort, output } => process_set_window_fields(input, partition_by.as_ref(), sort.as_ref(), output),
            PipelineStage::Sample { size } => process_sample(input, *size),
            PipelineStage::Densify { field, range, partition_by } => process_densify(input, field, range, partition_by.as_ref()),
            PipelineStage::Fill { sort, partition_by, output } => process_fill(input, sort, partition_by.as_ref(), output),
            PipelineStage::Redact(cond) => process_redact(input, cond),
            PipelineStage::SortByCount(field) => process_sort_by_count(input, field),
            PipelineStage::Facet(facets) => process_facet(input, facets),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_doc(id: i32, name: &str, age: i32) -> Document {
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(id));
        doc.insert("name", Bson::String(name.into()));
        doc.insert("age", Bson::Int32(age));
        doc
    }

    fn make_doc_with_dept(id: i32, name: &str, dept: &str, salary: i32) -> Document {
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(id));
        doc.insert("name", Bson::String(name.into()));
        doc.insert("department", Bson::String(dept.into()));
        doc.insert("salary", Bson::Int32(salary));
        doc
    }

    fn sample_docs() -> Vec<Document> {
        vec![
            make_doc(1, "Alice", 30),
            make_doc(2, "Bob", 25),
            make_doc(3, "Charlie", 35),
            make_doc(4, "Diana", 25),
        ]
    }

    // ─── Basic pipeline tests ──────────────────────────────────────────────

    #[test]
    fn pipeline_empty() {
        let pipeline = Pipeline::new();
        let result = pipeline.execute(sample_docs());
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn match_stage() {
        let mut filter = Document::new();
        filter.insert("age", Bson::Int32(25));
        let stage = PipelineStage::Match(filter);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|d| d.get("age").and_then(Bson::as_i64) == Some(25)));
    }

    #[test]
    fn match_stage_range() {
        let mut filter = Document::new();
        let mut inner = Document::new();
        inner.insert("$gte", Bson::Int32(30));
        filter.insert("age", Bson::Document(inner));
        let stage = PipelineStage::Match(filter);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn sort_stage_ascending() {
        let stage = PipelineStage::Sort(vec![("age".into(), SortDirection::Ascending)]);
        let result = stage.process(sample_docs());
        let ages: Vec<i64> = result.iter().filter_map(|d| d.get("age").and_then(Bson::as_i64)).collect();
        assert_eq!(ages, vec![25, 25, 30, 35]);
    }

    #[test]
    fn sort_stage_descending() {
        let stage = PipelineStage::Sort(vec![("age".into(), SortDirection::Descending)]);
        let result = stage.process(sample_docs());
        let ages: Vec<i64> = result.iter().filter_map(|d| d.get("age").and_then(Bson::as_i64)).collect();
        assert_eq!(ages, vec![35, 30, 25, 25]);
    }

    #[test]
    fn limit_stage() {
        let stage = PipelineStage::Limit(2);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn limit_stage_zero() {
        let stage = PipelineStage::Limit(0);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn skip_stage() {
        let stage = PipelineStage::Skip(2);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn skip_stage_more_than_len() {
        let stage = PipelineStage::Skip(100);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn count_stage() {
        let stage = PipelineStage::Count("total".into());
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("total").and_then(Bson::as_i64), Some(4));
    }

    #[test]
    fn add_fields_stage() {
        let mut fields = Document::new();
        fields.insert("extra", Bson::String("hello".into()));
        let stage = PipelineStage::AddFields(fields);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].get("extra").and_then(Bson::as_str), Some("hello"));
    }

    // ─── Project tests ─────────────────────────────────────────────────────

    #[test]
    fn project_stage_inclusion() {
        let mut proj = Document::new();
        proj.insert("name", Bson::Int32(1));
        let stage = PipelineStage::Project(proj);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
        for doc in &result {
            assert!(doc.get("name").is_some());
            assert!(doc.get("age").is_none());
        }
    }

    #[test]
    fn project_stage_exclusion() {
        let mut proj = Document::new();
        proj.insert("age", Bson::Int32(0));
        let stage = PipelineStage::Project(proj);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
        for doc in &result {
            assert!(doc.get("name").is_some());
            assert!(doc.get("age").is_none());
        }
    }

    #[test]
    fn project_computed_concat() {
        let mut proj = Document::new();
        proj.insert("_id", Bson::Int32(1));
        let mut concat_expr = Document::new();
        concat_expr.insert("$concat", Bson::Array(vec![
            Bson::String("$name".into()),
            Bson::String(" is ".into()),
            Bson::String("$age".into()),
        ]));
        proj.insert("fullName", Bson::Document(concat_expr));
        let stage = PipelineStage::Project(proj);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].get("fullName").and_then(Bson::as_str), Some("Alice is 30"));
    }

    #[test]
    fn project_computed_add() {
        let mut proj = Document::new();
        proj.insert("_id", Bson::Int32(1));
        let mut add_expr = Document::new();
        add_expr.insert("$add", Bson::Array(vec![Bson::String("$age".into()), Bson::Int32(10)]));
        proj.insert("agePlus10", Bson::Document(add_expr));
        let stage = PipelineStage::Project(proj);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].get("agePlus10").and_then(Bson::as_i64), Some(40));
    }

    #[test]
    fn project_computed_multiply() {
        let mut proj = Document::new();
        proj.insert("_id", Bson::Int32(1));
        let mut mul_expr = Document::new();
        mul_expr.insert("$multiply", Bson::Array(vec![Bson::String("$age".into()), Bson::Int32(2)]));
        proj.insert("doubleAge", Bson::Document(mul_expr));
        let stage = PipelineStage::Project(proj);
        let result = stage.process(sample_docs());
        assert_eq!(result[0].get("doubleAge").and_then(Bson::as_i64), Some(60));
    }

    #[test]
    fn project_computed_cond() {
        let mut proj = Document::new();
        proj.insert("_id", Bson::Int32(1));
        let mut cond_expr = Document::new();
        let mut gt_expr = Document::new();
        gt_expr.insert("$gt", Bson::Array(vec![Bson::String("$age".into()), Bson::Int32(28)]));
        cond_expr.insert("$cond", Bson::Array(vec![
            Bson::Document(gt_expr),
            Bson::String("senior".into()),
            Bson::String("junior".into()),
        ]));
        proj.insert("level", Bson::Document(cond_expr));
        let stage = PipelineStage::Project(proj);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].get("level").and_then(Bson::as_str), Some("senior"));
        assert_eq!(result[1].get("level").and_then(Bson::as_str), Some("junior"));
    }

    #[test]
    fn project_computed_if_null() {
        let mut proj = Document::new();
        proj.insert("_id", Bson::Int32(1));
        let mut ifnull_expr = Document::new();
        ifnull_expr.insert("$ifNull", Bson::Array(vec![
            Bson::String("$missing_field".into()),
            Bson::String("default_val".into()),
        ]));
        proj.insert("result", Bson::Document(ifnull_expr));
        let stage = PipelineStage::Project(proj);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].get("result").and_then(Bson::as_str), Some("default_val"));
    }

    #[test]
    fn project_computed_switch() {
        let mut proj = Document::new();
        proj.insert("_id", Bson::Int32(1));
        let mut switch_expr = Document::new();
        let mut case1 = Document::new();
        let mut gt1 = Document::new();
        gt1.insert("$gt", Bson::Array(vec![Bson::String("$age".into()), Bson::Int32(30)]));
        case1.insert("case", Bson::Document(gt1));
        case1.insert("then", Bson::String("older".into()));
        switch_expr.insert("$switch", Bson::Document({
            let mut d = Document::new();
            d.insert("branches", Bson::Array(vec![Bson::Document(case1)]));
            d.insert("default", Bson::String("younger".into()));
            d
        }));
        proj.insert("category", Bson::Document(switch_expr));
        let stage = PipelineStage::Project(proj);
        let result = stage.process(sample_docs());
        assert_eq!(result[0].get("category").and_then(Bson::as_str), Some("younger")); // age=30
        assert_eq!(result[2].get("category").and_then(Bson::as_str), Some("older"));   // age=35
    }

    // ─── Unwind tests ──────────────────────────────────────────────────────

    #[test]
    fn unwind_stage() {
        let mut doc1 = Document::new();
        doc1.insert("_id", Bson::Int32(1));
        doc1.insert("tags", Bson::Array(vec![Bson::String("a".into()), Bson::String("b".into())]));
        let mut doc2 = Document::new();
        doc2.insert("_id", Bson::Int32(2));
        doc2.insert("tags", Bson::Array(vec![Bson::String("c".into())]));
        let stage = PipelineStage::Unwind { field: "tags".into(), preserve_null_and_empty: false, include_array_index: None };
        let result = stage.process(vec![doc1, doc2]);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn unwind_empty_array() {
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("tags", Bson::Array(vec![]));
        let stage = PipelineStage::Unwind { field: "tags".into(), preserve_null_and_empty: false, include_array_index: None };
        let result = stage.process(vec![doc]);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn unwind_empty_array_preserve() {
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("tags", Bson::Array(vec![]));
        let stage = PipelineStage::Unwind { field: "tags".into(), preserve_null_and_empty: true, include_array_index: None };
        let result = stage.process(vec![doc]);
        assert_eq!(result.len(), 1);
        assert!(result[0].get("tags").unwrap().is_nullish());
    }

    #[test]
    fn unwind_missing_field_preserve() {
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        let stage = PipelineStage::Unwind { field: "tags".into(), preserve_null_and_empty: true, include_array_index: None };
        let result = stage.process(vec![doc]);
        assert_eq!(result.len(), 1);
        assert!(result[0].get("tags").unwrap().is_nullish());
    }

    #[test]
    fn unwind_include_array_index() {
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("tags", Bson::Array(vec![Bson::String("a".into()), Bson::String("b".into()), Bson::String("c".into())]));
        let stage = PipelineStage::Unwind { field: "tags".into(), preserve_null_and_empty: false, include_array_index: Some("idx".into()) };
        let result = stage.process(vec![doc]);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].get("idx").and_then(Bson::as_i64), Some(0));
        assert_eq!(result[1].get("idx").and_then(Bson::as_i64), Some(1));
        assert_eq!(result[2].get("idx").and_then(Bson::as_i64), Some(2));
    }

    // ─── Group tests ───────────────────────────────────────────────────────

    #[test]
    fn group_stage_literal_id() {
        let id = GroupId::Literal(Bson::String("all".into()));
        let accs = vec![
            ("count".into(), Accumulator::Count),
            ("total_age".into(), Accumulator::Sum("age".into())),
        ];
        let stage = PipelineStage::Group { id, accumulators: accs };
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("_id").and_then(Bson::as_str), Some("all"));
        assert_eq!(result[0].get("count").and_then(Bson::as_i64), Some(4));
        assert_eq!(result[0].get("total_age").and_then(Bson::as_i64), Some(115));
    }

    #[test]
    fn group_stage_field_id() {
        let id = GroupId::Field("age".into());
        let accs = vec![("names".into(), Accumulator::Push("name".into()))];
        let stage = PipelineStage::Group { id, accumulators: accs };
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn group_avg_accumulator() {
        let docs = vec![make_doc(1, "A", 10), make_doc(2, "B", 20), make_doc(3, "C", 30)];
        let id = GroupId::Literal(Bson::Null);
        let accs = vec![("avg_age".into(), Accumulator::Avg("age".into()))];
        let stage = PipelineStage::Group { id, accumulators: accs };
        let result = stage.process(docs);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("avg_age").and_then(Bson::as_f64), Some(20.0));
    }

    #[test]
    fn group_min_max_accumulators() {
        let docs = vec![make_doc(1, "A", 10), make_doc(2, "B", 30), make_doc(3, "C", 20)];
        let id = GroupId::Literal(Bson::Null);
        let accs = vec![
            ("min_age".into(), Accumulator::Min("age".into())),
            ("max_age".into(), Accumulator::Max("age".into())),
        ];
        let stage = PipelineStage::Group { id, accumulators: accs };
        let result = stage.process(docs);
        assert_eq!(result[0].get("min_age").and_then(Bson::as_i64), Some(10));
        assert_eq!(result[0].get("max_age").and_then(Bson::as_i64), Some(30));
    }

    #[test]
    fn group_expression_id() {
        let expr = Expression::Field("department".into());
        let id = GroupId::Expression(expr);
        let accs = vec![("total".into(), Accumulator::Sum("salary".into()))];
        let docs = vec![
            make_doc_with_dept(1, "A", "eng", 100),
            make_doc_with_dept(2, "B", "eng", 200),
            make_doc_with_dept(3, "C", "hr", 150),
        ];
        let stage = PipelineStage::Group { id, accumulators: accs };
        let result = stage.process(docs);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn group_expression_from_bson() {
        let val = Bson::Document({
            let mut d = Document::new();
            d.insert("$year", Bson::String("$date".into()));
            d
        });
        let id = GroupId::from_bson(&val);
        assert!(matches!(id, GroupId::Expression(Expression::Year(_))));
    }

    // ─── Bucket tests ──────────────────────────────────────────────────────

    #[test]
    fn bucket_stage() {
        let docs = vec![
            make_doc(1, "A", 15),
            make_doc(2, "B", 25),
            make_doc(3, "C", 35),
            make_doc(4, "D", 45),
        ];
        let boundaries = vec![Bson::Int32(0), Bson::Int32(20), Bson::Int32(40), Bson::Int32(60)];
        let stage = PipelineStage::Bucket { group_by: "age".into(), boundaries, default: None, output: None };
        let result = stage.process(docs);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn bucket_stage_with_default() {
        let docs = vec![make_doc(1, "A", 15), make_doc(2, "B", 100)];
        let boundaries = vec![Bson::Int32(0), Bson::Int32(20)];
        let stage = PipelineStage::Bucket { group_by: "age".into(), boundaries, default: Some(Bson::String("other".into())), output: None };
        let result = stage.process(docs);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn bucket_stage_with_output_sum() {
        let docs = vec![
            make_doc_with_dept(1, "A", "eng", 100),
            make_doc_with_dept(2, "B", "eng", 200),
            make_doc_with_dept(3, "C", "hr", 150),
        ];
        let mut output = Document::new();
        let mut sum_spec = Document::new();
        sum_spec.insert("$sum", Bson::String("$salary".into()));
        output.insert("total_salary", Bson::Document(sum_spec));
        let boundaries = vec![Bson::Int32(0), Bson::Int32(1000)];
        let stage = PipelineStage::Bucket { group_by: "salary".into(), boundaries, default: None, output: Some(output) };
        let result = stage.process(docs);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("total_salary").and_then(Bson::as_i64), Some(450));
    }

    // ─── Lookup tests ──────────────────────────────────────────────────────

    #[test]
    fn lookup_basic() {
        let stage = PipelineStage::Lookup {
            from: "other".into(),
            local_field: "ref_id".into(),
            foreign_field: "_id".into(),
            as_field: "joined".into(),
        };
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("ref_id", Bson::Int32(42));
        let result = stage.process(vec![doc]);
        assert_eq!(result.len(), 1);
        assert!(result[0].get("joined").and_then(Bson::as_array).unwrap().is_empty());
    }

    // ─── Facet tests ───────────────────────────────────────────────────────

    #[test]
    fn facet_stage() {
        let mut facet_pipeline = Pipeline::new();
        facet_pipeline.add_stage(PipelineStage::Limit(2));
        let stage = PipelineStage::Facet(vec![("limited".into(), facet_pipeline)]);
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 1);
        let limited = result[0].get("limited").and_then(Bson::as_array).unwrap();
        assert_eq!(limited.len(), 2);
    }

    // ─── Pipeline chain ────────────────────────────────────────────────────

    #[test]
    fn pipeline_chain() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(PipelineStage::Skip(1));
        pipeline.add_stage(PipelineStage::Limit(2));
        pipeline.add_stage(PipelineStage::Sort(vec![("age".into(), SortDirection::Descending)]));
        let result = pipeline.execute(sample_docs());
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn pipeline_complex() {
        let mut pipeline = Pipeline::new();
        let mut filter = Document::new();
        let mut inner = Document::new();
        inner.insert("$gte", Bson::Int32(25));
        filter.insert("age", Bson::Document(inner));
        pipeline.add_stage(PipelineStage::Match(filter));
        pipeline.add_stage(PipelineStage::Group {
            id: GroupId::Field("age".into()),
            accumulators: vec![("count".into(), Accumulator::Count)],
        });
        pipeline.add_stage(PipelineStage::Sort(vec![("count".into(), SortDirection::Descending)]));
        let result = pipeline.execute(sample_docs());
        assert_eq!(result.len(), 3);
    }

    // ─── SetWindowFields tests ─────────────────────────────────────────────

    #[test]
    fn window_row_number() {
        let stage = PipelineStage::SetWindowFields {
            partition_by: None,
            sort: Some(vec![("age".into(), SortDirection::Ascending)]),
            output: vec![("rowNum".into(), WindowFunction::RowNumber)],
        };
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
        let row_nums: Vec<i64> = result.iter().filter_map(|d| d.get("rowNum").and_then(Bson::as_i64)).collect();
        assert_eq!(row_nums, vec![1, 2, 3, 4]);
    }

    #[test]
    fn window_rank() {
        let mut d1 = make_doc(1, "A", 10);
        let mut d2 = make_doc(2, "B", 10);
        let mut d3 = make_doc(3, "C", 20);
        d1.insert("score", Bson::Int32(100));
        d2.insert("score", Bson::Int32(100));
        d3.insert("score", Bson::Int32(200));
        let stage = PipelineStage::SetWindowFields {
            partition_by: None,
            sort: Some(vec![("score".into(), SortDirection::Ascending)]),
            output: vec![("r".into(), WindowFunction::Rank)],
        };
        let result = stage.process(vec![d1, d2, d3]);
        assert_eq!(result[0].get("r").and_then(Bson::as_i64), Some(1));
        assert_eq!(result[1].get("r").and_then(Bson::as_i64), Some(1));
        assert_eq!(result[2].get("r").and_then(Bson::as_i64), Some(3));
    }

    #[test]
    fn window_dense_rank() {
        let mut d1 = make_doc(1, "A", 10);
        let mut d2 = make_doc(2, "B", 10);
        let mut d3 = make_doc(3, "C", 20);
        d1.insert("score", Bson::Int32(100));
        d2.insert("score", Bson::Int32(100));
        d3.insert("score", Bson::Int32(200));
        let stage = PipelineStage::SetWindowFields {
            partition_by: None,
            sort: Some(vec![("score".into(), SortDirection::Ascending)]),
            output: vec![("dr".into(), WindowFunction::DenseRank)],
        };
        let result = stage.process(vec![d1, d2, d3]);
        assert_eq!(result[0].get("dr").and_then(Bson::as_i64), Some(1));
        assert_eq!(result[1].get("dr").and_then(Bson::as_i64), Some(1));
        assert_eq!(result[2].get("dr").and_then(Bson::as_i64), Some(2));
    }

    #[test]
    fn window_running_total() {
        let stage = PipelineStage::SetWindowFields {
            partition_by: None,
            sort: Some(vec![("age".into(), SortDirection::Ascending)]),
            output: vec![("rt".into(), WindowFunction::RunningTotal("age".into()))],
        };
        let result = stage.process(sample_docs());
        let totals: Vec<i64> = result.iter().filter_map(|d| d.get("rt").and_then(Bson::as_i64)).collect();
        assert_eq!(totals, vec![25, 50, 80, 115]);
    }

    #[test]
    fn window_exp_moving_avg() {
        let stage = PipelineStage::SetWindowFields {
            partition_by: None,
            sort: Some(vec![("age".into(), SortDirection::Ascending)]),
            output: vec![("ema".into(), WindowFunction::ExpMovingAvg { input: "age".into(), alpha: Some(0.5) })],
        };
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
        // First value should be the raw value
        let first_ema = result[0].get("ema").and_then(Bson::as_f64).unwrap();
        assert!((first_ema - 25.0).abs() < 0.01);
    }

    #[test]
    fn window_partition_by() {
        let stage = PipelineStage::SetWindowFields {
            partition_by: Some("age".into()),
            sort: Some(vec![("name".into(), SortDirection::Ascending)]),
            output: vec![("rowNum".into(), WindowFunction::RowNumber)],
        };
        let result = stage.process(sample_docs());
        // Age 25 has 2 docs, age 30 has 1, age 35 has 1
        let age25_rows: Vec<i64> = result.iter()
            .filter(|d| d.get("age").and_then(Bson::as_i64) == Some(25))
            .filter_map(|d| d.get("rowNum").and_then(Bson::as_i64))
            .collect();
        assert_eq!(age25_rows, vec![1, 2]);
    }

    #[test]
    fn window_sum_in_partition() {
        let stage = PipelineStage::SetWindowFields {
            partition_by: Some("department".into()),
            sort: None,
            output: vec![("dept_sum".into(), WindowFunction::Sum("salary".into()))],
        };
        let docs = vec![
            make_doc_with_dept(1, "A", "eng", 100),
            make_doc_with_dept(2, "B", "eng", 200),
            make_doc_with_dept(3, "C", "hr", 150),
        ];
        let result = stage.process(docs);
        let eng_sums: Vec<i64> = result.iter()
            .filter(|d| d.get("department").and_then(Bson::as_str) == Some("eng"))
            .filter_map(|d| d.get("dept_sum").and_then(Bson::as_i64))
            .collect();
        // Running sum: 100, 300
        assert_eq!(eng_sums, vec![100, 300]);
        let hr_sums: Vec<i64> = result.iter()
            .filter(|d| d.get("department").and_then(Bson::as_str) == Some("hr"))
            .filter_map(|d| d.get("dept_sum").and_then(Bson::as_i64))
            .collect();
        assert_eq!(hr_sums, vec![150]);
    }

    // ─── Sample tests ──────────────────────────────────────────────────────

    #[test]
    fn sample_stage() {
        let stage = PipelineStage::Sample { size: 2 };
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn sample_stage_larger_than_input() {
        let stage = PipelineStage::Sample { size: 100 };
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn sample_stage_zero() {
        let stage = PipelineStage::Sample { size: 0 };
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 0);
    }

    // ─── Densify tests ─────────────────────────────────────────────────────

    #[test]
    fn densify_step() {
        let mut d1 = Document::new();
        d1.insert("ts", Bson::Int32(1));
        d1.insert("val", Bson::Int32(10));
        let mut d2 = Document::new();
        d2.insert("ts", Bson::Int32(3));
        d2.insert("val", Bson::Int32(30));
        let stage = PipelineStage::Densify {
            field: "ts".into(),
            range: DensifyRange::Step(Bson::Int32(1)),
            partition_by: None,
        };
        let result = stage.process(vec![d1, d2]);
        assert_eq!(result.len(), 3); // ts: 1, 2, 3
    }

    // ─── Fill tests ────────────────────────────────────────────────────────

    #[test]
    fn fill_prev() {
        let mut d1 = Document::new();
        d1.insert("ts", Bson::Int32(1));
        d1.insert("val", Bson::Int32(10));
        let mut d2 = Document::new();
        d2.insert("ts", Bson::Int32(2));
        // d2 has no "val" field
        let mut d3 = Document::new();
        d3.insert("ts", Bson::Int32(3));
        d3.insert("val", Bson::Int32(30));

        let mut output = Document::new();
        output.insert("val", Bson::Document(Document::new())); // empty doc defaults to "prev"

        let stage = PipelineStage::Fill {
            sort: vec![("ts".into(), SortDirection::Ascending)],
            partition_by: None,
            output,
        };
        let result = stage.process(vec![d1, d2, d3]);
        assert_eq!(result.len(), 3);
        assert_eq!(result[1].get("val").and_then(Bson::as_i64), Some(10)); // filled with prev
    }

    #[test]
    fn fill_value() {
        let mut d1 = Document::new();
        d1.insert("ts", Bson::Int32(1));
        // d1 has no "val"

        let mut output = Document::new();
        let mut val_spec = Document::new();
        val_spec.insert("value", Bson::Int32(99));
        output.insert("val", Bson::Document(val_spec));

        let stage = PipelineStage::Fill {
            sort: vec![("ts".into(), SortDirection::Ascending)],
            partition_by: None,
            output,
        };
        let result = stage.process(vec![d1]);
        assert_eq!(result[0].get("val").and_then(Bson::as_i64), Some(99));
    }

    // ─── Redact tests ──────────────────────────────────────────────────────

    #[test]
    fn redact_field_exists() {
        let mut d1 = Document::new();
        d1.insert("secret", Bson::String("classified".into()));
        let mut d2 = Document::new();
        d2.insert("name", Bson::String("public".into()));

        let mut condition = Document::new();
        condition.insert("secret", Bson::Boolean(true));

        let stage = PipelineStage::Redact(condition);
        let result = stage.process(vec![d1, d2]);
        assert_eq!(result.len(), 1);
        assert!(result[0].get("secret").is_some());
    }

    #[test]
    fn redact_nested_removal() {
        let mut inner = Document::new();
        inner.insert("level", Bson::Int32(1));
        let mut inner2 = Document::new();
        inner2.insert("level", Bson::Int32(2));

        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("a", Bson::Document(inner));
        doc.insert("b", Bson::Document(inner2));

        let mut condition = Document::new();
        let mut level_check = Document::new();
        level_check.insert("$gt", Bson::Array(vec![Bson::String("$level".into()), Bson::Int32(1)]));
        condition.insert("$and", Bson::Array(vec![Bson::Document(level_check)]));

        let stage = PipelineStage::Redact(condition);
        let result = stage.process(vec![doc]);
        assert_eq!(result.len(), 1);
        // "a" (level=1) should be removed, "b" (level=2) should remain
        assert!(result[0].get("a").is_none());
        assert!(result[0].get("b").is_some());
    }

    // ─── SortByCount tests ─────────────────────────────────────────────────

    #[test]
    fn sort_by_count() {
        let stage = PipelineStage::SortByCount("age".into());
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 3); // ages: 25(2), 30(1), 35(1)
        assert_eq!(result[0].get("_id").and_then(Bson::as_i64), Some(25));
        assert_eq!(result[0].get("count").and_then(Bson::as_i64), Some(2));
        assert_eq!(result[1].get("count").and_then(Bson::as_i64), Some(1));
    }

    // ─── Merge stage tests ─────────────────────────────────────────────────

    #[test]
    fn merge_stage_passthrough() {
        let stage = PipelineStage::Merge {
            into_collection: "target".into(),
            when_matched: MergeWhenMatched::Replace,
            when_not_matched: MergeWhenNotMatched::Insert,
        };
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn merge_defaults() {
        assert_eq!(MergeWhenMatched::default(), MergeWhenMatched::Replace);
        assert_eq!(MergeWhenNotMatched::default(), MergeWhenNotMatched::Insert);
    }

    // ─── GraphLookup stage tests ───────────────────────────────────────────

    #[test]
    fn graph_lookup_passthrough() {
        let stage = PipelineStage::GraphLookup {
            from: "employees".into(),
            start_with: "$managerId".into(),
            connect_from_field: "managerId".into(),
            connect_to_field: "_id".into(),
            as_field: "reportingChain".into(),
            max_depth: Some(5),
            depth_field: Some("depth".into()),
        };
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
    }

    // ─── CollectionSink trait tests ────────────────────────────────────────

    #[test]
    fn collection_sink_trait_object_safety() {
        struct MockSink;
        impl CollectionSink for MockSink {
            fn insert_many(&self, _docs: Vec<Document>) -> Result<(), String> { Ok(()) }
            fn replace_one(&self, _filter: &Document, _replacement: &Document, _upsert: bool) -> Result<(), String> { Ok(()) }
            fn delete_many(&self, _filter: &Document) -> Result<(), String> { Ok(()) }
        }
        let sink: Box<dyn CollectionSink> = Box::new(MockSink);
        assert!(sink.insert_many(vec![]).is_ok());
    }

    // ─── Date expression tests ─────────────────────────────────────────────

    #[test]
    fn group_expression_year() {
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("created_at", Bson::DateTime(1_700_000_000_000)); // 2023-ish

        let expr = Expression::Year("created_at".into());
        let result = evaluate_expression(&doc, &expr);
        assert!(result.is_some());
        if let Some(Bson::Int32(year)) = result {
            assert!(year >= 2020 && year <= 2030);
        }
    }

    #[test]
    fn group_expression_month() {
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("created_at", Bson::DateTime(1_700_000_000_000));

        let expr = Expression::Month("created_at".into());
        let result = evaluate_expression(&doc, &expr);
        assert!(result.is_some());
        if let Some(Bson::Int32(month)) = result {
            assert!(month >= 1 && month <= 12);
        }
    }

    // ─── LookupPipeline stage tests ────────────────────────────────────────

    #[test]
    fn lookup_pipeline_passthrough() {
        let mut inner_pipeline = Pipeline::new();
        inner_pipeline.add_stage(PipelineStage::Limit(5));
        let stage = PipelineStage::LookupPipeline {
            from: "orders".into(),
            let_vars: Document::new(),
            pipeline: inner_pipeline,
            as_field: "orders".into(),
        };
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
    }

    // ─── UnionWith stage tests ─────────────────────────────────────────────

    #[test]
    fn union_with_passthrough() {
        let stage = PipelineStage::UnionWith {
            collection: "other".into(),
            pipeline: None,
        };
        let result = stage.process(sample_docs());
        assert_eq!(result.len(), 4);
    }
}
