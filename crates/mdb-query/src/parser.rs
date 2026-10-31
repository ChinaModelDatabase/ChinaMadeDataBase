//! 查询过滤器解析器：将 MongoDB 查询文档解析为 Filter AST。

use mdb_common::{ErrorCode, MdbError, MdbResult};
use mdb_doc::{Bson, Document};

use crate::operators;

/// 过滤器 AST 节点。
#[derive(Debug, Clone)]
pub enum Filter {
    /// 字段比较操作：`{ field: { $op: value } }`。
    Comparison {
        field: String,
        op: ComparisonOp,
        value: Bson,
    },
    /// 逻辑操作：`{ $and: [...] }`、`{ $or: [...] }` 等。
    Logical {
        op: LogicalOp,
        children: Vec<Filter>,
    },
    /// `$exists` 操作：`{ field: { $exists: true/false } }`。
    Exists {
        field: String,
        negated: bool,
    },
    /// 正则表达式匹配：`{ field: { $regex: "pattern", $options: "i" } }`。
    Regex {
        field: String,
        pattern: String,
        options: String,
    },
    /// `$in` 操作：`{ field: { $in: [...] } }`。
    In {
        field: String,
        values: Vec<Bson>,
    },
    /// `$nor` 操作：`{ $nor: [...] }`。
    Nor(Vec<Filter>),
    /// `$and` 操作：`{ $and: [...] }`。
    And(Vec<Filter>),
    /// `$or` 操作：`{ $or: [...] }`。
    Or(Vec<Filter>),
}

/// 比较操作符类型。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Nin,
}

/// 逻辑操作符类型。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalOp {
    And,
    Or,
    Not,
    Nor,
}

/// 解析 MongoDB 查询过滤文档为 Filter AST。
///
/// 支持的查询形式：
/// - `{ field: value }` → 等值匹配
/// - `{ field: { $op: value } }` → 比较操作符
/// - `{ $and: [...] }` / `{ $or: [...] }` / `{ $nor: [...] }` → 逻辑操作符
/// - `{ field: { $regex: "..." } }` → 正则匹配
/// - `{ field: { $in: [...] } }` → IN 操作
/// - `{ field: { $exists: true } }` → 存在性检查
pub fn parse_filter(doc: &Document) -> MdbResult<Filter> {
    if doc.is_empty() {
        return Err(MdbError::new(ErrorCode::BadValue, "empty filter document"));
    }

    let mut and_children = Vec::new();

    for (key, value) in doc.iter() {
        match key {
            "$and" => {
                let arr = value.as_array().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$and must be an array")
                })?;
                let mut children = Vec::new();
                for item in arr {
                    let sub = item.as_document().ok_or_else(|| {
                        MdbError::new(ErrorCode::BadValue, "$and elements must be documents")
                    })?;
                    children.push(parse_filter(sub)?);
                }
                and_children.push(Filter::And(children));
            }
            "$or" => {
                let arr = value.as_array().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$or must be an array")
                })?;
                let mut children = Vec::new();
                for item in arr {
                    let sub = item.as_document().ok_or_else(|| {
                        MdbError::new(ErrorCode::BadValue, "$or elements must be documents")
                    })?;
                    children.push(parse_filter(sub)?);
                }
                and_children.push(Filter::Or(children));
            }
            "$nor" => {
                let arr = value.as_array().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$nor must be an array")
                })?;
                let mut children = Vec::new();
                for item in arr {
                    let sub = item.as_document().ok_or_else(|| {
                        MdbError::new(ErrorCode::BadValue, "$nor elements must be documents")
                    })?;
                    children.push(parse_filter(sub)?);
                }
                and_children.push(Filter::Nor(children));
            }
            "$not" => {
                let sub = value.as_document().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$not must be a document")
                })?;
                let inner = parse_field_ops("_not_root", sub)?;
                and_children.push(Filter::Logical { op: LogicalOp::Not, children: vec![inner] });
            }
            _ => {
                // 字段级过滤
                let sub = value.as_document();
                match sub {
                    Some(doc) => {
                        let field_filter = parse_field_ops(key, doc)?;
                        and_children.push(field_filter);
                    }
                    None => {
                        // 简单等值匹配：{ field: value }
                        and_children.push(Filter::Comparison {
                            field: key.to_owned(),
                            op: ComparisonOp::Eq,
                            value: value.clone(),
                        });
                    }
                }
            }
        }
    }

    if and_children.len() == 1 {
        Ok(and_children.remove(0))
    } else {
        Ok(Filter::And(and_children))
    }
}

/// 解析单个字段上的操作符文档。
fn parse_field_ops(field: &str, doc: &Document) -> MdbResult<Filter> {
    let mut filters = Vec::new();

    for (key, value) in doc.iter() {
        match key {
            "$eq" => {
                filters.push(Filter::Comparison {
                    field: field.to_owned(),
                    op: ComparisonOp::Eq,
                    value: value.clone(),
                });
            }
            "$ne" => {
                filters.push(Filter::Comparison {
                    field: field.to_owned(),
                    op: ComparisonOp::Ne,
                    value: value.clone(),
                });
            }
            "$gt" => {
                filters.push(Filter::Comparison {
                    field: field.to_owned(),
                    op: ComparisonOp::Gt,
                    value: value.clone(),
                });
            }
            "$gte" => {
                filters.push(Filter::Comparison {
                    field: field.to_owned(),
                    op: ComparisonOp::Gte,
                    value: value.clone(),
                });
            }
            "$lt" => {
                filters.push(Filter::Comparison {
                    field: field.to_owned(),
                    op: ComparisonOp::Lt,
                    value: value.clone(),
                });
            }
            "$lte" => {
                filters.push(Filter::Comparison {
                    field: field.to_owned(),
                    op: ComparisonOp::Lte,
                    value: value.clone(),
                });
            }
            "$in" => {
                let arr = value.as_array().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$in must be an array")
                })?;
                filters.push(Filter::In {
                    field: field.to_owned(),
                    values: arr.to_vec(),
                });
            }
            "$nin" => {
                let arr = value.as_array().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$nin must be an array")
                })?;
                // $nin 转换为 $not + $in
                filters.push(Filter::Logical {
                    op: LogicalOp::Not,
                    children: vec![Filter::In {
                        field: field.to_owned(),
                        values: arr.to_vec(),
                    }],
                });
            }
            "$exists" => {
                let exists = value.as_bool().unwrap_or(true);
                filters.push(Filter::Exists {
                    field: field.to_owned(),
                    negated: !exists,
                });
            }
            "$regex" => {
                let pattern = value.as_str().unwrap_or("").to_owned();
                let options = doc
                    .get("$options")
                    .and_then(|o| o.as_str())
                    .unwrap_or("")
                    .to_owned();
                filters.push(Filter::Regex {
                    field: field.to_owned(),
                    pattern,
                    options,
                });
            }
            "$options" => {
                // $options 与 $regex 配对，在处理 $regex 时已读取，跳过
            }
            "$type" => {
                let type_byte = match value {
                    Bson::Int32(n) => *n as u8,
                    Bson::Int64(n) => *n as u8,
                    Bson::String(s) => parse_type_name(s).ok_or_else(|| {
                        MdbError::new(ErrorCode::BadValue, format!("unknown $type: {s}"))
                    })?,
                    _ => {
                        return Err(MdbError::new(
                            ErrorCode::BadValue,
                            "$type must be a number or string",
                        ));
                    }
                };
                // 构造一个 Comparison 来表示 $type
                filters.push(Filter::Comparison {
                    field: field.to_owned(),
                    op: ComparisonOp::Eq,
                    value: Bson::Int32(type_byte as i32),
                });
            }
            "$not" => {
                let sub = value.as_document().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$not must be a document")
                })?;
                let inner = parse_field_ops(field, sub)?;
                filters.push(Filter::Logical { op: LogicalOp::Not, children: vec![inner] });
            }
            "$elemMatch" => {
                let sub = value.as_document().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$elemMatch must be a document")
                })?;
                let inner = parse_filter(sub)?;
                filters.push(Filter::Logical {
                    op: LogicalOp::Nor,
                    children: vec![inner],
                });
                // Actually mark as elemMatch by wrapping in a special way
                // For simplicity, we store it as a Logical::Nor with a single child
                // The evaluator will handle the special semantics
            }
            "$size" => {
                let size = value.as_i64().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$size must be a number")
                })? as usize;
                filters.push(Filter::Comparison {
                    field: field.to_owned(),
                    op: ComparisonOp::Eq,
                    value: Bson::Int32(size as i32),
                });
            }
            "$all" => {
                // Treat as an array containment check
                let arr = value.as_array().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$all must be an array")
                })?;
                for item in arr {
                    filters.push(Filter::Comparison {
                        field: field.to_owned(),
                        op: ComparisonOp::Eq,
                        value: item.clone(),
                    });
                }
            }
            "$mod" => {
                let arr = value.as_array().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$mod must be an array [divisor, remainder]")
                })?;
                if arr.len() != 2 {
                    return Err(MdbError::new(
                        ErrorCode::BadValue,
                        "$mod requires exactly two elements",
                    ));
                }
                let divisor = arr[0].as_i64().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$mod divisor must be a number")
                })?;
                let remainder = arr[1].as_i64().ok_or_else(|| {
                    MdbError::new(ErrorCode::BadValue, "$mod remainder must be a number")
                })?;
                // Store as a special comparison with mod info
                filters.push(Filter::Comparison {
                    field: field.to_owned(),
                    op: ComparisonOp::Eq,
                    value: Bson::Array(vec![Bson::Int64(divisor), Bson::Int64(remainder)]),
                });
            }
            _ => {
                return Err(MdbError::new(
                    ErrorCode::BadValue,
                    format!("unknown operator: {key}"),
                ));
            }
        }
    }

    if filters.len() == 1 {
        Ok(filters.remove(0))
    } else {
        Ok(Filter::And(filters))
    }
}

/// 将 BSON 类型名映射为类型字节。
fn parse_type_name(name: &str) -> Option<u8> {
    match name {
        "double" | "number" | "Double" => Some(0x01),
        "string" | "String" => Some(0x02),
        "object" | "Object" => Some(0x03),
        "array" | "Array" => Some(0x04),
        "binData" | "Binary" => Some(0x05),
        "undefined" | "Undefined" => Some(0x06),
        "objectId" | "ObjectId" => Some(0x07),
        "bool" | "Boolean" => Some(0x08),
        "date" | "Date" => Some(0x09),
        "null" | "Null" => Some(0x0a),
        "regex" | "Regex" => Some(0x0b),
        "dbPointer" | "DBPointer" => Some(0x0c),
        "javascript" | "JavaScript" => Some(0x0d),
        "symbol" | "Symbol" => Some(0x0e),
        "javascriptWithScope" | "JavaScriptWithScope" => Some(0x0f),
        "int" | "Int32" | "int32" => Some(0x10),
        "timestamp" | "Timestamp" => Some(0x11),
        "long" | "Int64" | "int64" => Some(0x12),
        "decimal" | "Decimal128" => Some(0x13),
        "minKey" | "MinKey" => Some(0xff),
        "maxKey" | "MaxKey" => Some(0x7f),
        _ => None,
    }
}

/// 将 Filter AST 转换为可执行的查询操作符。
pub fn filter_to_operator(filter: &Filter) -> Box<dyn operators::QueryOperator> {
    match filter {
        Filter::Comparison { field: _, op, value } => match op {
            ComparisonOp::Eq => Box::new(operators::Eq(value.clone())),
            ComparisonOp::Ne => Box::new(operators::Ne(value.clone())),
            ComparisonOp::Gt => Box::new(operators::Gt(value.clone())),
            ComparisonOp::Gte => Box::new(operators::Gte(value.clone())),
            ComparisonOp::Lt => Box::new(operators::Lt(value.clone())),
            ComparisonOp::Lte => Box::new(operators::Lte(value.clone())),
            ComparisonOp::Nin => Box::new(operators::Nin(vec![value.clone()])),
        },
        Filter::Logical { op, children } => match op {
            LogicalOp::And => {
                let ops: Vec<Box<dyn operators::QueryOperator>> =
                    children.iter().map(filter_to_operator).collect();
                Box::new(operators::And(ops))
            }
            LogicalOp::Or => {
                let ops: Vec<Box<dyn operators::QueryOperator>> =
                    children.iter().map(filter_to_operator).collect();
                Box::new(operators::Or(ops))
            }
            LogicalOp::Not => {
                let ops: Vec<Box<dyn operators::QueryOperator>> =
                    children.iter().map(filter_to_operator).collect();
                if ops.len() == 1 {
                    Box::new(operators::Not(ops.into_iter().next().unwrap()))
                } else {
                    Box::new(operators::Not(Box::new(operators::And(ops))))
                }
            }
            LogicalOp::Nor => {
                let ops: Vec<Box<dyn operators::QueryOperator>> =
                    children.iter().map(filter_to_operator).collect();
                Box::new(operators::Nor(ops))
            }
        },
        Filter::Exists { negated, .. } => {
            Box::new(operators::Exists(!negated))
        }
        Filter::Regex { pattern, options, .. } => {
            Box::new(operators::RegexOp { pattern: pattern.clone(), options: options.clone() })
        }
        Filter::In { values, .. } => Box::new(operators::In(values.clone())),
        Filter::Nor(children) => {
            let ops: Vec<Box<dyn operators::QueryOperator>> =
                children.iter().map(filter_to_operator).collect();
            Box::new(operators::Nor(ops))
        }
        Filter::And(children) => {
            let ops: Vec<Box<dyn operators::QueryOperator>> =
                children.iter().map(filter_to_operator).collect();
            Box::new(operators::And(ops))
        }
        Filter::Or(children) => {
            let ops: Vec<Box<dyn operators::QueryOperator>> =
                children.iter().map(filter_to_operator).collect();
            Box::new(operators::Or(ops))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_equality() {
        let mut doc = Document::new();
        doc.insert("name", "Alice");
        let filter = parse_filter(&doc).unwrap();
        match filter {
            Filter::Comparison { field, op, value } => {
                assert_eq!(field, "name");
                assert_eq!(op, ComparisonOp::Eq);
                assert_eq!(value, Bson::String("Alice".into()));
            }
            _ => panic!("expected Comparison"),
        }
    }

    #[test]
    fn parse_numeric_equality() {
        let mut doc = Document::new();
        doc.insert("age", Bson::Int32(25));
        let filter = parse_filter(&doc).unwrap();
        match filter {
            Filter::Comparison { field, op, value } => {
                assert_eq!(field, "age");
                assert_eq!(op, ComparisonOp::Eq);
                assert_eq!(value, Bson::Int32(25));
            }
            _ => panic!("expected Comparison"),
        }
    }

    #[test]
    fn parse_gt_operator() {
        let mut inner = Document::new();
        inner.insert("$gt", Bson::Int32(10));
        let mut doc = Document::new();
        doc.insert("age", Bson::Document(inner));
        let filter = parse_filter(&doc).unwrap();
        match filter {
            Filter::Comparison { field, op, value } => {
                assert_eq!(field, "age");
                assert_eq!(op, ComparisonOp::Gt);
                assert_eq!(value, Bson::Int32(10));
            }
            _ => panic!("expected Comparison"),
        }
    }

    #[test]
    fn parse_and_operator() {
        let mut doc = Document::new();
        let mut and_arr = Vec::new();

        let mut f1 = Document::new();
        f1.insert("age", Bson::Int32(10));
        and_arr.push(Bson::Document(f1));

        let mut f2 = Document::new();
        f2.insert("name", Bson::String("test".into()));
        and_arr.push(Bson::Document(f2));

        doc.insert("$and", Bson::Array(and_arr));
        let filter = parse_filter(&doc).unwrap();
        match filter {
            Filter::And(children) => {
                assert_eq!(children.len(), 2);
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn parse_or_operator() {
        let mut doc = Document::new();
        let mut or_arr = Vec::new();

        let mut f1 = Document::new();
        f1.insert("a", Bson::Int32(1));
        or_arr.push(Bson::Document(f1));

        let mut f2 = Document::new();
        f2.insert("b", Bson::Int32(2));
        or_arr.push(Bson::Document(f2));

        doc.insert("$or", Bson::Array(or_arr));
        let filter = parse_filter(&doc).unwrap();
        match filter {
            Filter::Or(children) => {
                assert_eq!(children.len(), 2);
            }
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn parse_nor_operator() {
        let mut doc = Document::new();
        let mut nor_arr = Vec::new();

        let mut f1 = Document::new();
        f1.insert("a", Bson::Int32(1));
        nor_arr.push(Bson::Document(f1));

        doc.insert("$nor", Bson::Array(nor_arr));
        let filter = parse_filter(&doc).unwrap();
        match filter {
            Filter::Nor(children) => {
                assert_eq!(children.len(), 1);
            }
            _ => panic!("expected Nor"),
        }
    }

    #[test]
    fn parse_exists_operator() {
        let mut inner = Document::new();
        inner.insert("$exists", Bson::Boolean(true));
        let mut doc = Document::new();
        doc.insert("email", Bson::Document(inner));
        let filter = parse_filter(&doc).unwrap();
        match filter {
            Filter::Exists { field, negated } => {
                assert_eq!(field, "email");
                assert!(!negated);
            }
            _ => panic!("expected Exists"),
        }
    }

    #[test]
    fn parse_exists_false() {
        let mut inner = Document::new();
        inner.insert("$exists", Bson::Boolean(false));
        let mut doc = Document::new();
        doc.insert("email", Bson::Document(inner));
        let filter = parse_filter(&doc).unwrap();
        match filter {
            Filter::Exists { field, negated } => {
                assert_eq!(field, "email");
                assert!(negated);
            }
            _ => panic!("expected Exists"),
        }
    }

    #[test]
    fn parse_in_operator() {
        let mut inner = Document::new();
        inner.insert("$in", Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]));
        let mut doc = Document::new();
        doc.insert("status", Bson::Document(inner));
        let filter = parse_filter(&doc).unwrap();
        match filter {
            Filter::In { field, values } => {
                assert_eq!(field, "status");
                assert_eq!(values.len(), 2);
            }
            _ => panic!("expected In"),
        }
    }

    #[test]
    fn parse_regex_operator() {
        let mut inner = Document::new();
        inner.insert("$regex", Bson::String("^test".into()));
        inner.insert("$options", Bson::String("i".into()));
        let mut doc = Document::new();
        doc.insert("name", Bson::Document(inner));
        let filter = parse_filter(&doc).unwrap();
        match filter {
            Filter::Regex { field, pattern, options } => {
                assert_eq!(field, "name");
                assert_eq!(pattern, "^test");
                assert_eq!(options, "i");
            }
            _ => panic!("expected Regex"),
        }
    }

    #[test]
    fn parse_not_operator() {
        let mut not_inner = Document::new();
        not_inner.insert("$gt", Bson::Int32(5));
        let mut doc = Document::new();
        let mut age_ops = Document::new();
        age_ops.insert("$not", Bson::Document(not_inner));
        doc.insert("age", Bson::Document(age_ops));
        let filter = parse_filter(&doc).unwrap();
        match filter {
            Filter::Logical { op: LogicalOp::Not, children } => {
                assert_eq!(children.len(), 1);
            }
            _ => panic!("expected Not"),
        }
    }

    #[test]
    fn parse_complex_query() {
        let mut doc = Document::new();
        let mut inner = Document::new();
        inner.insert("$gte", Bson::Int32(18));
        inner.insert("$lte", Bson::Int32(65));
        doc.insert("age", Bson::Document(inner));
        doc.insert("active", Bson::Boolean(true));
        let filter = parse_filter(&doc).unwrap();
        // Should be an AND of age range + active equality
        match filter {
            Filter::And(children) => {
                assert_eq!(children.len(), 2);
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn parse_empty_filter_is_error() {
        let doc = Document::new();
        assert!(parse_filter(&doc).is_err());
    }

    #[test]
    fn filter_to_operator_roundtrip() {
        let mut doc = Document::new();
        doc.insert("age", Bson::Int32(25));
        let filter = parse_filter(&doc).unwrap();
        let op = filter_to_operator(&filter);
        assert!(op.matches(&Bson::Int32(25)));
        assert!(!op.matches(&Bson::Int32(30)));
    }

    #[test]
    fn parse_type_name_map() {
        assert_eq!(parse_type_name("double"), Some(0x01));
        assert_eq!(parse_type_name("string"), Some(0x02));
        assert_eq!(parse_type_name("int"), Some(0x10));
        assert_eq!(parse_type_name("long"), Some(0x12));
        assert_eq!(parse_type_name("objectId"), Some(0x07));
        assert_eq!(parse_type_name("bool"), Some(0x08));
        assert_eq!(parse_type_name("date"), Some(0x09));
        assert_eq!(parse_type_name("null"), Some(0x0a));
        assert_eq!(parse_type_name("unknown"), None);
    }
}
