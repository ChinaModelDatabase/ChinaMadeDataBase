#![forbid(unsafe_code)]

//! 查询操作符：比较、逻辑、元素、求值、数组操作符。
//!
//! 每个操作符实现 `QueryOperator` trait 的 `matches` 方法，
//! 用于对单个 BSON 值进行匹配判断。

use std::fmt;

use mdb_doc::Bson;

/// 查询操作符的公共 trait。
pub trait QueryOperator: fmt::Debug {
    /// 判断 `value` 是否匹配该操作符的条件。
    fn matches(&self, value: &Bson) -> bool;
}

// ─── 比较操作符 ───────────────────────────────────────────────────────────────

/// `$eq`：等于。
#[derive(Debug, Clone)]
pub struct Eq(pub Bson);

impl QueryOperator for Eq {
    fn matches(&self, value: &Bson) -> bool {
        bson_eq(value, &self.0)
    }
}

/// `$ne`：不等于。
#[derive(Debug, Clone)]
pub struct Ne(pub Bson);

impl QueryOperator for Ne {
    fn matches(&self, value: &Bson) -> bool {
        !bson_eq(value, &self.0)
    }
}

/// `$gt`：大于。
#[derive(Debug, Clone)]
pub struct Gt(pub Bson);

impl QueryOperator for Gt {
    fn matches(&self, value: &Bson) -> bool {
        bson_cmp(value, &self.0) == Some(std::cmp::Ordering::Greater)
    }
}

/// `$gte`：大于等于。
#[derive(Debug, Clone)]
pub struct Gte(pub Bson);

impl QueryOperator for Gte {
    fn matches(&self, value: &Bson) -> bool {
        matches!(bson_cmp(value, &self.0), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
    }
}

/// `$lt`：小于。
#[derive(Debug, Clone)]
pub struct Lt(pub Bson);

impl QueryOperator for Lt {
    fn matches(&self, value: &Bson) -> bool {
        bson_cmp(value, &self.0) == Some(std::cmp::Ordering::Less)
    }
}

/// `$lte`：小于等于。
#[derive(Debug, Clone)]
pub struct Lte(pub Bson);

impl QueryOperator for Lte {
    fn matches(&self, value: &Bson) -> bool {
        matches!(bson_cmp(value, &self.0), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))
    }
}

/// `$in`：值在给定列表中。
#[derive(Debug, Clone)]
pub struct In(pub Vec<Bson>);

impl QueryOperator for In {
    fn matches(&self, value: &Bson) -> bool {
        self.0.iter().any(|v| bson_eq(value, v))
    }
}

/// `$nin`：值不在给定列表中。
#[derive(Debug, Clone)]
pub struct Nin(pub Vec<Bson>);

impl QueryOperator for Nin {
    fn matches(&self, value: &Bson) -> bool {
        self.0.iter().all(|v| !bson_eq(value, v))
    }
}

// ─── 逻辑操作符 ───────────────────────────────────────────────────────────────

/// `$and`：所有子条件都满足。
pub struct And(pub Vec<Box<dyn QueryOperator>>);

impl fmt::Debug for And {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("And").field(&self.0.len()).finish()
    }
}

impl QueryOperator for And {
    fn matches(&self, value: &Bson) -> bool {
        self.0.iter().all(|op| op.matches(value))
    }
}

/// `$or`：任一子条件满足。
pub struct Or(pub Vec<Box<dyn QueryOperator>>);

impl fmt::Debug for Or {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Or").field(&self.0.len()).finish()
    }
}

impl QueryOperator for Or {
    fn matches(&self, value: &Bson) -> bool {
        self.0.iter().any(|op| op.matches(value))
    }
}

/// `$not`：取反。
pub struct Not(pub Box<dyn QueryOperator>);

impl fmt::Debug for Not {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Not").field(&*self.0).finish()
    }
}

impl QueryOperator for Not {
    fn matches(&self, value: &Bson) -> bool {
        !self.0.matches(value)
    }
}

/// `$nor`：所有子条件都不满足。
pub struct Nor(pub Vec<Box<dyn QueryOperator>>);

impl fmt::Debug for Nor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Nor").field(&self.0.len()).finish()
    }
}

impl QueryOperator for Nor {
    fn matches(&self, value: &Bson) -> bool {
        self.0.iter().all(|op| !op.matches(value))
    }
}

// ─── 元素操作符 ───────────────────────────────────────────────────────────────

/// `$exists`：字段是否存在。
#[derive(Debug, Clone)]
pub struct Exists(pub bool);

impl QueryOperator for Exists {
    fn matches(&self, value: &Bson) -> bool {
        let exists = !matches!(value, Bson::Null);
        if self.0 { exists } else { !exists }
    }
}

/// `$type`：字段是否为指定 BSON 类型。
///
/// `type_byte` 是目标 BSON 类型码（如 `0x02` 表示字符串）。
#[derive(Debug, Clone)]
pub struct Type(pub u8);

impl QueryOperator for Type {
    fn matches(&self, value: &Bson) -> bool {
        value.type_byte() == self.0
    }
}

// ─── 求值操作符 ───────────────────────────────────────────────────────────────

/// `$regex`：正则表达式匹配。
///
/// `pattern` 为正则模式，`options` 为选项字符串（如 `"i"` 表示忽略大小写）。
#[derive(Debug, Clone)]
pub struct RegexOp {
    pub pattern: String,
    pub options: String,
}

impl QueryOperator for RegexOp {
    fn matches(&self, value: &Bson) -> bool {
        let s = match value {
            Bson::String(s) => s,
            _ => return false,
        };
        let flags = RegexFlags {
            case_insensitive: self.options.contains('i'),
            multiline: self.options.contains('m'),
            dotall: self.options.contains('s'),
            extended: self.options.contains('x'),
        };
        match compile_and_match(&self.pattern, s, &flags) {
            Ok(m) => m,
            Err(_) => false,
        }
    }
}

/// `$mod`：取模运算。`[除数, 余数]`。
#[derive(Debug, Clone)]
pub struct Mod(pub i64, pub i64);

impl QueryOperator for Mod {
    fn matches(&self, value: &Bson) -> bool {
        let n = match value.as_i64() {
            Some(n) => n,
            None => return false,
        };
        let divisor = self.0;
        let remainder = self.1;
        if divisor == 0 {
            return false;
        }
        n % divisor == remainder
    }
}

// ─── 数组操作符 ───────────────────────────────────────────────────────────────

/// `$all`：数组包含所有指定值。
#[derive(Debug, Clone)]
pub struct All(pub Vec<Bson>);

impl QueryOperator for All {
    fn matches(&self, value: &Bson) -> bool {
        let arr = match value.as_array() {
            Some(a) => a,
            None => return false,
        };
        self.0.iter().all(|v| arr.iter().any(|a| bson_eq(a, v)))
    }
}

/// `$elemMatch`：数组中至少有一个元素满足所有给定条件。
pub struct ElemMatch(pub Vec<Box<dyn QueryOperator>>);

impl fmt::Debug for ElemMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ElemMatch").field(&self.0.len()).finish()
    }
}

impl QueryOperator for ElemMatch {
    fn matches(&self, value: &Bson) -> bool {
        let arr = match value.as_array() {
            Some(a) => a,
            None => return false,
        };
        arr.iter().any(|elem| self.0.iter().all(|op| op.matches(elem)))
    }
}

/// `$size`：数组长度等于指定值。
#[derive(Debug, Clone)]
pub struct Size(pub usize);

impl QueryOperator for Size {
    fn matches(&self, value: &Bson) -> bool {
        match value.as_array() {
            Some(a) => a.len() == self.0,
            None => false,
        }
    }
}

// ─── BSON 比较函数 ────────────────────────────────────────────────────────────

/// BSON 相等比较（MongoDB 语义）。
pub fn bson_eq(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(a), Bson::Double(b)) => a == b,
        (Bson::String(a), Bson::String(b)) => a == b,
        (Bson::Document(a), Bson::Document(b)) => a == b,
        (Bson::Array(a), Bson::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| bson_eq(x, y))
        }
        (Bson::Binary(a), Bson::Binary(b)) => a == b,
        (Bson::ObjectId(a), Bson::ObjectId(b)) => a == b,
        (Bson::Boolean(a), Bson::Boolean(b)) => a == b,
        (Bson::DateTime(a), Bson::DateTime(b)) => a == b,
        (Bson::Null, Bson::Null) => true,
        (Bson::Regex(a), Bson::Regex(b)) => a == b,
        (Bson::Int32(a), Bson::Int32(b)) => a == b,
        (Bson::Int64(a), Bson::Int64(b)) => a == b,
        (Bson::Timestamp(a), Bson::Timestamp(b)) => a == b,
        (Bson::Int32(a), Bson::Int64(b)) => i64::from(*a) == *b,
        (Bson::Int64(a), Bson::Int32(b)) => *a == i64::from(*b),
        (Bson::Int32(a), Bson::Double(b)) => f64::from(*a) == *b,
        (Bson::Double(a), Bson::Int32(b)) => *a == f64::from(*b),
        (Bson::Int64(a), Bson::Double(b)) => *a as f64 == *b,
        (Bson::Double(a), Bson::Int64(b)) => *a == *b as f64,
        (Bson::MinKey, Bson::MinKey) => true,
        (Bson::MaxKey, Bson::MaxKey) => true,
        _ => false,
    }
}

/// BSON 排序比较（MongoDB 1.0 排序规则）。
///
/// 返回 `None` 表示两种类型不可比较。
#[inline]
pub fn bson_cmp(a: &Bson, b: &Bson) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;

    // Fast path: MinKey / MaxKey sentinels (check first, they appear in every comparison).
    match (a, b) {
        (Bson::MinKey, _) | (_, Bson::MinKey) => return Some(Ordering::Less),
        (_, Bson::MaxKey) | (Bson::MaxKey, _) => return Some(Ordering::Greater),
        _ => {}
    }

    // Fast path: same discriminant → same-type comparison without 20-arm match.
    if std::mem::discriminant(a) == std::mem::discriminant(b) {
        return match (a, b) {
            (Bson::Null, Bson::Null) => Some(Ordering::Equal),
            (Bson::Int32(x), Bson::Int32(y)) => Some(x.cmp(y)),
            (Bson::Int64(x), Bson::Int64(y)) => Some(x.cmp(y)),
            (Bson::Double(x), Bson::Double(y)) => x.partial_cmp(y),
            (Bson::String(x), Bson::String(y)) => Some(x.as_bytes().cmp(y.as_bytes())),
            (Bson::ObjectId(x), Bson::ObjectId(y)) => Some(x.cmp(y)),
            (Bson::Boolean(x), Bson::Boolean(y)) => Some(x.cmp(y)),
            (Bson::DateTime(x), Bson::DateTime(y)) => Some(x.cmp(y)),
            (Bson::Timestamp(x), Bson::Timestamp(y)) => Some(x.as_u64().cmp(&y.as_u64())),
            (Bson::Document(x), Bson::Document(y)) => Some(compare_documents(x, y)),
            (Bson::Array(x), Bson::Array(y)) => cmp_arrays(x, y),
            // Same discriminant but not handled above → equal (e.g. Undefined, Null, Binary, etc.)
            _ => Some(Ordering::Equal),
        };
    }

    // Cross-type numeric: Int32, Int64, Double share type_order 1.
    let ta = bson_type_order(a);
    let tb = bson_type_order(b);
    if ta != tb {
        return Some(ta.cmp(&tb));
    }

    // Same type_order but different discriminant → numeric cross-type only.
    match (a, b) {
        (Bson::Int32(a), Bson::Int64(b)) => Some(i64::from(*a).cmp(b)),
        (Bson::Int64(a), Bson::Int32(b)) => Some(a.cmp(&i64::from(*b))),
        (Bson::Int32(a), Bson::Double(b)) => f64::from(*a).partial_cmp(b),
        (Bson::Double(a), Bson::Int32(b)) => a.partial_cmp(&f64::from(*b)),
        (Bson::Int64(a), Bson::Double(b)) => (*a as f64).partial_cmp(b),
        (Bson::Double(a), Bson::Int64(b)) => a.partial_cmp(&(*b as f64)),
        _ => Some(Ordering::Equal),
    }
}

#[inline]
fn cmp_arrays(a: &[Bson], b: &[Bson]) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    for (x, y) in a.iter().zip(b.iter()) {
        if let Some(ord) = bson_cmp(x, y) {
            if ord != Ordering::Equal {
                return Some(ord);
            }
        }
    }
    Some(a.len().cmp(&b.len()))
}

fn compare_documents(a: &mdb_doc::Document, b: &mdb_doc::Document) -> std::cmp::Ordering {
    let mut akeys = a.keys();
    let mut bkeys = b.keys();
    loop {
        match (akeys.next(), bkeys.next()) {
            (Some(ak), Some(bk)) => match ak.cmp(bk) {
                std::cmp::Ordering::Equal => {
                    if let (Some(av), Some(bv)) = (a.get(ak), b.get(bk)) {
                        if let Some(ord) = bson_cmp(av, bv) {
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                    }
                }
                ord => return ord,
            },
            (None, None) => return std::cmp::Ordering::Equal,
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
        }
    }
}

fn bson_type_order(v: &Bson) -> u8 {
    match v {
        Bson::Null | Bson::Undefined => 0,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => 1,
        Bson::String(_) | Bson::Symbol(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::ObjectId(_) => 6,
        Bson::Boolean(_) => 7,
        Bson::DateTime(_) => 8,
        Bson::Timestamp(_) => 9,
        Bson::Regex(_) => 10,
        Bson::JavaScript(_) | Bson::JavaScriptWithScope { .. } => 11,
        Bson::DbPointer(_) => 12,
        Bson::MinKey => 254,
        Bson::MaxKey => 255,
    }
}

// ─── 正则引擎 ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct RegexFlags {
    case_insensitive: bool,
    multiline: bool,
    dotall: bool,
    extended: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ShorthandKind {
    Digit,
    Word,
    Space,
    NonDigit,
    NonWord,
    NonSpace,
}

#[derive(Debug, Clone)]
enum RegexAst {
    Literal(char),
    Dot,
    CharClass(Vec<(char, char)>),
    NegCharClass(Vec<(char, char)>),
    Shorthand(ShorthandKind),
    Star(Box<RegexAst>, bool),
    Plus(Box<RegexAst>, bool),
    Question(Box<RegexAst>, bool),
    Repeat(Box<RegexAst>, usize, Option<usize>, bool),
    Concat(Vec<RegexAst>),
    Alt(Vec<RegexAst>),
    AnchorStart,
    AnchorEnd,
    WordBoundary,
    NonWordBoundary,
}

fn compile_and_match(pattern: &str, input: &str, flags: &RegexFlags) -> Result<bool, ()> {
    let pat = if flags.extended { strip_extended(pattern) } else { pattern.to_string() };
    let ast = parse_regex(&pat)?;
    let input_chars: Vec<char> = if flags.case_insensitive {
        input.chars().flat_map(|c| c.to_lowercase()).collect()
    } else {
        input.chars().collect()
    };
    Ok(nfa_match(&ast, &input_chars, flags))
}

fn strip_extended(pattern: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '#' => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '\\' => {
                out.push(chars[i]);
                i += 1;
                if i < chars.len() {
                    out.push(chars[i]);
                    i += 1;
                }
            }
            '[' => {
                out.push(chars[i]);
                i += 1;
                if i < chars.len() && chars[i] == '^' {
                    out.push(chars[i]);
                    i += 1;
                }
                while i < chars.len() && chars[i] != ']' {
                    if chars[i] == '\\' {
                        out.push(chars[i]);
                        i += 1;
                        if i < chars.len() {
                            out.push(chars[i]);
                            i += 1;
                        }
                    } else {
                        out.push(chars[i]);
                        i += 1;
                    }
                }
                if i < chars.len() {
                    out.push(chars[i]);
                    i += 1;
                }
            }
            c if c.is_ascii_whitespace() => {
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn is_word_boundary(input: &[char], pos: usize) -> bool {
    let left_is_word = pos > 0 && is_word_char(input[pos - 1]);
    let right_is_word = pos < input.len() && is_word_char(input[pos]);
    left_is_word != right_is_word
}

fn shorthand_matches(kind: ShorthandKind, c: char) -> bool {
    match kind {
        ShorthandKind::Digit => c.is_ascii_digit(),
        ShorthandKind::Word => c.is_alphanumeric() || c == '_',
        ShorthandKind::Space => c.is_ascii_whitespace(),
        ShorthandKind::NonDigit => !c.is_ascii_digit(),
        ShorthandKind::NonWord => !(c.is_alphanumeric() || c == '_'),
        ShorthandKind::NonSpace => !c.is_ascii_whitespace(),
    }
}

fn char_eq(a: char, b: char, flags: &RegexFlags) -> bool {
    if flags.case_insensitive {
        a.to_lowercase().eq(b.to_lowercase())
    } else {
        a == b
    }
}

fn char_in_class(c: char, ranges: &[(char, char)], flags: &RegexFlags) -> bool {
    if flags.case_insensitive {
        let cl = c.to_lowercase().next().unwrap_or(c);
        ranges.iter().any(|(lo, hi)| {
            let lo_l = lo.to_lowercase().next().unwrap_or(*lo);
            let hi_l = hi.to_lowercase().next().unwrap_or(*hi);
            cl >= lo_l && cl <= hi_l
        })
    } else {
        ranges.iter().any(|(lo, hi)| c >= *lo && c <= *hi)
    }
}

fn char_in_neg_class(c: char, ranges: &[(char, char)], flags: &RegexFlags) -> bool {
    !char_in_class(c, ranges, flags)
}

fn nfa_match(ast: &RegexAst, input: &[char], flags: &RegexFlags) -> bool {
    let anchored = matches!(ast, RegexAst::AnchorStart);
    if anchored {
        try_match(ast, input, 0, flags).is_some()
    } else {
        for start in 0..=input.len() {
            if try_match(ast, input, start, flags).is_some() {
                return true;
            }
        }
        false
    }
}

fn try_match(ast: &RegexAst, input: &[char], pos: usize, flags: &RegexFlags) -> Option<usize> {
    match ast {
        RegexAst::Literal(c) => {
            if pos < input.len() && char_eq(input[pos], *c, flags) {
                Some(pos + 1)
            } else {
                None
            }
        }
        RegexAst::Dot => {
            if pos < input.len() && (flags.dotall || input[pos] != '\n') {
                Some(pos + 1)
            } else {
                None
            }
        }
        RegexAst::CharClass(ranges) => {
            if pos < input.len() && char_in_class(input[pos], ranges, flags) {
                Some(pos + 1)
            } else {
                None
            }
        }
        RegexAst::NegCharClass(ranges) => {
            if pos < input.len() && char_in_neg_class(input[pos], ranges, flags) {
                Some(pos + 1)
            } else {
                None
            }
        }
        RegexAst::Shorthand(kind) => {
            if pos < input.len() && shorthand_matches(*kind, input[pos]) {
                Some(pos + 1)
            } else {
                None
            }
        }
        RegexAst::Concat(parts) => try_match_concat(parts, 0, input, pos, flags),
        RegexAst::Alt(alts) => {
            for alt in alts {
                if let Some(p) = try_match(alt, input, pos, flags) {
                    return Some(p);
                }
            }
            None
        }
        RegexAst::Star(inner, lazy) => {
            let cont = |p: usize| -> Option<usize> { Some(p) };
            try_quantifier(inner, 0, None, *lazy, input, pos, flags, &cont)
        }
        RegexAst::Plus(inner, lazy) => {
            let cont = |p: usize| -> Option<usize> { Some(p) };
            try_quantifier(inner, 1, None, *lazy, input, pos, flags, &cont)
        }
        RegexAst::Question(inner, lazy) => {
            let cont = |p: usize| -> Option<usize> { Some(p) };
            try_quantifier(inner, 0, Some(1), *lazy, input, pos, flags, &cont)
        }
        RegexAst::Repeat(inner, min, max, lazy) => {
            let cont = |p: usize| -> Option<usize> { Some(p) };
            try_quantifier(inner, *min, *max, *lazy, input, pos, flags, &cont)
        }
        RegexAst::AnchorStart => {
            if pos == 0 {
                Some(pos)
            } else if flags.multiline && pos > 0 && input[pos - 1] == '\n' {
                Some(pos)
            } else {
                None
            }
        }
        RegexAst::AnchorEnd => {
            if pos == input.len() {
                Some(pos)
            } else if flags.multiline && pos < input.len() && input[pos] == '\n' {
                Some(pos)
            } else {
                None
            }
        }
        RegexAst::WordBoundary => {
            if is_word_boundary(input, pos) {
                Some(pos)
            } else {
                None
            }
        }
        RegexAst::NonWordBoundary => {
            if !is_word_boundary(input, pos) {
                Some(pos)
            } else {
                None
            }
        }
    }
}

fn try_match_concat(
    parts: &[RegexAst],
    idx: usize,
    input: &[char],
    pos: usize,
    flags: &RegexFlags,
) -> Option<usize> {
    if idx >= parts.len() {
        return Some(pos);
    }
    let part = &parts[idx];
    match part {
        RegexAst::Star(inner, lazy)
        | RegexAst::Plus(inner, lazy)
        | RegexAst::Question(inner, lazy) => {
            let (min, max) = match part {
                RegexAst::Star(..) => (0, None),
                RegexAst::Plus(..) => (1, None),
                RegexAst::Question(..) => (0, Some(1)),
                _ => unreachable!(),
            };
            let rest_idx = idx + 1;
            let rest_fn = |p: usize| try_match_concat(parts, rest_idx, input, p, flags);
            try_quantifier(inner, min, max, *lazy, input, pos, flags, &rest_fn)
        }
        RegexAst::Repeat(inner, min, max, lazy) => {
            let rest_idx = idx + 1;
            let rest_fn = |p: usize| try_match_concat(parts, rest_idx, input, p, flags);
            try_quantifier(inner, *min, *max, *lazy, input, pos, flags, &rest_fn)
        }
        _ => {
            let new_pos = try_match(part, input, pos, flags)?;
            try_match_concat(parts, idx + 1, input, new_pos, flags)
        }
    }
}

fn try_quantifier(
    inner: &RegexAst,
    min: usize,
    max: Option<usize>,
    lazy: bool,
    input: &[char],
    pos: usize,
    flags: &RegexFlags,
    cont: &dyn Fn(usize) -> Option<usize>,
) -> Option<usize> {
    let mut positions = vec![pos];
    let mut current = pos;
    let mut count = 0usize;
    loop {
        if let Some(m) = max {
            if count >= m {
                break;
            }
        }
        if let Some(next) = try_match(inner, input, current, flags) {
            if next > current {
                current = next;
                positions.push(current);
                count += 1;
            } else {
                break;
            }
        } else {
            break;
        }
    }

    let valid: Vec<usize> = positions.into_iter().skip(min).collect();

    if valid.is_empty() {
        return None;
    }

    if lazy {
        for p in valid {
            if let Some(result) = cont(p) {
                return Some(result);
            }
        }
    } else {
        for p in valid.into_iter().rev() {
            if let Some(result) = cont(p) {
                return Some(result);
            }
        }
    }
    None
}

// ─── 正则解析器 ──────────────────────────────────────────────────────────────

fn parse_regex(pattern: &str) -> Result<RegexAst, ()> {
    let chars: Vec<char> = pattern.chars().collect();
    let (ast, _) = parse_alt(&chars, 0)?;
    Ok(ast)
}

fn parse_alt(chars: &[char], pos: usize) -> Result<(RegexAst, usize), ()> {
    let (mut first, mut pos) = parse_concat(chars, pos)?;
    let single = |mut v: Vec<RegexAst>| -> RegexAst {
        if v.len() == 1 { v.remove(0) } else { RegexAst::Concat(v) }
    };
    if pos >= chars.len() || chars[pos] != '|' {
        return Ok((single(first), pos));
    }
    // 出现 `|` 后把后续所有分支收进同一个 `Alt`。
    let mut alts = vec![single(first)];
    while pos < chars.len() && chars[pos] == '|' {
        pos += 1;
        let (next, next_pos) = parse_concat(chars, pos)?;
        alts.push(single(next));
        pos = next_pos;
    }
    Ok((RegexAst::Alt(alts), pos))
}

fn parse_concat(chars: &[char], pos: usize) -> Result<(Vec<RegexAst>, usize), ()> {
    let mut parts = Vec::new();
    let mut pos = pos;
    while pos < chars.len() && chars[pos] != '|' && chars[pos] != ')' {
        let (atom, next_pos) = parse_atom(chars, pos)?;
        pos = next_pos;
        if pos < chars.len() {
            match chars[pos] {
                '*' => {
                    pos += 1;
                    let lazy = pos < chars.len() && chars[pos] == '?';
                    if lazy {
                        pos += 1;
                    }
                    parts.push(RegexAst::Star(Box::new(atom), lazy));
                    continue;
                }
                '+' => {
                    pos += 1;
                    let lazy = pos < chars.len() && chars[pos] == '?';
                    if lazy {
                        pos += 1;
                    }
                    parts.push(RegexAst::Plus(Box::new(atom), lazy));
                    continue;
                }
                '?' => {
                    pos += 1;
                    let lazy = pos < chars.len() && chars[pos] == '?';
                    if lazy {
                        pos += 1;
                    }
                    parts.push(RegexAst::Question(Box::new(atom), lazy));
                    continue;
                }
                '{' => {
                    if let Some((min, max, end_pos)) = parse_brace_range(chars, pos + 1) {
                        pos = end_pos;
                        let lazy = pos < chars.len() && chars[pos] == '?';
                        if lazy {
                            pos += 1;
                        }
                        if min == 0 && max == Some(1) {
                            parts.push(RegexAst::Question(Box::new(atom), lazy));
                        } else if min == 1 && max.is_none() {
                            parts.push(RegexAst::Plus(Box::new(atom), lazy));
                        } else if min == 0 && max.is_none() {
                            parts.push(RegexAst::Star(Box::new(atom), lazy));
                        } else {
                            parts.push(RegexAst::Repeat(Box::new(atom), min, max, lazy));
                        }
                        continue;
                    }
                }
                _ => {}
            }
        }
        parts.push(atom);
    }
    Ok((parts, pos))
}

fn parse_brace_range(chars: &[char], pos: usize) -> Option<(usize, Option<usize>, usize)> {
    let start = pos;
    let mut pos = pos;
    while pos < chars.len() && chars[pos].is_ascii_digit() {
        pos += 1;
    }
    if pos == start {
        return None;
    }
    let n: usize = chars[start..pos].iter().collect::<String>().parse().ok()?;
    if pos >= chars.len() {
        return None;
    }
    match chars[pos] {
        '}' => Some((n, Some(n), pos + 1)),
        ',' => {
            pos += 1;
            if pos >= chars.len() {
                return None;
            }
            if chars[pos] == '}' {
                return Some((n, None, pos + 1));
            }
            let m_start = pos;
            while pos < chars.len() && chars[pos].is_ascii_digit() {
                pos += 1;
            }
            if pos == m_start {
                return None;
            }
            let m: usize = chars[m_start..pos].iter().collect::<String>().parse().ok()?;
            if pos >= chars.len() || chars[pos] != '}' {
                return None;
            }
            if m < n {
                return Some((0, Some(0), pos + 1));
            }
            Some((n, Some(m), pos + 1))
        }
        _ => None,
    }
}

fn parse_atom(chars: &[char], pos: usize) -> Result<(RegexAst, usize), ()> {
    if pos >= chars.len() {
        return Err(());
    }
    match chars[pos] {
        '(' => {
            let (ast, pos) = parse_alt(chars, pos + 1)?;
            if pos < chars.len() && chars[pos] == ')' {
                Ok((ast, pos + 1))
            } else {
                Err(())
            }
        }
        '[' => {
            let (class, pos) = parse_char_class(chars, pos + 1)?;
            Ok((class, pos))
        }
        '^' => Ok((RegexAst::AnchorStart, pos + 1)),
        '$' => Ok((RegexAst::AnchorEnd, pos + 1)),
        '.' => Ok((RegexAst::Dot, pos + 1)),
        '\\' => {
            if pos + 1 < chars.len() {
                match chars[pos + 1] {
                    'd' => Ok((RegexAst::Shorthand(ShorthandKind::Digit), pos + 2)),
                    'D' => Ok((RegexAst::Shorthand(ShorthandKind::NonDigit), pos + 2)),
                    'w' => Ok((RegexAst::Shorthand(ShorthandKind::Word), pos + 2)),
                    'W' => Ok((RegexAst::Shorthand(ShorthandKind::NonWord), pos + 2)),
                    's' => Ok((RegexAst::Shorthand(ShorthandKind::Space), pos + 2)),
                    'S' => Ok((RegexAst::Shorthand(ShorthandKind::NonSpace), pos + 2)),
                    'b' => Ok((RegexAst::WordBoundary, pos + 2)),
                    'B' => Ok((RegexAst::NonWordBoundary, pos + 2)),
                    c => Ok((RegexAst::Literal(c), pos + 2)),
                }
            } else {
                Err(())
            }
        }
        c => Ok((RegexAst::Literal(c), pos + 1)),
    }
}

fn parse_class_atom(chars: &[char], pos: usize) -> Result<(char, usize), ()> {
    if pos >= chars.len() {
        return Err(());
    }
    match chars[pos] {
        '\\' => {
            if pos + 1 < chars.len() {
                Ok((chars[pos + 1], pos + 2))
            } else {
                Err(())
            }
        }
        c => Ok((c, pos + 1)),
    }
}

fn parse_shorthand_ranges(kind: ShorthandKind) -> Vec<(char, char)> {
    match kind {
        ShorthandKind::Digit | ShorthandKind::NonDigit => vec![('0', '9')],
        ShorthandKind::Word | ShorthandKind::NonWord => {
            vec![('0', '9'), ('A', 'Z'), ('a', 'z'), ('_', '_')]
        }
        ShorthandKind::Space | ShorthandKind::NonSpace => {
            vec![
                (' ', ' '),
                ('\t', '\t'),
                ('\n', '\n'),
                ('\r', '\r'),
                ('\x0C', '\x0C'),
                ('\x0B', '\x0B'),
            ]
        }
    }
}

fn parse_char_class(chars: &[char], pos: usize) -> Result<(RegexAst, usize), ()> {
    let mut ranges = Vec::new();
    let mut pos = pos;
    let negated = if pos < chars.len() && chars[pos] == '^' {
        pos += 1;
        true
    } else {
        false
    };

    if pos < chars.len() && chars[pos] == ']' {
        ranges.push((']', ']'));
        pos += 1;
    }

    while pos < chars.len() && chars[pos] != ']' {
        if chars[pos] == '\\' && pos + 1 < chars.len() {
            match chars[pos + 1] {
                'd' | 'D' | 'w' | 'W' | 's' | 'S' => {
                    let kind = match chars[pos + 1] {
                        'd' => ShorthandKind::Digit,
                        'D' => ShorthandKind::NonDigit,
                        'w' => ShorthandKind::Word,
                        'W' => ShorthandKind::NonWord,
                        's' => ShorthandKind::Space,
                        'S' => ShorthandKind::NonSpace,
                        _ => unreachable!(),
                    };
                    pos += 2;
                    let mut shorthand_ranges = parse_shorthand_ranges(kind);
                    if pos < chars.len()
                        && chars[pos] == '-'
                        && pos + 1 < chars.len()
                        && chars[pos + 1] != ']'
                    {
                        pos += 1;
                        let (end_ch, new_pos) = parse_class_atom(chars, pos)?;
                        pos = new_pos;
                        if matches!(
                            kind,
                            ShorthandKind::Digit
                                | ShorthandKind::Space
                                | ShorthandKind::NonDigit
                                | ShorthandKind::NonSpace
                        ) {
                            shorthand_ranges.push(('\0', end_ch));
                        }
                    }
                    ranges.extend(shorthand_ranges);
                }
                _ => {
                    let (ch, new_pos) = parse_class_atom(chars, pos)?;
                    pos = new_pos;
                    if pos < chars.len()
                        && chars[pos] == '-'
                        && pos + 1 < chars.len()
                        && chars[pos + 1] != ']'
                    {
                        pos += 1;
                        let (end_ch, new_pos) = parse_class_atom(chars, pos)?;
                        pos = new_pos;
                        ranges.push((ch, end_ch));
                    } else {
                        ranges.push((ch, ch));
                    }
                }
            }
        } else {
            let ch = chars[pos];
            pos += 1;
            if pos < chars.len()
                && chars[pos] == '-'
                && pos + 1 < chars.len()
                && chars[pos + 1] != ']'
            {
                pos += 1;
                let (end_ch, new_pos) = parse_class_atom(chars, pos)?;
                pos = new_pos;
                ranges.push((ch, end_ch));
            } else {
                ranges.push((ch, ch));
            }
        }
    }

    if pos < chars.len() {
        pos += 1;
    } else {
        return Err(());
    }

    if negated {
        Ok((RegexAst::NegCharClass(ranges), pos))
    } else {
        Ok((RegexAst::CharClass(ranges), pos))
    }
}


// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use mdb_doc::Document;

    #[test]
    fn bson_eq_numbers() {
        assert!(bson_eq(&Bson::Int32(42), &Bson::Int32(42)));
        assert!(!bson_eq(&Bson::Int32(1), &Bson::Int32(2)));
        assert!(bson_eq(&Bson::Int32(42), &Bson::Int64(42)));
        assert!(bson_eq(&Bson::Int32(1), &Bson::Double(1.0)));
    }

    #[test]
    fn bson_eq_strings() {
        assert!(bson_eq(&Bson::String("hello".into()), &Bson::String("hello".into())));
        assert!(!bson_eq(&Bson::String("a".into()), &Bson::String("b".into())));
    }

    #[test]
    fn bson_eq_arrays() {
        let a = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
        let b = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
        let c = Bson::Array(vec![Bson::Int32(1), Bson::Int32(3)]);
        assert!(bson_eq(&a, &b));
        assert!(!bson_eq(&a, &c));
    }

    #[test]
    fn bson_eq_documents() {
        let mut a = Document::new();
        a.insert("x", 1i32);
        let mut b = Document::new();
        b.insert("x", 1i32);
        assert!(bson_eq(&Bson::Document(a.clone()), &Bson::Document(b.clone())));
        let mut c = Document::new();
        c.insert("x", 2i32);
        assert!(!bson_eq(&Bson::Document(a), &Bson::Document(c)));
    }

    #[test]
    fn bson_eq_null() {
        assert!(bson_eq(&Bson::Null, &Bson::Null));
        assert!(!bson_eq(&Bson::Null, &Bson::Int32(0)));
    }

    #[test]
    fn bson_cmp_ordering() {
        assert_eq!(bson_cmp(&Bson::Int32(1), &Bson::Int32(2)), Some(std::cmp::Ordering::Less));
        assert_eq!(bson_cmp(&Bson::Int32(2), &Bson::Int32(1)), Some(std::cmp::Ordering::Greater));
        assert_eq!(bson_cmp(&Bson::Int32(1), &Bson::Int32(1)), Some(std::cmp::Ordering::Equal));
        assert_eq!(bson_cmp(&Bson::Int32(1), &Bson::Int64(2)), Some(std::cmp::Ordering::Less));
    }

    #[test]
    fn bson_cmp_strings() {
        assert_eq!(
            bson_cmp(&Bson::String("a".into()), &Bson::String("b".into())),
            Some(std::cmp::Ordering::Less)
        );
    }

    #[test]
    fn bson_cmp_documents() {
        let mut a = Document::new();
        a.insert("a", 1i32);
        let mut b = Document::new();
        b.insert("b", 1i32);
        assert_eq!(
            bson_cmp(&Bson::Document(a), &Bson::Document(b)),
            Some(std::cmp::Ordering::Less)
        );
    }

    #[test]
    fn bson_cmp_type_priority() {
        assert_eq!(bson_cmp(&Bson::Null, &Bson::Int32(1)), Some(std::cmp::Ordering::Less));
        assert_eq!(bson_cmp(&Bson::Int32(1), &Bson::String("a".into())), Some(std::cmp::Ordering::Less));
    }

    #[test]
    fn eq_op() {
        let op = Eq(Bson::Int32(5));
        assert!(op.matches(&Bson::Int32(5)));
        assert!(!op.matches(&Bson::Int32(6)));
    }

    #[test]
    fn ne_op() {
        let op = Ne(Bson::Int32(5));
        assert!(!op.matches(&Bson::Int32(5)));
        assert!(op.matches(&Bson::Int32(6)));
    }

    #[test]
    fn gt_op() {
        let op = Gt(Bson::Int32(5));
        assert!(op.matches(&Bson::Int32(6)));
        assert!(!op.matches(&Bson::Int32(5)));
        assert!(!op.matches(&Bson::Int32(4)));
    }

    #[test]
    fn gte_op() {
        let op = Gte(Bson::Int32(5));
        assert!(op.matches(&Bson::Int32(5)));
        assert!(op.matches(&Bson::Int32(6)));
        assert!(!op.matches(&Bson::Int32(4)));
    }

    #[test]
    fn lt_op() {
        let op = Lt(Bson::Int32(5));
        assert!(op.matches(&Bson::Int32(4)));
        assert!(!op.matches(&Bson::Int32(5)));
        assert!(!op.matches(&Bson::Int32(6)));
    }

    #[test]
    fn lte_op() {
        let op = Lte(Bson::Int32(5));
        assert!(op.matches(&Bson::Int32(5)));
        assert!(op.matches(&Bson::Int32(4)));
        assert!(!op.matches(&Bson::Int32(6)));
    }

    #[test]
    fn in_op() {
        let op = In(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]);
        assert!(op.matches(&Bson::Int32(2)));
        assert!(!op.matches(&Bson::Int32(4)));
    }

    #[test]
    fn nin_op() {
        let op = Nin(vec![Bson::Int32(1), Bson::Int32(2)]);
        assert!(op.matches(&Bson::Int32(3)));
        assert!(!op.matches(&Bson::Int32(1)));
    }

    #[test]
    fn and_op() {
        let op = And(vec![
            Box::new(Gt(Bson::Int32(0))),
            Box::new(Lt(Bson::Int32(10))),
        ]);
        assert!(op.matches(&Bson::Int32(5)));
        assert!(!op.matches(&Bson::Int32(15)));
        assert!(!op.matches(&Bson::Int32(-1)));
    }

    #[test]
    fn or_op() {
        let op = Or(vec![
            Box::new(Eq(Bson::Int32(1))),
            Box::new(Eq(Bson::Int32(2))),
        ]);
        assert!(op.matches(&Bson::Int32(1)));
        assert!(op.matches(&Bson::Int32(2)));
        assert!(!op.matches(&Bson::Int32(3)));
    }

    #[test]
    fn not_op() {
        let op = Not(Box::new(Eq(Bson::Int32(5))));
        assert!(!op.matches(&Bson::Int32(5)));
        assert!(op.matches(&Bson::Int32(6)));
    }

    #[test]
    fn nor_op() {
        let op = Nor(vec![
            Box::new(Eq(Bson::Int32(1))),
            Box::new(Eq(Bson::Int32(2))),
        ]);
        assert!(op.matches(&Bson::Int32(3)));
        assert!(!op.matches(&Bson::Int32(1)));
        assert!(!op.matches(&Bson::Int32(2)));
    }

    #[test]
    fn exists_op() {
        let op = Exists(true);
        assert!(op.matches(&Bson::Int32(1)));
        assert!(op.matches(&Bson::String("hi".into())));
        assert!(!op.matches(&Bson::Null));
        let op = Exists(false);
        assert!(!op.matches(&Bson::Int32(1)));
        assert!(op.matches(&Bson::Null));
    }

    #[test]
    fn type_op() {
        let op = Type(0x02);
        assert!(op.matches(&Bson::String("hi".into())));
        assert!(!op.matches(&Bson::Int32(1)));
    }

    #[test]
    fn mod_op() {
        let op = Mod(2, 0);
        assert!(op.matches(&Bson::Int32(4)));
        assert!(!op.matches(&Bson::Int32(5)));
        let op = Mod(0, 0);
        assert!(!op.matches(&Bson::Int32(5)));
    }

    #[test]
    fn all_op() {
        let op = All(vec![Bson::Int32(1), Bson::Int32(2)]);
        let arr = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]);
        let arr2 = Bson::Array(vec![Bson::Int32(1), Bson::Int32(3)]);
        assert!(op.matches(&arr));
        assert!(!op.matches(&arr2));
        assert!(!op.matches(&Bson::Int32(1)));
    }

    #[test]
    fn size_op() {
        let op = Size(3);
        let arr = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]);
        let arr2 = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
        assert!(op.matches(&arr));
        assert!(!op.matches(&arr2));
        assert!(!op.matches(&Bson::Int32(1)));
    }

    #[test]
    fn elem_match_op() {
        let op = ElemMatch(vec![
            Box::new(Gt(Bson::Int32(5))),
            Box::new(Lt(Bson::Int32(10))),
        ]);
        let arr = Bson::Array(vec![Bson::Int32(1), Bson::Int32(7), Bson::Int32(11)]);
        let arr2 = Bson::Array(vec![Bson::Int32(1), Bson::Int32(3)]);
        assert!(op.matches(&arr));
        assert!(!op.matches(&arr2));
    }

    // ─── regex tests ──────────────────────────────────────────────────────────

    #[test]
    fn regex_op_basic() {
        let op = RegexOp { pattern: "^hello".into(), options: "".into() };
        assert!(op.matches(&Bson::String("hello world".into())));
        assert!(!op.matches(&Bson::String("world hello".into())));
    }

    #[test]
    fn regex_op_case_insensitive() {
        let op = RegexOp { pattern: "hello".into(), options: "i".into() };
        assert!(op.matches(&Bson::String("HELLO".into())));
        assert!(op.matches(&Bson::String("Hello".into())));
    }

    #[test]
    fn regex_op_dot_star() {
        let op = RegexOp { pattern: "h.*o".into(), options: "".into() };
        assert!(op.matches(&Bson::String("hello".into())));
        assert!(op.matches(&Bson::String("h o".into())));
        assert!(!op.matches(&Bson::String("hxl".into())));
    }

    #[test]
    fn regex_op_char_class() {
        let op = RegexOp { pattern: "[abc]+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("abc".into())));
        assert!(op.matches(&Bson::String("a".into())));
        assert!(!op.matches(&Bson::String("xyz".into())));
    }

    #[test]
    fn regex_op_question_mark() {
        let op = RegexOp { pattern: "colou?r".into(), options: "".into() };
        assert!(op.matches(&Bson::String("color".into())));
        assert!(op.matches(&Bson::String("colour".into())));
        assert!(!op.matches(&Bson::String("colouur".into())));
    }

    #[test]
    fn regex_op_plus() {
        let op = RegexOp { pattern: "a+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("a".into())));
        assert!(op.matches(&Bson::String("aaa".into())));
        assert!(!op.matches(&Bson::String("b".into())));
    }

    #[test]
    fn regex_op_alternation() {
        let op = RegexOp { pattern: "cat|dog".into(), options: "".into() };
        assert!(op.matches(&Bson::String("cat".into())));
        assert!(op.matches(&Bson::String("dog".into())));
        assert!(!op.matches(&Bson::String("bird".into())));
    }

    #[test]
    fn regex_op_non_string() {
        let op = RegexOp { pattern: ".*".into(), options: "".into() };
        assert!(!op.matches(&Bson::Int32(42)));
    }

    #[test]
    fn regex_op_end_anchor() {
        let op = RegexOp { pattern: "world$".into(), options: "".into() };
        assert!(op.matches(&Bson::String("hello world".into())));
        assert!(!op.matches(&Bson::String("world hello".into())));
    }

    #[test]
    fn regex_digit_shorthand() {
        let op = RegexOp { pattern: r"\d+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("123".into())));
        assert!(!op.matches(&Bson::String("abc".into())));
    }

    #[test]
    fn regex_word_shorthand() {
        let op = RegexOp { pattern: r"\w+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("hello123".into())));
        assert!(op.matches(&Bson::String("_test".into())));
        assert!(!op.matches(&Bson::String("!@#".into())));
    }

    #[test]
    fn regex_space_shorthand() {
        let op = RegexOp { pattern: r"\s+".into(), options: "".into() };
        assert!(op.matches(&Bson::String(" ".into())));
        assert!(op.matches(&Bson::String("\t\n".into())));
        assert!(!op.matches(&Bson::String("abc".into())));
    }

    #[test]
    fn regex_non_digit_shorthand() {
        let op = RegexOp { pattern: r"\D+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("abc".into())));
        assert!(!op.matches(&Bson::String("123".into())));
    }

    #[test]
    fn regex_non_word_shorthand() {
        let op = RegexOp { pattern: r"\W+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("!@#".into())));
        assert!(!op.matches(&Bson::String("abc".into())));
    }

    #[test]
    fn regex_non_space_shorthand() {
        let op = RegexOp { pattern: r"\S+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("abc".into())));
        assert!(!op.matches(&Bson::String(" ".into())));
    }

    #[test]
    fn regex_brace_range_exact() {
        let op = RegexOp { pattern: "^a{3}$".into(), options: "".into() };
        assert!(op.matches(&Bson::String("aaa".into())));
        assert!(!op.matches(&Bson::String("aa".into())));
        assert!(!op.matches(&Bson::String("aaaa".into())));
    }

    #[test]
    fn regex_brace_range_min_max() {
        let op = RegexOp { pattern: "^a{2,4}$".into(), options: "".into() };
        assert!(op.matches(&Bson::String("aa".into())));
        assert!(op.matches(&Bson::String("aaa".into())));
        assert!(op.matches(&Bson::String("aaaa".into())));
        assert!(!op.matches(&Bson::String("a".into())));
        assert!(!op.matches(&Bson::String("aaaaa".into())));
    }

    #[test]
    fn regex_brace_range_min_only() {
        let op = RegexOp { pattern: "a{2,}".into(), options: "".into() };
        assert!(op.matches(&Bson::String("aa".into())));
        assert!(op.matches(&Bson::String("aaaaa".into())));
        assert!(!op.matches(&Bson::String("a".into())));
    }

    #[test]
    fn regex_brace_range_zero() {
        let op = RegexOp { pattern: "a{0}".into(), options: "".into() };
        assert!(op.matches(&Bson::String("".into())));
        assert!(op.matches(&Bson::String("b".into())));
    }

    #[test]
    fn regex_brace_range_invalid() {
        let op = RegexOp { pattern: "a{1,0}".into(), options: "".into() };
        assert!(op.matches(&Bson::String("".into())));
    }

    #[test]
    fn regex_lazy_star() {
        let op = RegexOp { pattern: "a*?b".into(), options: "".into() };
        assert!(op.matches(&Bson::String("b".into())));
        assert!(op.matches(&Bson::String("ab".into())));
        assert!(op.matches(&Bson::String("aaaab".into())));
    }

    #[test]
    fn regex_lazy_plus() {
        let op = RegexOp { pattern: "a+?b".into(), options: "".into() };
        assert!(op.matches(&Bson::String("ab".into())));
        assert!(op.matches(&Bson::String("aaaab".into())));
        assert!(!op.matches(&Bson::String("b".into())));
    }

    #[test]
    fn regex_lazy_question() {
        let op = RegexOp { pattern: "a??b".into(), options: "".into() };
        assert!(op.matches(&Bson::String("b".into())));
        assert!(op.matches(&Bson::String("ab".into())));
    }

    #[test]
    fn regex_lazy_brace() {
        let op = RegexOp { pattern: "a{2,4}?b".into(), options: "".into() };
        assert!(op.matches(&Bson::String("aab".into())));
        assert!(!op.matches(&Bson::String("ab".into())));
        assert!(!op.matches(&Bson::String("b".into())));
    }

    #[test]
    fn regex_negated_char_class() {
        let op = RegexOp { pattern: "[^abc]+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("xyz".into())));
        assert!(!op.matches(&Bson::String("a".into())));
        assert!(!op.matches(&Bson::String("b".into())));
    }

    #[test]
    fn regex_negated_char_class_range() {
        let op = RegexOp { pattern: "[^a-z]+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("ABC".into())));
        assert!(!op.matches(&Bson::String("abc".into())));
    }

    #[test]
    fn regex_negated_char_class_escaped() {
        let op = RegexOp { pattern: r"[^\]]+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("abc".into())));
        assert!(!op.matches(&Bson::String("]".into())));
    }

    #[test]
    fn regex_char_class_escaped_dash() {
        let op = RegexOp { pattern: r"[\-]+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("-".into())));
        assert!(!op.matches(&Bson::String("a".into())));
    }

    #[test]
    fn regex_multiline_caret() {
        let op = RegexOp { pattern: "^world".into(), options: "m".into() };
        assert!(op.matches(&Bson::String("hello\nworld".into())));
        assert!(!op.matches(&Bson::String("hello world".into())));
    }

    #[test]
    fn regex_multiline_dollar() {
        let op = RegexOp { pattern: "hello$".into(), options: "m".into() };
        assert!(op.matches(&Bson::String("hello\nworld".into())));
        assert!(!op.matches(&Bson::String("hello world".into())));
    }

    #[test]
    fn regex_dotall() {
        let op = RegexOp { pattern: "h.llo".into(), options: "s".into() };
        assert!(op.matches(&Bson::String("h\nllo".into())));
    }

    #[test]
    fn regex_dotall_no_newline() {
        let op = RegexOp { pattern: "h.llo".into(), options: "".into() };
        assert!(!op.matches(&Bson::String("h\nllo".into())));
    }

    #[test]
    fn regex_extended() {
        let op = RegexOp { pattern: "h e l l o".into(), options: "x".into() };
        assert!(op.matches(&Bson::String("hello".into())));
    }

    #[test]
    fn regex_extended_with_comment() {
        let op = RegexOp { pattern: "hello # comment\nworld".into(), options: "x".into() };
        assert!(op.matches(&Bson::String("helloworld".into())));
    }

    #[test]
    fn regex_word_boundary() {
        let op = RegexOp { pattern: r"\bcat\b".into(), options: "".into() };
        assert!(op.matches(&Bson::String("the cat sat".into())));
        assert!(!op.matches(&Bson::String("concatenate".into())));
    }

    #[test]
    fn regex_non_word_boundary() {
        let op = RegexOp { pattern: r"\Bcat\B".into(), options: "".into() };
        assert!(op.matches(&Bson::String("concatenate".into())));
        assert!(!op.matches(&Bson::String("the cat sat".into())));
    }

    #[test]
    fn regex_unicode_case_insensitive() {
        let op = RegexOp { pattern: "caf\u{e9}".into(), options: "i".into() };
        assert!(op.matches(&Bson::String("CAF\u{c9}".into())));
        assert!(op.matches(&Bson::String("Caf\u{e9}".into())));
    }

    #[test]
    fn regex_char_class_with_shorthand() {
        let op = RegexOp { pattern: r"[\dabc]+".into(), options: "".into() };
        assert!(op.matches(&Bson::String("a1b2c3".into())));
        assert!(!op.matches(&Bson::String("xyz".into())));
    }

    #[test]
    fn regex_repeated_group() {
        let op = RegexOp { pattern: "(ab){2}".into(), options: "".into() };
        assert!(op.matches(&Bson::String("abab".into())));
        assert!(!op.matches(&Bson::String("ab".into())));
    }
}
