//! Extended JSON：MongoDB 的 JSON 表示法。
//!
//! Extended JSON 分两套：
//!
//! - **Canonical（`v2`）**：每种类型都用带 `$` 前缀的对象表示，信息无损。
//!   存盘、跨语言传递、作为测试基准都用它。
//! - **Relaxed**：整数与日期用裸值表示，可读性好，但 `Int64` 超出
//!   2^53 后会丢精度，因此只在能精确往返时才用裸数字。
//!
//! # 精度陷阱
//!
//! `{"$numberLong": "9007199254740993"}` 里的值超出 IEEE-754 double 的
//! 可精确表示范围。任何用 `serde_json::Value` 之类的通用 JSON 库中转的
//! 代码都会把它悄悄改成 `9007199254740992`。本模块的 relaxed 输出因此
//! 在越界时**回退到 canonical 形式**，宁可难看也不丢精度。
//!
//! # Decimal128
//!
//! `$numberDecimal` 要求十进制字符串，而本 crate 的 [`Decimal128`] 是
//! 不透明字节类型，无法可靠转换，因此返回
//! [`Error::UnsupportedDecimal128`]。输出一个十六进制串会让下游解析器
//! 读出**错误的数值**——比直接失败危险得多。

use mdb_common::json::escape_into;

use crate::binary::{Binary, BinarySubtype};
use crate::error::{Error, Result};
use crate::oid::ObjectId;
use crate::timestamp::Timestamp;
use crate::value::{Bson, DbPointer, Document, Regex};

/// 把字符串渲染为带引号的 JSON 字符串字面量。
///
/// 供 [`crate::value`] 的 `Display` 实现复用，保证日志输出与协议输出
/// 用的是同一套转义规则——两处各写一份是历史性 bug 的常见来源。
pub fn quote(s: &str) -> String {
    mdb_common::json::escape(s)
}

/// 渲染风格。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Style {
    /// 无损表示，所有数值/日期都带类型包装。
    Canonical,
    /// 人类可读表示，数值与日期在无损时用裸值。
    Relaxed,
}

/// 以指定风格渲染为 Extended JSON。
pub fn to_json(value: &Bson, style: Style) -> Result<String> {
    let mut out = String::new();
    write_value(value, style, &mut out)?;
    Ok(out)
}

/// 以 Canonical 风格渲染。
pub fn to_canonical_json(value: &Bson) -> Result<String> {
    to_json(value, Style::Canonical)
}

/// 以 Relaxed 风格渲染。
pub fn to_relaxed_json(value: &Bson) -> Result<String> {
    to_json(value, Style::Relaxed)
}

fn write_document(doc: &Document, style: Style, out: &mut String) -> Result<()> {
    out.push('{');
    for (i, (k, v)) in doc.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        escape_into(k, out);
        out.push(':');
        write_value(v, style, out)?;
    }
    out.push('}');
    Ok(())
}

fn write_value(v: &Bson, style: Style, out: &mut String) -> Result<()> {
    match v {
        Bson::Double(d) => {
            // NaN / ±Infinity 不是合法 JSON 数字，relaxed 也必须保留
            // $numberDouble 包装，否则产出的是任何解析器都读不了的文本
            if style == Style::Relaxed && d.is_finite() {
                out.push_str(&format_f64(*d));
            } else {
                out.push_str("{\"$numberDouble\":");
                out.push_str(&escape(&format_f64(*d)));
                out.push('}');
            }
        }
        Bson::Int32(i) => {
            if style == Style::Relaxed {
                out.push_str(&i.to_string());
            } else {
                out.push_str(&format!("{{\"$numberInt\":\"{i}\"}}"));
            }
        }
        Bson::Int64(i) => {
            // 裸数字只在 double 能精确表示时安全，否则回退 canonical
            if style == Style::Relaxed && *i <= MAX_SAFE_INT && *i >= -MAX_SAFE_INT {
                out.push_str(&i.to_string());
            } else {
                out.push_str(&format!("{{\"$numberLong\":\"{i}\"}}"));
            }
        }
        Bson::String(s) => escape_into(s, out),
        Bson::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        Bson::Null | Bson::Undefined => {
            if matches!(v, Bson::Undefined) {
                out.push_str("{\"$undefined\":true}");
            } else {
                out.push_str("null");
            }
        }
        Bson::DateTime(ms) => {
            if style == Style::Relaxed {
                out.push_str(&format!("{{\"$date\":{}}}", escape(&iso8601(*ms))));
            } else {
                out.push_str(&format!("{{\"$date\":{{\"$numberLong\":\"{ms}\"}}}}"));
            }
        }
        Bson::Timestamp(ts) => {
            out.push_str(&format!(
                "{{\"$timestamp\":{{\"t\":{},\"i\":{}}}}}",
                ts.secs, ts.inc
            ));
        }
        Bson::ObjectId(oid) => {
            out.push_str(&format!("{{\"$oid\":\"{}\"}}", oid.to_hex()));
        }
        Bson::Binary(b) => write_binary(b, out),
        Bson::Regex(r) => {
            out.push_str(&format!(
                "{{\"$regularExpression\":{{\"pattern\":{},\"options\":{}}}}}",
                escape(&r.pattern),
                escape(&r.options)
            ));
        }
        Bson::DbPointer(p) => {
            out.push_str(&format!(
                "{{\"$dbPointer\":{{\"$ref\":{},\"$id\":{{\"$oid\":\"{}\"}}}}}}",
                escape(&p.namespace),
                p.id.to_hex()
            ));
        }
        Bson::JavaScript(code) => {
            out.push_str(&format!("{{\"$code\":{}}}", escape(code)));
        }
        Bson::Symbol(s) => {
            out.push_str(&format!("{{\"$symbol\":{}}}", escape(s)));
        }
        Bson::JavaScriptWithScope { code, scope } => {
            out.push_str(&format!("{{\"$code\":{},\"$scope\":", escape(code)));
            write_document(scope, style, out)?;
            out.push('}');
        }
        Bson::Document(d) => write_document(d, style, out)?,
        Bson::Array(a) => {
            out.push('[');
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(v, style, out)?;
            }
            out.push(']');
        }
        Bson::MinKey => out.push_str("{\"$minKey\":1}"),
        Bson::MaxKey => out.push_str("{\"$maxKey\":1}"),
        Bson::Decimal128(_) => return Err(Error::UnsupportedDecimal128),
    }
    Ok(())
}

fn write_binary(b: &Binary, out: &mut String) {
    out.push_str(&format!(
        "{{\"$binary\":{{\"base64\":{},\"subType\":\"{:02x}\"}}}}",
        escape(&base64_encode(&b.bytes)),
        b.subtype.code()
    ));
}

fn escape(s: &str) -> String {
    quote(s)
}

/// 2^53 - 1：超过它的整数无法用 f64 精确表示。
const MAX_SAFE_INT: i64 = 9_007_199_254_740_991;

/// 按 JSON 规则渲染 `f64`。
///
/// 关键点：`NaN`/`Infinity` **不是合法 JSON 数字**，必须用 `$numberDouble`
/// 的字符串形式承载，否则产出的文本无法被任何 JSON 解析器读取。
fn format_f64(d: f64) -> String {
    if d.is_nan() {
        return "NaN".to_owned();
    }
    if d.is_infinite() {
        return if d > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    // Rust 的 `{}` 对 1.0 输出 "1"，而 JSON 数字 "1" 会被读成 Int；
    // MongoDB 的 double 即使是整数值也保留小数形式，避免类型混淆。
    if d.fract() == 0.0 && d.is_finite() && d.abs() < 1e15 {
        format!("{d:.1}")
    } else {
        format!("{d}")
    }
}

/// 毫秒时间戳转 ISO-8601（UTC，毫秒精度），供 relaxed `$date` 使用。
///
/// 实现基于 Howard Hinnant 的 civil-from-days 算法，无依赖。
pub fn iso8601(millis: i64) -> String {
    let days = millis.div_euclid(86_400_000);
    let ms_of_day = millis.rem_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    let (h, mi, s, ms) = (
        ms_of_day / 3_600_000,
        (ms_of_day / 60_000) % 60,
        (ms_of_day / 1000) % 60,
        ms_of_day % 1000,
    );
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{ms:03}Z")
}

/// `days` 自 1970-01-01 起的偏移 -> `(年, 月, 日)`。
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// 解析 relaxed `$date` 的 ISO-8601 形式，如 `2023-11-14T22:13:20.000Z`。
///
/// relaxed EJSON 会把 BSON Date 直接渲染成 ISO 字符串，因此**输出这个格式就
/// 必须能读回来**，否则 `to_relaxed_json` 的结果不是闭环的。
///
/// pymongo 默认还可能输出 `+00:00` 时区偏移与省略毫秒的形式，这两种都接受。
/// 不接受非 UTC 偏移与闰秒：读错了会静默变成错误的时间，静默错值比报错更糟。
fn parse_iso8601(s: &str) -> Result<i64> {
    let bad = || Error::MissingNul("malformed ISO-8601 $date");
    let b = s.as_bytes();
    let num = |from: usize, len: usize| -> Result<i64> {
        let part = s.get(from..from + len).ok_or_else(bad)?;
        if !part.bytes().all(|c| c.is_ascii_digit()) {
            return Err(bad());
        }
        part.parse::<i64>().map_err(|_| bad())
    };

    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || !(b[10] == b'T' || b[10] == b't') {
        return Err(bad());
    }
    let (y, mo, d) = (num(0, 4)?, num(5, 2)?, num(8, 2)?);
    let (h, mi) = (num(11, 2)?, num(14, 2)?);
    if b[13] != b':' || b[16] != b':' {
        return Err(bad());
    }
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 {
        return Err(bad());
    }

    let mut i = 17;
    let mut sec = 0i64;
    if b.get(i) != Some(&b':') {
        sec = num(i, 2)?;
        i += 2; // 跨过两位秒，不含分隔符
    }
    let mut ms = 0i64;
    if b.get(i) == Some(&b'.') {
        // 截断到毫秒；BSON Date 本就只有毫秒精度
        let digits = b[i + 1..].iter().take_while(|c| c.is_ascii_digit()).count();
        if digits == 0 || digits > 9 {
            return Err(bad());
        }
        for k in 0..3 {
            ms = ms * 10 + i64::from(b[i + 1 + k.min(digits - 1)] - b'0');
        }
        if sec + ms / 1000 > 59 {
            return Err(bad()); // 小数部分溢出到秒，如 ":60.5"
        }
        i += 1 + digits;
    }
    if sec > 59 {
        return Err(bad());
    }

    let offset_ms: i64 = match b.get(i) {
        Some(b'Z') | Some(b'z') if i + 1 == b.len() => 0,
        Some(c @ (b'+' | b'-')) if i + 6 == b.len() => {
            let oh = num(i + 1, 2)?;
            let om = num(i + 4, 2)?;
            if b[i + 3] != b':' || oh > 23 || om > 59 {
                return Err(bad());
            }
            let magnitude = (oh * 3_600_000 + om * 60_000) * if *c == b'-' { -1 } else { 1 };
            // 本模块只处理 UTC 偏移：非零偏移在无外部时区库时无法可靠折算，
            // 宁可报错也不要给出错误时刻
            if magnitude != 0 {
                return Err(bad());
            }
            0
        }
        _ => return Err(bad()),
    };

    let days = days_from_civil(y, mo as u32, d as u32);
    let secs = days * 86_400 + h * 3_600 + mi * 60 + sec;
    Ok(secs.saturating_mul(1000).saturating_add(ms).saturating_sub(offset_ms))
}

/// `(年, 月, 日)` -> 自 1970-01-01 起的日偏移（[`civil_from_days`] 的逆运算）。
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    // 1 月与 2 月视为上一年的第 13、14 月
    let y = y - i64::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64; // [0, 11]
    let doy = (153 * mp + 2) / 5 + u64::from(d) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe as i64 - 719_468
}

// ---------------------------------------------------------------------------
// Base64
// ---------------------------------------------------------------------------

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// 标准 Base64 编码（带 `=` 填充）。
pub fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { B64[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { B64[n as usize & 63] as char } else { '=' });
    }
    out
}

/// 标准 Base64 解码。
pub fn base64_decode(s: &str) -> Result<Vec<u8>> {
    fn val(c: u8) -> Result<u32> {
        match c {
            b'A'..=b'Z' => Ok(u32::from(c - b'A')),
            b'a'..=b'z' => Ok(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Ok(u32::from(c - b'0') + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(Error::InvalidUtf8("base64")),
        }
    }
    let body: Vec<u8> = s.bytes().filter(|&c| c != b'=' && !c.is_ascii_whitespace()).collect();
    if body.len() % 4 == 1 {
        return Err(Error::NegativeLength("base64"));
    }
    let mut out = Vec::with_capacity(body.len() / 4 * 3);
    for chunk in body.chunks(4) {
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= val(c)? << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// 解析
// ---------------------------------------------------------------------------

/// 解析 Extended JSON / 原生 JSON 为 BSON 值。
///
/// 同时接受 canonical 与 relaxed 两种输入形式——两者是同一套语法的子集，
/// 因此解析器无需区分"风格"，只需识别带 `$` 前缀的包装对象。
pub fn from_json(text: &str) -> Result<Bson> {
    let mut p = Parser { b: text.as_bytes(), i: 0, depth: 0 };
    p.skip_ws();
    let v = p.value()?;
    p.skip_ws();
    if p.i != p.b.len() {
        return Err(Error::InvalidUtf8("trailing data after JSON value"));
    }
    Ok(v)
}

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
    depth: usize,
}

impl<'a> Parser<'a> {
    fn skip_ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn peek(&self) -> Result<u8> {
        self.b
            .get(self.i)
            .copied()
            .ok_or(Error::Truncated { need: 1, have: 0 })
    }

    fn expect(&mut self, c: u8) -> Result<()> {
        if self.peek()? != c {
            return Err(Error::MissingNul("json"));
        }
        self.i += 1;
        Ok(())
    }

    fn literal(&mut self, word: &str) -> Result<()> {
        if self.b[self.i..].starts_with(word.as_bytes()) {
            self.i += word.len();
            Ok(())
        } else {
            Err(Error::MissingNul("json literal"))
        }
    }

    fn value(&mut self) -> Result<Bson> {
        if self.depth > crate::codec::MAX_DEPTH {
            return Err(Error::DepthLimit { max: crate::codec::MAX_DEPTH });
        }
        match self.peek()? {
            b'{' => self.object(),
            b'[' => self.array(),
            b'"' => Ok(Bson::String(self.string()?)),
            b't' => {
                self.literal("true")?;
                Ok(Bson::Boolean(true))
            }
            b'f' => {
                self.literal("false")?;
                Ok(Bson::Boolean(false))
            }
            b'n' => {
                self.literal("null")?;
                Ok(Bson::Null)
            }
            _ => self.number(),
        }
    }

    fn object(&mut self) -> Result<Bson> {
        self.depth += 1;
        self.expect(b'{')?;
        let mut doc = Document::new();
        self.skip_ws();
        if self.peek()? == b'}' {
            self.i += 1;
            self.depth -= 1;
            return Ok(Bson::Document(doc));
        }
        loop {
            self.skip_ws();
            let key = self.string()?;
            self.skip_ws();
            self.expect(b':')?;
            self.skip_ws();
            let value = self.value()?;
            if doc.contains_key(&key) {
                return Err(Error::DuplicateKey(key));
            }
            doc.insert(key, value);
            self.skip_ws();
            match self.peek()? {
                b',' => self.i += 1,
                b'}' => {
                    self.i += 1;
                    break;
                }
                _ => return Err(Error::MissingNul("json object")),
            }
        }
        self.depth -= 1;
        // 单键 `$xxx` 包装对象即 Extended JSON 类型标记
        if doc.len() == 1 {
            let (k, v) = doc.iter().next().expect("len checked");
            if let Some(parsed) = parse_wrapper(k, v)? {
                return Ok(parsed);
            }
        }
        // `$code` 可带 `$scope` 组成 JavaScriptWithScope，是唯一合法的两键包装
        if let Some(parsed) = parse_code_with_scope(&doc)? {
            return Ok(parsed);
        }
        Ok(Bson::Document(doc))
    }

    fn array(&mut self) -> Result<Bson> {
        self.depth += 1;
        self.expect(b'[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek()? == b']' {
            self.i += 1;
            self.depth -= 1;
            return Ok(Bson::Array(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value()?);
            self.skip_ws();
            match self.peek()? {
                b',' => self.i += 1,
                b']' => {
                    self.i += 1;
                    break;
                }
                _ => return Err(Error::MissingNul("json array")),
            }
        }
        self.depth -= 1;
        Ok(Bson::Array(items))
    }

    fn string(&mut self) -> Result<String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let c = self.peek()?;
            self.i += 1;
            match c {
                b'"' => return Ok(out),
                b'\\' => {
                    let e = self.peek()?;
                    self.i += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{08}'),
                        b'f' => out.push('\u{0c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => out.push(self.unicode_escape()?),
                        _ => return Err(Error::InvalidUtf8("json escape")),
                    }
                }
                c if c < 0x20 => {
                    // 裸控制字符是非法 JSON
                    return Err(Error::InvalidUtf8("raw control char in JSON string"));
                }
                c if c < 0x80 => out.push(c as char),
                _ => {
                    // 多字节 UTF-8：回退并按完整序列拷贝
                    let start = self.i - 1;
                    let len = utf8_len(c);
                    if start + len > self.b.len() {
                        return Err(Error::InvalidUtf8("truncated UTF-8 in JSON"));
                    }
                    let s = std::str::from_utf8(&self.b[start..start + len])
                        .map_err(|_| Error::InvalidUtf8("json"))?;
                    out.push_str(s);
                    self.i = start + len;
                }
            }
        }
    }

    /// `\uXXXX`，含代理对。
    fn unicode_escape(&mut self) -> Result<char> {
        let hi = self.hex4()?;
        if (0xD800..0xDC00).contains(&hi) {
            // 高代理：必须紧跟 `\uDC00` 段
            if self.peek()? != b'\\' {
                return Err(Error::InvalidUtf8("lone surrogate"));
            }
            self.i += 1;
            if self.peek()? != b'u' {
                return Err(Error::InvalidUtf8("lone surrogate"));
            }
            self.i += 1;
            let lo = self.hex4()?;
            if !(0xDC00..0xE000).contains(&lo) {
                return Err(Error::InvalidUtf8("bad surrogate pair"));
            }
            let cp = 0x1_0000 + ((hi - 0xD800) << 10) + (lo - 0xDC00);
            return char::from_u32(cp).ok_or(Error::InvalidUtf8("surrogate pair"));
        }
        if (0xDC00..0xE000).contains(&hi) {
            return Err(Error::InvalidUtf8("unpaired low surrogate"));
        }
        char::from_u32(hi).ok_or(Error::InvalidUtf8("escape"))
    }

    fn hex4(&mut self) -> Result<u32> {
        let s = self
            .b
            .get(self.i..self.i + 4)
            .ok_or(Error::Truncated { need: 4, have: self.b.len() - self.i })?;
        let mut v = 0u32;
        for &c in s {
            let d = match c {
                b'0'..=b'9' => u32::from(c - b'0'),
                b'a'..=b'f' => u32::from(c - b'a') + 10,
                b'A'..=b'F' => u32::from(c - b'A') + 10,
                _ => return Err(Error::InvalidUtf8("hex escape")),
            };
            v = v * 16 + d;
        }
        self.i += 4;
        Ok(v)
    }

    fn number(&mut self) -> Result<Bson> {
        let start = self.i;
        if self.i < self.b.len() && (self.b[self.i] == b'-' || self.b[self.i] == b'+') {
            self.i += 1;
        }
        let mut is_float = false;
        while self.i < self.b.len() {
            match self.b[self.i] {
                b'0'..=b'9' => self.i += 1,
                b'.' | b'e' | b'E' => {
                    is_float = true;
                    self.i += 1;
                }
                b'-' | b'+' => self.i += 1,
                _ => break,
            }
        }
        let text = std::str::from_utf8(&self.b[start..self.i])
            .map_err(|_| Error::InvalidUtf8("number"))?;
        parse_number(text, is_float)
    }
}

/// 数字字面量 -> BSON。
///
/// 整数值优先落到 `Int32`，其次 `Int64`，最后 `Double`；这样
/// `{"n": 1}` 读回来仍是 `Int32`，与 MongoDB 行为一致。
fn parse_number(text: &str, is_float: bool) -> Result<Bson> {
    if !is_float {
        if let Ok(i) = text.parse::<i32>() {
            return Ok(Bson::Int32(i));
        }
        if let Ok(i) = text.parse::<i64>() {
            return Ok(Bson::Int64(i));
        }
    }
    let d: f64 = text
        .parse()
        .map_err(|_| Error::InvalidUtf8("number literal"))?;
    if d.is_infinite() {
        // JSON 字面量写不出无穷大；显式字符串形式请用 $numberDouble
        return Err(Error::InvalidUtf8("numeric literal out of range"));
    }
    Ok(Bson::Double(d))
}

fn utf8_len(first: u8) -> usize {
    match first {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

/// 包装对象的值必须是字符串。
fn need_str(v: &Bson) -> Result<&str> {
    v.as_str().ok_or(Error::MissingNul("wrapper"))
}

/// 包装对象的值必须可解释为整数。
fn need_int(v: &Bson) -> Result<i64> {
    v.as_i64().ok_or(Error::MissingNul("wrapper"))
}

/// 包装对象的值必须是子文档。
fn need_doc<'a>(v: &'a Bson, what: &'static str) -> Result<&'a Document> {
    v.as_document().ok_or(Error::MissingNul(what))
}

/// 识别 Extended JSON 包装对象。
///
/// 返回 `Ok(None)` 表示这不是包装对象，应按普通文档处理；
/// `$code` + 可选 `$scope` -> [`Bson::JavaScriptWithScope`]。
///
/// 这是 Extended JSON 里**唯一**合法的多键包装形式：
///
/// ```json
/// {"$code": "x = y", "$scope": {"y": 3}}
/// ```
///
/// 少一次判断就会退化成普通 `Document`，于是 `from_json(ejson(bson)) != bson`，
/// 差分测试里表现为 `EJSON/BSON disagreement`。
/// `$scope` 可缺省，此时等价于 `JavaScript`；`$scope` 与其他键混用则不是包装。
fn parse_code_with_scope(doc: &Document) -> Result<Option<Bson>> {
    let code = match doc.get("$code") {
        Some(v) => need_str(v)?.to_owned(),
        None => return Ok(None),
    };
    if doc.len() == 1 {
        return Ok(Some(Bson::JavaScript(code)));
    }
    match (doc.get("$scope"), doc.len()) {
        (Some(v), 2) => {
            Ok(Some(Bson::JavaScriptWithScope { code, scope: need_doc(v, "$scope")?.clone() }))
        }
        _ => Ok(None),
    }
}

/// `Err` 表示形似包装但内容非法（必须报错，不能静默当普通文档）。
fn parse_wrapper(key: &str, value: &Bson) -> Result<Option<Bson>> {
    Ok(Some(match key {
        "$oid" => Bson::ObjectId(
            ObjectId::from_hex(need_str(value)?).ok_or(Error::InvalidUtf8("$oid"))?,
        ),
        "$numberInt" => Bson::Int32(
            need_str(value)?
                .parse()
                .map_err(|_| Error::InvalidUtf8("$numberInt"))?,
        ),
        "$numberLong" => Bson::Int64(
            need_str(value)?
                .parse()
                .map_err(|_| Error::InvalidUtf8("$numberLong"))?,
        ),
        "$numberDouble" => Bson::Double(
            match need_str(value)? {
                "NaN" => f64::NAN,
                "Infinity" => f64::INFINITY,
                "-Infinity" => f64::NEG_INFINITY,
                other => other.parse().map_err(|_| Error::InvalidUtf8("$numberDouble"))?,
            },
        ),
        "$numberDecimal" => {
            return Err(Error::UnsupportedDecimal128);
        }
        "$binary" => {
            let d = need_doc(value, "$binary")?;
            let b64 = d
                .get("base64")
                .and_then(Bson::as_str)
                .ok_or(Error::MissingNul("$binary"))?;
            let sub = d
                .get("subType")
                .and_then(Bson::as_str)
                .ok_or(Error::MissingNul("$binary"))?;
            let code = u8::from_str_radix(sub, 16).map_err(|_| Error::InvalidUtf8("$binary"))?;
            Bson::Binary(Binary::new(BinarySubtype::from_u8(code), base64_decode(b64)?))
        }
        "$date" => match value {
            Bson::Int32(_) | Bson::Int64(_) => Bson::DateTime(need_int(value)?),
            Bson::Document(d) => match d.get("$numberLong") {
                Some(v) => Bson::DateTime(need_int(v)?),
                None => return Err(Error::MissingNul("$date")),
            },
            Bson::String(s) => Bson::DateTime(parse_iso8601(s)?),
            _ => return Err(Error::MissingNul("$date")),
        },
        "$timestamp" => {
            let d = need_doc(value, "$timestamp")?;
            let t = d.get("t").and_then(Bson::as_i64).ok_or(Error::MissingNul("$timestamp"))?;
            let i = d.get("i").and_then(Bson::as_i64).ok_or(Error::MissingNul("$timestamp"))?;
            Bson::Timestamp(Timestamp::new(t as u32, i as u32))
        }
        "$regularExpression" => {
            let d = need_doc(value, "$regularExpression")?;
            let p = d
                .get("pattern")
                .and_then(Bson::as_str)
                .ok_or(Error::MissingNul("$regularExpression"))?;
            let o = d
                .get("options")
                .and_then(Bson::as_str)
                .ok_or(Error::MissingNul("$regularExpression"))?;
            Bson::Regex(Regex::new(p, o))
        }
        "$code" => Bson::JavaScript(need_str(value)?.to_owned()),
        "$symbol" => Bson::Symbol(need_str(value)?.to_owned()),
        "$undefined" => Bson::Undefined,
        "$minKey" => Bson::MinKey,
        "$maxKey" => Bson::MaxKey,
        "$dbPointer" => {
            let d = need_doc(value, "$dbPointer")?;
            let ns = d
                .get("$ref")
                .and_then(Bson::as_str)
                .ok_or(Error::MissingNul("$dbPointer"))?;
            let id = match d.get("$id") {
                Some(Bson::ObjectId(o)) => *o,
                _ => return Err(Error::MissingNul("$dbPointer")),
            };
            Bson::DbPointer(DbPointer { namespace: ns.to_owned(), id })
        }
        _ => return Ok(None),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decimal128::Decimal128;

    #[test]
    fn base64_matches_rfc4648_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        for s in ["", "f", "fo", "foo", "foob", "fooba", "foobar"] {
            assert_eq!(base64_decode(&base64_encode(s.as_bytes())).unwrap(), s.as_bytes());
        }
    }

    #[test]
    fn iso8601_matches_known_instants() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601(1_700_000_000_000), "2023-11-14T22:13:20.000Z");
        // 闰年与闰秒边界附近的历法换算
        assert_eq!(iso8601(951_782_400_000), "2000-02-29T00:00:00.000Z");
        assert_eq!(iso8601(1_709_164_800_123), "2024-02-29T00:00:00.123Z");
        assert_eq!(iso8601(-86_400_000), "1969-12-31T00:00:00.000Z");
    }

    #[test]
    fn canonical_form_is_lossless_for_every_type() {
        let cases = [
            (Bson::Double(1.5), r#"{"$numberDouble":"1.5"}"#),
            (Bson::Int32(7), r#"{"$numberInt":"7"}"#),
            (Bson::Int64(8), r#"{"$numberLong":"8"}"#),
            (Bson::String("hi".into()), r#""hi""#),
            (Bson::Boolean(true), "true"),
            (Bson::Null, "null"),
            (Bson::Undefined, r#"{"$undefined":true}"#),
            (Bson::DateTime(1_700_000_000_000), r#"{"$date":{"$numberLong":"1700000000000"}}"#),
            (
                Bson::ObjectId(ObjectId::from_hex("0123456789abcdef01234567").unwrap()),
                r#"{"$oid":"0123456789abcdef01234567"}"#,
            ),
            (Bson::MinKey, r#"{"$minKey":1}"#),
            (Bson::MaxKey, r#"{"$maxKey":1}"#),
            (
                Bson::Binary(Binary::generic(vec![1, 2, 3])),
                r#"{"$binary":{"base64":"AQID","subType":"00"}}"#,
            ),
            (
                Bson::Regex(Regex::new("^a", "i")),
                r#"{"$regularExpression":{"pattern":"^a","options":"i"}}"#,
            ),
            (Bson::JavaScript("f()".into()), r#"{"$code":"f()"}"#),
            (Bson::Symbol("s".into()), r#"{"$symbol":"s"}"#),
            (Bson::Timestamp(Timestamp::new(1, 2)), r#"{"$timestamp":{"t":1,"i":2}}"#),
        ];
        for (value, expect) in cases {
            assert_eq!(to_canonical_json(&value).unwrap(), expect);
            assert_eq!(from_json(expect).unwrap(), value, "roundtrip for {value:?}");
        }
    }

    #[test]
    fn int64_falls_back_to_canonical_when_unsafe() {
        // 2^53 + 1 用裸数字会丢精度，relaxed 必须回退
        let v = Bson::Int64(9_007_199_254_740_993);
        assert_eq!(to_relaxed_json(&v).unwrap(), r#"{"$numberLong":"9007199254740993"}"#);
        assert_eq!(to_relaxed_json(&Bson::Int64(42)).unwrap(), "42");
        assert_eq!(from_json(&to_relaxed_json(&v).unwrap()).unwrap(), v);
    }

    #[test]
    fn non_finite_doubles_stay_parsable() {
        for (d, s) in [
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            let v = Bson::Double(d);
            let out = to_canonical_json(&v).unwrap();
            assert!(out.contains(s), "{out}");
            let back = from_json(&out).unwrap();
            match back {
                Bson::Double(x) => assert!(x.is_nan() == d.is_nan() && x == d || (d.is_nan() && x.is_nan())),
                other => panic!("expected double, got {other:?}"),
            }
        }
    }

    #[test]
    fn integral_double_keeps_a_decimal_point() {
        // 否则 "1.0" 会被读成 Int，类型在往返中改变
        assert_eq!(to_relaxed_json(&Bson::Double(1.0)).unwrap(), "1.0");
        assert!(matches!(from_json("1.0").unwrap(), Bson::Double(_)));
        assert!(matches!(from_json("1").unwrap(), Bson::Int32(_)));
    }

    #[test]
    fn decimal128_output_is_refused_not_guessed() {
        let d = Decimal128::from_hex("00000000000000000000000000004030").unwrap();
        let v = Bson::Decimal128(d);
        assert!(matches!(to_canonical_json(&v), Err(Error::UnsupportedDecimal128)));
        assert!(matches!(to_relaxed_json(&v), Err(Error::UnsupportedDecimal128)));
        assert!(matches!(from_json(r#"{"$numberDecimal":"1"}"#), Err(Error::UnsupportedDecimal128)));
    }

    #[test]
    fn parses_nested_documents_and_arrays() {
        let v = from_json(r#"{"a":{"b":[1,2,{"c":"d"}]},"e":null}"#).unwrap();
        let d = v.as_document().unwrap();
        assert_eq!(d.get_path("a.b").unwrap().as_array().unwrap().len(), 3);
        assert!(d.get("e").unwrap().is_nullish());
    }

    #[test]
    fn handles_string_escapes_and_surrogate_pairs() {
        // \u00e9 是 é，\ud83d\ude00 是代理对合成的 😀
        let v = from_json(r#""a\nbé😀""#).unwrap();
        assert_eq!(v.as_str().unwrap(), "a\nbé😀");
        assert_eq!(to_canonical_json(&v).unwrap(), r#""a\nbé😀""#);
    }

    #[test]
    fn rejects_unescaped_control_chars() {
        assert!(from_json("\"a\u{1b}\"").is_err());
    }

    #[test]
    fn rejects_trailing_garbage() {
        assert!(from_json("{} {}").is_err());
    }

    #[test]
    fn rejects_over_deep_json() {
        let deep = "[".repeat(crate::codec::MAX_DEPTH + 5) + &"]".repeat(crate::codec::MAX_DEPTH + 5);
        assert!(matches!(from_json(&deep), Err(Error::DepthLimit { .. })));
    }

    #[test]
    fn code_with_scope_roundtrips_through_json() {
        let mut scope = Document::new();
        scope.insert("v", 1i32);
        let v = Bson::JavaScriptWithScope { code: "f()".into(), scope };
        let s = to_canonical_json(&v).unwrap();
        assert!(s.contains("$code") && s.contains("$scope"), "{s}");
        // 关键：必须还原成同一个类型，而不是退化成普通 Document
        assert!(matches!(from_json(&s), Ok(Bson::JavaScriptWithScope { .. })), "{s}");
        assert_eq!(from_json(&s).unwrap(), v);
    }

    #[test]
    fn code_without_scope_collapses_to_javascript() {
        assert_eq!(from_json(r#"{"$code":"f()"}"#).unwrap(), Bson::JavaScript("f()".into()));
        // `$scope` 与其他键混用就不是包装对象，必须原样保留
        let mixed = r#"{"$code":"f()","$scope":{},"x":1}"#;
        assert!(matches!(from_json(mixed), Ok(Bson::Document(_))), "{mixed}");
    }

    #[test]
    fn relaxed_date_roundtrips_through_iso8601() {
        // relaxed 输出的是 ISO 字符串，因此必须能读回，否则不是闭环
        for ms in [
            0i64,
            -1,                     // 1969-12-31T23:59:59.999Z
            1_700_000_000_123,      // pymongo 语料用的那个值
            951_782_400_000,        // 2000-02-29，闰日
            4_102_444_800_000,      // 2100-01-01
        ] {
            let relaxed = to_relaxed_json(&Bson::DateTime(ms)).unwrap();
            let back = from_json(&relaxed).unwrap();
            assert_eq!(back, Bson::DateTime(ms), "iso8601 roundtrip failed for {ms}: {relaxed}");
        }
    }

    #[test]
    fn accepts_iso8601_variants_pymongo_emits() {
        let expect = 1_700_000_000_000i64;
        for s in [
            r#""2023-11-14T22:13:20.000Z""#,
            r#""2023-11-14T22:13:20Z""#,          // 无毫秒
            r#""2023-11-14T22:13:20.0Z""#,        // 一位毫秒
            r#""2023-11-14T22:13:20.123456Z""#,   // 微秒，截断到毫秒
            r#""2023-11-14T22:13:20.000+00:00""#,
        ] {
            let v = from_json(&format!(r#"{{"$date":{s}}}"#))
                .unwrap_or_else(|e| panic!("{s} rejected: {e}"));
            if s.contains("123456") {
                assert_eq!(v, Bson::DateTime(expect + 123), "{s}");
            } else {
                assert_eq!(v, Bson::DateTime(expect), "{s}");
            }
        }
    }

    #[test]
    fn rejects_iso8601_it_cannot_represent_correctly() {
        // 宁可报错也不要静默给出错误的时刻
        for s in [
            r#""not-a-date""#,
            r#""2023-11-14""#,                      // 缺时间部分
            r#""2023-13-14T22:13:20Z""#,            // 月 13
            r#""2023-11-14T25:13:20Z""#,           // 时 25
            r#""2023-11-14T22:13:20.Z""#,           // 有小数点无数字
            r#""2023-11-14T22:13:20+08:00""#,      // 非 UTC 偏移
            r#""2023-11-14T22:13:20"#,             // 无时区
            r#""2023-11-14T22:13:20Zjunk""#,        // 尾随垃圾
        ] {
            assert!(
                from_json(&format!(r#"{{"$date":{s}}}"#)).is_err(),
                "{s} should have been rejected"
            );
        }
    }

    #[test]
    fn non_finite_doubles_are_not_emitted_bare() {
        // 裸 NaN / Infinity 不是合法 JSON，任何解析器都读不了
        for (d, s) in [
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            let out = to_relaxed_json(&Bson::Double(d)).unwrap();
            assert!(out.contains("$numberDouble"), "relaxed bare {s}: {out}");
            assert!(from_json(&out).is_ok(), "unparseable: {out}");
        }
    }
}
