//! BSON 编码与解码。
//!
//! 实现遵循 <http://bsonspec.org/spec.html>。重点不是"能编能解"，
//! 而是**拒绝畸形输入**——wire protocol 会把不可信字节直接交给解码器，
//! 一次越界读或一次递归失控就是可远程触发的进程崩溃。
//!
//! # 防御性要点
//!
//! - 解码器持有显式的**区域上界** [`Decoder::end`]：子文档只能读到父文档
//!   声明的边界为止。信任长度字段很容易写出"内层文档吃掉外层剩余数据"的
//!   解析歧义。
//! - 长度字段一律校验；所有读取都经 [`Decoder::take`] 做剩余长度检查。
//! - cstring 必须 NUL 结尾；键名不得内嵌 NUL。
//! - `string` 与键名必须是合法 UTF-8。
//! - 嵌套深度上限 [`MAX_DEPTH`]，防止深层嵌套打爆栈。
//! - 拒绝重复键、拒绝尾随垃圾。
//! - 编码同样可失败：超深、含 NUL 的键、超过 `maxBsonObjectSize` 都返回
//!   `Err`，而不是截断或 `unwrap` 崩溃。

use crate::binary::{Binary, BinarySubtype};
use crate::decimal128::Decimal128;
use crate::error::{Error, Result};
use crate::oid::ObjectId;
use crate::timestamp::Timestamp;
use crate::value::{Bson, DbPointer, Document, Regex};

/// 允许的最大嵌套深度。
pub const MAX_DEPTH: usize = 100;

/// 单个文档的最大字节数（MongoDB `maxBsonObjectSize`）。
pub const MAX_BSON_OBJECT_SIZE: usize = 16 * 1024 * 1024;

/// BSON 文档的最小长度：int32(4) + 终止符(1)。
const MIN_DOC_LEN: usize = 5;

// ---------------------------------------------------------------------------
// 解码器
// ---------------------------------------------------------------------------

/// 带区域上界的有界读取器。
///
/// `end` 是当前可读取区域的排他上界。嵌套文档会把 `end` 收紧到自己的声明
/// 长度，退出时由调用方恢复，从而保证"子文档不得越界读取兄弟字段"。
struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
    end: usize,
}

impl<'a> Decoder<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0, end: buf.len() }
    }

    fn remaining(&self) -> usize {
        self.end.saturating_sub(self.pos)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(Error::Truncated { need: n, have: self.remaining() });
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn take_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn take_i32(&mut self) -> Result<i32> {
        let b = self.take(4)?;
        Ok(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn take_i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(i64::from_le_bytes(a))
    }

    fn take_u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }

    fn take_f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.take_u64()?))
    }

    /// 读取以 NUL 结尾的 cstring（不含 NUL）。
    fn take_cstring(&mut self) -> Result<&'a str> {
        let start = self.pos;
        let end = self.buf[start..self.end]
            .iter()
            .position(|&b| b == 0)
            .ok_or(Error::UnterminatedCstring)?;
        let bytes = &self.buf[start..start + end];
        self.pos = start + end + 1;
        std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8("cstring"))
    }

    /// 读取 BSON string：int32 长度（含末尾 NUL）+ 数据 + NUL。
    ///
    /// 长度字段**包含**那个结尾 NUL。BSON 规范原文：
    /// "The `int32` is the number of bytes in the `(byte*)` plus one for the
    /// trailing null byte."
    ///
    /// 这条极易写反：按"长度 = 数据字节数"实现时，解码器与编码器可以自洽、
    /// 自测全绿，但产出的字节没有任何真实驱动读得回来（libbson / pymongo /
    /// bson-js 一致要求 +1）。已用 pymongo 4.18 的样本验证。
    fn take_string(&mut self) -> Result<String> {
        let len = self.take_i32()?;
        // 最短的合法字符串是空串，长度为 1（只有那个 NUL）
        if len < 1 {
            return Err(Error::NegativeLength("string"));
        }
        let len = len as usize;
        let bytes = self.take(len)?;
        if bytes[len - 1] != 0 {
            return Err(Error::MissingNul("string"));
        }
        String::from_utf8(bytes[..len - 1].to_vec()).map_err(|_| Error::InvalidUtf8("string"))
    }

    /// 在当前位置解码一个内嵌文档，并把 `end` 收紧到其边界。
    fn take_document(&mut self, depth: usize) -> Result<Document> {
        if depth > MAX_DEPTH {
            return Err(Error::DepthLimit { max: MAX_DEPTH });
        }
        if self.remaining() < MIN_DOC_LEN {
            return Err(Error::Truncated { need: MIN_DOC_LEN, have: self.remaining() });
        }
        let declared = self.take_i32()?;
        if declared < MIN_DOC_LEN as i32 {
            return Err(Error::NegativeLength("document"));
        }
        let declared = declared as usize;
        if self.remaining() + 4 < declared {
            return Err(Error::LengthExceedsBuffer {
                declared,
                have: self.remaining() + 4,
            });
        }

        let outer_end = self.end;
        // declared 包含长度字段自身的 4 字节，而 take_i32 已把 pos 前移了 4
        let doc_end = self.pos + declared - 4;
        // 只信任 declared 划出的区域：后续元素读取不会越过本子文档
        self.end = doc_end;
        let result = self.decode_document_body(depth, doc_end);
        self.end = outer_end;
        self.pos = doc_end;
        result
    }

    /// 解译文档体（已定位到长度字段之后），直到 `end - 1` 处的终止 NUL。
    fn decode_document_body(&mut self, depth: usize, end: usize) -> Result<Document> {
        if self.take_u8_is_at(end - 1)? != 0x00 {
            return Err(Error::MissingNul("document"));
        }
        let mut doc = Document::new();
        while self.pos < end - 1 {
            let type_byte = self.take_u8()?;
            let key = self.take_cstring()?.to_owned();
            let value = self.decode_value(type_byte, depth + 1)?;
            if doc.contains_key(&key) {
                // 规范未禁止重复键，但 MongoDB 取最后一个。显式拒绝可以消除
                // "同名字段到底取哪个"这类解析歧义（不同语言实现并不一致）。
                return Err(Error::DuplicateKey(key));
            }
            doc.insert(key, value);
        }
        Ok(doc)
    }

    fn take_u8_is_at(&mut self, at: usize) -> Result<u8> {
        if at >= self.end || at >= self.buf.len() {
            return Err(Error::Truncated { need: 1, have: 0 });
        }
        Ok(self.buf[at])
    }

    fn decode_value(&mut self, t: u8, depth: usize) -> Result<Bson> {
        Ok(match t {
            0x01 => Bson::Double(self.take_f64()?),
            0x02 => Bson::String(self.take_string()?),
            0x03 => Bson::Document(self.take_document(depth)?),
            0x04 => Bson::Array(self.take_array(depth)?),
            0x05 => self.take_binary()?,
            0x06 => Bson::Undefined,
            0x07 => {
                let b = self.take(ObjectId::LEN)?;
                Bson::ObjectId(ObjectId::from_bytes(b.try_into().map_err(|_| {
                    Error::Truncated { need: ObjectId::LEN, have: 0 }
                })?))
            }
            0x08 => match self.take_u8()? {
                0 => Bson::Boolean(false),
                1 => Bson::Boolean(true),
                other => return Err(Error::InvalidBoolean(other)),
            },
            0x09 => Bson::DateTime(self.take_i64()?),
            0x0a => Bson::Null,
            0x0b => {
                let pattern = self.take_cstring()?.to_owned();
                let options = self.take_cstring()?.to_owned();
                Bson::Regex(Regex { pattern, options })
            }
            0x0c => {
                let namespace = self.take_string()?;
                let b = self.take(ObjectId::LEN)?;
                Bson::DbPointer(DbPointer {
                    namespace,
                    id: ObjectId::from_bytes(b.try_into().map_err(|_| {
                        Error::Truncated { need: ObjectId::LEN, have: 0 }
                    })?),
                })
            }
            0x0d => Bson::JavaScript(self.take_string()?),
            0x0e => Bson::Symbol(self.take_string()?),
            0x0f => self.take_code_with_scope(depth)?,
            0x10 => Bson::Int32(self.take_i32()?),
            0x11 => Bson::Timestamp(Timestamp::from_u64(self.take_u64()?)),
            0x12 => Bson::Int64(self.take_i64()?),
            0x13 => {
                let b = self.take(Decimal128::LEN)?;
                Bson::Decimal128(
                    Decimal128::from_bytes(b.try_into().map_err(|_| {
                        Error::Truncated { need: Decimal128::LEN, have: 0 }
                    })?),
                )
            }
            0x7f => Bson::MaxKey,
            0xff => Bson::MinKey,
            other => return Err(Error::UnknownType(other)),
        })
    }

    fn take_binary(&mut self) -> Result<Bson> {
        let len = self.take_i32()?;
        if len < 0 {
            return Err(Error::NegativeLength("binary"));
        }
        let subtype = BinarySubtype::from_u8(self.take_u8()?);
        if subtype != BinarySubtype::BinaryOld {
            return Ok(Bson::Binary(Binary::new(subtype, self.take(len as usize)?.to_vec())));
        }
        // subtype 0x02 的历史布局多一个 int32 长度前缀：`len | 0x02 | inner_len | data`。
        //
        // BSON 规范原文写作"内外层长度与子类型都重复"，但 libbson / pymongo /
        // bson-js 一致地**只重复长度**、不重复子类型。遵循实现而非规范字面，
        // 否则写出的数据没有任何真实驱动读得回来。已用 pymongo 4.18 的字节
        // 样本验证。
        let inner = self.take_i32()?;
        if inner < 0 || inner as u64 > len as u64 - 4 {
            return Err(Error::BinaryOldMismatch { outer: len as u32, inner: inner.max(0) as u32 });
        }
        Ok(Bson::Binary(Binary::new(subtype, self.take(inner as usize)?.to_vec())))
    }

    fn take_code_with_scope(&mut self, depth: usize) -> Result<Bson> {
        // int32 total | string code | document scope
        //
        // total **包含自身 4 字节**（与 pymongo / libbson 一致，已用字节样本验证）。
        let total = self.take_i32()?;
        if total < 4 {
            return Err(Error::NegativeLength("code with scope"));
        }
        let body = total as usize - 4;
        if self.remaining() < body {
            return Err(Error::LengthExceedsBuffer { declared: total as usize, have: self.remaining() });
        }
        let outer_end = self.end;
        let body_end = self.pos + body;
        self.end = body_end;
        let code = self.take_string()?;
        let scope = self.take_document(depth)?;
        self.end = outer_end;
        self.pos = body_end;
        Ok(Bson::JavaScriptWithScope { code, scope })
    }

    /// 数组在字节层面就是文档，但键必须是 `"0"`,`"1"`,… 的十进制序号。
    fn take_array(&mut self, depth: usize) -> Result<Vec<Bson>> {
        let doc = self.take_document(depth)?;
        let mut out = Vec::with_capacity(doc.len());
        for (index, (key, value)) in doc.iter().enumerate() {
            if key != index.to_string() {
                return Err(Error::NonSequentialArrayKey {
                    index,
                    key: key.to_owned(),
                });
            }
            out.push(value.clone());
        }
        Ok(out)
    }
}

/// 解码文档，缓冲区必须被精确消费完。
pub fn from_bytes(buf: &[u8]) -> Result<Document> {
    if buf.len() > MAX_BSON_OBJECT_SIZE {
        return Err(Error::TooLarge { len: buf.len(), max: MAX_BSON_OBJECT_SIZE });
    }
    let mut d = Decoder::new(buf);
    let doc = d.take_document(0)?;
    if d.remaining() != 0 {
        return Err(Error::TrailingBytes { consumed: d.pos, len: buf.len() });
    }
    Ok(doc)
}

/// 解码文档并返回消耗的字节数（允许尾部有其它数据）。
///
/// `getMore` 等场景中一条消息内含多个文档，必须用这个变体，
/// 否则"忽略尾随字节"会允许把额外文档藏在同一条消息里。
pub fn from_bytes_prefix(buf: &[u8]) -> Result<(Document, usize)> {
    if buf.len() > MAX_BSON_OBJECT_SIZE {
        return Err(Error::TooLarge { len: buf.len(), max: MAX_BSON_OBJECT_SIZE });
    }
    let mut d = Decoder::new(buf);
    let doc = d.take_document(0)?;
    Ok((doc, d.pos))
}

// ---------------------------------------------------------------------------
// 编码
// ---------------------------------------------------------------------------

/// 编码文档。
///
/// 之所以可失败：超深嵌套、含 NUL 的键、超大文档都必须显式失败。
/// 若这里改成截断或 `panic`，畸形文档会在写盘路径上变成进程崩溃。
pub fn to_bytes(doc: &Document) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(256);
    encode_document(&mut out, doc, 0)?;
    if out.len() > MAX_BSON_OBJECT_SIZE {
        return Err(Error::TooLarge { len: out.len(), max: MAX_BSON_OBJECT_SIZE });
    }
    Ok(out)
}

/// 编码单个 BSON 值，形式为 **一个类型字节 + 该类型的值体**（即"无字段名的
/// 元素"）。[`from_bson`] 是它的逆运算。
///
/// 类型字节不可省：否则单独一段字节无法自描述，解码端拿不到类型，
/// `to_bson`/`from_bson` 就构不成闭环。`to_bson(&Bson::Int32(1))` 得到
/// `10 01 00 00 00`。
///
/// 这里必须写出**完整的值体**。只写一个类型字节的版本能通过"调用没 panic"
/// 这类测试，却在需要真实值的地方静默返回垃圾。
pub fn to_bson(value: &Bson) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(64);
    out.push(value.type_byte());
    match value {
        Bson::Document(d) => encode_document(&mut out, d, 0)?,
        other => encode_value(&mut out, other, 0)?,
    }
    if out.len() > MAX_BSON_OBJECT_SIZE {
        return Err(Error::TooLarge { len: out.len(), max: MAX_BSON_OBJECT_SIZE });
    }
    Ok(out)
}

/// 解码 [`to_bson`] 的输出。允许尾部有多余字节，因为调用方常从流中切出一段再传进来。
pub fn from_bson(bytes: &[u8]) -> Result<Bson> {
    let mut d = Decoder::new(bytes);
    let t = d.take_u8()?;
    d.decode_value(t, 0)
}

fn encode_document(out: &mut Vec<u8>, doc: &Document, depth: usize) -> Result<()> {
    if depth > MAX_DEPTH {
        return Err(Error::DepthLimit { max: MAX_DEPTH });
    }
    let start = out.len();
    out.extend_from_slice(&[0u8; 4]); // 长度回填位
    for (key, value) in doc.iter() {
        if key.as_bytes().contains(&0) {
            return Err(Error::KeyContainsNul(key.to_owned()));
        }
        out.push(value.type_byte());
        push_cstring(out, key, "key")?;
        encode_value(out, value, depth + 1)?;
    }
    out.push(0x00);
    let len = out.len() - start;
    if len > MAX_BSON_OBJECT_SIZE {
        return Err(Error::TooLarge { len, max: MAX_BSON_OBJECT_SIZE });
    }
    out[start..start + 4].copy_from_slice(&(len as i32).to_le_bytes());
    Ok(())
}

fn encode_value(out: &mut Vec<u8>, value: &Bson, depth: usize) -> Result<()> {
    match value {
        Bson::Double(v) => out.extend_from_slice(&v.to_le_bytes()),
        Bson::String(s) => encode_string(out, s),
        Bson::Document(d) => encode_document(out, d, depth)?,
        Bson::Array(a) => {
            let mut doc = Document::with_capacity(a.len());
            for (i, v) in a.iter().enumerate() {
                doc.insert(i.to_string(), v.clone());
            }
            encode_document(out, &doc, depth)?;
        }
        Bson::Binary(b) => {
            if b.subtype == BinarySubtype::BinaryOld {
                // 历史布局：外层长度覆盖"内层长度 + 数据"
                out.extend_from_slice(&((b.bytes.len() + 4) as i32).to_le_bytes());
            } else {
                out.extend_from_slice(&(b.bytes.len() as i32).to_le_bytes());
            }
            out.push(b.subtype.code());
            if b.subtype == BinarySubtype::BinaryOld {
                out.extend_from_slice(&(b.bytes.len() as i32).to_le_bytes());
            }
            out.extend_from_slice(&b.bytes);
        }
        Bson::Undefined => {}
        Bson::ObjectId(oid) => out.extend_from_slice(oid.bytes()),
        Bson::Boolean(b) => out.push(u8::from(*b)),
        Bson::DateTime(ms) => out.extend_from_slice(&ms.to_le_bytes()),
        Bson::Null => {}
        Bson::Regex(r) => {
            push_cstring(out, &r.pattern, "regex pattern")?;
            push_cstring(out, &r.options, "regex options")?;
        }
        Bson::DbPointer(p) => {
            encode_string(out, &p.namespace);
            out.extend_from_slice(p.id.bytes());
        }
        Bson::JavaScript(code) => encode_string(out, code),
        Bson::Symbol(s) => encode_string(out, s),
        Bson::JavaScriptWithScope { code, scope } => {
            let body_start = out.len() + 4; // 跳过 total 字段自身
            out.extend_from_slice(&[0u8; 4]);
            encode_string(out, code);
            encode_document(out, scope, depth)?;
            // total 含自身 4 字节
            let total = out.len() - body_start + 4;
            out[body_start - 4..body_start].copy_from_slice(&(total as i32).to_le_bytes());
        }
        Bson::Int32(v) => out.extend_from_slice(&v.to_le_bytes()),
        Bson::Timestamp(ts) => out.extend_from_slice(&ts.as_u64().to_le_bytes()),
        Bson::Int64(v) => out.extend_from_slice(&v.to_le_bytes()),
        Bson::Decimal128(d) => out.extend_from_slice(d.bytes()),
        Bson::MinKey | Bson::MaxKey => {}
    }
    Ok(())
}

fn encode_string(out: &mut Vec<u8>, s: &str) {
    // 长度含结尾 NUL，见 take_string 的说明
    out.extend_from_slice(&((s.len() + 1) as i32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

/// 写 BSON cstring（以 NUL 结尾的字节串）。
///
/// NUL 必须在**写入前**拒绝：cstring 靠第一个 NUL 定界，模式串里藏一个 NUL
/// 会让解码器把后半段当成独立字段，后续所有字段错位。BSON 的 cstring 无法
/// 表示 NUL，所以这是数据本身非法，只能报错而不是截断或替换。
fn push_cstring(out: &mut Vec<u8>, s: &str, what: &'static str) -> Result<()> {
    if s.as_bytes().contains(&0) {
        return Err(Error::ValueContainsNul(what));
    }
    out.extend_from_slice(s.as_bytes());
    out.push(0);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_bson_and_from_bson_roundtrip_every_type() {
        // `to_bson` 曾经对非 Document 只写一个类型字节：调用不报错，返回的却是
        // 垃圾。`to_bson`/`from_bson` 必须构成真正的闭环。
        let mut scope = Document::new();
        scope.insert("y", 3i32);
        let oid = ObjectId::from_hex("0123456789abcdef01234567").unwrap();
        let values = vec![
            Bson::Double(1.5),
            Bson::String("hello".into()),
            Bson::Document({ let mut d = Document::new(); d.insert("k", 1i32); d }),
            Bson::Array(vec![Bson::Int32(1), Bson::Null]),
            Bson::Binary(Binary::generic(vec![1, 2, 3])),
            Bson::Binary(Binary::new(BinarySubtype::from_u8(0x80), vec![9])),
            Bson::Undefined,
            Bson::ObjectId(oid),
            Bson::Boolean(true),
            Bson::DateTime(1_700_000_000_123),
            Bson::Null,
            Bson::Regex(Regex::new("^a", "i")),
            Bson::DbPointer(DbPointer { namespace: "db.c".into(), id: oid }),
            Bson::JavaScript("function(){}".into()),
            Bson::Symbol("sym".into()),
            Bson::JavaScriptWithScope { code: "x".into(), scope },
            Bson::Int32(7),
            Bson::Timestamp(Timestamp::new(1_700_000_000, 42)),
            Bson::Int64(-5),
            Bson::Decimal128(Decimal128::from_bytes([
                0x30, 0x40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01,
            ])),
            Bson::MinKey,
            Bson::MaxKey,
        ];
        for v in values {
            let bytes = to_bson(&v).unwrap_or_else(|e| panic!("encode {v:?}: {e}"));
            let back = from_bson(&bytes).unwrap_or_else(|e| panic!("decode {v:?}: {e}"));
            assert_eq!(back, v, "single-value roundtrip broke for {v:?}");
        }
    }

    #[test]
    fn to_bson_emits_a_type_byte_plus_the_value_body() {
        assert_eq!(to_bson(&Bson::Int32(1)).unwrap(), [0x10, 1, 0, 0, 0]);
        assert_eq!(to_bson(&Bson::String("ab".into())).unwrap(), b"\x02\x03\0\0\0ab\0".to_vec());
    }

    #[test]
    fn rejects_nul_inside_regex_cstrings() {
        // cstring 靠第一个 NUL 定界，放过 NUL 会让解码器把后半段当成独立字段
        let mut d = Document::new();
        d.insert("r", Bson::Regex(Regex::new("a\0b", "")));
        assert!(matches!(to_bytes(&d), Err(Error::ValueContainsNul("regex pattern"))));

        let mut d = Document::new();
        d.insert("r", Bson::Regex(Regex::new("a", "i\0m")));
        assert!(matches!(to_bytes(&d), Err(Error::ValueContainsNul("regex options"))));
    }

    fn all_types_doc() -> Document {
        let mut inner = Document::new();
        inner.insert("x", 1i32);
        let mut d = Document::new();
        d.insert("double", Bson::Double(1.5))
            .insert("string", "hello")
            .insert("doc", Bson::Document(inner))
            .insert("array", Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]))
            .insert("binary", Bson::Binary(Binary::generic(vec![1, 2, 3])))
            .insert("undefined", Bson::Undefined)
            .insert("oid", Bson::ObjectId(ObjectId::from_hex("0123456789abcdef01234567").unwrap()))
            .insert("bool", true)
            .insert("date", Bson::DateTime(1_700_000_000_000))
            .insert("null", Bson::Null)
            .insert("regex", Bson::Regex(Regex::new("^a", "i")))
            .insert("dbpointer", Bson::DbPointer(DbPointer {
                namespace: "db.c".into(),
                id: ObjectId::from_hex("0123456789abcdef01234567").unwrap(),
            }))
            .insert("js", Bson::JavaScript("function(){}".into()))
            .insert("symbol", Bson::Symbol("sym".into()))
            .insert("code_scope", Bson::JavaScriptWithScope {
                code: "x".into(),
                scope: Document::new(),
            })
            .insert("int32", 7i32)
            .insert("timestamp", Bson::Timestamp(Timestamp::new(1, 2)))
            .insert("int64", 8i64)
            .insert(
                "decimal",
                Bson::Decimal128(Decimal128::from_hex("00000000000000000000000000004030").unwrap()),
            )
            .insert("min", Bson::MinKey)
            .insert("max", Bson::MaxKey);
        d
    }

    #[test]
    fn empty_document_matches_spec_bytes() {
        // 规范最小文档：05 00 00 00 00
        assert_eq!(to_bytes(&Document::new()).unwrap(), vec![5, 0, 0, 0, 0]);
        assert!(from_bytes(&[5, 0, 0, 0, 0]).unwrap().is_empty());
    }

    #[test]
    fn roundtrip_all_types() {
        let d = all_types_doc();
        let bytes = to_bytes(&d).unwrap();
        assert_eq!(from_bytes(&bytes).unwrap(), d, "roundtrip must be lossless");
    }

    #[test]
    fn roundtrip_is_byte_stable() {
        let d = all_types_doc();
        let once = to_bytes(&d).unwrap();
        let twice = to_bytes(&from_bytes(&once).unwrap()).unwrap();
        assert_eq!(once, twice, "encode(decode(x)) must be stable");
    }

    #[test]
    fn preserves_field_order() {
        let mut d = Document::new();
        d.insert("z", 1i32).insert("a", 2i32).insert("m", 3i32);
        let back = from_bytes(&to_bytes(&d).unwrap()).unwrap();
        assert_eq!(back.keys().collect::<Vec<_>>(), ["z", "a", "m"]);
    }

    #[test]
    fn string_example_matches_spec_bytes() {
        // {"hello": "world"}
        // string 的长度字段含结尾 NUL，所以是 6 而不是 5
        let mut d = Document::new();
        d.insert("hello", "world");
        let expect = [
            0x16, 0, 0, 0, // 总长 22 = 4 + 1 + 6 + 4 + 6 + 1
            0x02, b'h', b'e', b'l', b'l', b'o', 0, // key
            0x06, 0, 0, 0, // 字符串长度 6 = 5 字节 + NUL
            b'w', b'o', b'r', b'l', b'd', 0, // 数据 + NUL
            0x00,
        ];
        assert_eq!(to_bytes(&d).unwrap(), expect);
        assert_eq!(from_bytes(&expect).unwrap().get("hello").and_then(Bson::as_str), Some("world"));
    }

    #[test]
    fn empty_string_encodes_as_length_one() {
        let mut d = Document::new();
        d.insert("k", "");
        // 空串的长度字段是 1（只有那个 NUL）
        assert_eq!(&to_bytes(&d).unwrap()[7..11], &[1, 0, 0, 0]);
        assert_eq!(from_bytes(&to_bytes(&d).unwrap()).unwrap().get("k").and_then(Bson::as_str), Some(""));
    }

    #[test]
    fn rejects_string_length_below_one() {
        // 长度 0 表示"没有 NUL"，非法
        let mut body = vec![0x02];
        body.extend_from_slice(b"k\0");
        body.extend_from_slice(&0i32.to_le_bytes());
        let mut bytes = ((body.len() + 5) as i32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&body);
        bytes.push(0);
        assert!(matches!(from_bytes(&bytes), Err(Error::NegativeLength("string"))));
    }

    #[test]
    fn rejects_string_whose_final_byte_is_not_nul() {
        let mut body = vec![0x02];
        body.extend_from_slice(b"k\0");
        body.extend_from_slice(&3i32.to_le_bytes());
        body.extend_from_slice(b"abc"); // 末字节不是 NUL
        let mut bytes = ((body.len() + 5) as i32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&body);
        bytes.push(0);
        assert!(matches!(from_bytes(&bytes), Err(Error::MissingNul("string"))));
    }

    #[test]
    fn code_with_scope_matches_pymongo_total_size() {
        // pymongo：Code("x = y", scope={"y": 3})
        // total = 4(自身) + 10(string) + 12(scope) = 26
        let mut scope = Document::new();
        scope.insert("y", 3i32);
        let mut d = Document::new();
        d.insert("c", Bson::JavaScriptWithScope { code: "x = y".into(), scope });
        let bytes = to_bytes(&d).unwrap();
        let total = i32::from_le_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);
        assert_eq!(total, 26, "total_size must include its own 4 bytes");
        let back = from_bytes(&bytes).unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn timestamp_field_uses_bson_bit_order() {
        // Timestamp(secs=1, inc=2) 的 8 字节必须是 01 00 00 00 02 00 00 00
        let mut d = Document::new();
        d.insert("ts", Bson::Timestamp(Timestamp::new(123_456_789, 42)));
        let bytes = to_bytes(&d).unwrap();
        let ts_at = bytes.len() - 9;
        // 低 32 位 = inc = 42，高 32 位 = secs = 123456789（bson-corpus 权威向量）
        assert_eq!(&bytes[ts_at..ts_at + 8], &[0x2A, 0, 0, 0, 0x15, 0xCD, 0x5B, 0x07]);
    }

    #[test]
    fn nested_document_cannot_eat_sibling_fields() {
        // 内层文档声明长度故意写大，若解码器不收紧上界就会越界读走后面的字段。
        let mut buf = Vec::new();
        buf.extend_from_slice(&0i32.to_le_bytes());
        buf.push(0x03);
        buf.extend_from_slice(b"a\0");
        buf.extend_from_slice(&1000i32.to_le_bytes()); // 谎报长度
        buf.extend_from_slice(&[0u8; 5]);
        buf.push(0x10);
        buf.extend_from_slice(b"b\0");
        buf.extend_from_slice(&7i32.to_le_bytes());
        buf.push(0x00);
        // 顶层文档长度覆盖整个缓冲区（含长度字段自身的 4 字节）
        let n = buf.len() as i32;
        buf[0..4].copy_from_slice(&n.to_le_bytes());

        let err = from_bytes(&buf).expect_err("must not trust an oversized child length");
        assert!(
            matches!(err, Error::LengthExceedsBuffer { .. } | Error::Truncated { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn rejects_truncation_at_every_offset() {
        let bytes = to_bytes(&all_types_doc()).unwrap();
        for cut in 0..bytes.len() {
            assert!(from_bytes(&bytes[..cut]).is_err(), "cut at {cut} must fail");
        }
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut bad = to_bytes(&Document::new()).unwrap();
        bad.push(0xff);
        assert!(matches!(from_bytes(&bad), Err(Error::TrailingBytes { .. })));
    }

    #[test]
    fn from_bytes_prefix_allows_trailing_data() {
          let a = to_bytes(Document::new().insert("a", 1i32)).unwrap();
          let b = to_bytes(Document::new().insert("b", 2i32)).unwrap();
        let mut joined = a.clone();
        joined.extend_from_slice(&b);
        let (doc, used) = from_bytes_prefix(&joined).unwrap();
        assert_eq!(used, a.len());
        assert_eq!(doc.get("a").and_then(Bson::as_i64), Some(1));
    }

    #[test]
    fn rejects_bad_declared_length() {
        assert!(from_bytes(&[0x7f, 0xff, 0xff, 0xff, 0x00]).is_err());
        assert!(from_bytes(&[1, 0, 0, 0, 0]).is_err(), "length < 5");
        assert!(from_bytes(&[]).is_err());
    }

    #[test]
    fn rejects_length_that_cuts_into_a_key() {
        // 文档声明长度把键名拦腰截断。错误种类取决于截断位置，
        // 要保证的是"必须报错"——放行会让解析器读出半个键名并继续解析。
        for declared in 5..=8usize {
            let mut buf = (declared as i32).to_le_bytes().to_vec();
            buf.extend_from_slice(&[0x10, b'k', b'e', b'y', b'v', 0x01, 0x02, 0x03]);
            assert!(
                from_bytes(&buf).is_err(),
                "declared length {declared} must be rejected"
            );
        }
    }

    #[test]
    fn cstring_scan_is_bounded_by_the_document_region() {
        // 键名 NUL 之前的扫描不得越过本文档边界：否则一个短文档会
        // 借后面兄弟字段的字节"续上"自己的键名。
        let mut inner = 5u32.to_le_bytes().to_vec();
        inner.extend_from_slice(&[0x10, b'a', 0x01, 0x00, 0x00]); // 键 "a" 缺 NUL

        let mut body = vec![0x03];
        body.extend_from_slice(b"a\0");
        body.extend_from_slice(&inner);
        body.push(0x10);
        body.extend_from_slice(b"b\0");
        body.extend_from_slice(&7i32.to_le_bytes());

        let mut bytes = ((body.len() + 5) as i32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&body);
        bytes.push(0);
        assert!(from_bytes(&bytes).is_err(), "short inner doc must not borrow sibling bytes");
    }

    #[test]
    fn rejects_invalid_utf8_in_value_string() {
        // 长度 2：1 字节数据 + NUL，数据字节 0xff 不是合法 UTF-8
        let mut body = Vec::new();
        body.push(0x02);
        body.extend_from_slice(b"k\0");
        body.extend_from_slice(&2i32.to_le_bytes());
        body.push(0xff);
        body.push(0x00);
        let mut bytes = ((body.len() + 5) as i32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&body);
        bytes.push(0);
        assert!(matches!(from_bytes(&bytes), Err(Error::InvalidUtf8("string"))));
    }

    #[test]
    fn rejects_invalid_utf8_in_key() {
        let mut body = vec![0x10];
        body.push(0xff);
        body.push(0x00);
        body.extend_from_slice(&1i32.to_le_bytes());
        let mut bytes = ((body.len() + 5) as i32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&body);
        bytes.push(0);
        assert!(matches!(from_bytes(&bytes), Err(Error::InvalidUtf8("cstring"))));
    }

    #[test]
    fn rejects_duplicate_keys() {
        let mut body = Vec::new();
        for v in [1i32, 2] {
            body.push(0x10);
            body.extend_from_slice(b"a\0");
            body.extend_from_slice(&v.to_le_bytes());
        }
        let mut bytes = ((body.len() + 5) as i32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&body);
        bytes.push(0);
        assert!(matches!(from_bytes(&bytes), Err(Error::DuplicateKey(_))));
    }

    #[test]
    fn rejects_unknown_type_byte() {
        let mut body = vec![0x99];
        body.extend_from_slice(b"k\0");
        let mut bytes = ((body.len() + 5) as i32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&body);
        bytes.push(0);
        assert!(matches!(from_bytes(&bytes), Err(Error::UnknownType(0x99))));
    }

    #[test]
    fn rejects_invalid_boolean() {
        for v in [2u8, 255] {
            let mut body = vec![0x08];
            body.extend_from_slice(b"k\0");
            body.push(v);
            let mut bytes = ((body.len() + 5) as i32).to_le_bytes().to_vec();
            bytes.extend_from_slice(&body);
            bytes.push(0);
            assert!(matches!(from_bytes(&bytes), Err(Error::InvalidBoolean(_))));
        }
    }

    #[test]
    fn rejects_non_sequential_array_keys() {
        // 数组元素的键跳过了 "0"，必须报错而不是静默接受
        let mut inner = Document::new();
        inner.insert("1", 1i32);
        let mut unused = Document::new();
        unused.insert("arr", Bson::Array(vec![Bson::Int32(1)]));
        // 手工构造 {"arr": [ {"1": 1} ]}
        let inner_bytes = to_bytes(&inner).unwrap();
        let mut body = vec![0x04];
        body.extend_from_slice(b"arr\0");
        body.extend_from_slice(&inner_bytes);
        let mut bytes = ((body.len() + 5) as i32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&body);
        bytes.push(0);
        assert!(matches!(from_bytes(&bytes), Err(Error::NonSequentialArrayKey { .. })));
    }

    #[test]
    fn rejects_over_deep_nesting_on_decode() {
        let mut value = Bson::Int32(1);
        for _ in 0..(MAX_DEPTH + 10) {
            value = Bson::Array(vec![value]);
        }
        // 编码侧先拒绝，因此这里手工堆字节绕过编码检查
        let mut buf = 0u32.to_le_bytes().to_vec();
        for _ in 0..(MAX_DEPTH + 10) {
            let mut inner = 0u32.to_le_bytes().to_vec();
            inner.push(0x04);
            inner.extend_from_slice(b"0\0");
            inner.extend_from_slice(&buf);
            inner.push(0);
            let n = inner.len() as u32;
            inner[0..4].copy_from_slice(&n.to_le_bytes());
            buf = inner;
        }
        buf.push(0);
        assert!(matches!(from_bytes(&buf), Err(Error::DepthLimit { .. })));
    }

    #[test]
    fn rejects_over_deep_nesting_on_encode() {
        let mut value = Bson::Int32(1);
        for _ in 0..(MAX_DEPTH + 10) {
            value = Bson::Array(vec![value]);
        }
        let mut doc = Document::new();
        doc.insert("deep", value);
        assert!(matches!(to_bytes(&doc), Err(Error::DepthLimit { .. })));
    }

    #[test]
    fn rejects_key_containing_nul() {
        let mut doc = Document::new();
        doc.insert("a\0b", 1i32);
        assert!(matches!(to_bytes(&doc), Err(Error::KeyContainsNul(_))));
    }

    #[test]
    fn rejects_oversized_document() {
        let big = vec![0u8; MAX_BSON_OBJECT_SIZE + 1];
        assert!(matches!(from_bytes(&big), Err(Error::TooLarge { .. })));
    }

    #[test]
    fn binary_old_keeps_its_length_prefix() {
        // subtype 0x02：外层 int32 覆盖 (内层长度 + 数据)，子类型**不重复**
        let b = Binary::new(BinarySubtype::BinaryOld, vec![9, 9]);
        let mut d = Document::new();
        d.insert("b", Bson::Binary(b.clone()));
        let back = from_bytes(&to_bytes(&d).unwrap()).unwrap();
        match back.get("b") {
            Some(Bson::Binary(got)) => {
                assert_eq!(got.subtype, BinarySubtype::BinaryOld);
                assert_eq!(got.bytes, b.bytes);
            }
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn binary_old_matches_pymongo_byte_layout() {
        // pymongo 4.18：Binary(b"old", 2) 的字段体为
        // 07 00 00 00 | 02 | 03 00 00 00 | "old"
        let mut d = Document::new();
        d.insert("b", Bson::Binary(Binary::new(BinarySubtype::BinaryOld, b"old".to_vec())));
        let bytes = to_bytes(&d).unwrap();
        assert_eq!(
            &bytes[7..],
            &[0x07, 0x00, 0x00, 0x00, 0x02, 0x03, 0x00, 0x00, 0x00, b'o', b'l', b'd', 0x00]
        );
        assert_eq!(from_bytes(&bytes).unwrap(), d, "and it must decode back");
    }

    #[test]
    fn binary_old_rejects_inconsistent_inner_length() {
        // int32 6 | 0x02 | int32 99 | "abc" —— 内层长度超出外层可容纳范围
        let mut body = vec![0x05];
        body.extend_from_slice(b"b\0");
        body.extend_from_slice(&6i32.to_le_bytes());
        body.push(0x02);
        body.extend_from_slice(&99i32.to_le_bytes());
        body.extend_from_slice(b"abc");
        let mut bytes = ((body.len() + 5) as i32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&body);
        bytes.push(0);
        assert!(matches!(from_bytes(&bytes), Err(Error::BinaryOldMismatch { .. })));
    }

    #[test]
    fn unknown_binary_subtype_survives_roundtrip() {
        let b = Binary::new(BinarySubtype::from_u8(0x42), vec![7]);
        let mut d = Document::new();
        d.insert("b", Bson::Binary(b));
        let back = from_bytes(&to_bytes(&d).unwrap()).unwrap();
        match back.get("b") {
            Some(Bson::Binary(bin)) => assert_eq!(bin.subtype.code(), 0x42),
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn code_with_scope_roundtrips() {
        let mut scope = Document::new();
        scope.insert("v", 9i32);
        let mut d = Document::new();
        d.insert("c", Bson::JavaScriptWithScope { code: "f()".into(), scope });
        let back = from_bytes(&to_bytes(&d).unwrap()).unwrap();
        match back.get("c") {
            Some(Bson::JavaScriptWithScope { code, scope }) => {
                assert_eq!(code, "f()");
                assert_eq!(scope.get("v").and_then(Bson::as_i64), Some(9));
            }
            other => panic!("expected code with scope, got {other:?}"),
        }
    }

    #[test]
    fn unicode_keys_and_values_survive() {
        let mut d = Document::new();
        d.insert("键", "值🚀");
        assert_eq!(from_bytes(&to_bytes(&d).unwrap()).unwrap(), d);
    }
}
