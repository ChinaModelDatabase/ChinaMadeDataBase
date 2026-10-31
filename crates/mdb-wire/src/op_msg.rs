//! `OP_MSG`（opCode 2013）编解码。
//!
//! # 布局
//!
//! ```text
//! +------------------------------------------+
//! | MessageHeader (16 bytes)                 |
//! +------------------------------------------+
//! | flagBits (uint32)                        |
//! +------------------------------------------+
//! | section*                                 |
//! |   kind 0: body            (单个 BSON 文档) |
//! |   kind 1: documentSequence              |
//! +------------------------------------------+
//! | checksum (uint32, 仅当 checksumPresent)  |
//! +------------------------------------------+
//! ```
//!
//! # 规范约束（每条都有对应测试）
//!
//! 1. **body 段必须在最前**：MongoDB 要求 kind 0 若存在则为第一个段。
//!    编码时恒定先写 body；解码时发现 body 不在首位即报错。
//! 2. **至多一个** body 段。重复 body 会让服务端行为未定义。
//! 3. **checksumPresent 时必须有非空 sections**。`flagBits` 之后立刻就是
//!    checksum，没有任何字段可供承载长度，此时空段列表无法自洽。
//! 4. **checksum 覆盖 `flagBits` 到 sections 末尾**，不含 16 字节头，
//!    也不含 checksum 自身。算法为 CRC32C。
//!
//! # `moreToCome` 语义
//!
//! 该位表示"发送方还有后续消息，且本消息不应产生应答"。它属于**协议约定**
//! 而非编码，本模块只做携带与解析，不断言调用方用对了。

use mdb_common::{ErrorCode, MdbError, MdbResult};
use mdb_crypto::crc32c::crc32c;
use mdb_doc::{codec, Document};

use crate::header::{MessageHeader, OpCode, HEADER_SIZE, MAX_BSON_OBJECT_SIZE};

/// 存在尾部校验和。
pub const FLAG_CHECKSUM_PRESENT: u32 = 1 << 0;
/// 发送方还有后续消息，本消息不需应答。
pub const FLAG_MORE_TO_COME: u32 = 1 << 1;
/// 允许 `exhaust` 游标。
pub const FLAG_EXHAUST_ALLOWED: u32 = 1 << 16;

/// `OP_MSG` 段类型：单文档 body。
pub const SECTION_BODY: u8 = 0;
/// `OP_MSG` 段类型：文档序列。
pub const SECTION_DOCUMENT_SEQUENCE: u8 = 1;

/// 文档序列段。
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentSequence {
    /// 序列标识符（cstring），如 `"documents"`、`"cursors.firstBatch"`。
    pub identifier: String,
    /// 序列中的文档，按发送顺序。
    pub documents: Vec<Document>,
}

impl DocumentSequence {
    /// 新建空序列。
    pub fn new(identifier: impl Into<String>) -> Self {
        Self {
            identifier: identifier.into(),
            documents: Vec::new(),
        }
    }

    /// 追加一个文档。
    pub fn push(&mut self, doc: Document) -> &mut Self {
        self.documents.push(doc);
        self
    }
}

/// 一条 `OP_MSG` 消息。
#[derive(Debug, Clone, PartialEq)]
pub struct OpMsg {
    /// 本消息 id。
    pub request_id: i32,
    /// 被应答的消息 id；新请求为 0。
    pub response_to: i32,
    /// 标志位。
    pub flags: u32,
    /// body 段（命令本体）。允许缺省（如 `moreToCome` 的免应答写）。
    pub body: Option<Document>,
    /// 文档序列段，按 wire 顺序。
    pub document_sequences: Vec<DocumentSequence>,
}

impl OpMsg {
    /// 构造一条只有 body 的请求。
    pub fn request(request_id: i32, body: Document) -> Self {
        Self {
            request_id,
            response_to: 0,
            flags: 0,
            body: Some(body),
            document_sequences: Vec::new(),
        }
    }

    /// 构造一条对 `request_id` 的应答。
    pub fn reply(response_to: i32, body: Document) -> Self {
        Self {
            request_id: 0,
            response_to,
            flags: 0,
            body: Some(body),
            document_sequences: Vec::new(),
        }
    }

    /// 是否带 `moreToCome`。
    pub const fn more_to_come(&self) -> bool {
        self.flags & FLAG_MORE_TO_COME != 0
    }

    /// 设置 `moreToCome`。
    pub fn set_more_to_come(&mut self, on: bool) -> &mut Self {
        if on {
            self.flags |= FLAG_MORE_TO_COME;
        } else {
            self.flags &= !FLAG_MORE_TO_COME;
        }
        self
    }

    /// 是否带 `checksumPresent`。
    pub const fn checksum_present(&self) -> bool {
        self.flags & FLAG_CHECKSUM_PRESENT != 0
    }

    /// body 中指定字段的字符串值。
    pub fn body_str(&self, key: &str) -> Option<&str> {
        self.body.as_ref()?.get(key)?.as_str()
    }

    /// body 中指定字段的整数值。
    pub fn body_i32(&self, key: &str) -> Option<i32> {
        self.body.as_ref()?.get(key)?.as_i64().and_then(|v| i32::try_from(v).ok())
    }

    /// 序列化为完整线格式（含 16 字节头）。
    pub fn to_bytes(&self) -> MdbResult<Vec<u8>> {
        // flagBits 之后 sections 的字节数
        let mut sections = Vec::new();
        if let Some(body) = &self.body {
            sections.push(SECTION_BODY);
            let doc = codec::to_bytes(body)?;
            check_bson_size(doc.len())?;
            sections.extend_from_slice(&doc);
        }
        for seq in &self.document_sequences {
            // 只编码一次：先拿到全部文档字节，size 字段需要它们的长度
            let encoded: Vec<Vec<u8>> = seq
                .documents
                .iter()
                .map(|d| {
                    let bytes = codec::to_bytes(d)?;
                    check_bson_size(bytes.len())?;
                    Ok(bytes)
                })
                .collect::<MdbResult<Vec<_>>>()?;
            // size 覆盖自身 4 字节 + cstring + 全部文档
            let payload_len = seq.identifier.len() + 1 + encoded.iter().map(Vec::len).sum::<usize>();
            let size = 4 + payload_len;
            if size > i32::MAX as usize {
                return Err(MdbError::new(
                    ErrorCode::InvalidLength,
                    format!("document sequence size {size} overflows int32"),
                ));
            }
            sections.push(SECTION_DOCUMENT_SEQUENCE);
            sections.extend_from_slice(&(size as i32).to_le_bytes());
            sections.extend_from_slice(seq.identifier.as_bytes());
            sections.push(0);
            for bytes in encoded {
                sections.extend_from_slice(&bytes);
            }
        }

        let payload_len = 4 + sections.len() + if self.checksum_present() { 4 } else { 0 };
        let total = HEADER_SIZE + payload_len;
        if total > crate::header::MAX_MESSAGE_SIZE_BYTES as usize {
            return Err(MdbError::new(
                ErrorCode::InvalidLength,
                format!("encoded OP_MSG of {total} bytes exceeds the 48,000,000-byte limit"),
            ));
        }

        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(
            &MessageHeader {
                message_length: total as i32,
                request_id: self.request_id,
                response_to: self.response_to,
                op_code: OpCode::Msg,
            }
            .to_bytes(),
        );
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&sections);
        if self.checksum_present() {
            // 覆盖 flagBits + sections，不含头、不含自身
            out.extend_from_slice(&crc32c(&out[HEADER_SIZE..]).to_le_bytes());
        }
        Ok(out)
    }

    /// 解析一条完整 `OP_MSG` 消息。
    ///
    /// # 错误
    ///
    /// 长度不匹配、段类型未知、body 不在首位、重复 body、校验和不符时返回错误。
    pub fn parse(buf: &[u8]) -> MdbResult<Self> {
        let header = MessageHeader::parse(buf)?.expect_op_code(OpCode::Msg, "OP_MSG")?;
        let total = header.message_length as usize;
        if buf.len() < total {
            return Err(MdbError::new(
                ErrorCode::InvalidLength,
                format!("OP_MSG declares {total} bytes but only {} are available", buf.len()),
            ));
        }
        // 拒绝尾随数据：多出一条消息是分帧错误，宽松接受会掩盖协议缺陷
        if buf.len() > total {
            return Err(MdbError::new(
                ErrorCode::InvalidLength,
                format!(
                    "OP_MSG declares {total} bytes but {} were supplied (trailing data)",
                    buf.len()
                ),
            ));
        }
        let body_bytes = &buf[..total];

        if total < HEADER_SIZE + 4 {
            return Err(MdbError::new(
                ErrorCode::InvalidLength,
                format!("OP_MSG of {total} bytes is too short to hold flagBits"),
            ));
        }
        let flags = u32::from_le_bytes([body_bytes[16], body_bytes[17], body_bytes[18], body_bytes[19]]);

        let has_checksum = flags & FLAG_CHECKSUM_PRESENT != 0;
        let mut cursor = HEADER_SIZE + 4;
        if has_checksum && total < HEADER_SIZE + 8 {
            return Err(MdbError::new(
                ErrorCode::InvalidLength,
                format!("OP_MSG of {total} bytes is too short to hold flagBits plus checksum"),
            ));
        }
        // checksum 占据最后 4 字节，不参与段解析
        let sections_end = if has_checksum { total - 4 } else { total };
        // 先验完整性再解释结构：段解析要读长度字段，直接对未校验的
        // 字节做长度运算等于把攻击面放大。定位 checksum 本身不依赖
        // 段内容，只依赖已校验上界的 messageLength。
        if has_checksum {
            let want = u32::from_le_bytes([
                body_bytes[sections_end],
                body_bytes[sections_end + 1],
                body_bytes[sections_end + 2],
                body_bytes[sections_end + 3],
            ]);
            let got = crc32c(&body_bytes[HEADER_SIZE..sections_end]);
            if want != got {
                return Err(MdbError::new(
                    ErrorCode::DataCorruption,
                    format!("OP_MSG checksum mismatch: header says {want:#010x}, computed {got:#010x}"),
                ));
            }
            // 规范约束 3：checksumPresent 时必须有非空 sections。
            // `flagBits` 之后没有任何字段可承载长度，空段列表无法自洽。
            if sections_end <= cursor {
                return Err(MdbError::new(
                    ErrorCode::FailedToParse,
                    "checksumPresent requires at least one section",
                ));
            }
        }

        let mut msg = Self {
            request_id: header.request_id,
            response_to: header.response_to,
            flags,
            body: None,
            document_sequences: Vec::new(),
        };
        let mut seen_body = false;
        while cursor < sections_end {
            let kind = body_bytes[cursor];
            cursor += 1;
            match kind {
                SECTION_BODY => {
                    if seen_body {
                        return Err(MdbError::new(
                            ErrorCode::FailedToParse,
                            "OP_MSG contains more than one body section",
                        ));
                    }
                    // 规范约束 1
                    if !msg.document_sequences.is_empty() {
                        return Err(MdbError::new(
                            ErrorCode::FailedToParse,
                            "OP_MSG body section must precede document sequences",
                        ));
                    }
                    let (doc, used) = codec::from_bytes_prefix(&body_bytes[cursor..sections_end])?;
                    msg.body = Some(doc);
                    cursor += used;
                    seen_body = true;
                }
                SECTION_DOCUMENT_SEQUENCE => {
                    if sections_end - cursor < 4 {
                        return Err(MdbError::new(
                            ErrorCode::InvalidLength,
                            "document sequence section is missing its int32 size",
                        ));
                    }
                    let size = i32::from_le_bytes([
                        body_bytes[cursor],
                        body_bytes[cursor + 1],
                        body_bytes[cursor + 2],
                        body_bytes[cursor + 3],
                    ]);
                    if size < 5 {
                        return Err(MdbError::new(
                            ErrorCode::InvalidLength,
                            format!("document sequence size {size} is too small"),
                        ));
                    }
                    let size = size as usize;
                    if size > sections_end - cursor {
                        return Err(MdbError::new(
                            ErrorCode::InvalidLength,
                            format!(
                                "document sequence declares {size} bytes but only {} remain",
                                sections_end - cursor
                            ),
                        ));
                    }
                    let seq_area = &body_bytes[cursor..cursor + size];
                    // size 自身占 4 字节，其余是 cstring + 文档
                    let after_size = &seq_area[4..];
                    let nul = after_size
                        .iter()
                        .position(|&b| b == 0)
                        .ok_or_else(|| {
                            MdbError::new(
                                ErrorCode::FailedToParse,
                                "document sequence identifier is not NUL-terminated",
                            )
                        })?;
                    let identifier = std::str::from_utf8(&after_size[..nul])
                        .map_err(|_| {
                            MdbError::new(ErrorCode::FailedToParse, "sequence identifier is not UTF-8")
                        })?
                        .to_owned();
                    let mut documents = Vec::new();
                    let mut inner = nul + 1;
                    while inner < after_size.len() {
                        let (doc, used) = codec::from_bytes_prefix(&after_size[inner..])?;
                        documents.push(doc);
                        inner += used;
                    }
                    msg.document_sequences
                        .push(DocumentSequence { identifier, documents });
                    cursor += size;
                }
                other => {
                    return Err(MdbError::new(
                        ErrorCode::FailedToParse,
                        format!("unknown OP_MSG section kind {other}"),
                    ));
                }
            }
        }
        Ok(msg)
    }
}

fn check_bson_size(len: usize) -> MdbResult<()> {
    if len > MAX_BSON_OBJECT_SIZE as usize {
        return Err(MdbError::new(
            ErrorCode::InvalidLength,
            format!("BSON document of {len} bytes exceeds maxBsonObjectSize"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdb_doc::Bson;

    fn cmd_doc(name: &str) -> Document {
        let mut d = Document::new();
        d.insert(name, 1i32);
        d
    }

    #[test]
    fn minimal_roundtrip() {
        let msg = OpMsg::request(42, cmd_doc("ping"));
        let bytes = msg.to_bytes().unwrap();
        let back = OpMsg::parse(&bytes).unwrap();
        assert_eq!(back.request_id, 42);
        assert_eq!(back.body_str("ping"), None); // 值为 i32 而非 string
        assert_eq!(back.body.unwrap().get("ping"), Some(&Bson::Int32(1)));
        assert!(back.document_sequences.is_empty());
    }

    #[test]
    fn message_length_field_matches_actual() {
        let bytes = OpMsg::request(1, cmd_doc("ping")).to_bytes().unwrap();
        let declared =
            i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        assert_eq!(declared, bytes.len(), "messageLength must equal the byte count");
    }

    #[test]
    fn checksum_is_computed_over_flags_and_sections_only() {
        let mut msg = OpMsg::request(9, cmd_doc("find"));
        msg.flags |= FLAG_CHECKSUM_PRESENT;
        let bytes = msg.to_bytes().unwrap();
        let total = bytes.len();
        let stored = u32::from_le_bytes([
            bytes[total - 4],
            bytes[total - 3],
            bytes[total - 2],
            bytes[total - 1],
        ]);
        // 覆盖范围是 [HEADER_SIZE, total-4)
        let expected = crc32c(&bytes[HEADER_SIZE..total - 4]);
        assert_eq!(stored, expected);
        assert_eq!(OpMsg::parse(&bytes).unwrap().flags & FLAG_CHECKSUM_PRESENT, FLAG_CHECKSUM_PRESENT);
    }

    #[test]
    fn detects_corrupted_checksum() {
        let mut msg = OpMsg::request(1, cmd_doc("ping"));
        msg.flags |= FLAG_CHECKSUM_PRESENT;
        let mut bytes = msg.to_bytes().unwrap();
        // 篡改 body 段内的一个字节
        bytes[HEADER_SIZE + 4 + 4] ^= 0xFF;
        let err = OpMsg::parse(&bytes).unwrap_err();
        assert_eq!(err.code(), ErrorCode::DataCorruption, "{err}");
        assert!(err.to_string().contains("checksum mismatch"), "{err}");
    }

    #[test]
    fn document_sequence_roundtrip() {
        let mut msg = OpMsg::request(5, cmd_doc("insert"));
        let mut seq = DocumentSequence::new("documents");
        seq.push(cmd_doc("a")).push(cmd_doc("b"));
        msg.document_sequences.push(seq);
        let bytes = msg.to_bytes().unwrap();
        let back = OpMsg::parse(&bytes).unwrap();
        assert_eq!(back.document_sequences.len(), 1);
        let got = &back.document_sequences[0];
        assert_eq!(got.identifier, "documents");
        assert_eq!(got.documents.len(), 2);
        assert_eq!(got.documents[0].get("a"), Some(&Bson::Int32(1)));
        assert_eq!(got.documents[1].get("b"), Some(&Bson::Int32(1)));
    }

    #[test]
    fn empty_document_sequence_roundtrip() {
        let mut msg = OpMsg::request(5, cmd_doc("insert"));
        msg.document_sequences.push(DocumentSequence::new("documents"));
        let bytes = msg.to_bytes().unwrap();
        let back = OpMsg::parse(&bytes).unwrap();
        assert_eq!(back.document_sequences[0].documents.len(), 0);
    }

    #[test]
    fn body_comes_first_on_the_wire() {
        let mut msg = OpMsg::request(1, cmd_doc("insert"));
        msg.document_sequences.push(DocumentSequence::new("documents"));
        let bytes = msg.to_bytes().unwrap();
        // 偏移 20 是第一个段类型
        assert_eq!(bytes[HEADER_SIZE + 4], SECTION_BODY);
    }

    #[test]
    fn rejects_body_after_document_sequence() {
        let mut msg = OpMsg::request(1, cmd_doc("insert"));
        msg.document_sequences.push(DocumentSequence::new("documents"));
        let bytes = msg.to_bytes().unwrap();
        // 手工把两个段对调：把 kind 0 段移到末尾
        let (head, tail) = bytes.split_at(HEADER_SIZE + 4);
        let body_len = 1 + mdb_doc::codec::to_bytes(&cmd_doc("insert")).unwrap().len();
        let reordered: Vec<u8> = head
            .iter()
            .chain(&tail[body_len..])
            .chain(&tail[..body_len])
            .copied()
            .collect();
        let err = OpMsg::parse(&reordered).unwrap_err();
        assert!(
            err.to_string().contains("must precede"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_duplicate_body_section() {
        let doc = codec::to_bytes(&cmd_doc("ping")).unwrap();
        let mut bytes = Vec::new();
        let total = HEADER_SIZE + 4 + (1 + doc.len()) * 2;
        bytes.extend_from_slice(&(total as i32).to_le_bytes());
        bytes.extend_from_slice(&1i32.to_le_bytes());
        bytes.extend_from_slice(&0i32.to_le_bytes());
        bytes.extend_from_slice(&2013i32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.push(SECTION_BODY);
        bytes.extend_from_slice(&doc);
        bytes.push(SECTION_BODY);
        bytes.extend_from_slice(&doc);
        let err = OpMsg::parse(&bytes).unwrap_err();
        assert!(err.to_string().contains("more than one body"), "{err}");
    }

    #[test]
    fn rejects_unknown_section_kind() {
        let mut bytes = Vec::new();
        let total = HEADER_SIZE + 4 + 1;
        bytes.extend_from_slice(&(total as i32).to_le_bytes());
        bytes.extend_from_slice(&1i32.to_le_bytes());
        bytes.extend_from_slice(&0i32.to_le_bytes());
        bytes.extend_from_slice(&2013i32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.push(7);
        let err = OpMsg::parse(&bytes).unwrap_err();
        assert!(err.to_string().contains("unknown OP_MSG section kind 7"), "{err}");
    }

    #[test]
    fn rejects_checksum_with_no_sections() {
        // 构造"结构不合法但校验和正确"的消息，确保走到结构检查而非
        // 完整性检查——校验和先于结构解释是刻意的安全顺序。
        let flags = FLAG_CHECKSUM_PRESENT.to_le_bytes();
        let total = HEADER_SIZE + 4 + 4;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(total as i32).to_le_bytes());
        bytes.extend_from_slice(&1i32.to_le_bytes());
        bytes.extend_from_slice(&0i32.to_le_bytes());
        bytes.extend_from_slice(&2013i32.to_le_bytes());
        bytes.extend_from_slice(&flags);
        let sum = crc32c(&flags);
        bytes.extend_from_slice(&sum.to_le_bytes());
        let err = OpMsg::parse(&bytes).unwrap_err();
        assert!(err.to_string().contains("at least one section"), "{err}");
    }

    #[test]
    fn checksum_is_verified_before_structure_is_interpreted() {
        // 校验和错误的畸形消息应报完整性错误，而不是解析错误
        let mut bytes = Vec::new();
        let total = HEADER_SIZE + 4 + 4;
        bytes.extend_from_slice(&(total as i32).to_le_bytes());
        bytes.extend_from_slice(&1i32.to_le_bytes());
        bytes.extend_from_slice(&0i32.to_le_bytes());
        bytes.extend_from_slice(&2013i32.to_le_bytes());
        bytes.extend_from_slice(&FLAG_CHECKSUM_PRESENT.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes()); // 错误的校验和
        let err = OpMsg::parse(&bytes).unwrap_err();
        assert_eq!(err.code(), ErrorCode::DataCorruption, "{err}");
    }

    #[test]
    fn rejects_trailing_data() {
        let mut bytes = OpMsg::request(1, cmd_doc("ping")).to_bytes().unwrap();
        bytes.push(0);
        let err = OpMsg::parse(&bytes).unwrap_err();
        assert!(err.to_string().contains("trailing data"), "{err}");
    }

    #[test]
    fn rejects_truncated_message() {
        let bytes = OpMsg::request(1, cmd_doc("ping")).to_bytes().unwrap();
        let err = OpMsg::parse(&bytes[..bytes.len() - 3]).unwrap_err();
        assert!(err.to_string().contains("only"), "{err}");
    }

    #[test]
    fn rejects_wrong_op_code() {
        let mut bytes = OpMsg::request(1, cmd_doc("ping")).to_bytes().unwrap();
        bytes[12..16].copy_from_slice(&2004i32.to_le_bytes());
        let err = OpMsg::parse(&bytes).unwrap_err();
        assert!(err.to_string().contains("2013"), "{err}");
    }

    #[test]
    fn more_to_come_roundtrips_and_toggles() {
        let mut msg = OpMsg::request(3, cmd_doc("insert"));
        assert!(!msg.more_to_come());
        msg.set_more_to_come(true);
        assert!(msg.more_to_come());
        let back = OpMsg::parse(&msg.to_bytes().unwrap()).unwrap();
        assert!(back.more_to_come());
        msg.set_more_to_come(false);
        assert!(!msg.more_to_come());
    }

    #[test]
    fn bodyless_message_roundtrips() {
        let msg = OpMsg {
            request_id: 1,
            response_to: 0,
            flags: FLAG_MORE_TO_COME,
            body: None,
            document_sequences: Vec::new(),
        };
        let bytes = msg.to_bytes().unwrap();
        let back = OpMsg::parse(&bytes).unwrap();
        assert!(back.body.is_none());
        assert!(back.more_to_come());
    }

    #[test]
    fn accessors_read_typed_fields() {
        let mut d = Document::new();
        d.insert("maxTimeMS", 500i32);
        d.insert("name", Bson::String("x".into()));
        let msg = OpMsg::request(1, d);
        assert_eq!(msg.body_i32("maxTimeMS"), Some(500));
        assert_eq!(msg.body_str("name"), Some("x"));
        assert_eq!(msg.body_i32("missing"), None);
    }
}
