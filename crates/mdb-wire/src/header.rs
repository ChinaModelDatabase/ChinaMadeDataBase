//! MongoDB 线协议消息头与操作码。
//!
//! # 布局
//!
//! 所有 MongoDB 线协议消息共用 16 字节定长头，**全部为小端**：
//!
//! ```text
//! offset  size  字段
//! 0       4     messageLength  含头本身的总字节数
//! 4       4     requestID
//! 8       4     responseTo    被应答的 requestID；新请求为 0
//! 12      4     opCode
//! ```
//!
//! # 关键校验
//!
//! `messageLength` 是**唯一**的长度来源（无其他终止符），因此它是攻击面：
//!
//! - 必须 `<= 48_000_000`（[`MAX_MESSAGE_SIZE_BYTES`]），否则拒绝；
//! - 必须 `>= 16`（头本身），否则拒绝；
//! - 读取时**不得**信任它来做 `read_exact` 之后再校验，必须先校验再分配。

use mdb_common::{ErrorCode, MdbError, MdbResult};

/// 线协议消息头固定长度。
pub const HEADER_SIZE: usize = 16;

/// MongoDB 允许的单条消息最大字节数（`maxMessageSizeBytes` 默认值）。
pub const MAX_MESSAGE_SIZE_BYTES: i32 = 48_000_000;

/// MongoDB 允许的单个 BSON 文档最大字节数（`maxBsonObjectSize` 默认值）。
pub const MAX_BSON_OBJECT_SIZE: i32 = 16 * 1024 * 1024;

/// 线协议操作码。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpCode {
    /// 旧版查询应答。
    Reply,
    /// 旧版查询。
    Query,
    /// 取游标下一批。
    GetMore,
    /// 关闭游标。
    KillCursors,
    /// 压缩包装的消息。
    Compressed,
    /// 现代通用消息（MongoDB 3.6+）。
    Msg,
    /// 未知操作码。
    Unknown(i32),
}

impl OpCode {
    /// 由线上的 `i32` 构造。
    pub const fn from_i32(v: i32) -> Self {
        match v {
            1 => OpCode::Reply,
            2004 => OpCode::Query,
            2005 => OpCode::GetMore,
            2007 => OpCode::KillCursors,
            2012 => OpCode::Compressed,
            2013 => OpCode::Msg,
            other => OpCode::Unknown(other),
        }
    }

    /// 转为线上的 `i32`。
    pub const fn as_i32(self) -> i32 {
        match self {
            OpCode::Reply => 1,
            OpCode::Query => 2004,
            OpCode::GetMore => 2005,
            OpCode::KillCursors => 2007,
            OpCode::Compressed => 2012,
            OpCode::Msg => 2013,
            OpCode::Unknown(v) => v,
        }
    }
}

/// 16 字节消息头。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHeader {
    /// 含头本身的总字节数。
    pub message_length: i32,
    /// 本消息 id。
    pub request_id: i32,
    /// 被应答的消息 id；非应答消息为 0。
    pub response_to: i32,
    /// 操作码。
    pub op_code: OpCode,
}

impl MessageHeader {
    /// 构造一个新请求的头。
    pub const fn request(request_id: i32, op_code: OpCode, payload_len: usize) -> Self {
        Self {
            message_length: (HEADER_SIZE + payload_len) as i32,
            request_id,
            response_to: 0,
            op_code,
        }
    }

    /// 从字节切片解析消息头。
    ///
    /// # 错误
    ///
    /// 长度不足、长度越界或操作码非 `OP_MSG` 时返回错误。
    pub fn parse(buf: &[u8]) -> MdbResult<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(bad(format!(
                "message header needs {HEADER_SIZE} bytes, got {}",
                buf.len()
            )));
        }
        let message_length = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let request_id = i32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let response_to = i32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let op_code = OpCode::from_i32(i32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]));

        if message_length < HEADER_SIZE as i32 {
            return Err(bad(format!(
                "messageLength {message_length} is smaller than the {HEADER_SIZE}-byte header"
            )));
        }
        if message_length > MAX_MESSAGE_SIZE_BYTES {
            return Err(bad(format!(
                "messageLength {message_length} exceeds the {MAX_MESSAGE_SIZE_BYTES}-byte limit"
            )));
        }
        Ok(Self {
            message_length,
            request_id,
            response_to,
            op_code,
        })
    }

    /// 序列化为 16 字节。
    pub fn to_bytes(self) -> [u8; HEADER_SIZE] {
        let mut out = [0u8; HEADER_SIZE];
        out[0..4].copy_from_slice(&self.message_length.to_le_bytes());
        out[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.response_to.to_le_bytes());
        out[12..16].copy_from_slice(&self.op_code.as_i32().to_le_bytes());
        out
    }

    /// 校验操作码是期望值。
    pub fn expect_op_code(self, expected: OpCode, what: &str) -> MdbResult<Self> {
        if self.op_code != expected {
            return Err(bad(format!(
                "{what}: expected opCode {} but got {}",
                expected.as_i32(),
                self.op_code.as_i32()
            )));
        }
        Ok(self)
    }
}

fn bad(msg: String) -> MdbError {
    MdbError::new(ErrorCode::FailedToParse, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> MessageHeader {
        MessageHeader {
            message_length: 21,
            request_id: 7,
            response_to: 0,
            op_code: OpCode::Msg,
        }
    }

    #[test]
    fn roundtrips_through_bytes() {
        let h = sample();
        let bytes = h.to_bytes();
        assert_eq!(bytes.len(), HEADER_SIZE);
        assert_eq!(MessageHeader::parse(&bytes).unwrap(), h);
    }

    #[test]
    fn all_op_codes_roundtrip() {
        for code in [
            OpCode::Reply,
            OpCode::Query,
            OpCode::GetMore,
            OpCode::KillCursors,
            OpCode::Compressed,
            OpCode::Msg,
            OpCode::Unknown(9999),
        ] {
            assert_eq!(OpCode::from_i32(code.as_i32()), code);
        }
    }

    #[test]
    fn parses_real_op_msg_header_layout() {
        // 手工构造 21 字节 OP_MSG：16 头 + 4 flagBits + 1 section kind。
        let mut buf = Vec::new();
        buf.extend_from_slice(&21i32.to_le_bytes());
        buf.extend_from_slice(&1234i32.to_le_bytes());
        buf.extend_from_slice(&0i32.to_le_bytes());
        buf.extend_from_slice(&2013i32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.push(0);
        let h = MessageHeader::parse(&buf).unwrap();
        assert_eq!(h.request_id, 1234);
        assert_eq!(h.op_code, OpCode::Msg);
    }

    #[test]
    fn rejects_short_buffer() {
        assert!(MessageHeader::parse(&[0u8; 15]).is_err());
    }

    #[test]
    fn rejects_length_below_header_size() {
        let mut bytes = sample().to_bytes();
        bytes[0..4].copy_from_slice(&15i32.to_le_bytes());
        let err = MessageHeader::parse(&bytes).unwrap_err();
        assert!(err.to_string().contains("smaller than"), "{err}");
    }

    #[test]
    fn rejects_length_above_maximum() {
        let mut bytes = sample().to_bytes();
        bytes[0..4].copy_from_slice(&(MAX_MESSAGE_SIZE_BYTES + 1).to_le_bytes());
        let err = MessageHeader::parse(&bytes).unwrap_err();
        assert!(err.to_string().contains("exceeds"), "{err}");
    }

    #[test]
    fn rejects_negative_length() {
        let mut bytes = sample().to_bytes();
        bytes[0..4].copy_from_slice(&(-1i32).to_le_bytes());
        assert!(MessageHeader::parse(&bytes).is_err());
    }

    #[test]
    fn rejects_wrong_op_code() {
        let h = MessageHeader {
            op_code: OpCode::Query,
            ..sample()
        };
        let err = h.expect_op_code(OpCode::Msg, "test").unwrap_err();
        assert!(err.to_string().contains("2004"), "{err}");
    }

    #[test]
    fn request_helper_computes_total_length() {
        let h = MessageHeader::request(1, OpCode::Msg, 100);
        assert_eq!(h.message_length, 116);
        assert_eq!(h.response_to, 0);
    }
}
