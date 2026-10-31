//! TLS 记录层（RFC 5246 §5）。
//!
//! TLS 记录层负责将上层协议消息分帧并加密传输。
//!
//! # 记录格式
//!
//! ```text
//! type (1 byte) | version (2 bytes) | length (2 bytes) | payload (N bytes)
//! ```
//!
//! # 记录类型
//!
//! - `Handshake` (22): 握手消息
//! - `ChangeCipherSpec` (20): 加密变更通知
//! - `Alert` (21): 告警消息
//! - `ApplicationData` (23): 应用数据（加密后）

use std::io::{Read, Write};

use mdb_common::{ErrorCode, MdbError, MdbResult};

use crate::cipher::{self, GcmOutput, GCM_IV_LEN, GCM_TAG_LEN};
use crate::alert::Alert;

/// TLS 协议版本号：TLCP 1.1 → 0x0303（TLS 1.2 版本号）。
pub const TLS_VERSION: [u8; 2] = [0x03, 0x03];

/// 记录类型常量。
pub mod content_type {
    pub const CHANGE_CIPHER_SPEC: u8 = 20;
    pub const ALERT: u8 = 21;
    pub const HANDSHAKE: u8 = 22;
    pub const APPLICATION_DATA: u8 = 23;
}

/// TLS 记录头长度。
pub const RECORD_HEADER_SIZE: usize = 5;

/// 单条记录的最大载荷长度。
pub const MAX_RECORD_PAYLOAD: usize = 16384;

/// 单条记录的最大密文长度（含 GCM tag）。
pub const MAX_RECORD_CIPHERTEXT: usize = MAX_RECORD_PAYLOAD + GCM_TAG_LEN + 1;

/// TLS 记录。
#[derive(Debug, Clone)]
pub struct TlsRecord {
    pub content_type: u8,
    pub version: [u8; 2],
    pub payload: Vec<u8>,
}

impl TlsRecord {
    /// 解析记录头（5 字节），返回 `(content_type, version, payload_len)`。
    pub fn parse_header(buf: &[u8; RECORD_HEADER_SIZE]) -> MdbResult<(u8, [u8; 2], usize)> {
        let content_type = buf[0];
        let version = [buf[1], buf[2]];
        let len = u16::from_be_bytes([buf[3], buf[4]]) as usize;

        if len > MAX_RECORD_PAYLOAD {
            return Err(MdbError::new(
                ErrorCode::InvalidLength,
                format!("record payload length {len} exceeds maximum {MAX_RECORD_PAYLOAD}"),
            ));
        }

        Ok((content_type, version, len))
    }

    /// 序列化记录（不含加密）。
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(RECORD_HEADER_SIZE + self.payload.len());
        buf.push(self.content_type);
        buf.extend_from_slice(&self.version);
        buf.extend_from_slice(&(self.payload.len() as u16).to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }
}

/// 记录层读取器：从底层流读取并解密 TLS 记录。
#[derive(Debug)]
pub struct RecordReader {
    buf: Vec<u8>,
    /// 已消费位置。
    start: usize,
}

impl RecordReader {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(16 * 1024),
            start: 0,
        }
    }

    /// 从缓冲区读取一条明文记录（handshake 或 alert 阶段）。
    pub fn read_plaintext_record<S: Read>(
        &mut self,
        stream: &mut S,
    ) -> MdbResult<TlsRecord> {
        // 读取5字节头
        self.fill(stream, RECORD_HEADER_SIZE)?;
        let header = [self.buf[self.start], self.buf[self.start + 1], self.buf[self.start + 2],
                     self.buf[self.start + 3], self.buf[self.start + 4]];
        let (content_type, version, payload_len) = TlsRecord::parse_header(&header)?;

        // 读取 payload
        self.fill(stream, RECORD_HEADER_SIZE + payload_len)?;
        let payload = self.buf[self.start + RECORD_HEADER_SIZE..self.start + RECORD_HEADER_SIZE + payload_len].to_vec();
        self.start += RECORD_HEADER_SIZE + payload_len;

        Ok(TlsRecord { content_type, version, payload })
    }

    /// 从缓冲区读取一条加密记录并解密。
    pub fn read_encrypted_record<S: Read>(
        &mut self,
        stream: &mut S,
        key: &[u8; 16],
        iv: &[u8; GCM_IV_LEN],
        seq: u64,
    ) -> MdbResult<TlsRecord> {
        self.fill(stream, RECORD_HEADER_SIZE)?;
        let header = [self.buf[self.start], self.buf[self.start + 1], self.buf[self.start + 2],
                     self.buf[self.start + 3], self.buf[self.start + 4]];
        let (content_type, _version, payload_len) = TlsRecord::parse_header(&header)?;

        self.fill(stream, RECORD_HEADER_SIZE + payload_len)?;
        let ciphertext = &self.buf[self.start + RECORD_HEADER_SIZE..self.start + RECORD_HEADER_SIZE + payload_len];
        self.start += RECORD_HEADER_SIZE + payload_len;

        if ciphertext.len() < GCM_TAG_LEN {
            return Err(MdbError::new(
                ErrorCode::FailedToParse,
                "encrypted record too short for GCM tag",
            ));
        }

        let (ct, tag_bytes) = ciphertext.split_at(ciphertext.len() - GCM_TAG_LEN);
        let mut tag = [0u8; GCM_TAG_LEN];
        tag.copy_from_slice(tag_bytes);

        // AAD = content_type || version || length（5 字节头）
        let mut aad = Vec::with_capacity(RECORD_HEADER_SIZE);
        aad.push(content_type);
        aad.extend_from_slice(&TLS_VERSION);
        aad.extend_from_slice(&(payload_len as u16).to_be_bytes());

        // IV = base_iv XOR seq_number
        let record_iv = cipher::counter_iv(iv, seq);

        let plaintext = cipher::gcm_decrypt(key, &record_iv, ct, &aad, &tag).map_err(|_| {
            MdbError::new(ErrorCode::FailedToParse, "GCM authentication failed")
        })?;

        Ok(TlsRecord { content_type, version: TLS_VERSION, payload: plaintext })
    }

    /// 清理已消费的缓冲区。
    pub fn compact(&mut self) {
        if self.start > 0 {
            self.buf.drain(..self.start);
            self.start = 0;
        }
    }

    fn fill<S: Read>(&mut self, stream: &mut S, need: usize) -> MdbResult<()> {
        while self.buf.len() - self.start < need {
            let old_len = self.buf.len();
            self.buf.resize(old_len + 4096, 0);
            match stream.read(&mut self.buf[old_len..]) {
                Ok(0) => {
                    self.buf.truncate(old_len);
                    return Err(MdbError::new(ErrorCode::IllegalOperation, "connection closed"));
                }
                Ok(n) => self.buf.truncate(old_len + n),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                    self.buf.truncate(old_len);
                }
                Err(e) => {
                    self.buf.truncate(old_len);
                    return Err(MdbError::new(ErrorCode::IllegalOperation, format!("read error: {e}")));
                }
            }
        }
        Ok(())
    }
}

impl Default for RecordReader {
    fn default() -> Self {
        Self::new()
    }
}

/// 记录层写入器：加密并写出 TLS 记录。
#[derive(Debug)]
pub struct RecordWriter;

impl RecordWriter {
    /// 写出一条明文记录。
    pub fn write_plaintext<S: Write>(
        stream: &mut S,
        content_type: u8,
        payload: &[u8],
    ) -> MdbResult<()> {
        let record = TlsRecord {
            content_type,
            version: TLS_VERSION,
            payload: payload.to_vec(),
        };
        let encoded = record.encode();
        stream.write_all(&encoded).map_err(|e| {
            MdbError::new(ErrorCode::IllegalOperation, format!("write error: {e}"))
        })
    }

    /// 写出一条加密记录。
    pub fn write_encrypted<S: Write>(
        stream: &mut S,
        content_type: u8,
        plaintext: &[u8],
        key: &[u8; 16],
        iv: &[u8; GCM_IV_LEN],
        seq: u64,
    ) -> MdbResult<()> {
        // AAD = content_type || version || length
        // ciphertext length == plaintext length for GCM
        let ct_len = plaintext.len();
        let total_ct_len = ct_len + GCM_TAG_LEN;
        let mut aad = Vec::with_capacity(RECORD_HEADER_SIZE);
        aad.push(content_type);
        aad.extend_from_slice(&TLS_VERSION);
        aad.extend_from_slice(&(total_ct_len as u16).to_be_bytes());

        let record_iv = cipher::counter_iv(iv, seq);
        let GcmOutput { ciphertext, tag } = cipher::gcm_encrypt(key, &record_iv, plaintext, &aad).map_err(|_| {
            MdbError::new(ErrorCode::FailedToParse, "GCM encryption failed")
        })?;

        let mut buf = Vec::with_capacity(RECORD_HEADER_SIZE + total_ct_len);
        buf.push(content_type);
        buf.extend_from_slice(&TLS_VERSION);
        buf.extend_from_slice(&(total_ct_len as u16).to_be_bytes());
        buf.extend_from_slice(&ciphertext);
        buf.extend_from_slice(&tag);

        stream.write_all(&buf).map_err(|e| {
            MdbError::new(ErrorCode::IllegalOperation, format!("write error: {e}"))
        })
    }
}

/// 发送 TLS 告警。
pub fn send_alert<S: Write>(
    stream: &mut S,
    alert: &Alert,
    write_key: Option<(&[u8; 16], &[u8; GCM_IV_LEN], u64)>,
) -> MdbResult<()> {
    let encoded = alert.encode();
    if let Some((key, iv, seq)) = write_key {
        RecordWriter::write_encrypted(stream, content_type::ALERT, &encoded, key, iv, seq)
    } else {
        RecordWriter::write_plaintext(stream, content_type::ALERT, &encoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn record_roundtrip_plaintext() {
        let payload = b"hello tls";
        let record = TlsRecord {
            content_type: content_type::HANDSHAKE,
            version: TLS_VERSION,
            payload: payload.to_vec(),
        };
        let encoded = record.encode();
        assert_eq!(encoded[0], content_type::HANDSHAKE);
        assert_eq!(&encoded[1..3], &TLS_VERSION);
        assert_eq!(u16::from_be_bytes([encoded[3], encoded[4]]), payload.len() as u16);
        assert_eq!(&encoded[5..], payload);
    }

    #[test]
    fn record_reader_plaintext() {
        let payload = b"test data";
        let mut buf = Vec::new();
        buf.push(content_type::HANDSHAKE);
        buf.extend_from_slice(&TLS_VERSION);
        buf.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        buf.extend_from_slice(payload);

        let mut reader = RecordReader::new();
        let mut cursor = Cursor::new(&buf);
        let record = reader.read_plaintext_record(&mut cursor).unwrap();
        assert_eq!(record.content_type, content_type::HANDSHAKE);
        assert_eq!(record.payload, payload);
    }

    #[test]
    fn encrypted_record_roundtrip() {
        let key = [0x42u8; 16];
        let iv = [0x99u8; GCM_IV_LEN];
        let plaintext = b"encrypted handshake data";
        let content_type = content_type::HANDSHAKE;

        let mut buf = Vec::new();
        // 先写入加密记录
        {
            let mut cursor = Cursor::new(&mut buf);
            RecordWriter::write_encrypted(&mut cursor, content_type, plaintext, &key, &iv, 0).unwrap();
        }

        // 读回
        let mut reader = RecordReader::new();
        let mut cursor = Cursor::new(&buf);
        let record = reader.read_encrypted_record(&mut cursor, &key, &iv, 0).unwrap();
        assert_eq!(record.content_type, content_type);
        assert_eq!(record.payload, plaintext);
    }

    #[test]
    fn record_header_parse() {
        let header = [22, 0x03, 0x03, 0x00, 0x10];
        let (ct, ver, len) = TlsRecord::parse_header(&header).unwrap();
        assert_eq!(ct, 22);
        assert_eq!(ver, [0x03, 0x03]);
        assert_eq!(len, 16);
    }

    #[test]
    fn record_header_too_long() {
        let header = [22, 0x03, 0x03, 0x40, 0x01]; //16385 > 16384
        assert!(TlsRecord::parse_header(&header).is_err());
    }
}
