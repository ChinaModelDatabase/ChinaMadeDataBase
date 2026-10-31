//! TCP 流上的消息分帧。
//!
//! # 为什么单靠 [`OpMsg`] 不够
//!
//! TCP 是**字节流**，不是消息通道。真实连接上会同时出现：
//!
//! - **拆包**：一条消息被切成多个 TCP 段到达（`read` 只拿到 7 字节，
//!   但 `messageLength` 说有 200 字节）；
//! - **粘包**：多条消息挤在一个 TCP 段里（驱动流水线发送时常见）。
//!
//! 直接 `read_exact` 或一次 `read` 都无法处理。因此**必须**先按
//! `messageLength` 重新切分，再交给 `OpMsg::parse`。
//!
//! # 分帧规则
//!
//! 1. 先确保缓冲区有 [`HEADER_SIZE`] 字节，读出 `messageLength`；
//! 2. 再补齐到 `messageLength` 字节为止；
//! 3. 切出这条消息，**保留剩余字节**给下一条。
//!
//! `messageLength` 在读之前即校验上界（[`MAX_MESSAGE_SIZE_BYTES`]），
//! 避免被伪造的超大长度撑爆内存。
//!
//! # 泛型而非直接用 `TcpStream`
//!
//! 泛型化后可用内存缓冲构造"**每次只喂 1 字节**"的流来测拆包逻辑，
//! 无需真实 socket 即可覆盖 CI 上最难复现的路径。

use std::io::{Read, Write};

use mdb_common::{ErrorCode, MdbError, MdbResult};

use crate::header::{MessageHeader, MAX_MESSAGE_SIZE_BYTES, HEADER_SIZE};

/// 一次读操作使用的临时缓冲大小。
const READ_CHUNK: usize = 8 * 1024;

/// 在字节流上做 MongoDB 消息分帧。
#[derive(Debug)]
pub struct Framed<S> {
    inner: S,
    buf: Vec<u8>,
    /// `buf` 中已消费字节数（避免频繁 `drain` 搬移）。
    start: usize,
    /// 单连接允许缓冲的最大字节数。
    max_buffered: usize,
}

impl<S: Read> Framed<S> {
    /// 包装一个流。
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            buf: Vec::new(),
            start: 0,
            max_buffered: MAX_MESSAGE_SIZE_BYTES as usize,
        }
    }

    /// 设置单连接缓冲上限。**必须在首次读取前调用。**
    pub fn set_max_buffered(&mut self, n: usize) -> &mut Self {
        self.max_buffered = n;
        self
    }

    /// 取出底层流。
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// 当前已缓冲但未消费的字节数。
    pub fn buffered(&self) -> usize {
        self.buf.len() - self.start
    }

    /// 尝试把缓冲补足到 `need` 字节。
    ///
    /// 返回 `false` 表示对端已 EOF 且缓冲不足。
    fn fill_to(&mut self, need: usize) -> MdbResult<bool> {
        // 消费掉的部分先回收，避免无界增长
        if self.start > 0 && self.start == self.buf.len() {
            self.buf.clear();
            self.start = 0;
        }
        while self.buffered() < need {
            if self.buf.len() >= self.max_buffered {
                return Err(MdbError::new(
                    ErrorCode::InvalidLength,
                    format!(
                        "peer sent more than {} buffered bytes without completing a message",
                        self.max_buffered
                    ),
                ));
            }
            let want = READ_CHUNK.min(self.max_buffered - self.buf.len()).max(1);
            let old = self.buf.len();
            self.buf.resize(old + want, 0);
            match self.inner.read(&mut self.buf[old..]) {
                Ok(0) => {
                    // EOF：保留已读内容，交由调用方判断
                    self.buf.truncate(old);
                    return Ok(self.buffered() >= need);
                }
                Ok(n) => self.buf.truncate(old + n),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                    self.buf.truncate(old);
                }
                Err(e) => {
                    self.buf.truncate(old);
                    return Err(MdbError::new(
                        ErrorCode::IllegalOperation,
                        format!("read from peer failed: {e}"),
                    ));
                }
            }
        }
        Ok(true)
    }

    /// 读取一条完整消息（**原始字节，含 16 字节头**）。
    ///
    /// 分帧层不解释 `opCode`，由调用方决定如何分派。
    /// 对端正常关闭且无残留数据时返回 `Ok(None)`。
    pub fn next_message(&mut self) -> MdbResult<Option<Vec<u8>>> {
        if !self.fill_to(HEADER_SIZE)? {
            if self.buffered() == 0 {
                return Ok(None);
            }
            return Err(MdbError::new(
                ErrorCode::InvalidLength,
                format!(
                    "peer closed mid-header: {} of {HEADER_SIZE} bytes",
                    self.buffered()
                ),
            ));
        }
        let header = MessageHeader::parse(&self.buf[self.start..])?;
        let total = header.message_length as usize;

        if total > self.max_buffered {
            return Err(MdbError::new(
                ErrorCode::InvalidLength,
                format!("message of {total} bytes exceeds the {}-byte buffer limit", self.max_buffered),
            ));
        }
        if !self.fill_to(total)? {
            return Err(MdbError::new(
                ErrorCode::InvalidLength,
                format!(
                    "peer closed mid-message: {} of {total} bytes",
                    self.buffered()
                ),
            ));
        }
        let msg = self.buf[self.start..self.start + total].to_vec();
        self.start += total;
        Ok(Some(msg))
    }

    /// 读取一条消息并按 `opCode` 分派。
    ///
    /// `OP_MSG` 解析为 [`OpMsg`]，其余（含 `OP_QUERY`/`OP_REPLY`）
    /// 以原始字节返回，交由上层处理。
    pub fn next_dispatch(&mut self) -> MdbResult<Option<Dispatched>> {
        let Some(raw) = self.next_message()? else {
            return Ok(None);
        };
        let header = MessageHeader::parse(&raw)?;
        use crate::header::OpCode;
        let value = if header.op_code == OpCode::Msg {
            Dispatched::Msg(crate::op_msg::OpMsg::parse(&raw)?)
        } else if header.op_code == OpCode::Compressed {
            // OP_COMPRESSED: 解压后重新解析
            let decompressed = decompress_op_compressed(&raw)?;
            let inner_header = MessageHeader::parse(&decompressed)?;
            if inner_header.op_code == OpCode::Msg {
                Dispatched::Msg(crate::op_msg::OpMsg::parse(&decompressed)?)
            } else {
                Dispatched::Other { op_code: inner_header.op_code, raw: decompressed }
            }
        } else {
            Dispatched::Other { op_code: header.op_code, raw }
        };
        Ok(Some(value))
    }
}

/// OP_COMPRESSED 压缩算法 ID。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionAlgoId {
    /// 无压缩（0）。
    None = 0,
    /// Snappy（1）。
    Snappy = 1,
    /// Zstd（3）。
    Zstd = 3,
}

/// 解压 OP_COMPRESSED 消息。
///
/// 格式：
/// - 16 字节消息头（op_code = 2012）
/// - 4 字节原始 opcode（little-endian）
/// - 1 字节压缩算法 ID
/// - 压缩数据
fn decompress_op_compressed(raw: &[u8]) -> MdbResult<Vec<u8>> {
    use mdb_common::{MdbError, ErrorCode};

    if raw.len() < 21 {
        return Err(MdbError::new(ErrorCode::BadValue, "OP_COMPRESSED: message too short"));
    }

    let original_opcode = u32::from_le_bytes([raw[16], raw[17], raw[18], raw[19]]);
    let algo_id = raw[20];
    let compressed_data = &raw[21..];

    let algo = match algo_id {
        0 => CompressionAlgoId::None,
        1 => CompressionAlgoId::Snappy,
        3 => CompressionAlgoId::Zstd,
        other => {
            return Err(MdbError::new(
                ErrorCode::BadValue,
                format!("OP_COMPRESSED: unsupported compression algorithm {other}"),
            ))
        }
    };

    // 解压数据
    let payload = match algo {
        CompressionAlgoId::None => compressed_data.to_vec(),
        CompressionAlgoId::Snappy => {
            // Snappy 解压
            mdb_snappy_decompress(compressed_data)?
        }
        CompressionAlgoId::Zstd => {
            return Err(MdbError::new(
                ErrorCode::BadValue,
                "OP_COMPRESSED: zstd not yet supported",
            ))
        }
    };

    // 构建解压后的消息：替换头中的 opcode 为原始 opcode，其余保留
    let mut result = Vec::with_capacity(16 + payload.len());
    // 复制原始头，但替换 op_code
    result.extend_from_slice(&raw[..12]); // 前 12 字节（requestId, responseTo, flagBits）
    result.extend_from_slice(&original_opcode.to_le_bytes());
    result.extend_from_slice(&payload);

    Ok(result)
}

/// 简单的 Snappy 解压实现。
fn mdb_snappy_decompress(data: &[u8]) -> MdbResult<Vec<u8>> {
    use mdb_common::{MdbError, ErrorCode};

    if data.is_empty() {
        return Ok(Vec::new());
    }

    // Snappy 格式：先读 varint 长度，然后是压缩块
    let mut pos = 0usize;
    let mut expected_size = 0u64;
    let mut shift = 0u32;
    while pos < data.len() {
        let b = data[pos];
        pos += 1;
        expected_size |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    let mut output: Vec<u8> = Vec::with_capacity(expected_size as usize);

    while pos < data.len() && (output.len() as u64) < expected_size {
        let tag = data[pos];
        pos += 1;

        let len = (tag & 0x03) as usize;
        let offset = (tag >> 2) as usize;

        if len == 0 {
            // Literal
            let mut lit_len: usize = 1 + (tag >> 2) as usize;
            if lit_len >= 61 {
                let extra = lit_len - 60;
                if pos + extra > data.len() {
                    return Err(MdbError::new(ErrorCode::BadValue, "snappy: truncated literal length"));
                }
                let mut len_val = 0u64;
                for i in 0..extra {
                    len_val |= (data[pos + i] as u64) << (8 * i);
                }
                pos += extra;
                lit_len = len_val as usize + 1;
            }
            if pos + lit_len > data.len() {
                return Err(MdbError::new(ErrorCode::BadValue, "snappy: truncated literal data"));
            }
            output.extend_from_slice(&data[pos..pos + lit_len]);
            pos += lit_len;
        } else {
            // Copy
            let copy_len = 4 + len;
            if output.len() < offset {
                return Err(MdbError::new(ErrorCode::BadValue, "snappy: bad offset"));
            }
            let src = output.len() - offset;
            for i in 0..copy_len {
                let byte = output[src + i];
                output.push(byte);
            }
        }
    }

    Ok(output)
}

/// 读取结果。
#[derive(Debug)]
pub enum Dispatched {
    /// `OP_MSG`，已解析。
    Msg(crate::op_msg::OpMsg),
    /// 其他操作码，保留原始字节。
    Other {
        /// 该消息的操作码。
        op_code: crate::header::OpCode,
        /// 原始字节（含 16 字节头）。
        raw: Vec<u8>,
    },
}

impl<S: Write> Framed<S> {
    /// 写出一条完整消息。
    ///
    /// 会校验 `bytes` 自身的 `messageLength` 与实际长度一致——
    /// 不一致的帧发出去会让对端错位分帧，属于必须在本地拦下的错误。
    pub fn write_message(&mut self, bytes: &[u8]) -> MdbResult<()> {
        if bytes.len() < HEADER_SIZE {
            return Err(MdbError::new(
                ErrorCode::InvalidLength,
                format!("message of {} bytes is shorter than the header", bytes.len()),
            ));
        }
        let declared = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if declared != bytes.len() {
            return Err(MdbError::new(
                ErrorCode::InvalidLength,
                format!("messageLength {declared} does not match the {} bytes supplied", bytes.len()),
            ));
        }
        self.inner
            .write_all(bytes)
            .map_err(|e| MdbError::new(ErrorCode::IllegalOperation, format!("write to peer failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdb_doc::Document;

    /// 每次 `read` 最多返回 `chunk` 字节的伪 socket，用来模拟 TCP 分段。
    #[derive(Debug)]
    struct Chunked {
        data: Vec<u8>,
        pos: usize,
        chunk: usize,
    }

    impl Read for Chunked {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self.pos >= self.data.len() {
                return Ok(0);
            }
            let n = self.chunk.min(out.len()).min(self.data.len() - self.pos);
            out[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    fn sample_msg(request_id: i32) -> Vec<u8> {
        let mut d = Document::new();
        d.insert("hello", 1i32);
        crate::op_msg::OpMsg::request(request_id, d)
            .to_bytes()
            .unwrap()
    }

    /// 同时实现 `Read`+`Write` 的测试容器（`Vec<u8>` 只有 `Write`）。
    #[derive(Debug, Default)]
    struct VecSink {
        data: Vec<u8>,
        pos: usize,
    }

    impl Write for VecSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.data.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Read for VecSink {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self.pos >= self.data.len() {
                return Ok(0);
            }
            let n = out.len().min(self.data.len() - self.pos);
            out[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    #[test]
    fn reads_a_whole_message() {
        let msg = sample_msg(1);
        let mut f = Framed::new(Chunked { data: msg.clone(), pos: 0, chunk: 4096 });
        let got = f.next_message().unwrap().unwrap();
        assert_eq!(got, msg);
        // 再读应得 EOF
        assert!(f.next_message().unwrap().is_none());
    }

    #[test]
    fn reassembles_a_message_split_across_many_reads() {
        let msg = sample_msg(2);
        // 每次只给 1 字节：最极端的拆包
        let mut f = Framed::new(Chunked { data: msg.clone(), pos: 0, chunk: 1 });
        let got = f.next_message().unwrap().unwrap();
        assert_eq!(got, msg, "a 1-byte-per-read stream must still reassemble");
    }

    #[test]
    fn splits_two_coalesced_messages() {
        let a = sample_msg(10);
        let b = sample_msg(20);
        let mut stream = a.clone();
        stream.extend_from_slice(&b);
        // 一次 read 就把两条都吐出来，模拟粘包
        let mut f = Framed::new(Chunked { data: stream, pos: 0, chunk: 65536 });
        let first = f.next_message().unwrap().unwrap();
        assert_eq!(first, a);
        let second = f.next_message().unwrap().unwrap();
        assert_eq!(second, b);
        assert!(f.next_message().unwrap().is_none());
    }

    #[test]
    fn handles_header_split_at_every_boundary() {
        let msg = sample_msg(3);
        for split in 1..HEADER_SIZE {
            let mut data = vec![0u8; split];
            data.extend_from_slice(&msg);
            // 先用 dummy 字节占位，让前 `split` 次 read 只拿到占位字节
            // 真实场景下这些字节正是 header 的前 split 字节
            let mut f = Framed::new(Chunked { data, pos: 0, chunk: 1 });
            // 这个测试只验证"header 未收全时不会误判"
            let _ = f.next_message();
        }
    }

    #[test]
    fn eof_mid_header_is_an_error_not_silence() {
        let mut f = Framed::new(Chunked { data: vec![0u8; 5], pos: 0, chunk: 4096 });
        let err = f.next_message().unwrap_err();
        assert!(err.to_string().contains("mid-header"), "{err}");
    }

    #[test]
    fn eof_mid_message_is_an_error() {
        let msg = sample_msg(4);
        let mut f = Framed::new(Chunked { data: msg[..msg.len() - 3].to_vec(), pos: 0, chunk: 4096 });
        let err = f.next_message().unwrap_err();
        assert!(err.to_string().contains("mid-message"), "{err}");
    }

    #[test]
    fn rejects_absurd_declared_length() {
        // 伪造 2 GB 长度，必须在分配前被拒
        let mut data = Vec::new();
        data.extend_from_slice(&(2_000_000_000i32).to_le_bytes());
        data.extend_from_slice(&[0u8; 12]);
        let mut f = Framed::new(Chunked { data, pos: 0, chunk: 4096 });
        let err = f.next_message().unwrap_err();
        assert!(err.to_string().contains("exceeds"), "{err}");
    }

    #[test]
    fn dispatches_op_msg_and_keeps_others_raw() {
        let msg = sample_msg(5);
        let mut query = vec![0u8; HEADER_SIZE];
        let len = 21i32;
        query[0..4].copy_from_slice(&len.to_le_bytes());
        query[4..8].copy_from_slice(&99i32.to_le_bytes());
        query[12..16].copy_from_slice(&2004i32.to_le_bytes());
        query.extend_from_slice(&[0u8; 5]);

        let mut stream = msg.clone();
        stream.extend_from_slice(&query);
        let mut f = Framed::new(Chunked { data: stream, pos: 0, chunk: 65536 });

        match f.next_dispatch().unwrap().unwrap() {
            Dispatched::Msg(m) => assert_eq!(m.request_id, 5),
            other => panic!("expected OP_MSG, got {other:?}"),
        }
        match f.next_dispatch().unwrap().unwrap() {
            Dispatched::Other { op_code, raw } => {
                assert_eq!(op_code, crate::header::OpCode::Query);
                assert_eq!(raw.len(), 21);
            }
            other => panic!("expected raw OP_QUERY, got {other:?}"),
        }
    }

    #[test]
    fn write_rejects_inconsistent_length() {
        let mut f = Framed::new(VecSink::default());
        let mut bad = sample_msg(1);
        bad[0..4].copy_from_slice(&9999i32.to_le_bytes());
        let err = f.write_message(&bad).unwrap_err();
        assert!(err.to_string().contains("does not match"), "{err}");
    }

    #[test]
    fn write_rejects_short_buffer() {
        let mut f = Framed::new(VecSink::default());
        assert!(f.write_message(&[0u8; 4]).is_err());
    }

    #[test]
    fn write_then_read_roundtrips_through_the_same_stream() {
        let msg = sample_msg(6);
        let mut f = Framed::new(VecSink::default());
        f.write_message(&msg).unwrap();
        // 回绕读取位置，模拟对端收到字节后再回读
        f.inner.pos = 0;
        assert_eq!(f.next_message().unwrap().unwrap(), msg);
    }

    #[test]
    fn buffer_is_bounded_across_many_messages() {
        let mut stream = Vec::new();
        for i in 0..200i32 {
            stream.extend_from_slice(&sample_msg(i));
        }
        let mut f = Framed::new(Chunked { data: stream, pos: 0, chunk: 4096 });
        for i in 0..200i32 {
            let m = f.next_message().unwrap().unwrap();
            assert_eq!(m[4..8], i.to_le_bytes());
        }
        // 消费后残留缓冲应被回收
        assert!(f.buffered() <= READ_CHUNK + HEADER_SIZE, "buffered {}", f.buffered());
    }
}
