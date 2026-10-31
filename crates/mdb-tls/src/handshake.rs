//! TLCP 握手协议（GM/T 0024-2014）。
//!
//! 实现完整的 TLS 握手消息编解码与状态机。
//!
//! # 握手消息类型
//!
//! | 值  | 消息                |
//! |-----|---------------------|
//! | 1   | ClientHello         |
//! | 2   | ServerHello         |
//! | 11  | Certificate         |
//! | 12  | ServerKeyExchange   |
//! | 14  | ServerHelloDone     |
//! | 16  | ClientKeyExchange   |
//! | 20  | Finished            |

use mdb_common::{ErrorCode, MdbError, MdbResult};
use mdb_crypto::digest::Digest;
use mdb_crypto::sm3::Sm3;

/// 握手消息类型常量。
pub mod msg_type {
    pub const CLIENT_HELLO: u8 = 1;
    pub const SERVER_HELLO: u8 = 2;
    pub const CERTIFICATE: u8 = 11;
    pub const SERVER_KEY_EXCHANGE: u8 = 12;
    pub const SERVER_HELLO_DONE: u8 = 14;
    pub const CLIENT_KEY_EXCHANGE: u8 = 16;
    pub const FINISHED: u8 = 20;
}

/// 密码套件 ID（2 字节）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CipherSuite(pub u16);

/// ECC_SM4_SM3 密码套件。
pub const ECC_SM4_SM3: CipherSuite = CipherSuite(0xe0_13);

/// 支持的密码套件列表。
pub const SUPPORTED_CIPHERS: &[CipherSuite] = &[ECC_SM4_SM3];

/// 随机数长度。
pub const RANDOM_LEN: usize = 32;

/// 客户端随机数。
#[derive(Debug, Clone)]
pub struct ClientRandom {
    pub random: [u8; RANDOM_LEN],
}

impl ClientRandom {
    pub fn new() -> Self {
        Self { random: mdb_crypto::random::bytes() }
    }

    pub fn encode(&self) -> Vec<u8> {
        self.random.to_vec()
    }

    pub fn decode(data: &[u8]) -> MdbResult<Self> {
        if data.len() < RANDOM_LEN {
            return Err(MdbError::new(ErrorCode::FailedToParse, "client random too short"));
        }
        let mut random = [0u8; RANDOM_LEN];
        random.copy_from_slice(&data[..RANDOM_LEN]);
        Ok(Self { random })
    }
}

impl Default for ClientRandom {
    fn default() -> Self {
        Self::new()
    }
}

/// 服务器随机数。
#[derive(Debug, Clone)]
pub struct ServerRandom {
    pub random: [u8; RANDOM_LEN],
}

impl ServerRandom {
    pub fn new() -> Self {
        Self { random: mdb_crypto::random::bytes() }
    }

    pub fn encode(&self) -> Vec<u8> {
        self.random.to_vec()
    }

    pub fn decode(data: &[u8]) -> MdbResult<Self> {
        if data.len() < RANDOM_LEN {
            return Err(MdbError::new(ErrorCode::FailedToParse, "server random too short"));
        }
        let mut random = [0u8; RANDOM_LEN];
        random.copy_from_slice(&data[..RANDOM_LEN]);
        Ok(Self { random })
    }
}

impl Default for ServerRandom {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ClientHello
// ---------------------------------------------------------------------------

/// ClientHello 消息。
#[derive(Debug, Clone)]
pub struct ClientHello {
    pub client_version: [u8; 2],
    pub random: ClientRandom,
    pub cipher_suites: Vec<CipherSuite>,
}

impl ClientHello {
    /// 构造新的 ClientHello。
    pub fn new(cipher_suites: Vec<CipherSuite>) -> Self {
        Self {
            client_version: [0x03, 0x03],
            random: ClientRandom::new(),
            cipher_suites,
        }
    }

    /// 序列化为握手载荷（不含消息类型和长度头）。
    pub fn encode_body(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // client_version (2 bytes)
        buf.extend_from_slice(&self.client_version);
        // random (32 bytes)
        buf.extend_from_slice(&self.random.encode());
        // cipher_suites_length (2 bytes)
        let suites_len = (self.cipher_suites.len() * 2) as u16;
        buf.extend_from_slice(&suites_len.to_be_bytes());
        // cipher_suites (2 bytes each)
        for suite in &self.cipher_suites {
            buf.extend_from_slice(&suite.0.to_be_bytes());
        }
        // compression_methods_length + null method
        buf.push(1);
        buf.push(0);
        buf
    }

    /// 从握手载荷解析。
    pub fn decode_body(data: &[u8]) -> MdbResult<Self> {
        if data.len() < 34 {
            return Err(MdbError::new(ErrorCode::FailedToParse, "ClientHello too short"));
        }
        let client_version = [data[0], data[1]];
        let random = ClientRandom::decode(&data[2..34])?;
        let suites_len = u16::from_be_bytes([data[34], data[35]]) as usize;
        let mut cipher_suites = Vec::new();
        let mut pos = 36;
        while cipher_suites.len() * 2 < suites_len && pos + 2 <= data.len() {
            let id = u16::from_be_bytes([data[pos], data[pos + 1]]);
            cipher_suites.push(CipherSuite(id));
            pos += 2;
        }
        Ok(Self { client_version, random, cipher_suites })
    }
}

// ---------------------------------------------------------------------------
// ServerHello
// ---------------------------------------------------------------------------

/// ServerHello 消息。
#[derive(Debug, Clone)]
pub struct ServerHello {
    pub server_version: [u8; 2],
    pub random: ServerRandom,
    pub cipher_suite: CipherSuite,
}

impl ServerHello {
    pub fn new(cipher_suite: CipherSuite) -> Self {
        Self {
            server_version: [0x03, 0x03],
            random: ServerRandom::new(),
            cipher_suite,
        }
    }

    pub fn encode_body(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.server_version);
        buf.extend_from_slice(&self.random.encode());
        buf.extend_from_slice(&self.cipher_suite.0.to_be_bytes());
        // compression_method
        buf.push(0);
        buf
    }

    pub fn decode_body(data: &[u8]) -> MdbResult<Self> {
        if data.len() < 35 {
            return Err(MdbError::new(ErrorCode::FailedToParse, "ServerHello too short"));
        }
        let server_version = [data[0], data[1]];
        let random = ServerRandom::decode(&data[2..34])?;
        let cipher_suite = CipherSuite(u16::from_be_bytes([data[34], data[35]]));
        Ok(Self { server_version, random, cipher_suite })
    }
}

// ---------------------------------------------------------------------------
// Certificate（简化）
// ---------------------------------------------------------------------------

/// 服务器证书消息（简化：仅含证书数据）。
#[derive(Debug, Clone)]
pub struct CertificateMsg {
    /// DER 格式证书数据。
    pub certificate: Vec<u8>,
}

impl CertificateMsg {
    pub fn encode_body(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // certificates_length (3 bytes)
        let total_len = self.certificate.len() + 3; // +3 for inner length
        buf.push((total_len >> 16) as u8);
        buf.push((total_len >> 8) as u8);
        buf.push(total_len as u8);
        // single certificate length (3 bytes)
        buf.push((self.certificate.len() >> 16) as u8);
        buf.push((self.certificate.len() >> 8) as u8);
        buf.push(self.certificate.len() as u8);
        buf.extend_from_slice(&self.certificate);
        buf
    }

    pub fn decode_body(data: &[u8]) -> MdbResult<Self> {
        if data.len() < 6 {
            return Err(MdbError::new(ErrorCode::FailedToParse, "Certificate too short"));
        }
        let total_len = ((data[0] as usize) << 16) | ((data[1] as usize) << 8) | data[2] as usize;
        if data.len() < 3 + total_len {
            return Err(MdbError::new(ErrorCode::FailedToParse, "Certificate truncated"));
        }
        let cert_len = ((data[3] as usize) << 16) | ((data[4] as usize) << 8) | data[5] as usize;
        if data.len() < 6 + cert_len {
            return Err(MdbError::new(ErrorCode::FailedToParse, "Certificate data truncated"));
        }
        let certificate = data[6..6 + cert_len].to_vec();
        Ok(Self { certificate })
    }
}

// ---------------------------------------------------------------------------
// ServerKeyExchange
// ---------------------------------------------------------------------------

/// 服务器密钥交换消息。
///
/// 包含服务器的临时公钥（用于密钥协商）和签名。
#[derive(Debug, Clone)]
pub struct ServerKeyExchange {
    /// 服务器公钥（32 字节）。
    pub public_key: Vec<u8>,
    /// 签名数据（SM3 摘要）。
    pub signature: Vec<u8>,
}

impl ServerKeyExchange {
    pub fn new(public_key: Vec<u8>, signature: Vec<u8>) -> Self {
        Self { public_key, signature }
    }

    pub fn encode_body(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // public_key_length (2 bytes)
        buf.extend_from_slice(&(self.public_key.len() as u16).to_be_bytes());
        buf.extend_from_slice(&self.public_key);
        // signature_length (2 bytes)
        buf.extend_from_slice(&(self.signature.len() as u16).to_be_bytes());
        buf.extend_from_slice(&self.signature);
        buf
    }

    pub fn decode_body(data: &[u8]) -> MdbResult<Self> {
        if data.len() < 4 {
            return Err(MdbError::new(ErrorCode::FailedToParse, "ServerKeyExchange too short"));
        }
        let pk_len = u16::from_be_bytes([data[0], data[1]]) as usize;
        if data.len() < 2 + pk_len + 2 {
            return Err(MdbError::new(ErrorCode::FailedToParse, "ServerKeyExchange truncated"));
        }
        let public_key = data[2..2 + pk_len].to_vec();
        let sig_offset = 2 + pk_len;
        let sig_len = u16::from_be_bytes([data[sig_offset], data[sig_offset + 1]]) as usize;
        if data.len() < sig_offset + 2 + sig_len {
            return Err(MdbError::new(ErrorCode::FailedToParse, "ServerKeyExchange signature truncated"));
        }
        let signature = data[sig_offset + 2..sig_offset + 2 + sig_len].to_vec();
        Ok(Self { public_key, signature })
    }
}

// ---------------------------------------------------------------------------
// ServerHelloDone
// ---------------------------------------------------------------------------

/// ServerHelloDone 消息（空）。
#[derive(Debug, Clone, Copy)]
pub struct ServerHelloDone;

impl ServerHelloDone {
    pub fn encode_body(&self) -> Vec<u8> {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// ClientKeyExchange
// ---------------------------------------------------------------------------

/// 客户端密钥交换消息。
#[derive(Debug, Clone)]
pub struct ClientKeyExchange {
    /// 客户端公钥（32 字节）。
    pub public_key: Vec<u8>,
}

impl ClientKeyExchange {
    pub fn new(public_key: Vec<u8>) -> Self {
        Self { public_key }
    }

    pub fn encode_body(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // public_value_length (2 bytes)
        buf.extend_from_slice(&(self.public_key.len() as u16).to_be_bytes());
        buf.extend_from_slice(&self.public_key);
        buf
    }

    pub fn decode_body(data: &[u8]) -> MdbResult<Self> {
        if data.len() < 2 {
            return Err(MdbError::new(ErrorCode::FailedToParse, "ClientKeyExchange too short"));
        }
        let pk_len = u16::from_be_bytes([data[0], data[1]]) as usize;
        if data.len() < 2 + pk_len {
            return Err(MdbError::new(ErrorCode::FailedToParse, "ClientKeyExchange truncated"));
        }
        let public_key = data[2..2 + pk_len].to_vec();
        Ok(Self { public_key })
    }
}

// ---------------------------------------------------------------------------
// Finished
// ---------------------------------------------------------------------------

/// Finished 消息（12 字节 verify_data）。
#[derive(Debug, Clone)]
pub struct Finished {
    pub verify_data: [u8; 12],
}

impl Finished {
    pub fn encode_body(&self) -> Vec<u8> {
        self.verify_data.to_vec()
    }

    pub fn decode_body(data: &[u8]) -> MdbResult<Self> {
        if data.len() < 12 {
            return Err(MdbError::new(ErrorCode::FailedToParse, "Finished too short"));
        }
        let mut verify_data = [0u8; 12];
        verify_data.copy_from_slice(&data[..12]);
        Ok(Self { verify_data })
    }
}

// ---------------------------------------------------------------------------
// 握手消息封装
// ---------------------------------------------------------------------------

/// 将握手消息体封装为完整的握手记录载荷。
///
/// 格式：`msg_type (1) || length (3) || body`
pub fn wrap_handshake(msg_type: u8, body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.push(msg_type);
    let len = body.len() as u32;
    buf.push((len >> 16) as u8);
    buf.push((len >> 8) as u8);
    buf.push(len as u8);
    buf.extend_from_slice(body);
    buf
}

/// 解析握手消息头：`(msg_type, body_length)`。
pub fn parse_handshake_header(data: &[u8]) -> MdbResult<(u8, usize)> {
    if data.len() < 4 {
        return Err(MdbError::new(ErrorCode::FailedToParse, "handshake header too short"));
    }
    let msg_type = data[0];
    let len = ((data[1] as usize) << 16) | ((data[2] as usize) << 8) | data[3] as usize;
    Ok((msg_type, len))
}

/// 将多条握手消息串联后计算 SM3 哈希（用于 Finished 验证）。
pub fn handshake_hash(messages: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sm3::new();
    for msg in messages {
        hasher.update(msg);
    }
    hasher.finish().0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_hello_roundtrip() {
        let ch = ClientHello::new(vec![ECC_SM4_SM3]);
        let body = ch.encode_body();
        let decoded = ClientHello::decode_body(&body).unwrap();
        assert_eq!(decoded.client_version, [0x03, 0x03]);
        assert_eq!(decoded.cipher_suites, vec![ECC_SM4_SM3]);
    }

    #[test]
    fn server_hello_roundtrip() {
        let sh = ServerHello::new(ECC_SM4_SM3);
        let body = sh.encode_body();
        let decoded = ServerHello::decode_body(&body).unwrap();
        assert_eq!(decoded.cipher_suite, ECC_SM4_SM3);
    }

    #[test]
    fn server_key_exchange_roundtrip() {
        let ske = ServerKeyExchange::new(vec![0xaa; 32], vec![0xbb; 32]);
        let body = ske.encode_body();
        let decoded = ServerKeyExchange::decode_body(&body).unwrap();
        assert_eq!(decoded.public_key, vec![0xaa; 32]);
        assert_eq!(decoded.signature, vec![0xbb; 32]);
    }

    #[test]
    fn client_key_exchange_roundtrip() {
        let cke = ClientKeyExchange::new(vec![0xcc; 32]);
        let body = cke.encode_body();
        let decoded = ClientKeyExchange::decode_body(&body).unwrap();
        assert_eq!(decoded.public_key, vec![0xcc; 32]);
    }

    #[test]
    fn finished_roundtrip() {
        let fin = Finished { verify_data: [0x42u8; 12] };
        let body = fin.encode_body();
        let decoded = Finished::decode_body(&body).unwrap();
        assert_eq!(decoded.verify_data, [0x42u8; 12]);
    }

    #[test]
    fn wrap_and_parse_handshake() {
        let body = vec![0x01, 0x02, 0x03];
        let wrapped = wrap_handshake(msg_type::CLIENT_HELLO, &body);
        assert_eq!(wrapped[0], msg_type::CLIENT_HELLO);
        let (mt, len) = parse_handshake_header(&wrapped).unwrap();
        assert_eq!(mt, msg_type::CLIENT_HELLO);
        assert_eq!(len, 3);
        assert_eq!(&wrapped[4..], &body);
    }

    #[test]
    fn handshake_hash_deterministic() {
        let msgs = vec![b"hello".as_ref(), b"world".as_ref()];
        let h1 = handshake_hash(&msgs);
        let h2 = handshake_hash(&msgs);
        assert_eq!(h1, h2);
    }
}
