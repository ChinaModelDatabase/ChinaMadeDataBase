//! TLS 配置。

use std::path::PathBuf;

/// TLS 配置。
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// 是否启用 TLS。
    pub enabled: bool,
    /// 服务器证书文件路径。
    pub cert_file: Option<PathBuf>,
    /// 服务器私钥文件路径。
    pub key_file: Option<PathBuf>,
    /// CA 证书文件路径（用于验证客户端证书）。
    pub ca_file: Option<PathBuf>,
    /// 支持的密码套件。
    pub cipher_suites: Vec<u16>,
    /// 是否要求客户端证书。
    pub require_client_cert: bool,
}

impl TlsConfig {
    /// 创建默认 TLS 配置。
    pub fn new() -> Self {
        Self {
            enabled: false,
            cert_file: None,
            key_file: None,
            ca_file: None,
            cipher_suites: vec![0xe013], // ECC_SM4_SM3
            require_client_cert: false,
        }
    }

    /// 启用 TLS。
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// 设置证书文件路径。
    pub fn with_cert_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.cert_file = Some(path.into());
        self
    }

    /// 设置私钥文件路径。
    pub fn with_key_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.key_file = Some(path.into());
        self
    }

    /// 设置 CA 文件路径。
    pub fn with_ca_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_file = Some(path.into());
        self
    }

    /// 要求客户端证书。
    pub fn with_require_client_cert(mut self, require: bool) -> Self {
        self.require_client_cert = require;
        self
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// TLS 会话状态（握手中间态）。
#[derive(Debug, Clone)]
pub struct TlsSession {
    /// 客户端随机数。
    pub client_random: [u8; 32],
    /// 服务器随机数。
    pub server_random: [u8; 32],
    /// 选中的密码套件。
    pub cipher_suite: u16,
    /// 主密钥。
    pub master_secret: Option<[u8; 48]>,
    /// 读密钥（解密对端发来的数据）。
    pub read_key: Option<[u8; 16]>,
    /// 写密钥（加密本端发出的数据）。
    pub write_key: Option<[u8; 16]>,
    /// 读 IV。
    pub read_iv: Option<[u8; 12]>,
    /// 写 IV。
    pub write_iv: Option<[u8; 12]>,
    /// 读记录序号。
    pub read_seq: u64,
    /// 写记录序号。
    pub write_seq: u64,
    /// 是否已完成握手。
    pub handshake_complete: bool,
    /// 已发送的握手消息（用于 Finished 校验）。
    pub handshake_messages: Vec<Vec<u8>>,
}

impl TlsSession {
    pub fn new() -> Self {
        Self {
            client_random: [0u8; 32],
            server_random: [0u8; 32],
            cipher_suite: 0,
            master_secret: None,
            read_key: None,
            write_key: None,
            read_iv: None,
            write_iv: None,
            read_seq: 0,
            write_seq: 0,
            handshake_complete: false,
            handshake_messages: Vec::new(),
        }
    }

    /// 推进读序号。
    pub fn next_read_seq(&mut self) -> u64 {
        let seq = self.read_seq;
        self.read_seq += 1;
        seq
    }

    /// 推进写序号。
    pub fn next_write_seq(&mut self) -> u64 {
        let seq = self.write_seq;
        self.write_seq += 1;
        seq
    }
}

impl Default for TlsSession {
    fn default() -> Self {
        Self::new()
    }
}
