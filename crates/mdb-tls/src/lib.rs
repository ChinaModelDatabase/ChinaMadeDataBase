//! MDB TLCP/TLS transport encryption。
//!
//! 基于国密算法的 TLS 实现，提供传输层加密：
//! - SM4-GCM 认证加密（AEAD）
//! - SM3 密码杂凑（PRF / Finished / 证书指纹）
//! - 简化的密钥交换协议（PSK + 随机贡献）
//! - 完整的 TLS 记录层与握手协议
//!
//! # 架构
//!
//! ```text
//! TlsStream<TcpStream>
//!   └─ record layer (加密/解密)
//!       └─ handshake (密钥协商)
//!           └─ cipher (SM4-GCM)
//!               └─ prf (SM3-based)
//! ```
//!
//! # TLCP 1.1 握手流程
//!
//! ```text
//! Client                              Server
//!   |--- ClientHello ------------------>|
//!   |    (client_random, suites)        |
//!   |<-- ServerHello -------------------|
//!   |    (server_random, suite)         |
//!   |<-- Certificate --------------------|
//!   |<-- ServerKeyExchange ------------>|
//!   |<-- ServerHelloDone ---------------|
//!   |--- ClientKeyExchange ------------>|
//!   |--- ChangeCipherSpec + Finished -->|
//!   |<-- ChangeCipherSpec + Finished ---|
//! ```

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod alert;
pub mod cert;
pub mod cipher;
pub mod config;
pub mod handshake;
pub mod prf;
pub mod record;
pub mod stream;

pub use config::TlsConfig;
pub use stream::TlsStream;
