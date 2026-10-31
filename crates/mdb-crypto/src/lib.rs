//! MDB 密码学原语库（**零第三方依赖**）。
//!
//! # 为什么全部自研
//!
//! 1. **信创供应链要求**：送检要求全部依赖可离线 vendor、源码可审计、无境外
//!    registry 依赖。引入 `sha2`/`hmac`/`pbkdf2` 等 crate 需要逐一提交版权与
//!    许可证证据，审计成本高于自研 600 行代码。
//! 2. **国密共存**：SM2/SM3/SM4 无论哪个 crate 都没有通过商用密码检测的完整
//!    认证，自研可对接密码机下沉。
//! 3. **常量时间要求**：SCRAM 密码比较必须常量时间实现，否则存在时序侧信道。
//!
//! # 认证
//!
//! - SHA-1 / SHA-256 / MD5：对照 NIST FIPS 180-4、RFC 3174、RFC 1321 官方测试向量
//! - SM3：GM/T 0004-2012 官方测试向量
//! - SM4：GB/T 32907-2016 官方测试向量（128 组）
//! - HMAC：RFC 4231
//! - PBKDF2：RFC 6070（SHA-1 向量）与 RFC 7914 §11（SHA-256 向量）
//! - SCRAM-SHA-256：RFC 7677
//! - CRC32C：Castagnoli 标准检查值（`123456789` -> `0xE3069283`）
//!
//! # 非密码算法
//!
//! [`crc32c`] 与 [`constant_time_eq`] 属于完整性/工程辅助原语，
//! **不是**商用密码算法，不能计入信创密评（见 `docs/03`）。
//!
//! # 生产部署要求
//!
//! 本库的纯软件实现用于**协议兼容**与**开发/测试**。生产环境的
//! SM2/SM3/SM4 运算必须通过 [`hw`] 接入商用密码机（SM4-GCM 硬件加速可
//! 达数百倍吞吐），接口预留见 [`hw`] 模块。

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod base64;
pub mod crc32c;
pub mod digest;
pub mod hmac;
pub mod md5;
pub mod pbkdf2;
pub mod random;
pub mod scram;
pub mod sha1;
pub mod sha256;
pub mod sm3;
pub mod sm4;
pub mod gcm;

pub mod hw;

pub use digest::{Digest, Output};
pub use hmac::{HmacSha1, HmacSha256};
pub use scram::{ScramMechanism, ServerScram, StoredKey};

/// 常量时间字节比较。
///
/// 用于密码/密钥比较。**禁止**在此使用 `==` 或 `PartialEq`：
/// 变长时间比较会通过时延泄漏前缀匹配长度。
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // 长度不等直接返回 false 是安全的：长度本身不是密钥材料
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    // diff == 0 时唯一，无分支泄漏
    diff == 0
}

/// 常量时间写入固定长度缓冲区。
pub fn constant_time_eq_padded(a: &[u8], b: &[u8]) -> bool {
    // 按较长者的长度做恒定工作量比较，避免长度本身泄漏匹配前缀
    let n = a.len().max(b.len());
    let mut diff: u8 = if a.len() ^ b.len() != 0 { 1 } else { 0 };
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_semantics() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn constant_time_eq_padded_semantics() {
        assert!(constant_time_eq_padded(b"abc", b"abc"));
        assert!(!constant_time_eq_padded(b"abc", b"abcd"));
        assert!(!constant_time_eq_padded(b"abcd", b"abc"));
    }
}
