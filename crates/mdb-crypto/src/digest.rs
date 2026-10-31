//! 摘要 trait 与输出类型。
//!
//! # 为什么用 const 泛型参数而不是关联常量
//!
//! 关联常量（`const OUTPUT_LEN: usize`）**不能**用作类型中的数组长度，
//! 当 `Self` 或 `D` 是泛型参数时（`generic parameters may not be used in
//! const operations`）。这会导致 `Hmac<D>` 无法持有展开的 ipad/opad 块。
//!
//! 因此本 crate 一律使用 const 泛型参数：
//!
//! ```ignore
//! pub trait Digest<const OUT: usize, const BLOCK: usize>
//! ```
//!
//! 所有 MDB 支持的摘要（SHA-1/256、MD5、SM3）块大小均为 64 字节，
//! 仅有输出长度不同。

/// 摘要输出。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Output<const N: usize>(pub [u8; N]);

impl<const N: usize> Output<N> {
    pub const fn new(bytes: [u8; N]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; N] {
        &self.0
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// 定长引用，便于向 HMAC 等接口传参。
    pub const fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl<const N: usize> AsRef<[u8]> for Output<N> {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<const N: usize> From<Output<N>> for [u8; N] {
    fn from(o: Output<N>) -> Self {
        o.0
    }
}

impl<const N: usize> core::fmt::Display for Output<N> {
    /// 渲染为小写十六进制。
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// 可分块更新的摘要。
pub trait Digest<const OUT: usize, const BLOCK: usize>: Sized {
    /// 算法名，与 OpenSSL / MongoDB `buildInfo` 中一致。
    const NAME: &'static str;

    /// 创建新实例。
    fn new() -> Self;
    /// 吸收数据。
    fn update(&mut self, data: &[u8]);
    /// 输出摘要（内部状态被消费）。
    fn finish(self) -> Output<OUT>;
    /// 一次性摘要。
    fn digest(data: &[u8]) -> Output<OUT> {
        let mut h = Self::new();
        h.update(data);
        h.finish()
    }
}

/// 渲染为小写十六进制。
pub fn hex(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(LUT[(b >> 4) as usize] as char);
        s.push(LUT[(b & 0x0f) as usize] as char);
    }
    s
}

/// 解析小写或大写十六进制。
pub fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 2);
    for pair in b.chunks(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        assert_eq!(hex(&[0x00, 0x0f, 0xff, 0xa5]), "000fffa5");
        assert_eq!(unhex("000fffa5").unwrap(), vec![0x00, 0x0f, 0xff, 0xa5]);
        assert_eq!(unhex("00FF").unwrap(), vec![0x00, 0xff]);
        assert!(unhex("0").is_none());
        assert!(unhex("zz").is_none());
    }
}
