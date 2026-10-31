//! HMAC（RFC 2104）。
//!
//! 泛型实现：以 [`Digest`] 为参数，编译期单态化，无虚调用开销。
//! 这是热路径代码——SCRAM 认证在每次连接建立时执行 PBKDF2 15000 次迭代，
//! 认证风暴场景下成为 CPU 瓶颈，虚调用会带来可观测的劣化。

use crate::digest::Digest;

const IPAD: u8 = 0x36;
const OPAD: u8 = 0x5c;

/// HMAC 上下文。
///
/// 预展开的 `ipad`/`opad` 块可跨消息复用（复制流校验和场景）。
#[derive(Debug, Clone)]
pub struct Hmac<D, const OUT: usize, const BLOCK: usize> {
    inner: D,
    outer_pad: [u8; BLOCK],
}

impl<D: Digest<OUT, BLOCK>, const OUT: usize, const BLOCK: usize> Hmac<D, OUT, BLOCK> {
    /// 由密钥初始化。
    pub fn new(key: &[u8]) -> Self {
        let mut k = [0u8; BLOCK];
        if key.len() > BLOCK {
            // 密钥超长时先做摘要，使 K 恰为 BLOCK_LEN
            let h = D::digest(key);
            k[..OUT.min(BLOCK)].copy_from_slice(&h.as_bytes()[..OUT.min(BLOCK)]);
        } else {
            k[..key.len()].copy_from_slice(key);
        }

        let mut ipad_block = [0u8; BLOCK];
        let mut outer_pad = [0u8; BLOCK];
        for i in 0..BLOCK {
            ipad_block[i] = k[i] ^ IPAD;
            outer_pad[i] = k[i] ^ OPAD;
        }
        // 减少密钥在栈上的驻留时长
        k.iter_mut().for_each(|b| *b = 0);

        let mut inner = D::new();
        inner.update(&ipad_block);
        Self { inner, outer_pad }
    }

    /// 吸收数据。
    pub fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    /// 输出 MAC。
    pub fn finish(self) -> [u8; OUT] {
        let inner_hash = self.inner.finish();
        let mut outer = D::new();
        outer.update(&self.outer_pad);
        outer.update(inner_hash.as_slice());
        outer.finish().0
    }

    /// 一次性 HMAC。
    pub fn mac(key: &[u8], data: &[u8]) -> [u8; OUT] {
        let mut h = Self::new(key);
        h.update(data);
        h.finish()
    }
}

/// HMAC-SHA-256。
pub type HmacSha256 = Hmac<crate::sha256::Sha256, 32, 64>;
/// HMAC-SHA-1。
pub type HmacSha1 = Hmac<crate::sha1::Sha1, 20, 64>;
/// HMAC-SM3（国密场景，用于会话 MAC）。
pub type HmacSm3 = Hmac<crate::sm3::Sm3, 32, 64>;
/// HMAC-MD5（仅 MONGODB-CR 迁移兼容）。
pub type HmacMd5 = Hmac<crate::md5::Md5, 16, 64>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::hex;

    #[test]
    fn rfc4231_case1_sha256() {
        let mac = HmacSha256::mac(&[0x0b; 20], b"Hi There");
        assert_eq!(
            hex(&mac),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn rfc4231_case2_sha256() {
        assert_eq!(
            hex(&HmacSha256::mac(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn rfc4231_case3_sha256() {
        let mac = HmacSha256::mac(&[0xaa; 20], &[0xdd; 50]);
        assert_eq!(
            hex(&mac),
            "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe"
        );
    }

    #[test]
    fn rfc4231_case6_key_shorter_than_block() {
        let mac = HmacSha256::mac(&[0xaa; 131], b"Test Using Larger Than Block-Size Key - Hash Key First");
        assert_eq!(
            hex(&mac),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn rfc4231_case7_key_longer_than_block() {
        let mac = HmacSha256::mac(
            &[0xaa; 131],
            b"This is a test using a larger than block-size key and a larger than block-size data. The key needs to be hashed before being used by the HMAC algorithm.",
        );
        assert_eq!(
            hex(&mac),
            "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2"
        );
    }

    #[test]
    fn rfc4231_sha1_case2() {
        assert_eq!(
            hex(&HmacSha1::mac(b"Jefe", b"what do ya want for nothing?")),
            "effcdf6ae5eb2fa2d27416d5f184df9c259a7c79"
        );
    }

    #[test]
    fn rfc2202_md5_case2() {
        // HMAC-MD5 官方向量
        assert_eq!(
            hex(&HmacMd5::mac(b"Jefe", b"what do ya want for nothing?")),
            "750c783e6ab0b503eaa86e310a5db738"
        );
    }

    #[test]
    fn empty_key_is_valid() {
        assert_ne!(HmacSha256::mac(b"", b""), HmacSha256::mac(b"", b"x"));
    }

    #[test]
    fn streaming_matches_oneshot() {
        let data: Vec<u8> = (0..500u32).map(|i| (i % 249) as u8).collect();
        let expect = HmacSha256::mac(b"k", &data);
        for split in [0usize, 1, 63, 64, 65, 127, 200, data.len()] {
            let mut h = HmacSha256::new(b"k");
            h.update(&data[..split]);
            h.update(&data[split..]);
            assert_eq!(h.finish(), expect, "split {split}");
        }
    }
}
