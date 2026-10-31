//! PBKDF2-HMAC（RFC 8018）。
//!
//! SCRAM 的核心 KDF：把口令派生为固定长度的 Salted Password。
//!
//! # 迭代次数
//!
//! SCRAM-SHA-256 要求 `i >= 4096`；MongoDB 默认 15000，MDB 默认同。
//! 这是**安全参数**，不是性能参数：
//!
//! - 提高 → 抗离线爆破能力线性增强，但认证 CPU 成本线性增长
//! - 部署时必须 ≥ 4096（MongoDB 显式拒绝更低值），MDB 同样在配置校验中拒绝
//! - 已创建账号的迭代次数**记录在账号文档内**，不同账号可用不同迭代次数
//!
//! # 抗 GPU 攻击
//!
//! PBKDF2 仅是单向 KDF，无内存硬性要求。MDB 的 TDE 主密钥、CSFLE 数据密钥
//! **不使用** PBKDF2，改用 SM4-GCM + 密码机随机密钥（见 `mdb-server` 密钥管理），
//! 避免 GPU/ASIC 离线爆破风险。

use crate::digest::Digest;
use crate::hmac::Hmac;

/// PBKDF2 派生 `output.len()` 字节。
///
/// # 错误
///
/// `iterations` 为 0 时返回全零；RFC 8018 要求 `iterations >= 1`。
/// 调用方（[`crate::scram`]）强制拒绝 0 迭代，避免静默产生弱密钥。
pub fn pbkdf2_hmac<D, const OUT: usize, const BLOCK: usize>(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    output: &mut [u8],
) where
    D: Digest<OUT, BLOCK>,
{
    if iterations == 0 {
        output.fill(0);
        return;
    }
    for (block_index, chunk) in (1_u32..).zip(output.chunks_mut(OUT)) {
        // U1 = PRF(P, S || INT_32_BE(i))
        let mut u = {
            let mut h = Hmac::<D, OUT, BLOCK>::new(password);
            h.update(salt);
            h.update(&block_index.to_be_bytes());
            h.finish()
        };
        let mut acc = u;
        for _ in 1..iterations {
            u = Hmac::<D, OUT, BLOCK>::mac(password, &u);
            for (a, x) in acc.iter_mut().zip(u.iter()) {
                *a ^= x;
            }
        }
        let n = chunk.len();
        chunk.copy_from_slice(&acc[..n]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::hex;
    use crate::sha1::Sha1;
    use crate::sha256::Sha256;

    #[test]
    fn rfc6070_sha1_vectors() {
        // RFC 6070 的全部 4 组
        let mut out = [0u8; 20];
        pbkdf2_hmac::<Sha1, 20, 64>(b"password", b"salt", 1, &mut out);
        assert_eq!(hex(&out), "0c60c80f961f0e71f3a9b524af6012062fe037a6");

        pbkdf2_hmac::<Sha1, 20, 64>(b"password", b"salt", 2, &mut out);
        assert_eq!(hex(&out), "ea6c014dc72d6f8ccd1ed92ace1d41f0d8de8957");

        pbkdf2_hmac::<Sha1, 20, 64>(b"password", b"salt", 4096, &mut out);
        assert_eq!(hex(&out), "4b007901b765489abead49d926f721d065a429c1");

        pbkdf2_hmac::<Sha1, 20, 64>(
            b"passwordPASSWORDpassword",
            b"saltSALTsaltSALTsaltSALTsaltSALTsalt",
            4096,
            &mut out,
        );
        assert_eq!(hex(&out), "3d2eec4fe41c849b80c8d83662c0e44a8b291a96");
    }

    #[test]
    fn rfc7914_sha256_vector() {
        // RFC 7914 §11 PBKDF2-HMAC-SHA256 测试向量
        let mut out = [0u8; 64];
        pbkdf2_hmac::<Sha256, 32, 64>(b"passwd", b"salt", 1, &mut out);
        assert_eq!(
            hex(&out),
            "55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc\
             49ca9cccf179b645991664b39d77ef317c71b845b1e30bd509112041d3a19783"
        );

        pbkdf2_hmac::<Sha256, 32, 64>(b"Password", b"NaCl", 80000, &mut out);
        assert_eq!(
            hex(&out),
            "4ddcd8f60b98be21830cee5ef22701f9641a4418d04c0414aeff08876b34ab56\
             a1d425a1225833549adb841b51c9b3176a272bdebba1d078478f62b397f33c8d"
        );
    }

    #[test]
    fn output_shorter_than_digest_length() {
        // SCRAM-SHA-256 固定 dkLen=32，与 SHA-256 等长；
        // 但 SHA-1 SCRAM dkLen=20。用更短输出验证截断逻辑。
        let mut out = [0u8; 16];
        pbkdf2_hmac::<Sha256, 32, 64>(b"password", b"salt", 4096, &mut out);
        let mut full = [0u8; 32];
        pbkdf2_hmac::<Sha256, 32, 64>(b"password", b"salt", 4096, &mut full);
        assert_eq!(&out[..], &full[..16]);
    }

    #[test]
    fn output_longer_than_digest_spans_blocks() {
        let mut out = [0u8; 80];
        pbkdf2_hmac::<Sha256, 32, 64>(b"pw", b"salt", 10, &mut out);
        // 第 1 块 = U1，独立校验
        let mut u1 = [0u8; 32];
        pbkdf2_hmac::<Sha256, 32, 64>(b"pw", b"salt", 10, &mut u1);
        assert_eq!(&out[..32], &u1[..]);
        // 第 2 块与第 1 块必须不同
        assert_ne!(&out[..32], &out[32..64]);
    }

    #[test]
    fn zero_iteration_yields_zeros() {
        let mut out = [0xffu8; 32];
        pbkdf2_hmac::<Sha256, 32, 64>(b"pw", b"salt", 0, &mut out);
        assert_eq!(out, [0u8; 32]);
    }
}
