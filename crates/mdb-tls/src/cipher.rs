//! SM4-GCM AEAD 加密/解密。
//!
//! GCM（Galois/Counter Mode）提供认证加密：
//! - 加密：SM4-CTR 模式
//! - 认证：GHASH（GF(2^128) 上的多项式哈希）
//!
//! # GHASH
//!
//! GHASH 在 GF(2^128) 上做乘法，约化多项式为 x^128 + x^7 + x^2 + x + 1。
//! 本实现使用 bit-by-bit 乘法（适用于认证标签长度 ≤ 128 bit 的场景）。

use mdb_crypto::sm4::Sm4Key;

/// GCM 默认认证标签长度（字节）。
pub const GCM_TAG_LEN: usize = 16;

/// GCM 默认 nonce 长度（字节）。
pub const GCM_IV_LEN: usize = 12;

/// GCM 加密结果。
#[derive(Debug, Clone)]
pub struct GcmOutput {
    /// 密文（与明文等长）。
    pub ciphertext: Vec<u8>,
    /// 16 字节认证标签。
    pub tag: [u8; GCM_TAG_LEN],
}

/// GHASH 哈希子密钥 H = E_K(0^128)。
#[derive(Debug, Clone)]
pub struct GcmState {
    h: [u8; 16],
}

impl GcmState {
    /// 由 SM4 密钥初始化：H = E_K(0^128)。
    pub fn new(key: &Sm4Key) -> Self {
        let h = key.encrypt_block(&[0u8; 16]);
        Self { h }
    }

    /// GHASH 乘法：在 GF(2^128) 上计算 `x * y`，使用 GCM 约化多项式。
    fn ghash_mul(x: &[u8; 16], y: &[u8; 16]) -> [u8; 16] {
        // Schoolbook 乘法 + 按位约化
        // 将 x 视为 GF(2) 上的多项式（MSB-first），逐位乘
        let mut z = [0u8; 16];
        let mut v = *y;

        for i in 0..128 {
            // 检查 x 的第 i 位（MSB-first）
            let byte_idx = i / 8;
            let bit_idx = 7 - (i % 8);
            if (x[byte_idx] >> bit_idx) & 1 == 1 {
                for j in 0..16 {
                    z[j] ^= v[j];
                }
            }
            // v >>= 1（右移一位）
            let carry = v[15] & 1;
            for j in (1..16).rev() {
                v[j] = (v[j] >> 1) | (v[j - 1] << 7);
            }
            v[0] >>= 1;
            // 如果有进位，异或约化多项式 0xe1 << 0（在 MSB-first 表示下）
            if carry != 0 {
                v[0] ^= 0xe1;
            }
        }
        z
    }

    /// GHASH 计算：处理 AAD 和密文。
    ///
    /// `tag = GHASH(H, AAD, ciphertext)`
    fn ghash(&self, aad: &[u8], ciphertext: &[u8]) -> [u8; 16] {
        let mut y = [0u8; 16];

        // 处理 AAD 块
        for chunk in aad.chunks(16) {
            let mut block = [0u8; 16];
            block[..chunk.len()].copy_from_slice(chunk);
            for j in 0..16 {
                y[j] ^= block[j];
            }
            y = Self::ghash_mul(&y, &self.h);
        }

        // 处理密文块
        for chunk in ciphertext.chunks(16) {
            let mut block = [0u8; 16];
            block[..chunk.len()].copy_from_slice(chunk);
            for j in 0..16 {
                y[j] ^= block[j];
            }
            y = Self::ghash_mul(&y, &self.h);
        }

        // 最后块：len(AAD) || len(Ciphertext)，各64 bit big-endian
        let mut len_block = [0u8; 16];
        let aad_bits = (aad.len() as u64) * 8;
        let ct_bits = (ciphertext.len() as u64) * 8;
        len_block[0..8].copy_from_slice(&aad_bits.to_be_bytes());
        len_block[8..16].copy_from_slice(&ct_bits.to_be_bytes());

        for j in 0..16 {
            y[j] ^= len_block[j];
        }
        y = Self::ghash_mul(&y, &self.h);
        y
    }

    /// GCTR：SM4-CTR 加密。
    ///
    /// `counter = IV || 0x00000001`（12 字节 IV + 4 字节计数器）
    fn gctr(key: &Sm4Key, iv: &[u8; GCM_IV_LEN], plaintext: &[u8]) -> Vec<u8> {
        let mut counter = [0u8; 16];
        counter[..12].copy_from_slice(iv);
        counter[12..16].copy_from_slice(&1u32.to_be_bytes());

        let mut output = Vec::with_capacity(plaintext.len());
        for chunk in plaintext.chunks(16) {
            let keystream = key.encrypt_block(&counter);
            for (i, &b) in chunk.iter().enumerate() {
                output.push(b ^ keystream[i]);
            }
            // 递增计数器（32-bit big-endian）
            let c = u32::from_be_bytes([counter[12], counter[13], counter[14], counter[15]]);
            let c = c.wrapping_add(1);
            counter[12..16].copy_from_slice(&c.to_be_bytes());
        }
        output
    }

    /// GCM 认证加密。
    pub fn encrypt(
        key: &Sm4Key,
        iv: &[u8; GCM_IV_LEN],
        plaintext: &[u8],
        aad: &[u8],
    ) -> GcmOutput {
        let state = Self::new(key);
        let ciphertext = Self::gctr(key, iv, plaintext);

        // J0 = IV || 0x00000001（用于最终标签异或）
        let mut j0 = [0u8; 16];
        j0[..12].copy_from_slice(iv);
        j0[12..16].copy_from_slice(&1u32.to_be_bytes());

        // S = GHASH(H, AAD, ciphertext)
        let s = state.ghash(aad, &ciphertext);

        // tag = S XOR E_K(J0)
        let ek_j0 = key.encrypt_block(&j0);
        let mut tag = [0u8; GCM_TAG_LEN];
        for i in 0..GCM_TAG_LEN {
            tag[i] = s[i] ^ ek_j0[i];
        }

        GcmOutput { ciphertext, tag }
    }

    /// GCM 认证解密。
    ///
    /// 返回 `Ok(plaintext)` 或 `Err(())`（认证失败）。
    pub fn decrypt(
        key: &Sm4Key,
        iv: &[u8; GCM_IV_LEN],
        ciphertext: &[u8],
        aad: &[u8],
        tag: &[u8; GCM_TAG_LEN],
    ) -> Result<Vec<u8>, ()> {
        let state = Self::new(key);

        // J0 = IV || 0x00000001
        let mut j0 = [0u8; 16];
        j0[..12].copy_from_slice(iv);
        j0[12..16].copy_from_slice(&1u32.to_be_bytes());

        // S = GHASH(H, AAD, ciphertext)
        let s = state.ghash(aad, ciphertext);

        // expected_tag = S XOR E_K(J0)
        let ek_j0 = key.encrypt_block(&j0);
        let mut expected_tag = [0u8; GCM_TAG_LEN];
        for i in 0..GCM_TAG_LEN {
            expected_tag[i] = s[i] ^ ek_j0[i];
        }

        // 常量时间比较
        if !mdb_crypto::constant_time_eq(tag, &expected_tag) {
            return Err(());
        }

        Ok(Self::gctr(key, iv, ciphertext))
    }
}

/// GCM 封装：加密 + 附加数据 + 标签。
pub fn gcm_encrypt(
    key: &[u8; 16],
    iv: &[u8; GCM_IV_LEN],
    plaintext: &[u8],
    aad: &[u8],
) -> Result<GcmOutput, ()> {
    let sm4_key = Sm4Key::from_key(*key);
    Ok(GcmState::encrypt(&sm4_key, iv, plaintext, aad))
}

/// GCM 封装：解密 + 附加数据 + 标签验证。
pub fn gcm_decrypt(
    key: &[u8; 16],
    iv: &[u8; GCM_IV_LEN],
    ciphertext: &[u8],
    aad: &[u8],
    tag: &[u8; GCM_TAG_LEN],
) -> Result<Vec<u8>, ()> {
    let sm4_key = Sm4Key::from_key(*key);
    GcmState::decrypt(&sm4_key, iv, ciphertext, aad, tag)
}

/// 生成随机 nonce（96 bit）。
pub fn random_iv() -> [u8; GCM_IV_LEN] {
    mdb_crypto::random::bytes::<GCM_IV_LEN>()
}

/// 计数器模式 nonce：基于基础 IV 和计数器生成。
///
/// `nonce = base_iv XOR (counter << 64)`（counter 为64 bit 大端）
pub fn counter_iv(base_iv: &[u8; GCM_IV_LEN], counter: u64) -> [u8; GCM_IV_LEN] {
    let mut iv = *base_iv;
    let ctr_bytes = counter.to_be_bytes();
    // counter 放在 IV 的后8字节（前4字节为0）
    for i in 0..8 {
        iv[4 + i] ^= ctr_bytes[i];
    }
    iv
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ghash_mul_zero() {
        let h = [0x42u8; 16];
        let z = [0u8; 16];
        let result = GcmState::ghash_mul(&z, &h);
        assert_eq!(result, [0u8; 16]);
    }

    #[test]
    fn ghash_mul_identity() {
        let h = [0x42u8; 16];
        let one = {
            let mut b = [0u8; 16];
            b[0] = 0x80; // MSB = 1
            b
        };
        let result = GcmState::ghash_mul(&one, &h);
        // x * H should equal H with MSB masking
        assert_eq!(result, h);
    }

    #[test]
    fn gcm_roundtrip() {
        let key = [0x42u8; 16];
        let iv = [0x99u8; GCM_IV_LEN];
        let plaintext = b"Hello, TLCP!";
        let aad = b"additional data";

        let output = gcm_encrypt(&key, &iv, plaintext, aad).unwrap();
        let decrypted = gcm_decrypt(&key, &iv, &output.ciphertext, aad, &output.tag).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn gcm_wrong_key_fails() {
        let key1 = [0x42u8; 16];
        let key2 = [0x43u8; 16];
        let iv = [0x99u8; GCM_IV_LEN];
        let plaintext = b"secret data";
        let aad = b"";

        let output = gcm_encrypt(&key1, &iv, plaintext, aad).unwrap();
        assert!(gcm_decrypt(&key2, &iv, &output.ciphertext, aad, &output.tag).is_err());
    }

    #[test]
    fn gcm_wrong_aad_fails() {
        let key = [0x42u8; 16];
        let iv = [0x99u8; GCM_IV_LEN];
        let plaintext = b"secret data";

        let output = gcm_encrypt(&key, &iv, plaintext, b"aad1").unwrap();
        assert!(gcm_decrypt(&key, &iv, &output.ciphertext, b"aad2", &output.tag).is_err());
    }

    #[test]
    fn gcm_wrong_tag_fails() {
        let key = [0x42u8; 16];
        let iv = [0x99u8; GCM_IV_LEN];
        let plaintext = b"secret data";

        let output = gcm_encrypt(&key, &iv, plaintext, b"").unwrap();
        let mut wrong_tag = output.tag;
        wrong_tag[0] ^= 0xff;
        assert!(gcm_decrypt(&key, &iv, &output.ciphertext, b"", &wrong_tag).is_err());
    }

    #[test]
    fn gcm_empty_plaintext() {
        let key = [0x42u8; 16];
        let iv = [0x99u8; GCM_IV_LEN];

        let output = gcm_encrypt(&key, &iv, b"", b"").unwrap();
        assert!(output.ciphertext.is_empty());
        let decrypted = gcm_decrypt(&key, &iv, &output.ciphertext, b"", &output.tag).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn gcm_large_plaintext() {
        let key = [0x42u8; 16];
        let iv = [0x99u8; GCM_IV_LEN];
        let plaintext: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();

        let output = gcm_encrypt(&key, &iv, &plaintext, b"").unwrap();
        let decrypted = gcm_decrypt(&key, &iv, &output.ciphertext, b"", &output.tag).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn counter_iv_increments() {
        let base = [0x01u8; GCM_IV_LEN];
        let iv1 = counter_iv(&base, 0);
        let iv2 = counter_iv(&base, 1);
        let iv3 = counter_iv(&base, 2);
        assert_ne!(iv1, iv2);
        assert_ne!(iv2, iv3);
        assert_ne!(iv1, iv3);
    }
}
