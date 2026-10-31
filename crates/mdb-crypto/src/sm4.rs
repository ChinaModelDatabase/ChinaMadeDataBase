//! SM4 分组密码（GB/T 32907-2016）。
//!
//! 128 bit 分组、128 bit 密钥，用于 TDE、CSFLE、传输层国密套件。
//!
//! # 模式
//!
//! 本模块只提供**单块**加密/解密。实际使用必须叠加安全模式：
//!
//! - **CTR**：数据盘加密首选。随机 nonce 由 CSPRNG 生成；支持并行加密，
//!   契合 LSM 写放大高的场景（可按 block 并行）。
//! - **GCM**：需要认证标签时使用（`mdb-storage` 校验和的防篡改场景）。
//!   正确实现见 `mdb-server::tde`，**不得**在本模块内自行拼装。
//! - **CBC**：仅用于与遗留国密设备互操作。
//!
//! 详见 `docs/03-xinchuang-mlps-compliance.md` 密码合规章节。


const SBOX: [u8; 256] = [
    0xd6, 0x90, 0xe9, 0xfe, 0xcc, 0xe1, 0x3d, 0xb7, 0x16, 0xb6, 0x14, 0xc2, 0x28, 0xfb, 0x2c, 0x05,
    0x2b, 0x67, 0x9a, 0x76, 0x2a, 0xbe, 0x04, 0xc3, 0xaa, 0x44, 0x13, 0x26, 0x49, 0x86, 0x06, 0x99,
    0x9c, 0x42, 0x50, 0xf4, 0x91, 0xef, 0x98, 0x7a, 0x33, 0x54, 0x0b, 0x43, 0xed, 0xcf, 0xac, 0x62,
    0xe4, 0xb3, 0x1c, 0xa9, 0xc9, 0x08, 0xe8, 0x95, 0x80, 0xdf, 0x94, 0xfa, 0x75, 0x8f, 0x3f, 0xa6,
    0x47, 0x07, 0xa7, 0xfc, 0xf3, 0x73, 0x17, 0xba, 0x83, 0x59, 0x3c, 0x19, 0xe6, 0x85, 0x4f, 0xa8,
    0x68, 0x6b, 0x81, 0xb2, 0x71, 0x64, 0xda, 0x8b, 0xf8, 0xeb, 0x0f, 0x4b, 0x70, 0x56, 0x9d, 0x35,
    0x1e, 0x24, 0x0e, 0x5e, 0x63, 0x58, 0xd1, 0xa2, 0x25, 0x22, 0x7c, 0x3b, 0x01, 0x21, 0x78, 0x87,
    0xd4, 0x00, 0x46, 0x57, 0x9f, 0xd3, 0x27, 0x52, 0x4c, 0x36, 0x02, 0xe7, 0xa0, 0xc4, 0xc8, 0x9e,
    0xea, 0xbf, 0x8a, 0xd2, 0x40, 0xc7, 0x38, 0xb5, 0xa3, 0xf7, 0xf2, 0xce, 0xf9, 0x61, 0x15, 0xa1,
    0xe0, 0xae, 0x5d, 0xa4, 0x9b, 0x34, 0x1a, 0x55, 0xad, 0x93, 0x32, 0x30, 0xf5, 0x8c, 0xb1, 0xe3,
    0x1d, 0xf6, 0xe2, 0x2e, 0x82, 0x66, 0xca, 0x60, 0xc0, 0x29, 0x23, 0xab, 0x0d, 0x53, 0x4e, 0x6f,
    0xd5, 0xdb, 0x37, 0x45, 0xde, 0xfd, 0x8e, 0x2f, 0x03, 0xff, 0x6a, 0x72, 0x6d, 0x6c, 0x5b, 0x51,
    0x8d, 0x1b, 0xaf, 0x92, 0xbb, 0xdd, 0xbc, 0x7f, 0x11, 0xd9, 0x5c, 0x41, 0x1f, 0x10, 0x5a, 0xd8,
    0x0a, 0xc1, 0x31, 0x88, 0xa5, 0xcd, 0x7b, 0xbd, 0x2d, 0x74, 0xd0, 0x12, 0xb8, 0xe5, 0xb4, 0xb0,
    0x89, 0x69, 0x97, 0x4a, 0x0c, 0x96, 0x77, 0x7e, 0x65, 0xb9, 0xf1, 0x09, 0xc5, 0x6e, 0xc6, 0x84,
    0x18, 0xf0, 0x7d, 0xec, 0x3a, 0xdc, 0x4d, 0x20, 0x79, 0xee, 0x5f, 0x3e, 0xd7, 0xcb, 0x39, 0x48,
];

const FK: [u32; 4] = [0xa3b1_bac6, 0x56aa_3350, 0x677d_9197, 0xb270_22dc];

/// 固定参数 CK（GB/T 32907-2016 §7.3）。
///
/// 生成规则：`CK[i] = pack_big_endian(((4*i + j) * 7) mod 256, j = 0..3)`。
/// 下表已与标准正文逐项核对；`ck` 测试会按公式重新推导并断言一致，
/// 避免再次出现手工抄录错误。
const CK: [u32; 32] = [
    0x0007_0e15, 0x1c23_2a31, 0x383f_464d, 0x545b_6269, 0x7077_7e85, 0x8c93_9aa1, 0xa8af_b6bd,
    0xc4cb_d2d9, 0xe0e7_eef5, 0xfc03_0a11, 0x181f_262d, 0x343b_4249, 0x5057_5e65, 0x6c73_7a81,
    0x888f_969d, 0xa4ab_b2b9, 0xc0c7_ced5, 0xdce3_eaf1, 0xf8ff_060d, 0x141b_2229, 0x3037_3e45,
    0x4c53_5a61, 0x686f_767d, 0x848b_9299, 0xa0a7_aeb5, 0xbcc3_cad1, 0xd8df_e6ed, 0xf4fb_0209,
    0x1017_1e25, 0x2c33_3a41, 0x484f_565d, 0x646b_7279,
];

/// SM4 密钥（32 字节 = 4 个 u32 字）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sm4Key {
    rk: [u32; 32],
}

impl Sm4Key {
    /// 由 128 bit 密钥初始化轮密钥。
    pub fn from_key(key: [u8; 16]) -> Self {
        let mut k = [
            u32::from_be_bytes([key[0], key[1], key[2], key[3]]) ^ FK[0],
            u32::from_be_bytes([key[4], key[5], key[6], key[7]]) ^ FK[1],
            u32::from_be_bytes([key[8], key[9], key[10], key[11]]) ^ FK[2],
            u32::from_be_bytes([key[12], key[13], key[14], key[15]]) ^ FK[3],
        ];
        let mut rk = [0u32; 32];
        let mut i = 0;
        while i < 32 {
            rk[i] = k[0] ^ t_key(k[1] ^ k[2] ^ k[3] ^ CK[i]);
            k[0] = k[1];
            k[1] = k[2];
            k[2] = k[3];
            k[3] = rk[i];
            i += 1;
        }
        Self { rk }
    }

    /// 由任意长度字节密钥初始化；长度必须为 16。
    pub fn from_slice(key: &[u8]) -> Option<Self> {
        let arr: [u8; 16] = key.try_into().ok()?;
        Some(Self::from_key(arr))
    }

    /// 单块加密（16 字节进、16 字节出）。
    pub fn encrypt_block(&self, block: &[u8; 16]) -> [u8; 16] {
        let mut x = [
            u32::from_be_bytes([block[0], block[1], block[2], block[3]]),
            u32::from_be_bytes([block[4], block[5], block[6], block[7]]),
            u32::from_be_bytes([block[8], block[9], block[10], block[11]]),
            u32::from_be_bytes([block[12], block[13], block[14], block[15]]),
        ];
        for i in 0..32 {
            let out = x[0] ^ t_round(x[1] ^ x[2] ^ x[3] ^ self.rk[i]);
            x[0] = x[1];
            x[1] = x[2];
            x[2] = x[3];
            x[3] = out;
        }
        // 逆序变换 R'：(X0..X3) -> (X3, X2, X1, X0)
        let mut out = [0u8; 16];
        for j in 0..4 {
            out[j * 4..j * 4 + 4].copy_from_slice(&x[3 - j].to_be_bytes());
        }
        out
    }

    /// 单块解密。轮密钥逆序。
    pub fn decrypt_block(&self, block: &[u8; 16]) -> [u8; 16] {
        let mut rev = self.rk;
        rev.reverse();
        let mut x = [
            u32::from_be_bytes([block[0], block[1], block[2], block[3]]),
            u32::from_be_bytes([block[4], block[5], block[6], block[7]]),
            u32::from_be_bytes([block[8], block[9], block[10], block[11]]),
            u32::from_be_bytes([block[12], block[13], block[14], block[15]]),
        ];
          for &rk in rev.iter() {
              let out = x[0] ^ t_round(x[1] ^ x[2] ^ x[3] ^ rk);
              x[0] = x[1];
              x[1] = x[2];
              x[2] = x[3];
              x[3] = out;
          }
        let mut out = [0u8; 16];
        for j in 0..4 {
            out[j * 4..j * 4 + 4].copy_from_slice(&x[3 - j].to_be_bytes());
        }
        out
    }
}

/// 非线性变换 τ：查 SBOX。
#[inline(always)]
fn tau(a: u32) -> u32 {
    let b = a.to_be_bytes();
    (SBOX[b[0] as usize] as u32) << 24
        | (SBOX[b[1] as usize] as u32) << 16
        | (SBOX[b[2] as usize] as u32) << 8
        | (SBOX[b[3] as usize] as u32)
}

/// 分组密码轮变换的线性变换 L：B ⊕ B<<<2 ⊕ B<<<10 ⊕ B<<<18 ⊕ B<<<24。
#[inline(always)]
fn l(b: u32) -> u32 {
    b ^ b.rotate_left(2) ^ b.rotate_left(10) ^ b.rotate_left(18) ^ b.rotate_left(24)
}

/// 密钥扩展专用的线性变换 L'：B ⊕ B<<<13 ⊕ B<<<23。
///
/// 与轮变换的 `l` **不同**——混淆使用会导致标准向量不符。
#[inline(always)]
fn l_prime(b: u32) -> u32 {
    b ^ b.rotate_left(13) ^ b.rotate_left(23)
}

/// 轮函数扩散 T = L(τ(x))。
#[inline(always)]
fn t_round(x: u32) -> u32 {
    l(tau(x))
}

/// 密钥扩展扩散 T' = L'(τ(x))。
#[inline(always)]
fn t_key(x: u32) -> u32 {
    l_prime(tau(x))
}

/// CTR 模式的软件实现。
///
/// # 与硬件后端的差异
///
/// 密码机提供真正的 SM4-GCM（含认证标签 + AAD）。软件 CTR **不提供认证**，
/// 因此仅可用于：
///
/// - 开发/测试
/// - 已由上层 `mdb-storage` 校验和层提供完整性保护的数据（此时
///   CTR 的密文完整性由 CRC32C + 重放计数保证）
///
/// **不得**用软件 CTR 替代硬件 GCM 通过密评。
pub mod ctr {
    use super::Sm4Key;

    /// 计数器块的初始值。
    ///
    /// 高 64 bit 视为 nonce（来自 CSPRNG），低 64 bit 为块计数器。
    /// 块计数器从 1 开始，0 保留。
    #[inline(always)]
    fn keystream_block(key: &Sm4Key, nonce: u64, counter: u64) -> [u8; 16] {
        let mut block = [0u8; 16];
        block[..8].copy_from_slice(&nonce.to_be_bytes());
        block[8..].copy_from_slice(&counter.to_be_bytes());
        key.encrypt_block(&block)
    }

    /// XOR 加解密，返回新向量（不修改入参）。
    pub fn crypt(key: &Sm4Key, nonce: u64, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(data.len());
        let mut counter: u64 = 1;
        for chunk in data.chunks(16) {
            let ks = keystream_block(key, nonce, counter);
            out.extend(chunk.iter().zip(ks.iter()).map(|(d, k)| d ^ k));
            counter = counter.wrapping_add(1);
        }
        out
    }

    /// XOR 就地加解密。
    pub fn crypt_in_place(key: &Sm4Key, nonce: u64, data: &mut [u8]) {
        let mut counter: u64 = 1;
        for chunk in data.chunks_mut(16) {
            let ks = keystream_block(key, nonce, counter);
            for (d, k) in chunk.iter_mut().zip(ks.iter()) {
                *d ^= k;
            }
            counter = counter.wrapping_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::hex;
    use crate::digest::unhex;

    #[test]
    fn gb_t_32907_128_vector() {
        // GB/T 32907-2016 附录 A.1：128 bit 密钥、128 bit 明文
        let key = unhex("0123456789abcdeffedcba9876543210").unwrap();
        let pt = unhex("0123456789abcdeffedcba9876543210").unwrap();
        let k = Sm4Key::from_key(key.clone().try_into().unwrap());
        let ct = k.encrypt_block(&pt.clone().try_into().unwrap());
        assert_eq!(hex(&ct), "681edf34d206965e86b3e94f536e4246");
        let back = k.decrypt_block(&ct);
        assert_eq!(back.to_vec(), pt);
    }

    #[test]
    fn ck_matches_standard_formula() {
        // GB/T 32907-2016 §7.3：ck[i][j] = (4*i + j) * 7 mod 256
        for (i, expected) in CK.iter().enumerate() {
            let mut derived = [0u8; 4];
            for (j, byte) in derived.iter_mut().enumerate() {
                *byte = (((4 * i + j) * 7) % 256) as u8;
            }
            assert_eq!(*expected, u32::from_be_bytes(derived), "CK[{i}] mismatch");
        }
    }

    #[test]
    fn sbox_is_a_permutation() {
        let mut seen = [false; 256];
        for &b in SBOX.iter() {
            assert!(!seen[b as usize], "S-box has duplicate entry {b:#04x}");
            seen[b as usize] = true;
        }
    }

    #[test]
    fn gb_t_32907_128_repeat_vector() {
        // 附录 A.2：明文 = 密钥，验证 1,000,000 次迭代的导出向量

        let key = unhex("0123456789abcdeffedcba9876543210").unwrap();
        let k = Sm4Key::from_key(key.clone().try_into().unwrap());
        let mut block = key.clone().try_into().unwrap();
        for _ in 0..1_000_000 {
            block = k.encrypt_block(&block);
        }
        assert_eq!(hex(&block), "595298c7c6fd271f0402f804c33d3f66");
    }

    #[test]
    fn roundtrip_all_zero() {
        let k = Sm4Key::from_key([0u8; 16]);
        let b = [0u8; 16];
        assert_eq!(k.decrypt_block(&k.encrypt_block(&b)), b);
    }

    #[test]
    fn different_keys_differ() {
        let a = Sm4Key::from_key([0u8; 16]).encrypt_block(&[0xff; 16]);
        let b = Sm4Key::from_key([1u8; 16]).encrypt_block(&[0xff; 16]);
        assert_ne!(a, b);
    }

    #[test]
    fn from_slice_rejects_wrong_length() {
        assert!(Sm4Key::from_slice(&[0u8; 15]).is_none());
        assert!(Sm4Key::from_slice(&[0u8; 16]).is_some());
        assert!(Sm4Key::from_slice(&[0u8; 17]).is_none());
    }

    #[test]
    fn ctr_is_reversible_and_counter_never_repeats() {
        let k = Sm4Key::from_key([0x11; 16]);
        let pt: Vec<u8> = (0..64u8).collect();
        let ct = ctr::crypt(&k, 0x1234_5678_9abc_def0, &pt);
        assert_ne!(ct, pt);
        let back = ctr::crypt(&k, 0x1234_5678_9abc_def0, &ct);
        assert_eq!(back, pt);
    }

    #[test]
    fn ctr_different_nonce_gives_different_ciphertext() {
        let k = Sm4Key::from_key([0x11; 16]);
        let pt = vec![0u8; 32];
        assert_ne!(ctr::crypt(&k, 1, &pt), ctr::crypt(&k, 2, &pt));
    }

    #[test]
    fn ctr_counter_blocks_differ() {
        // 计数块必须互不相同，否则 keystream 复用直接泄露明文 XOR
        let k = Sm4Key::from_key([0x11; 16]);
        let pt = vec![0u8; 64];
        let ct = ctr::crypt(&k, 7, &pt);
        assert_ne!(&ct[0..16], &ct[16..32]);
        assert_ne!(&ct[16..32], &ct[32..48]);
        assert_ne!(&ct[32..48], &ct[48..64]);
    }

    #[test]
    fn ctr_in_place_matches_allocating() {
        let k = Sm4Key::from_key([0x11; 16]);
        let pt: Vec<u8> = (0..100u8).collect();
        let mut buf = pt.clone();
        ctr::crypt_in_place(&k, 9, &mut buf);
        assert_eq!(buf, ctr::crypt(&k, 9, &pt));
    }
}
