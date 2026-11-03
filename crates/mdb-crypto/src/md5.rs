//! MD5（RFC 1321）。
//!
//! **仅用于 MongoDB 4.4 及更早版本 `MONGODB-CR` 认证算法的迁移兼容**。
//!
//! # 禁用范围
//!
//! MDB 默认不开启 `MONGODB-CR`。若启用（`security.enableMongoDBCR=true`，
//! 仅用于旧客户端强制升级前的过渡期，且仅监听 localhost），需：
//!
//! - 记录审计日志，等保要求"使用身份鉴别技术"且需可追溯
//! - 在 `serverStatus.security.authenticationMechanisms` 中标注
//! - 完成 `MONGODB-X509` 或 SCRAM 升级后立即关闭

use crate::digest::{Digest, Output};

const S: [u32; 64] = [
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9,
    14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15,
    21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
];

const T: [u32; 64] = [
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
    0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
    0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
    0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
    0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
];

/// MD5 增量状态。
#[derive(Debug, Clone)]
pub struct Md5 {
    state: [u32; 4],
    buffer: [u8; 64],
    buffered: usize,
    total_len: u64,
}

impl Default for Md5 {
    fn default() -> Self {
        Self::new()
    }
}

impl Md5 {
    fn compress(state: &mut [u32; 4], block: &[u8; 64]) {
        let mut m = [0u32; 16];
        for i in 0..16 {
            m[i] = u32::from_le_bytes([
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ]);
        }
        let [mut a, mut b, mut c, mut d] = *state;
        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | ((!b) & d), i),
                16..=31 => ((d & b) | ((!d) & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | (!d)), (7 * i) % 16),
            };
            let tmp = d;
            d = c;
            c = b;
            let sum = a
                .wrapping_add(f)
                .wrapping_add(T[i])
                .wrapping_add(m[g]);
            b = b.wrapping_add(sum.rotate_left(S[i]));
            a = tmp;
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
    }
}

impl Digest<16, 64> for Md5 {
    const NAME: &'static str = "MD5";

    fn new() -> Self {
        Self { state: [0x6745_2301, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476], buffer: [0u8; 64], buffered: 0, total_len: 0 }
    }

    fn update(&mut self, mut data: &[u8]) {
        self.total_len = self.total_len.wrapping_add(data.len() as u64);
        if self.buffered > 0 {
            let need = 64 - self.buffered;
            if data.len() < need {
                self.buffer[self.buffered..self.buffered + data.len()].copy_from_slice(data);
                self.buffered += data.len();
                return;
            }
            let (head, tail) = data.split_at(need);
            self.buffer[self.buffered..64].copy_from_slice(head);
            let block = self.buffer;
            Self::compress(&mut self.state, &block);
            self.buffered = 0;
            data = tail;
        }
        let mut chunks = data.chunks_exact(64);
        for c in &mut chunks {
            let mut block = [0u8; 64];
            block.copy_from_slice(c);
            Self::compress(&mut self.state, &block);
        }
        let rem = chunks.remainder();
        self.buffer[..rem.len()].copy_from_slice(rem);
        self.buffered = rem.len();
    }

    fn finish(mut self) -> Output<16> {
        let bit_len = self.total_len.wrapping_mul(8);
        self.update(&[0x80]);
        while self.buffered != 56 {
            self.update(&[0x00]);
        }
        self.update(&bit_len.to_le_bytes());
        let mut out = [0u8; 16];
        for (i, w) in self.state.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&w.to_le_bytes());
        }
        Output::new(out)
    }
}

/// SCRAM-SHA-1 的口令输入：`md5Hex(username + ":mongo:" + password)`。
///
/// MongoDB 的 SCRAM-SHA-1 凭证不是直接用明文口令派生，而是先用这个
/// digest（历史上为了与 MONGODB-CR 共用同一份口令材料）。官方驱动
/// （pymongo 等）在 SCRAM-SHA-1 分支里同样先算 digest，所以服务端必须
/// 一致，否则客户端 proof 永远对不上，表现为 "user not found"。
pub fn scram_sha1_password(username: &str, password: &str) -> String {
    use crate::digest::hex;
    hex(Md5::digest(format!("{username}:mongo:{password}").as_bytes()).as_bytes())
}

/// MONGODB-CR 旧认证：`<md5(md5Hex(username:mongo:db) + ':' + password)>`。
///
/// 返回小写十六进制串（32 字符），即 `authenticationKey` 之外的明文比较值。
///
/// 该值等价于密码，不可视为哈希后安全存储；MDB 内部仅在内存中比较。
pub fn mongodb_cr(username: &str, db: &str, password: &str) -> String {
    use crate::digest::hex;
    let inner = hex(Md5::digest(format!("{username}:mongo:{db}").as_bytes()).as_bytes());
    hex(Md5::digest(format!("{inner}:{password}").as_bytes()).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc1321_vectors() {
        assert_eq!(Md5::digest(b"").to_string(), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(Md5::digest(b"a").to_string(), "0cc175b9c0f1b6a831c399e269772661");
        assert_eq!(Md5::digest(b"abc").to_string(), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(
            Md5::digest(b"message digest").to_string(),
            "f96b697d7cb7938d525a2f31aaf161d0"
        );
        assert_eq!(
            Md5::digest(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
                .to_string(),
            "d174ab98d277d9f5a5611c2c9f419d9f"
        );
        assert_eq!(
            Md5::digest(b"12345678901234567890123456789012345678901234567890123456789012345678901234567890")
                .to_string(),
            "57edf4a22be3c955ac49da2e2107b67a"
        );
    }

    #[test]
    fn streaming_matches_oneshot() {
        let data: Vec<u8> = (0..130u32).map(|i| (i % 255) as u8).collect();
        let expect = Md5::digest(&data).to_string();
        for split in 0..=data.len() {
            let mut h = Md5::new();
            h.update(&data[..split]);
            h.update(&data[split..]);
            assert_eq!(h.finish().to_string(), expect, "split {split}");
        }
    }

    #[test]
    fn mongodb_cr_is_deterministic_and_case_sensitive() {
        let a = mongodb_cr("root", "admin", "secret");
        assert_eq!(a, mongodb_cr("root", "admin", "secret"));
        assert_ne!(a, mongodb_cr("root", "admin", "Secret"));
        assert_eq!(a.len(), 32);
    }
}
