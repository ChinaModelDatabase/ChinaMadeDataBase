//! SHA-1（RFC 3174）。
//!
//! **仅用于 SCRAM-SHA-1**。SCRAM-SHA-1 的用途是兼容 MongoDB 4.0 及更早
//! 版本创建的账号、以及 Java 驱动默认协商到的机制。
//!
//! # 安全声明
//!
//! SHA-1 已被 NIST 认定为不安全，**禁止**用于任何签名、证书、完整性校验。
//! MDB 中 SHA-1 的唯一用途是 PBKDF2 内部的 HMAC-SHA-1（SCRAM-SHA-1 强制要求），
//! 其安全性不依赖 SHA-1 的抗碰撞性而依赖其预像抗性。
//!
//! MongoDB 已废弃 SCRAM-SHA-1，MDB 默认仅开启 SCRAM-SHA-256；
//! 保留此实现仅为迁移期旧账号兼容，且需 `security.enableSha1SCRAM` 显式开启。

use crate::digest::{Digest, Output};

/// SHA-1 增量状态。
#[derive(Debug, Clone)]
pub struct Sha1 {
    state: [u32; 5],
    buffer: [u8; 64],
    buffered: usize,
    total_len: u64,
}

impl Default for Sha1 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha1 {
    fn compress(state: &mut [u32; 5], block: &[u8; 64]) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let [mut a, mut b, mut c, mut d, mut e] = *state;
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5a82_7999u32),
                20..=39 => (b ^ c ^ d, 0x6ed9_eba1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8f1b_bcdc),
                _ => (b ^ c ^ d, 0xca62_c1d6),
            };
            let tmp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = tmp;
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
    }
}

impl Digest<20, 64> for Sha1 {
    const NAME: &'static str = "SHA1";

    fn new() -> Self {
        Self {
            state: [0x6745_2301, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476, 0xc3d2_e1f0],
            buffer: [0u8; 64],
            buffered: 0,
            total_len: 0,
        }
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

    fn finish(mut self) -> Output<20> {
        let bit_len = self.total_len.wrapping_mul(8);
        self.update(&[0x80]);
        while self.buffered != 56 {
            self.update(&[0x00]);
        }
        self.update(&bit_len.to_be_bytes());
        let mut out = [0u8; 20];
        for (i, w) in self.state.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&w.to_be_bytes());
        }
        Output::new(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::unhex;

    #[test]
    fn rfc3174_vectors() {
        assert_eq!(Sha1::digest(b"").to_string(), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(Sha1::digest(b"abc").to_string(), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(
            Sha1::digest(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq").to_string(),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
    }

    #[test]
    fn million_a_vector() {
        let mut h = Sha1::new();
        for _ in 0..1_000_000 {
            h.update(b"a");
        }
        assert_eq!(h.finish().to_string(), "34aa973cd4c4daa4f61eeb2bdbad27316534016f");
    }

    #[test]
    fn streaming_matches_oneshot() {
        let data: Vec<u8> = (0..200u32).map(|i| (i % 253) as u8).collect();
        let expect = Sha1::digest(&data).to_string();
        for split in 0..=data.len() {
            let mut h = Sha1::new();
            h.update(&data[..split]);
            h.update(&data[split..]);
            assert_eq!(h.finish().to_string(), expect, "split {split}");
        }
        let _ = unhex("00").unwrap();
    }
}
