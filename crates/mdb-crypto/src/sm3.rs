//! SM3 密码杂凑（GM/T 0004-2012）。
//!
//! 输出 256 bit，与 SHA-256 等长。用于国密体系下的会话签名、TLS 指纹、
//! 数据完整性校验。
//!
//! # 与 SHA-256 的关系
//!
//! 结构同为 Merkle–Damgård + Davies–Meyer，但**常数不同且不可互换**。
//! 国密合规场景禁止使用 SHA 系列替代 SM3，反之亦然。

use crate::digest::{Digest, Output};

/// SM3 初始值 IV（GM/T 0004-2012 §4.1）。
///
/// # 注意：第三个字是 `172442d7`
///
/// 网上流传的 `17242d7d` 是**错的**（多个二级来源抄错）。OSCCA 发布的
/// GM/T 0004-2012 原文与 gmbz.org.cn 均为 `172442d7`。填错会导致所有
/// 摘要值不符官方附录 A 向量，但不会有任何编译/运行期告警。
const SM3_IV: [u32; 8] = [
    0x7380_166f, 0x4914_b2b9, 0x1724_42d7, 0xda8a_0600, 0xa96f_30bc, 0x1631_38aa, 0xe38d_ee4d,
    0xb0fb_0e4e,
];

const T_FIRST: u32 = 0x79cc_4519; // 0 <= j <= 15
const T_SECOND: u32 = 0x7a87_9d8a; // 16 <= j <= 63

/// SM3 增量状态。
#[derive(Debug, Clone)]
pub struct Sm3 {
    state: [u32; 8],
    buffer: [u8; 64],
    buffered: usize,
    total_len: u64,
}

impl Default for Sm3 {
    fn default() -> Self {
        Self::new()
    }
}

#[inline(always)]
fn p0(x: u32) -> u32 {
    x ^ x.rotate_left(9) ^ x.rotate_left(17)
}

#[inline(always)]
fn p1(x: u32) -> u32 {
    x ^ x.rotate_left(15) ^ x.rotate_left(23)
}

impl Sm3 {
    fn compress(state: &mut [u32; 8], block: &[u8; 64]) {
        let mut w = [0u32; 68];
        let mut w1 = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ]);
        }
        for j in 16..68 {
            w[j] = p1(w[j - 16] ^ w[j - 9] ^ w[j - 3].rotate_left(15))
                ^ w[j - 13].rotate_left(7)
                ^ w[j - 6];
        }
        for j in 0..64 {
            w1[j] = w[j] ^ w[j + 4];
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *state;
        for j in 0..64 {
            let t = if j < 16 { T_FIRST } else { T_SECOND };
            // SS1 = ((A <<< 12) + E + (T_j <<< (j mod 32))) <<< 7
            let a12 = a.rotate_left(12);
            let ss1 = a12
                .wrapping_add(e)
                .wrapping_add(t.rotate_left((j % 32) as u32))
                .rotate_left(7);
            let ss2 = ss1 ^ a12;
            // FF_j(A,B,C)：0..15 为异或，16..63 为多数函数
            let ff = if j < 16 {
                a ^ b ^ c
            } else {
                (a & b) | (a & c) | (b & c)
            };
            // GG_j(E,F,G)：0..15 为异或，16..63 为**选择函数**
            // (E∧F) ∨ (~E ∧ G)，注意与 FF 的多数函数不同
            let gg = if j < 16 {
                e ^ f ^ g
            } else {
                (e & f) | (!e & g)
            };
            let tt1 = ff.wrapping_add(d).wrapping_add(ss2).wrapping_add(w1[j]);
            let tt2 = gg.wrapping_add(h).wrapping_add(ss1).wrapping_add(w[j]);
            d = c;
            c = b.rotate_left(9);
            b = a;
            a = tt1;
            h = g;
            g = f.rotate_left(19);
            f = e;
            e = p0(tt2);
        }
        state[0] ^= a;
        state[1] ^= b;
        state[2] ^= c;
        state[3] ^= d;
        state[4] ^= e;
        state[5] ^= f;
        state[6] ^= g;
        state[7] ^= h;
    }
}

impl Digest<32, 64> for Sm3 {
    const NAME: &'static str = "SM3";

    fn new() -> Self {
        Self { state: SM3_IV, buffer: [0u8; 64], buffered: 0, total_len: 0 }
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

    fn finish(mut self) -> Output<32> {
        let bit_len = self.total_len.wrapping_mul(8);
        self.update(&[0x80]);
        while self.buffered != 56 {
            self.update(&[0x00]);
        }
        self.update(&bit_len.to_be_bytes());
        let mut out = [0u8; 32];
        for (i, w) in self.state.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&w.to_be_bytes());
        }
        Output::new(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::hex;

    #[test]
    fn gm_t_0004_vectors() {
        // GM/T 0004-2012 附录 A 标准示例
        assert_eq!(
            hex(Sm3::digest(b"abc").as_bytes()),
            "66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0"
        );
        // 512 bit 'abcd' 重复 16 次
        let msg = b"abcd".repeat(16);
        assert_eq!(
            hex(Sm3::digest(&msg).as_bytes()),
            "debe9ff92275b8a138604889c18e5a4d6fdb70e5387e5765293dcba39c0c5732"
        );
    }

    #[test]
    fn empty_vector() {
        assert_eq!(
            hex(Sm3::digest(b"").as_bytes()),
            "1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b"
        );
    }

    #[test]
    fn streaming_matches_oneshot() {
        let data: Vec<u8> = (0..400u32).map(|i| (i % 247) as u8).collect();
        let expect = Sm3::digest(&data);
        for split in [0usize, 1, 55, 56, 63, 64, 65, 128, data.len()] {
            let mut h = Sm3::new();
            h.update(&data[..split]);
            h.update(&data[split..]);
            assert_eq!(h.finish(), expect, "split {split}");
        }
    }
}
