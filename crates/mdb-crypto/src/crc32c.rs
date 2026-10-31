//! CRC32C（Castagnoli）校验和。
//!
//! # 为什么放在 `mdb-crypto` 而不是调用方
//!
//! CRC32C 有**两个**消费者，自研一份即可，避免两处实现漂移：
//!
//! | 消费者 | 用途 |
//! |--------|------|
//! | [`mdb_wire`] `OP_MSG` | `checksumPresent` 标志位的消息尾校验和 |
//! | `mdb-storage` | SSTable 块完整性（`docs/02` §8.2） |
//!
//! [`mdb_wire`]: https://github.com/mongodb/specifications
//!
//! # 为什么不用 CRC32/CRC64
//!
//! 1. MongoDB 自身的 `OP_MSG` 校验和即为 CRC32C，行为可比，便于差分测试。
//! 2. 解除 `forbid(unsafe_code)` 后可直通 SSE4.2 `crc32` 指令，无需改磁盘格式。
//!
//! # 注意：本算法不是密码算法
//!
//! CRC32C 只能发现**偶发位翻转**，不能发现恶意篡改（无密钥，可被构造）。
//! **不得**用它替代 AEAD 标签或 MAC。`docs/02` §8.2 中"CRC32C + 重放计数"
//! 的表述指存储层对 CTR 模式的完整性保护，与密评所需的认证加密是**两件事**，
//! 不能互相替代。
//!
//! # 认证
//!
//! Castagnoli 标准检查值（多项式 `0x1EDC6F41`，反射形式 `0x82F63B78`）：

/// 反射形式多项式（Castagnoli）。
///
/// 标准写法 `0x1EDC6F41`，位反射后为 `0x82F63B78`。
const POLY_REFLECTED: u32 = 0x82F6_3B78;

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLY_REFLECTED
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static TABLE: [u32; 256] = build_table();

const fn build_slice_table(byte_pos: usize) -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0u32;
    while i < 256 {
        let mut crc = 0u32;
        let mut byte_idx = 0usize;
        while byte_idx < 4 {
            let byte = if byte_idx == byte_pos { i } else { 0 };
            let idx = ((crc ^ byte) & 0xFF) as usize;
            crc = (crc >> 8) ^ TABLE[idx];
            byte_idx += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
}

static SLICE0: [u32; 256] = build_slice_table(0);
static SLICE1: [u32; 256] = build_slice_table(1);
static SLICE2: [u32; 256] = build_slice_table(2);
static SLICE3: [u32; 256] = build_slice_table(3);

/// 一次性 CRC32C。
pub fn crc32c(data: &[u8]) -> u32 {
    let mut c = Crc32c::new();
    c.update(data);
    c.finish()
}

/// 流式 CRC32C。
#[derive(Debug, Clone)]
pub struct Crc32c {
    state: u32,
}

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32c {
    pub const fn new() -> Self {
        Self {
            state: 0xFFFF_FFFF,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        let mut crc = self.state;
        let mut offset = 0usize;
        let len = data.len();

        while offset < len && (offset & 3) != 0 {
            let byte = data[offset];
            crc = (crc >> 8) ^ TABLE[((crc ^ byte as u32) & 0xFF) as usize];
            offset += 1;
        }

        while offset + 4 <= len {
            let word = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            crc = SLICE0[((crc ^ word) & 0xFF) as usize]
                ^ SLICE1[(((crc >> 8) ^ (word >> 8)) & 0xFF) as usize]
                ^ SLICE2[(((crc >> 16) ^ (word >> 16)) & 0xFF) as usize]
                ^ SLICE3[(((crc >> 24) ^ (word >> 24)) & 0xFF) as usize];
            offset += 4;
        }

        while offset < len {
            let byte = data[offset];
            crc = (crc >> 8) ^ TABLE[((crc ^ byte as u32) & 0xFF) as usize];
            offset += 1;
        }

        self.state = crc;
    }

    pub fn finish(&self) -> u32 {
        self.state ^ 0xFFFF_FFFF
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_castagnoli_check_values() {
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"a"), 0xC1D0_4330);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn single_bit_flip_is_detected() {
        let mut data = *b"the quick brown fox";
        let base = crc32c(&data);
        for i in 0..data.len() {
            data[i] ^= 0x01;
            assert_ne!(crc32c(&data), base, "bit flip at {i} went undetected");
            data[i] ^= 0x01;
        }
        assert_eq!(crc32c(&data), base, "restore did not return to the original");
    }

    #[test]
    fn streaming_equals_one_shot() {
        let data: Vec<u8> = (0u8..=255).cycle().take(1000).collect();
        let one_shot = crc32c(&data);
        for split in [0, 1, 7, 64, 255, 512, 999, 1000] {
            let mut c = Crc32c::new();
            c.update(&data[..split]);
            c.update(&data[split..]);
            assert_eq!(c.finish(), one_shot, "split at {split}");
        }
    }

    #[test]
    fn appending_changes_the_sum() {
        let base = crc32c(b"payload");
        assert_ne!(crc32c(b"payload\x00"), base);
        assert_ne!(crc32c(b"payloadextra"), base);
    }
}
