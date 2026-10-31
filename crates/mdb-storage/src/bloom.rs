use std::f64::consts::LN_2;

use mdb_crypto::crc32c;

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: u32,
    k: u32,
}

impl BloomFilter {
    pub fn new(expected_items: u32, bits_per_key: u32) -> Self {
        let total_bits = (expected_items as f64 * bits_per_key as f64).ceil() as u32;
        let num_bits = total_bits.max(64);
        let k = ((bits_per_key as f64 * LN_2).ceil() as u32).max(1);
        let num_words = (num_bits + 63) / 64;
        Self {
            bits: vec![0u64; num_words as usize],
            num_bits,
            k,
        }
    }

    fn positions(&self, key: &[u8]) -> (u32, u32) {
        let h1 = crc32c::crc32c(key);
        let h2 = (crc32c::crc32c(&h1.to_be_bytes())).rotate_left(15);
        (h1, h2)
    }

    pub fn add(&mut self, key: &[u8]) {
        let (h1, h2) = self.positions(key);
        for i in 0..self.k {
            let pos = (h1.wrapping_add(h2.wrapping_mul(i))) % self.num_bits;
            let word = pos / 64;
            let bit = pos % 64;
            self.bits[word as usize] |= 1u64 << bit;
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.positions(key);
        for i in 0..self.k {
            let pos = (h1.wrapping_add(h2.wrapping_mul(i))) % self.num_bits;
            let word = pos / 64;
            let bit = pos % 64;
            if self.bits[word as usize] & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }

    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|&w| w == 0)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.num_bits.to_le_bytes());
        buf.extend_from_slice(&self.k.to_le_bytes());
        for w in &self.bits {
            buf.extend_from_slice(&w.to_le_bytes());
        }
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let num_bits = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let k = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        let num_words = (num_bits + 63) / 64;
        let expected = 8 + num_words as usize * 8;
        if data.len() < expected {
            return None;
        }
        let mut bits = Vec::with_capacity(num_words as usize);
        for i in 0..num_words as usize {
            let off = 8 + i * 8;
            bits.push(u64::from_le_bytes([
                data[off], data[off + 1], data[off + 2], data[off + 3],
                data[off + 4], data[off + 5], data[off + 6], data[off + 7],
            ]));
        }
        Some(Self { bits, num_bits, k })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn false_positive_rate() {
        let mut bloom = BloomFilter::new(1000, 10);
        for i in 0..1000u32 {
            let key = i.to_le_bytes();
            bloom.add(&key);
        }
        let mut false_positives = 0u32;
        for i in 1000..2000u32 {
            let key = i.to_le_bytes();
            if bloom.contains(&key) {
                false_positives += 1;
            }
        }
        let fp_rate = false_positives as f64 / 1000.0;
        assert!(fp_rate < 0.02, "false positive rate too high: {fp_rate}");
    }

    #[test]
    fn no_false_negatives() {
        let mut bloom = BloomFilter::new(100, 10);
        for i in 0..100u32 {
            let key = format!("key_{i}");
            bloom.add(key.as_bytes());
        }
        for i in 0..100u32 {
            let key = format!("key_{i}");
            assert!(bloom.contains(key.as_bytes()), "false negative for key_{i}");
        }
    }

    #[test]
    fn serialize_roundtrip() {
        let mut bloom = BloomFilter::new(50, 10);
        for i in 0..50u32 {
            let key = i.to_le_bytes();
            bloom.add(&key);
        }
        let data = bloom.serialize();
        let restored = BloomFilter::deserialize(&data).unwrap();
        for i in 0..50u32 {
            let key = i.to_le_bytes();
            assert!(restored.contains(&key));
        }
        let key = 999u32.to_le_bytes();
        assert!(!restored.contains(&key));
    }

    #[test]
    fn empty_filter() {
        let bloom = BloomFilter::new(100, 10);
        assert!(!bloom.contains(b"anything"));
        assert!(bloom.is_empty());
    }

    #[test]
    fn optimal_k_calculation() {
        let bloom = BloomFilter::new(1000, 10);
        assert_eq!(bloom.k, 7, "optimal k for 10 bits/key should be 7");
    }
}
