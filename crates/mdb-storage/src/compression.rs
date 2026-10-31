use mdb_common::MdbResult;
use mdb_crypto::crc32c;

use crate::error::*;

const COMPRESSED_HEADER_SIZE: usize = 8;

pub trait Compressor: Send + Sync {
    fn compress(&self, input: &[u8]) -> Vec<u8>;
    fn decompress(&self, input: &[u8], expected_size: usize) -> MdbResult<Vec<u8>>;
    fn name(&self) -> &str;
    fn ratio(&self) -> f64;
}

impl std::fmt::Debug for dyn Compressor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Compressor({})", self.name())
    }
}

pub fn wrap_compressed_block(compressed: Vec<u8>) -> Vec<u8> {
    let data_len = compressed.len() as u32;
    let checksum = crc32c::crc32c(&compressed);
    let mut out = Vec::with_capacity(COMPRESSED_HEADER_SIZE + compressed.len());
    out.extend_from_slice(&data_len.to_le_bytes());
    out.extend_from_slice(&checksum.to_be_bytes());
    out.extend_from_slice(&compressed);
    out
}

pub fn unwrap_compressed_block(raw: &[u8], expected_size: usize) -> MdbResult<Vec<u8>> {
    if raw.len() < COMPRESSED_HEADER_SIZE {
        return Err(corrupt_err("compressed block too small for header"));
    }
    let data_len = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let stored_crc = u32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]);
    let compressed = &raw[COMPRESSED_HEADER_SIZE..];
    if compressed.len() != data_len {
        return Err(corrupt_err(format!(
            "compressed block length mismatch: header says {data_len}, got {}",
            compressed.len()
        )));
    }
    let computed_crc = crc32c::crc32c(compressed);
    if stored_crc != computed_crc {
        return Err(corrupt_err(format!(
            "compressed block CRC: stored={:#x} computed={:#x}",
            stored_crc, computed_crc
        )));
    }
    let _ = expected_size;
    Ok(compressed.to_vec())
}

// ---------------------------------------------------------------------------
// NoCompression
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct NoCompression;
impl Compressor for NoCompression {
    fn compress(&self, input: &[u8]) -> Vec<u8> { input.to_vec() }
    fn decompress(&self, input: &[u8], _: usize) -> MdbResult<Vec<u8>> { Ok(input.to_vec()) }
    fn name(&self) -> &str { "none" }
    fn ratio(&self) -> f64 { 1.0 }
}

// ---------------------------------------------------------------------------
// LZ4-like block compression
//
// Two-pass: find matches, then emit.
// Every middle token: [token][ext_lit_len?][literals][offset_2b][ext_match_len?]
// Last token (if literals only): [token][ext_lit_len?][literals]
//   (decompressor breaks before reading offset when output is full)
// ---------------------------------------------------------------------------

const LZ4_MIN_MATCH: usize = 4;
const LZ4_MAX_MATCH: usize = LZ4_MIN_MATCH + 0x0FFF;

#[derive(Debug)]
pub struct Lz4Compression;

impl Compressor for Lz4Compression {
    fn compress(&self, input: &[u8]) -> Vec<u8> {
        if input.is_empty() { return Vec::new(); }

        // Pass 1: find all match positions using a simple backward scan
        // This is O(n*w) but correct and simple
        let mut ops: Vec<(usize, Option<(usize, usize)>)> = Vec::new(); // (position, Some((len, offset)))
        let mut pos = 0;

        while pos < input.len() {
            let mut best_len = 0usize;
            let mut best_offset = 0usize;

            if pos + LZ4_MIN_MATCH <= input.len() {
                let window_start = pos.saturating_sub(65535);
                let max_len = (input.len() - pos).min(LZ4_MAX_MATCH);
                for cand in window_start..pos {
                    if cand + max_len > input.len() { continue; }
                    let mut len = 0usize;
                    while len < max_len && input[cand + len] == input[pos + len] { len += 1; }
                    if len > best_len {
                        best_len = len;
                        best_offset = pos - cand;
                        if len >= LZ4_MAX_MATCH { break; }
                    }
                }
            }

            if best_len >= LZ4_MIN_MATCH {
                ops.push((pos, Some((best_len, best_offset))));
                pos += best_len;
            } else {
                // Start a literal run
                let lit_start = pos;
                pos += 1;
                // Extend literal run until we find a match
                while pos < input.len() {
                    if pos + LZ4_MIN_MATCH <= input.len() {
                        let window_start = pos.saturating_sub(65535);
                        let max_len = (input.len() - pos).min(LZ4_MAX_MATCH);
                        let mut found = false;
                        for cand in window_start..pos {
                            let mut len = 0usize;
                            while len < max_len && input[cand + len] == input[pos + len] { len += 1; }
                            if len >= LZ4_MIN_MATCH { found = true; break; }
                        }
                        if found { break; }
                    }
                    pos += 1;
                }
                ops.push((lit_start, None)); // None = literal run from lit_start to pos
            }
        }

        // Pass 2: emit compressed stream
        let mut output = Vec::with_capacity(input.len() * 3 / 4);
        let mut dp = 0usize;
        let mut idx = 0;

        while idx < ops.len() {
            let (start, ref m) = ops[idx];
            match m {
                Some((match_len, match_offset)) => {
                    let lit_count = start - dp;
                    let ll = lit_count.min(15);
                    let ml_adj = (*match_len - LZ4_MIN_MATCH) as u32;
                    let token: u8 = ((ll as u8) << 4) | (ml_adj.min(15) as u8 & 0x0F);
                    output.push(token);
                    Self::emit_literal_length(&mut output, lit_count as u32);
                    output.extend_from_slice(&input[dp..dp + lit_count]);
                    output.push((*match_offset & 0xFF) as u8);
                    output.push(((*match_offset >> 8) & 0xFF) as u8);
                    Self::emit_match_length(&mut output, *match_len as u32);
                    dp += lit_count + match_len;
                    idx += 1;
                }
                None => {
                    if idx + 1 < ops.len() {
                        // Merge with next match op
                        let (next_start, ref next_m) = ops[idx + 1];
                        if let Some((next_len, next_off)) = next_m {
                            let total_lit = next_start - start;
                            let ll = total_lit.min(15);
                            let ml_adj = (*next_len - LZ4_MIN_MATCH) as u32;
                            let token: u8 = ((ll as u8) << 4) | (ml_adj.min(15) as u8 & 0x0F);
                            output.push(token);
                            Self::emit_literal_length(&mut output, total_lit as u32);
                            output.extend_from_slice(&input[start..start + total_lit]);
                            output.push((*next_off & 0xFF) as u8);
                            output.push(((*next_off >> 8) & 0xFF) as u8);
                            Self::emit_match_length(&mut output, *next_len as u32);
                            dp = next_start + next_len;
                            idx += 2;
                        } else {
                            idx += 1;
                        }
                    } else {
                        // Last op: literals only
                        let lit_count = input.len() - start;
                        let ll = lit_count.min(15);
                        let token: u8 = (ll as u8) << 4;
                        output.push(token);
                        Self::emit_literal_length(&mut output, lit_count as u32);
                        output.extend_from_slice(&input[start..]);
                        dp = input.len();
                        idx += 1;
                    }
                }
            }
        }

        output
    }

    fn decompress(&self, input: &[u8], expected_size: usize) -> MdbResult<Vec<u8>> {
        let mut output = Vec::with_capacity(expected_size);
        let mut ip = 0;

        while ip < input.len() && output.len() < expected_size {
            let token = input[ip];
            ip += 1;

            let mut lit_len = ((token >> 4) & 0x0F) as usize;
            let mut match_len = (token & 0x0F) as usize + LZ4_MIN_MATCH;

            if lit_len == 15 {
                loop {
                    if ip >= input.len() { return Err(corrupt_err("lz4: truncated literal length")); }
                    let extra = input[ip] as usize;
                    ip += 1;
                    lit_len += extra;
                    if extra < 255 { break; }
                }
            }

            if ip + lit_len > input.len() {
                return Err(corrupt_err("lz4: truncated literals"));
            }
            output.extend_from_slice(&input[ip..ip + lit_len]);
            ip += lit_len;

            if output.len() >= expected_size { break; }

            // Read match offset
            if ip + 2 > input.len() {
                return Err(corrupt_err("lz4: truncated match offset"));
            }
            let offset = (input[ip] as usize) | ((input[ip + 1] as usize) << 8);
            ip += 2;

            if offset == 0 || offset > output.len() {
                return Err(corrupt_err(format!("lz4: bad match offset {offset}, output len {}", output.len())));
            }

            if (token & 0x0F) == 15 {
                loop {
                    if ip >= input.len() { return Err(corrupt_err("lz4: truncated match length")); }
                    let extra = input[ip] as usize;
                    ip += 1;
                    match_len += extra;
                    if extra < 255 { break; }
                }
            }

            let src = output.len() - offset;
            for i in 0..match_len {
                let b = output[src + (i % offset)];
                output.push(b);
            }
        }

        if output.len() != expected_size {
            return Err(corrupt_err(format!("lz4: size mismatch: expected {expected_size}, got {}", output.len())));
        }
        Ok(output)
    }

    fn name(&self) -> &str { "lz4" }
    fn ratio(&self) -> f64 { 0.50 }
}

impl Lz4Compression {
    fn emit_literal_length(output: &mut Vec<u8>, len: u32) {
        if len >= 15 {
            let mut rem = len - 15;
            loop {
                if rem < 255 { output.push(rem as u8); break; }
                output.push(255);
                rem -= 255;
            }
        }
    }
    fn emit_match_length(output: &mut Vec<u8>, len: u32) {
        let adj = len - LZ4_MIN_MATCH as u32;
        if adj >= 15 {
            let mut rem = adj - 15;
            loop {
                if rem < 255 { output.push(rem as u8); break; }
                output.push(255);
                rem -= 255;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SnappyLikeCompression
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct SnappyLikeCompression;

impl Compressor for SnappyLikeCompression {
    fn compress(&self, input: &[u8]) -> Vec<u8> {
        if input.is_empty() { return Vec::new(); }

        // Two-pass: find matches, then emit
        let mut ops: Vec<(usize, Option<(usize, usize)>)> = Vec::new();
        let mut pos = 0;

        while pos < input.len() {
            let mut best_len = 0usize;
            let mut best_offset = 0usize;

            if pos + 4 <= input.len() {
                let window_start = pos.saturating_sub(65535);
                let max_len = (input.len() - pos).min(64);
                for cand in window_start..pos {
                    let mut len = 0usize;
                    while len < max_len && input[cand + len] == input[pos + len] { len += 1; }
                    if len > best_len {
                        best_len = len;
                        best_offset = pos - cand;
                        if len >= 64 { break; }
                    }
                }
            }

            if best_len >= 4 && best_offset >= best_len {
                ops.push((pos, Some((best_len, best_offset))));
                pos += best_len;
            } else {
                let lit_start = pos;
                pos += 1;
                while pos < input.len() {
                    if pos + 4 <= input.len() {
                        let window_start = pos.saturating_sub(65535);
                        let max_len = (input.len() - pos).min(64);
                        let mut found = false;
                        for cand in window_start..pos {
                            let mut len = 0usize;
                            while len < max_len && input[cand + len] == input[pos + len] { len += 1; }
                            if len >= 4 && (pos - cand) >= len { found = true; break; }
                        }
                        if found { break; }
                    }
                    pos += 1;
                }
                ops.push((lit_start, None));
            }
        }

        let mut output = Vec::with_capacity(input.len() * 3 / 4);
        let mut dp = 0usize;
        let mut idx = 0;

        while idx < ops.len() {
            let (start, ref m) = ops[idx];
            match m {
                Some((match_len, match_offset)) => {
                    let lit_count = start - dp;
                    if lit_count > 0 {
                        Self::emit_literal_tag(&mut output, &input[dp..dp + lit_count]);
                    }
                    Self::emit_copy(&mut output, *match_len, *match_offset);
                    dp = start + match_len;
                    idx += 1;
                }
                None => {
                    if idx + 1 < ops.len() {
                        let (next_start, ref next_m) = ops[idx + 1];
                        if let Some((next_len, next_off)) = next_m {
                            let total_lit = next_start - start;
                            if total_lit > 0 {
                                Self::emit_literal_tag(&mut output, &input[start..start + total_lit]);
                            }
                            Self::emit_copy(&mut output, *next_len, *next_off);
                            dp = next_start + next_len;
                            idx += 2;
                        } else {
                            idx += 1;
                        }
                    } else {
                        let lit_count = input.len() - start;
                        if lit_count > 0 {
                            Self::emit_literal_tag(&mut output, &input[start..]);
                        }
                        dp = input.len();
                        idx += 1;
                    }
                }
            }
        }

        output
    }

    fn decompress(&self, input: &[u8], expected_size: usize) -> MdbResult<Vec<u8>> {
        let mut output = Vec::with_capacity(expected_size);
        let mut ip = 0;

        while ip < input.len() && output.len() < expected_size {
            let tag = input[ip];
            ip += 1;
            let tag_type = tag & 0x03;
            let mut length = ((tag >> 2) & 0x3F) as usize;

            match tag_type {
                0 => {
                    length += 1;
                    if length == 61 {
                        if ip >= input.len() { return Err(corrupt_err("snappy: truncated literal length")); }
                        length = input[ip] as usize + 61; ip += 1;
                    } else if length == 62 {
                        if ip + 1 > input.len() { return Err(corrupt_err("snappy: truncated literal length")); }
                        length = (input[ip] as usize) + ((input[ip + 1] as usize) << 8) + 61; ip += 2;
                    } else if length == 63 {
                        if ip + 2 > input.len() { return Err(corrupt_err("snappy: truncated literal length")); }
                        length = (input[ip] as usize) + ((input[ip + 1] as usize) << 8) + ((input[ip + 2] as usize) << 16) + 61; ip += 3;
                    }
                    if ip + length > input.len() { return Err(corrupt_err("snappy: truncated literal data")); }
                    output.extend_from_slice(&input[ip..ip + length]);
                    ip += length;
                }
                1 => {
                    length += 4;
                    if ip >= input.len() { return Err(corrupt_err("snappy: truncated 2-byte copy")); }
                    let offset = input[ip] as usize + 1; ip += 1;
                    if offset > output.len() { return Err(corrupt_err(format!("snappy: bad offset {offset}"))); }
                    let src = output.len() - offset;
                    for j in 0..length { output.push(output[src + (j % offset)]); }
                }
                2 => {
                    length += 4;
                    if ip + 1 > input.len() { return Err(corrupt_err("snappy: truncated 3-byte copy")); }
                    let offset = (input[ip] as usize) + ((input[ip + 1] as usize) << 8) + 1; ip += 2;
                    if offset > output.len() { return Err(corrupt_err(format!("snappy: bad offset {offset}"))); }
                    let src = output.len() - offset;
                    for j in 0..length { output.push(output[src + (j % offset)]); }
                }
                3 => {
                    length += 4;
                    if ip + 3 > input.len() { return Err(corrupt_err("snappy: truncated 4-byte copy")); }
                    let offset = (input[ip] as usize) + ((input[ip + 1] as usize) << 8) + ((input[ip + 2] as usize) << 16) + ((input[ip + 3] as usize) << 24) + 1; ip += 4;
                    if offset > output.len() { return Err(corrupt_err(format!("snappy: bad offset {offset}"))); }
                    let src = output.len() - offset;
                    for j in 0..length { output.push(output[src + (j % offset)]); }
                }
                _ => unreachable!(),
            }
        }

        if output.len() != expected_size {
            return Err(corrupt_err(format!("snappy: size mismatch: expected {expected_size}, got {}", output.len())));
        }
        Ok(output)
    }

    fn name(&self) -> &str { "snappy" }
    fn ratio(&self) -> f64 { 0.55 }
}

impl SnappyLikeCompression {
    fn emit_literal_tag(output: &mut Vec<u8>, lits: &[u8]) {
        let len_m1 = lits.len() - 1;
        if len_m1 < 60 {
            output.push((len_m1 as u8) << 2);
        } else if len_m1 <= 0xFF + 60 {
            output.push(60 << 2);
            output.push((len_m1 - 60) as u8);
        } else if len_m1 <= 0xFFFF + 60 {
            output.push(61 << 2);
            let n = len_m1 - 60;
            output.push((n & 0xFF) as u8);
            output.push(((n >> 8) & 0xFF) as u8);
        } else {
            output.push(62 << 2);
            let n = len_m1 - 60;
            output.push((n & 0xFF) as u8);
            output.push(((n >> 8) & 0xFF) as u8);
            output.push(((n >> 16) & 0xFF) as u8);
        }
        output.extend_from_slice(lits);
    }

    fn emit_copy(output: &mut Vec<u8>, match_len: usize, offset: usize) {
        let mlen = match_len - 4;
        if offset <= 256 {
            let tag: u8 = 0x01 | ((mlen.min(63) as u8) << 2);
            output.push(tag);
            if mlen >= 63 { output.push((mlen - 63) as u8); }
            output.push(((offset - 1) & 0xFF) as u8);
        } else if offset <= 65536 {
            let tag: u8 = 0x02 | ((mlen.min(63) as u8) << 2);
            output.push(tag);
            if mlen >= 63 { output.push((mlen - 63) as u8); }
            let o = offset - 1;
            output.push((o & 0xFF) as u8);
            output.push(((o >> 8) & 0xFF) as u8);
        } else {
            let tag: u8 = 0x03 | ((mlen.min(63) as u8) << 2);
            output.push(tag);
            if mlen >= 63 { output.push((mlen - 63) as u8); }
            let o = offset - 1;
            output.push((o & 0xFF) as u8);
            output.push(((o >> 8) & 0xFF) as u8);
            output.push(((o >> 16) & 0xFF) as u8);
            output.push(((o >> 24) & 0xFF) as u8);
        }
    }
}

// ---------------------------------------------------------------------------
// Zstd compression
// ---------------------------------------------------------------------------

/// Zstd 压缩器。
#[derive(Debug)]
pub struct ZstdCompression;

impl Compressor for ZstdCompression {
    fn compress(&self, data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }
        zstd::encode_all(data, 3).unwrap_or_else(|_| data.to_vec())
    }

    fn decompress(&self, data: &[u8], expected_size: usize) -> MdbResult<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        zstd::decode_all(data)
            .map_err(|e| crate::error::corrupt_err(format!("zstd decompress: {e}")))
            .and_then(|result| {
                if result.len() != expected_size && expected_size > 0 {
                    Err(crate::error::corrupt_err(format!(
                        "zstd size mismatch: expected {expected_size}, got {}",
                        result.len()
                    )))
                } else {
                    Ok(result)
                }
            })
    }

    fn name(&self) -> &str {
        "zstd"
    }

    fn ratio(&self) -> f64 {
        0.3 // zstd 典型压缩比
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm { None, Lz4, SnappyLike, Zstd }

impl CompressionAlgorithm {
    pub const fn as_str(self) -> &'static str {
        match self { Self::None => "none", Self::Lz4 => "lz4", Self::SnappyLike => "snappy", Self::Zstd => "zstd" }
    }
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" | "off" => Some(Self::None), "lz4" => Some(Self::Lz4),
            "snappy" | "snappylike" => Some(Self::SnappyLike),
            "zstd" => Some(Self::Zstd), _ => None,
        }
    }
    pub fn create_compressor(self) -> Box<dyn Compressor> {
        match self { Self::None => Box::new(NoCompression), Self::Lz4 => Box::new(Lz4Compression), Self::SnappyLike => Box::new(SnappyLikeCompression), Self::Zstd => Box::new(ZstdCompression) }
    }
}
impl Default for CompressionAlgorithm { fn default() -> Self { Self::Lz4 } }

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(comp: &dyn Compressor, data: &[u8]) {
        let compressed = comp.compress(data);
        let decompressed = comp.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data, "{} roundtrip failed for {} bytes (compressed={})", comp.name(), data.len(), compressed.len());
    }

    fn roundtrip_with_block_wrap(comp: &dyn Compressor, data: &[u8]) {
        let compressed = comp.compress(data);
        let wrapped = wrap_compressed_block(compressed);
        let unwrapped = unwrap_compressed_block(&wrapped, data.len()).unwrap();
        let decompressed = comp.decompress(&unwrapped, data.len()).unwrap();
        assert_eq!(decompressed, data, "{} block-wrap roundtrip failed", comp.name());
    }

    #[test] fn no_compression_roundtrip() { let c = NoCompression; roundtrip(&c, b"hello world"); roundtrip(&c, &[]); roundtrip(&c, &vec![0u8; 1000]); }
    #[test] fn lz4_empty() { roundtrip(&Lz4Compression, &[]); }
    #[test] fn lz4_no_match() { roundtrip(&Lz4Compression, b"abcdefghijklmnop"); }
    #[test] fn lz4_repeated_data() { let c = Lz4Compression; let d = b"abcabcabcabcabcabc"; roundtrip(&c, d); assert!(c.compress(d).len() < d.len()); }
    #[test] fn lz4_long_repeated() { let c = Lz4Compression; let d: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect(); roundtrip(&c, &d); assert!(c.compress(&d).len() < d.len()); }
    #[test] fn lz4_overlapping_match() { roundtrip(&Lz4Compression, b"abcabc"); }
    #[test]
    fn lz4_large_data() {
        let c = Lz4Compression;
        let mut d = Vec::new();
        for i in 0..10000 { let key = format!("key_{:06}", i); d.extend_from_slice(key.as_bytes()); }
        roundtrip(&c, &d);
        let ratio = c.compress(&d).len() as f64 / d.len() as f64;
        assert!(ratio < 0.90, "ratio={:.2}", ratio);
    }
    #[test] fn snappy_empty() { roundtrip(&SnappyLikeCompression, &[]); }
    #[test] fn snappy_roundtrip() { roundtrip(&SnappyLikeCompression, b"hello world"); roundtrip(&SnappyLikeCompression, &vec![0u8; 100]); }
    #[test]
    fn snappy_repeated() {
        let comp = SnappyLikeCompression;
        // Simple case with a match
        let data = b"abcabcabc";
        let compressed = comp.compress(data);
        let decompressed = comp.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data, "simple snappy roundtrip failed");

        // Original test case
        let data = b"the quick brown fox jumps over the lazy dog ";
        let compressed = comp.compress(data);
        let decompressed = comp.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data, "snappy roundtrip failed for {} bytes (compressed={})", data.len(), compressed.len());
    }
    #[test]
    fn snappy_large_data() {
        let mut d = Vec::new();
        for i in 0..1000 { let key = format!("key_{:06}", i); d.extend_from_slice(key.as_bytes()); }
        roundtrip(&SnappyLikeCompression, &d);
    }
    #[test] fn block_wrap_roundtrip() { roundtrip_with_block_wrap(&Lz4Compression, b"test data for block wrapping"); }
    #[test] fn block_wrap_corrupted_crc() { let c = Lz4Compression; let d = b"test data"; let mut w = wrap_compressed_block(c.compress(d)); let last = w.len()-1; w[last] ^= 0xFF; assert!(unwrap_compressed_block(&w, d.len()).is_err()); }
    #[test] fn compression_algorithm_factory() { assert_eq!(Lz4Compression.compress(b"test").len() > 0, true); }
    #[test] fn compression_algorithm_parse() { assert_eq!(CompressionAlgorithm::from_str("lz4"), Some(CompressionAlgorithm::Lz4)); assert_eq!(CompressionAlgorithm::from_str("none"), Some(CompressionAlgorithm::None)); assert_eq!(CompressionAlgorithm::from_str("unknown"), None); }
    #[test] fn compression_algorithm_parse_zstd() { assert_eq!(CompressionAlgorithm::from_str("zstd"), Some(CompressionAlgorithm::Zstd)); }
    #[test] fn zstd_empty() { roundtrip(&ZstdCompression, &[]); }
    #[test] fn zstd_roundtrip() { roundtrip(&ZstdCompression, b"hello world"); roundtrip(&ZstdCompression, &vec![0u8; 100]); }
    #[test]
    fn zstd_repeated_data() {
        let c = ZstdCompression;
        // 使用足够大的数据以确保压缩有效
        let d: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        roundtrip(&c, &d);
        assert!(c.compress(&d).len() < d.len());
    }
    #[test]
    fn zstd_large_data() {
        let c = ZstdCompression;
        let mut d = Vec::new();
        for i in 0..10000 { let key = format!("key_{:06}", i); d.extend_from_slice(key.as_bytes()); }
        roundtrip(&c, &d);
        let ratio = c.compress(&d).len() as f64 / d.len() as f64;
        assert!(ratio < 0.50, "zstd ratio should be < 0.50, got {:.2}", ratio);
    }
}
