//! Base64（RFC 4648 标准字母表 + URL-safe 变体）。
//!
//! SCRAM 的 `s=`/`p=`/`v=` 字段全部为 Base64，SCRAM **必须用标准字母表**
//! （带 `+` `/`），使用 URL-safe 会导致认证失败。
//!
//! 引擎同时用 Base64 编码 wire 协议中的 `X.509` 证书字段，此处用 URL-safe。

use mdb_common::{ErrorCode, MdbError, MdbResult};

const STD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// 解码错误。
fn bad(msg: &'static str) -> MdbError {
    MdbError::new(ErrorCode::FailedToParse, msg)
}

fn encode_with(alphabet: &[u8; 64], pad: bool, data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    let mut chunks = data.chunks_exact(3);
    for c in &mut chunks {
        let n = ((c[0] as u32) << 16) | ((c[1] as u32) << 8) | c[2] as u32;
        out.push(alphabet[(n >> 18) as usize & 63] as char);
        out.push(alphabet[(n >> 12) as usize & 63] as char);
        out.push(alphabet[(n >> 6) as usize & 63] as char);
        out.push(alphabet[n as usize & 63] as char);
    }
    match chunks.remainder() {
        [a] => {
            let n = (*a as u32) << 16;
            out.push(alphabet[(n >> 18) as usize & 63] as char);
            out.push(alphabet[(n >> 12) as usize & 63] as char);
            if pad {
                out.push('=');
                out.push('=');
            }
        }
        [a, b] => {
            let n = ((*a as u32) << 16) | ((*b as u32) << 8);
            out.push(alphabet[(n >> 18) as usize & 63] as char);
            out.push(alphabet[(n >> 12) as usize & 63] as char);
            out.push(alphabet[(n >> 6) as usize & 63] as char);
            if pad {
                out.push('=');
            }
        }
        _ => {}
    }
    out
}

/// 标准 Base64（含 `=` 填充）。
pub fn encode(data: &[u8]) -> String {
    encode_with(STD, true, data)
}

/// 标准 Base64，无填充。
pub fn encode_nopad(data: &[u8]) -> String {
    encode_with(STD, false, data)
}

/// URL-safe Base64（含填充）。
pub fn encode_url(data: &[u8]) -> String {
    encode_with(URL, true, data)
}

/// URL-safe Base64，无填充。
pub fn encode_url_nopad(data: &[u8]) -> String {
    encode_with(URL, false, data)
}

fn decode_value(c: u8, url: bool) -> Option<u32> {
    match c {
        b'A'..=b'Z' => Some((c - b'A') as u32),
        b'a'..=b'z' => Some((c - b'a') as u32 + 26),
        b'0'..=b'9' => Some((c - b'0') as u32 + 52),
        b'+' if !url => Some(62),
        b'/' if !url => Some(63),
        b'-' if url => Some(62),
        b'_' if url => Some(63),
        _ => None,
    }
}

/// 解码标准或 URL-safe Base64，自动忽略空白与尾部 `=`。
pub fn decode(s: &str) -> MdbResult<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    for &c in s.as_bytes() {
        if c == b'=' {
            break;
        }
        // 忽略空白：SCRAM 解析后常有换行；X.509 PEM 更依赖此行为
        if c.is_ascii_whitespace() {
            continue;
        }
        let v = decode_value(c, false)
            .or_else(|| decode_value(c, true))
            .ok_or_else(|| bad("invalid base64 character"))?;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc4648_test_vectors() {
        // RFC 4648 §10
        let cases = [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ];
        for (plain, encoded) in cases {
            assert_eq!(encode(plain.as_bytes()), encoded, "encode {plain:?}");
            assert_eq!(decode(encoded).unwrap(), plain.as_bytes(), "decode {encoded:?}");
        }
    }

    #[test]
    fn nopad_forms() {
        assert_eq!(encode_nopad(b"f"), "Zg");
        assert_eq!(encode_nopad(b"fo"), "Zm8");
        assert_eq!(encode_nopad(b"foo"), "Zm9v");
        // 解码必须同时接受有/无填充
        assert_eq!(decode("Zg").unwrap(), b"f");
        assert_eq!(decode("Zg==").unwrap(), b"f");
    }

    #[test]
    fn url_safe_differs_from_standard() {
        // 字节 0xFB 0xFF 在标准字母表中产生 '+' 与 '/'
        let data = [0xfbu8, 0xff, 0xbf];
        assert!(encode(&data).contains('+'));
        assert!(encode(&data).contains('/'));
        assert!(encode_url(&data).contains('-'));
        assert!(encode_url(&data).contains('_'));
        // 两者解码结果必须相同
        assert_eq!(decode(&encode(&data)).unwrap(), data.to_vec());
        assert_eq!(decode(&encode_url(&data)).unwrap(), data.to_vec());
    }

    #[test]
    fn whitespace_and_newlines_ignored() {
        assert_eq!(decode("Zm9v\nYmFy").unwrap(), b"foobar");
        assert_eq!(decode("  Zm9v  ").unwrap(), b"foo");
    }

    #[test]
    fn invalid_char_rejected() {
        assert!(decode("!!!!").is_err());
    }

    #[test]
    fn all_byte_values_roundtrip() {
        let all: Vec<u8> = (0..=255u8).collect();
        assert_eq!(decode(&encode(&all)).unwrap(), all);
    }

    #[test]
    fn scram_style_salt_is_16_bytes() {
        // SCRAM 规范：salt 长度 16，base64 后为 24 字符带填充
        let salt = [0x5au8; 16];
        let s = encode(&salt);
        assert_eq!(s.len(), 24);
        assert_eq!(decode(&s).unwrap(), salt.to_vec());
    }
}
