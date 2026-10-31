use std::cmp::Ordering;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type SequenceNumber = u64;
pub type ColumnFamilyId = u16;

pub const OP_PUT: u8 = 1;
pub const OP_DELETE: u8 = 2;
pub const TOMBSTONE_VALUE: &[u8] = b"";

pub const SST_MAGIC: u64 = 0x4D_44_42_53_54_4C_30_31;
pub const SST_FORMAT_VERSION: u32 = 1;
pub const SST_FORMAT_VERSION_V2: u32 = 2;
pub const FOOTER_SIZE: usize = 56;
pub const BLOCK_HEADER_SIZE: usize = 27;
pub const RESTART_INTERVAL: usize = 16;

pub fn invert_seqno(seqno: SequenceNumber) -> u64 {
    u64::MAX - seqno
}

pub fn real_seqno(inverted: u64) -> SequenceNumber {
    u64::MAX - inverted
}

/// 内部键布局：`escaped_user_key || 0x00 || inverted_seqno_be64`
///
/// # 转义方案
///
/// 用户键里的字节按下列规则改写，使**键体中永不出现 `0x00`**：
///
/// | 原始字节 | 转义后           |
/// |----------|------------------|
/// | `0x00`   | `0x01 0x01`      |
/// | `0x01`   | `0x01 0x02`      |
/// | `0x02..=0xFE` | 原样        |
/// | `0xFF`   | 原样             |
///
/// 该方案同时满足两个硬性要求：
///
/// 1. **定界无歧义**：键体不含 `0x00`，所以 `0x00` 只可能是定界符。
/// 2. **保序且前缀安全**：转义是前缀码，因此 `esc(a)` 是 `esc(b)` 的前缀
///    当且仅当 `a` 是 `b` 的前缀；且 `0x00 < 0x01 <= 所有键体字节`，
///    所以"用户键是另一用户键前缀"时短键一定排在长键之前。
///    这正是 `range_scan` / 前缀扫描所依赖的顺序。
///
/// 不要改用 `0x00 -> 0x00 0xFF`：那样键体仍以 `0x00` 开头，
/// 定界符无法与转义序列区分，且当后继字节为 `0x00` 时前缀顺序会被 seqno 翻转。
///
/// 这里刻意**不存长度前缀**：长度前缀位于键首会让 `BTreeMap` 先按长度排序，
/// `range_scan("a", "b")` 命中不到长度为 2.. 的键，前缀扫描会整片漏数据。
/// 没有长度前缀时字节序即用户键字典序，`BTreeMap<Vec<u8>, _>` 的裸字节序
/// 就等于期望顺序，memtable 无需自定义比较器。
///
/// 排序语义：用户键之间按字典序；同一用户键内按 inverted seqno 升序
/// （即 seqno 降序，新版本在前）。
pub fn escape_user_key(user_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + 4);
    for &b in user_key {
        match b {
            0x00 => out.extend_from_slice(&[0x01, 0x01]),
            0x01 => out.extend_from_slice(&[0x01, 0x02]),
            other => out.push(other),
        }
    }
    out
}

pub fn make_internal_key(user_key: &[u8], seqno: SequenceNumber) -> Vec<u8> {
    let escaped = escape_user_key(user_key);
    let mut buf = Vec::with_capacity(escaped.len() + 1 + 8);
    buf.extend_from_slice(&escaped);
    buf.push(0x00);
    buf.extend_from_slice(&invert_seqno(seqno).to_be_bytes());
    buf
}

/// 内部键的最小长度：1 字节定界 + 8 字节 seqno。
pub const INTERNAL_KEY_MIN_LEN: usize = 9;

/// 定位定界 `0x00`：由尾部 seqno 的固定宽度确定，位置为 `len - 9`。
fn find_delimiter(ik: &[u8]) -> Option<usize> {
    let d = ik.len().checked_sub(INTERNAL_KEY_MIN_LEN)?;
    (ik.get(d) == Some(&0x00)).then_some(d)
}

/// 解析内部键，返回**未解转义**的用户键视图。
///
/// 含 `0x00` / `0x01` 的键需要解转义，请改用 [`parse_internal_key_owned`]。
pub fn parse_internal_key(ik: &[u8]) -> Option<(&[u8], SequenceNumber)> {
    if ik.len() < INTERNAL_KEY_MIN_LEN {
        return None;
    }
    let d = find_delimiter(ik)?;
    let inv = u64::from_be_bytes(ik.get(d + 1..d + 9)?.try_into().ok()?);
    Some((&ik[..d], real_seqno(inv)))
}

/// 把转义序列还原并追加到 `out`。
fn unescape_into(escaped: &[u8], out: &mut Vec<u8>) {
    let mut i = 0;
    while i < escaped.len() {
        if escaped[i] == 0x01 && i + 1 < escaped.len() && escaped[i + 1] <= 0x02 {
            out.push(escaped[i + 1] - 0x01);
            i += 2;
        } else {
            out.push(escaped[i]);
            i += 1;
        }
    }
}

/// 返回用户键的拥有副本（总是正确解转义）。
pub fn user_key_of_owned(ik: &[u8]) -> Vec<u8> {
    let Some(d) = find_delimiter(ik) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(d);
    unescape_into(&ik[..d], &mut out);
    out
}

/// 与 `parse_internal_key` 相同，但总是解转义，返回拥有副本。
pub fn parse_internal_key_owned(ik: &[u8]) -> Option<(Vec<u8>, SequenceNumber)> {
    if ik.len() < INTERNAL_KEY_MIN_LEN {
        return None;
    }
    let d = find_delimiter(ik)?;
    let inv = u64::from_be_bytes(ik.get(d + 1..d + 9)?.try_into().ok()?);
    let mut out = Vec::with_capacity(d);
    unescape_into(&ik[..d], &mut out);
    Some((out, real_seqno(inv)))
}

/// 用户键视图（**已转义**，未解转义）。仅供内部排序/二分使用。
///
/// 要拿到真实用户键字节，请用 [`user_key_of_owned`] 或 [`parse_internal_key_owned`]。
pub fn user_key_of(ik: &[u8]) -> &[u8] {
    let Some(d) = find_delimiter(ik) else {
        return &[];
    };
    &ik[..d]
}

/// 把**原始**（未转义）用户键与内部键 `ik` 里的用户键比较。
///
/// 内部键里存的是转义后的键，直接和原始键比大小会出错，
/// 因此所有"拿外部原始键去和 SST/memtable 里的键比"的地方都必须走这里。
/// 转义是保序的，所以比较转义后的两侧与比较原始键等价。
pub fn cmp_ik_user_key(ik: &[u8], raw_user_key: &[u8]) -> Ordering {
    let escaped = escape_user_key(raw_user_key);
    user_key_of(ik).cmp(&escaped)
}

/// [`cmp_ik_user_key`] 的相等版本。
pub fn ik_user_key_eq(ik: &[u8], raw_user_key: &[u8]) -> bool {
    cmp_ik_user_key(ik, raw_user_key) == Ordering::Equal
}

/// 用户键是否落在 `[start, end)` 区间内（原始键语义）。
pub fn ik_user_key_in_range(ik: &[u8], start: &[u8], end: &[u8]) -> bool {
    cmp_ik_user_key(ik, start) != Ordering::Less && cmp_ik_user_key(ik, end) == Ordering::Less
}

/// 用户键是否落在 `[start, +inf)` 区间内（原始键语义）。
pub fn ik_user_key_ge(ik: &[u8], start: &[u8]) -> bool {
    cmp_ik_user_key(ik, start) != Ordering::Less
}

pub fn seqno_of(ik: &[u8]) -> SequenceNumber {
    let Some(d) = find_delimiter(ik) else {
        return 0;
    };
    let Some(bytes) = ik.get(d + 1..d + 9) else {
        return 0;
    };
    let arr: [u8; 8] = bytes.try_into().unwrap_or([0; 8]);
    real_seqno(u64::from_be_bytes(arr))
}

/// 内部键总长度：转义后的用户键 + 1 字节定界 + 8 字节 seqno。
///
/// `user_key_len` 传原始用户键长度，返回值是**上界**：
/// 每个 `0x00` / `0x01` 会多占 1 字节。用于预估容量时够用，
/// 需要精确值时直接用 `make_internal_key(..).len()`。
pub fn internal_key_len(user_key_len: usize) -> usize {
    user_key_len + 1 + 8
}

pub fn compare_internal_keys(a: &[u8], b: &[u8]) -> Ordering {
    let ka = user_key_of(a);
    let kb = user_key_of(b);
    match ka.cmp(kb) {
        Ordering::Equal => {
            let sa = seqno_of(a);
            let sb = seqno_of(b);
            invert_seqno(sa).cmp(&invert_seqno(sb))
        }
        ord => ord,
    }
}

pub fn put_uvarint(buf: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        buf.push((x as u8) | 0x80);
        x >>= 7;
    }
    buf.push(x as u8);
}

pub fn read_uvarint(data: &[u8]) -> Option<(u64, usize)> {
    let mut x: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &b) in data.iter().enumerate() {
        if b < 0x80 {
            return Some((x | (b as u64) << shift, i + 1));
        }
        x |= (b as u64 & 0x7f) << shift;
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

pub fn encode_collection_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len() * 3);
    for b in name.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' => out.push(b as char),
            b => {
                out.push('_');
                out.push_str(&format!("{:02x}", b));
            }
        }
    }
    out
}

pub fn decode_collection_name(encoded: &str) -> Option<String> {
    let mut out = String::new();
    let mut chars = encoded.chars();
    while let Some(c) = chars.next() {
        if c == '_' {
            let h1 = chars.next()?;
            let h2 = chars.next()?;
            let hex = format!("{h1}{h2}");
            let byte = u8::from_str_radix(&hex, 16).ok()?;
            out.push(byte as char);
        } else {
            out.push(c);
        }
    }
    Some(out)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub seqno: SequenceNumber,
}

impl Snapshot {
    pub fn new(seqno: SequenceNumber) -> Self {
        Self { seqno }
    }
}

#[derive(Debug, Clone)]
pub struct SstFileMeta {
    pub file_number: u64,
    pub seqno_min: SequenceNumber,
    pub seqno_max: SequenceNumber,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub level: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uvarint_roundtrip() {
        for val in [0u64, 1, 127, 128, 255, 256, 16383, 16384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            put_uvarint(&mut buf, val);
            let (decoded, consumed) = read_uvarint(&buf).unwrap();
            assert_eq!(decoded, val, "uvarint roundtrip failed for {val}");
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn internal_key_roundtrip() {
        let user_key = b"hello world";
        let seqno = 42u64;
        let ik = make_internal_key(user_key, seqno);
        let (uk, sn) = parse_internal_key(&ik).unwrap();
        assert_eq!(uk, user_key);
        assert_eq!(sn, seqno);
    }

    #[test]
    fn internal_key_ordering() {
        let ik1 = make_internal_key(b"aaa", 10);
        let ik2 = make_internal_key(b"aaa", 5);
        let ik3 = make_internal_key(b"bbb", 10);
        assert!(compare_internal_keys(&ik1, &ik2) == Ordering::Less, "newer seqno should sort first (lower inverted value)");
        assert!(compare_internal_keys(&ik1, &ik3) == Ordering::Less, "aaa < bbb");
        assert!(compare_internal_keys(&ik3, &ik1) == Ordering::Greater);
    }

    #[test]
    fn tombstone_internal_key() {
        let ik = make_internal_key(b"del", 99);
        let (_, sn) = parse_internal_key(&ik).unwrap();
        assert_eq!(sn, 99);
    }

    #[test]
    fn collection_name_roundtrip() {
        for name in &["test", "my.collection", "col$name", "a/b", "hello.world$test"] {
            let enc = encode_collection_name(name);
            let dec = decode_collection_name(&enc).unwrap();
            assert_eq!(&dec, name, "roundtrip failed for {name}");
        }
    }

    #[test]
    fn collection_name_special_chars() {
        let enc = encode_collection_name("a.b$c");
        assert!(!enc.contains('.'));
        assert!(!enc.contains('$'));
        let dec = decode_collection_name(&enc).unwrap();
        assert_eq!(dec, "a.b$c");
    }
}
