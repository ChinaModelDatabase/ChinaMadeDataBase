//! BSON ObjectId（`0x07`）。
//!
//! 12 字节布局（BSON spec）：
//!
//! ```text
//!  0        4        8        12
//!  +--------+--------+--------+
//!  | 4 byte 时间戳 | 5 byte 随机值 | 3 字节计数器 |
//!  +--------+--------+--------+
//! ```
//!
//! 前 4 字节是 Unix 秒（big-endian），保证 ObjectId 大体按生成时间有序，
//! 因此可直接用于分页排序与索引区间扫描。

use std::fmt;

/// 12 字节的 BSON ObjectId。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId([u8; 12]);

impl ObjectId {
    /// ObjectId 固定字节数。
    pub const LEN: usize = 12;

    /// 由 12 个原始字节构造。
    ///
    /// 按值语义接受数组而非 `&[u8]`：长度是类型系统层面的不变量，
    /// 这样就不必在每个调用点做长度检查。
    pub const fn from_bytes(b: [u8; 12]) -> Self {
        Self(b)
    }

    /// 由任意长度的切片构造，长度必须为 12。
    pub fn from_slice(b: &[u8]) -> Option<Self> {
        let arr: [u8; 12] = b.try_into().ok()?;
        Some(Self(arr))
    }

    /// 返回底层 12 字节。
    pub const fn bytes(&self) -> &[u8; 12] {
        &self.0
    }

    /// 小写十六进制表示（24 字符）。
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(24);
        for b in self.0 {
            s.push(char::from_digit((b >> 4) as u32, 16).expect("nibble is < 16"));
            s.push(char::from_digit((b & 0x0f) as u32, 16).expect("nibble is < 16"));
        }
        s
    }

    /// 解析 24 字符十六进制字符串。
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 24 {
            return None;
        }
        let mut out = [0u8; 12];
        let bytes = s.as_bytes();
        for (i, slot) in out.iter_mut().enumerate() {
            let hi = hex_val(bytes[i * 2])?;
            let lo = hex_val(bytes[i * 2 + 1])?;
            *slot = (hi << 4) | lo;
        }
        Some(Self(out))
    }

    /// 生成新的 ObjectId：4 字节秒级时间戳 + 5 字节进程随机值 + 3 字节计数器。
    ///
    /// 5 字节进程随机值保证不同进程/不同次启动不会碰撞；3 字节计数器在
    /// 同一秒内保证唯一。
    ///
    /// 计数器在**秒边界重置**。这一点很关键：ObjectId 的字节序比较只有在
    /// 同一秒内计数器严格递增时才是全序；跨秒不重置会让同一秒内的 id 与
    /// 下一秒初的 id 大小关系随机，破坏"按 `_id` 排序 == 按插入时间排序"
    /// 这一核心假设。
    ///
    /// 计数器用满 24 位时返回 `None`——必须换秒而不是回绕，回绕会导致
    /// 同秒内重复主键。
    pub fn new() -> Option<Self> {
        let machine = process_random();
        let next = next_counter(u32::try_from(unix_secs()).ok()?)?;
        let secs = (next >> 32) as u32;
        let ctr = next as u32;
        Some(Self::from_bytes([
            (secs >> 24) as u8,
            (secs >> 16) as u8,
            (secs >> 8) as u8,
            secs as u8,
            machine[0],
            machine[1],
            machine[2],
            machine[3],
            machine[4],
            (ctr >> 16) as u8,
            (ctr >> 8) as u8,
            ctr as u8,
        ]))
    }

    /// 前 4 字节解释为 Unix 秒。
    pub fn timestamp_secs(&self) -> u32 {
        u32::from_be_bytes([self.0[0], self.0[1], self.0[2], self.0[3]])
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl From<[u8; 12]> for ObjectId {
    fn from(b: [u8; 12]) -> Self {
        Self(b)
    }
}

impl TryFrom<&[u8]> for ObjectId {
    type Error = mdb_common::MdbError;

    fn try_from(b: &[u8]) -> Result<Self, Self::Error> {
        Self::from_slice(b).ok_or_else(|| {
            mdb_common::MdbError::new(
                mdb_common::ErrorCode::FailedToParse,
                format!("ObjectId requires 12 bytes, got {}", b.len()),
            )
        })
    }
}

impl TryFrom<&str> for ObjectId {
    type Error = mdb_common::MdbError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::from_hex(s).ok_or_else(|| {
            mdb_common::MdbError::new(
                mdb_common::ErrorCode::FailedToParse,
                format!("invalid ObjectId hex: {s:?}"),
            )
        })
    }
}

use std::sync::atomic::{AtomicU64, Ordering};

/// 24 位计数器的最大值：超过即本秒内用尽。
const MAX_COUNTER: u32 = 0x00FF_FFFF;

/// 进程内共享的 `(秒, 计数器)`，打包成高 32 位秒 / 低 32 位计数。
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 取下一个 `(秒, 计数器)`，返回高 32 位秒、低 32 位计数。
///
/// # 为什么必须全进程共享
///
/// 机器段已经是进程级常量了，计数器若按线程各一份，两个线程在同一秒内会
/// 取到相同的 `(秒, 计数)`，产出**完全相同**的 ObjectId。之前的
/// `thread_local!` 计数器只是被"每线程不同的机器段"意外掩盖掉了。
///
/// # 时钟回拨
///
/// `secs` 取 `max(系统时钟, 上次使用的秒)`。NTP 校时或运维改表都可能让
/// `SystemTime` 倒退；若直接用系统时钟，秒数变小会让计数器重置为 0，从而
/// 复用曾经用过的 `(秒, 计数)` 组合。取最大值就退化成"沿用上一秒并接着数"，
/// 代价是回拨期间 ObjectId 内嵌的时间戳略有超前，但绝不会重复。
///
/// 24 位计数耗尽时返回 `None`，此时调用方应拒绝生成而不是回绕——回绕会产生
/// 重复主键。要用到 1.67e7 次/秒才会发生。
fn next_counter(secs: u32) -> Option<u64> {
    let mut cur = COUNTER.load(Ordering::Relaxed);
    loop {
        let last_secs = (cur >> 32) as u32;
        let secs = secs.max(last_secs);
        // 换秒则从 0 开始；同一秒内则必须严格 +1
        let ctr = if secs == last_secs {
            let next = (cur as u32).checked_add(1)?;
            if next > MAX_COUNTER {
                return None;
            }
            next
        } else {
            0
        };
        let next = ((secs as u64) << 32) | ctr as u64;
        // 打包位与解包位必须一致，且失败时必须基于最新值重算，
        // 不能拿旧的 `next` 反复重试
        match COUNTER.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return Some(next),
            Err(actual) => cur = actual,
        }
    }
}

/// 5 字节机器标识，**整个进程生命周期内恒定**。
///
/// 必须用 `OnceLock` 而不是 `thread_local!`：ObjectId 布局中第 5..10 字节的
/// 语义是"进程/主机标识"，若按线程各生成一份，同一进程在不同线程上产出的
/// ObjectId 会带着不同的机器段，等于谎报了自己的来源。
///
/// 用 `OnceLock` 也不用 `static mut`，因此不需要 `unsafe`。
static PROCESS_RANDOM: std::sync::OnceLock<[u8; 5]> = std::sync::OnceLock::new();

fn process_random() -> [u8; 5] {
    use std::collections::hash_map::RandomState;
    use std::hash::BuildHasher;

    *PROCESS_RANDOM.get_or_init(|| {
        // ObjectId 的机器段只要求"跨进程不碰撞"，不要求不可预测，
        // 因此直接用 std 的 RandomState 派生，无需内核 CSPRNG，也不必
        // 让 mdb-doc 依赖 mdb-crypto。
        //
        // `hash_one` 每次调用会新建一个带随机种子的 `RandomState`，
        // 种子不保证跨进程一致，但**进程内**足够唯一——而机器段本来
        // 就只要求"跨进程不碰撞"，同一进程内只会派生一次。
        let mut fresh = [0u8; 5];
        fresh.copy_from_slice(&RandomState::new().hash_one("mdb.oid.machine").to_le_bytes()[..5]);
        // 播种值全 0 会被误判为"尚未播种"，这里换个不冲突的组合
        if fresh == [0; 5] {
            fresh[0] = 1;
        }
        fresh
    })
}

/// 让 `PROCESS_RANDOM` 首次访问时完成播种。
///
/// 仅测试使用：生产路径的 [`ObjectId::new`] 会自行触发惰性播种。
#[cfg(test)]
fn ensure_seeded() {
    let _ = process_random();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let b = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67,
        ];
        let oid = ObjectId::from_bytes(b);
        assert_eq!(oid.to_hex(), "0123456789abcdef01234567");
        assert_eq!(ObjectId::from_hex("0123456789abcdef01234567"), Some(oid));
    }

    #[test]
    fn rejects_bad_hex() {
        assert!(ObjectId::from_hex("").is_none());
        assert!(ObjectId::from_hex("0123456789abcdef0123456").is_none());
        assert!(ObjectId::from_hex("0123456789abcdef0123456g").is_none());
        assert!(ObjectId::from_slice(&[0u8; 11]).is_none());
    }

    #[test]
    fn new_is_unique_and_time_ordered() {
        ensure_seeded();
        let a = ObjectId::new().unwrap();
        let b = ObjectId::new().unwrap();
        assert_ne!(a, b, "ObjectId must not repeat");
        assert!(a < b, "ObjectId must be monotonic within a process");
        assert!(a.timestamp_secs() > 1_600_000_000, "timestamp must be plausible");
    }

    #[test]
    fn machine_segment_is_the_same_on_every_thread() {
        ensure_seeded();
        // 第 5..10 字节是"进程/主机标识"，必须全进程一致。
        // 之前它挂在 thread_local! 上，于是同一进程的不同线程产出的 ObjectId
        // 带着不同的机器段，格式语义直接失真。
        let main = ObjectId::new().unwrap();
        let other = std::thread::spawn(|| {
            (0..8).map(|_| ObjectId::new().unwrap()).collect::<Vec<_>>()
        })
        .join()
        .unwrap();

        for oid in other {
            assert_eq!(
                &oid.bytes()[5..10],
                &main.bytes()[5..10],
                "machine segment must not depend on the generating thread"
            );
        }
    }

    #[test]
    fn distinct_threads_produce_distinct_ids() {
        ensure_seeded();
        // 机器段相同，区分靠计数器与随机起始值；不能因此撞号。
        let a: Vec<ObjectId> =
            std::thread::spawn(|| (0..500).map(|_| ObjectId::new().unwrap()).collect()).join().unwrap();
        let b: Vec<ObjectId> =
            std::thread::spawn(|| (0..500).map(|_| ObjectId::new().unwrap()).collect()).join().unwrap();
        let mut all: Vec<ObjectId> = a.into_iter().chain(b).collect();
        let before = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), before, "ObjectIds generated on two threads collided");
    }
}
