//! CSPRNG。
//!
//! # 信创要求
//!
//! 等保 2.0 三级 8.1.4.1 要求"应采用密码技术保证重要数据的传输过程中的
//! 机密性"。MDB 的 nonce / IV / 会话令牌 / ObjectId 计数器种子全部来自本模块。
//!
//! # 实现策略与能力边界
//!
//! 本模块**没有**实现 `getrandom(2)` 系统调用。在 `forbid(unsafe_code)` 与
//! "零第三方依赖"两条约束下无法发出系统调用（`getrandom` 与 Windows
//! `BCryptGenRandom` 都需要 FFI，即 `unsafe`），因此实际只有两级：
//!
//! 1. `/dev/urandom` —— Linux/统信/麒麟/欧拉等，标准做法是内核 CSPRNG。
//! 2. [`fill_from_drbg`] —— 上者不可用时的降级路径。
//!
//! # 熵源分级：nonce/salt 与密钥材料的要求不同
//!
//! 这两类用途的安全需求不同，不应一刀切：
//!
//! | 用途 | 需求 | 降级 DRBG 是否可接受 |
//! |------|------|---------------------|
//! | ObjectId 随机段、缓存键 | 抗碰撞 | 可 |
//! | SCRAM nonce / salt | 不可预测（阻止重放、预计算） | 可* |
//! | DEK / KEK / TDE 数据密钥 | 密码学强度、不可穷举 | **不可** |
//!
//! \* 降级 DRBG 的种子来自 `std` 的 `RandomState`，在 Linux/Windows/macOS
//! 主流实现上确实由操作系统 CSPRNG 播种（128 bit 量级）。对 24 字节 nonce
//! 与 16 字节 salt 而言这足够。**但**该保证是平台相关且未写入 `std` 文档的
//! 契约，因此对密钥材料一律从严。
//!
//! API 体现这一分级：
//! - [`fill`] —— 通用随机字节，调用方自行判断。
//! - [`fill_key_material`] —— 密钥材料；熵源降级时默认**拒绝**，需显式
//!   `allow_degraded` 才能继续。
//!
//! [`is_degraded`] 供启动自检与信创审计使用。
//!
//! 要在信创生产环境获得合规的密钥级随机数，应在构建时启用平台熵源后端
//! （引入受控的 `unsafe` FFI 隔离层），或放宽"零依赖"约束引入成熟库。
//! 本模块不打算用自研软件算法掩盖这一点。
//!
//! # 与 SM 计数器的关系
//!
//! SM4-CTR 需要**绝不复用**的 counter。MDB 为每个数据密钥分配一个
//! 32 bit counter 字段，保存在加密后的元数据中；counter 耗尽时必须轮换
//! 数据密钥（见 `mdb-server::tde`）。这是密钥管理中最容易出错的地方，
//! counter 重用会泄露明文 XOR 结构。

use std::io::Read;

/// 是否已确认高熵源可用。
static HAVE_KERNEL_ENTROPY: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// 是否运行在降级熵源（DRBG）上。
static DEGRADED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// 填充 `buf` 为随机字节。
///
/// 熵源降级时**照常填充**（见 [`RngQuality`]）。调用方若用于机密材料必须
/// 改用 [`fill_key_material`]。
pub fn fill(buf: &mut [u8]) {
    if fill_from_urandom(buf) {
        HAVE_KERNEL_ENTROPY.store(true, std::sync::atomic::Ordering::Relaxed);
        return;
    }
    // 无内核熵源：Windows 开发机、精简内核等。退到 DRBG 并标记降级。
    fill_from_drbg(buf);
    DEGRADED.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// 当前熵源质量。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RngQuality {
    /// 已确认从 `/dev/urandom` 取到内核熵，可用于密钥材料。
    Kernel,
    /// 降级为 `RandomState` 播种的 DRBG，**不可**用于密钥材料。
    Degraded,
    /// 尚未探测：还没有任何一次 `fill()` 走完过。
    ///
    /// 必须与 [`RngQuality::Degraded`] 区分开，因为它报告的是**另一种**
    /// 事实："还不知道"，而不是"已知降级"。
    ///
    /// 之前只有 `Kernel` / `Degraded` 两态，而 `quality()` 用
    /// `if is_degraded() { Degraded } else { Kernel }` 判定——在任何一次
    /// `fill()` 发生之前 `DEGRADED` 天然为 `false`，于是 Windows 这类**没有**
    /// `/dev/urandom` 的机器会立刻被报成 `Kernel`，`permits_key_material()`
    /// 随之返回 `true`。等于"从没验证过"被当成"验证过且通过"，
    /// 信创启动自检会据此放行密钥材料。
    Unknown,
}

impl RngQuality {
    /// 此质量下是否允许生成密钥材料。
    ///
    /// 只有**确认**拿到内核熵才放行；`Unknown` 与 `Degraded` 都不放行。
    pub const fn permits_key_material(self) -> bool {
        matches!(self, RngQuality::Kernel)
    }
}

/// 当前熵源质量。
///
/// 只有在**确实观测到**一次成功的 `/dev/urandom` 读取后才返回 [`RngQuality::Kernel`]。
pub fn quality() -> RngQuality {
    use std::sync::atomic::Ordering::Relaxed;
    if DEGRADED.load(Relaxed) {
        // 降级是粘性的：一旦发生就永久记录，不因后续偶然成功而改口
        RngQuality::Degraded
    } else if HAVE_KERNEL_ENTROPY.load(Relaxed) {
        RngQuality::Kernel
    } else {
        RngQuality::Unknown
    }
}

/// 填充密钥材料。
///
/// 与 [`fill`] 的区别：当运行在降级熵源上时默认**返回错误并拒绝**，
/// 避免"降级状态被无声忽略"这类事故。要在开发/测试环境继续，必须显式
/// 传 `allow_degraded = true`，责任由调用方承担。
pub fn fill_key_material(buf: &mut [u8], allow_degraded: bool) -> Result<(), RngError> {
    fill(buf);
    if is_degraded() && !allow_degraded {
        // 不泄露：退回全零由调用方处理，但错误必须可见。
        buf.fill(0);
        return Err(RngError::DegradedForKeyMaterial);
    }
    Ok(())
}

/// 熵源相关错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RngError {
    /// 熵源降级，拒绝生成密钥材料。
    DegradedForKeyMaterial,
}

impl core::fmt::Display for RngError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RngError::DegradedForKeyMaterial => f.write_str(
                "refusing to generate key material: no kernel entropy source available \
                 (RandomState-seeded DRBG is not a CSPRNG); pass allow_degraded=true only \
                 for non-production use",
            ),
        }
    }
}

impl std::error::Error for RngError {}

/// 当前是否具备可信高熵源。
pub fn has_kernel_entropy() -> bool {
    HAVE_KERNEL_ENTROPY.load(std::sync::atomic::Ordering::Relaxed)
}

/// 当前是否运行在降级熵源上（应记审计告警）。
pub fn is_degraded() -> bool {
    DEGRADED.load(std::sync::atomic::Ordering::Relaxed)
}

/// 是否**已观测到**至少一次成功的 `/dev/urandom` 读取。
///
/// 与 [`is_degraded`] 分开，因为两者回答的是不同问题：本函数为 `true` 表示
/// "确认有内核熵源"，而 `is_degraded()` 为 `false` 只表示"还没降级过"，
/// 在未探测时同样是 `false`。
pub fn have_kernel_entropy() -> bool {
    HAVE_KERNEL_ENTROPY.load(std::sync::atomic::Ordering::Relaxed)
}

/// 生成 `[u8; N]` 随机数组。
pub fn bytes<const N: usize>() -> [u8; N] {
    let mut b = [0u8; N];
    fill(&mut b);
    b
}

/// 生成 `u64` 随机数。
pub fn u64_value() -> u64 {
    u64::from_le_bytes(bytes::<8>())
}

/// 生成 `u32` 随机数。
pub fn u32_value() -> u32 {
    u32::from_le_bytes(bytes::<4>())
}

/// 生成 `[f64; N]` 均匀分布随机数（`[0, 1)`）。
pub fn uniform<const N: usize>() -> [f64; N] {
    let mut out = [0.0f64; N];
    for slot in &mut out {
        // 取 53 bit 保证 f64 精度
        let bits = u64_value() >> 11;
        *slot = bits as f64 / (1u64 << 53) as f64;
    }
    out
}

// ---------------------------------------------------------------------------
// 熵源实现
// ---------------------------------------------------------------------------

fn fill_from_urandom(buf: &mut [u8]) -> bool {
    if buf.is_empty() {
        return true;
    }
    match std::fs::File::open("/dev/urandom") {
        Ok(mut f) => f.read_exact(buf).is_ok(),
        Err(_) => false,
    }
}

/// 降级 DRBG：`SHA-256(state || counter)` 计数器模式。
///
/// # 定位
///
/// 这是一个**降级**方案，不是 CSPRNG，也不试图成为 CSPRNG。它存在的唯一
/// 目的是让无内核熵源的环境（Windows 开发机）不至于直接 panic，从而可以
/// 跑测试。它由 [`fill_key_material`] 强制拦截在密钥材料之外。
///
/// # 种子材料（务必如实理解）
///
/// 4 个 `RandomState` 实例 + 单调时间。`RandomState::new()` 的键来自 `std`
/// 线程局部初始化，**后续实例是对同一组键递增**，因此 4 个实例不是 4 份
/// 独立的 64 bit OS 熵。有效熵上界 = `std` 初始键熵（约 128 bit），且随
/// 平台而异。注释中"256 bit 种子"的说法已移除。
///
/// # 状态推进
///
/// 每次调用先快照 `state`，输出 `SHA-256(state||0..n)`，再把状态更新为
/// `SHA-256(state||n)`。因此同一状态下每个 counter 只用一次，跨调用不
/// 重放。（这不是"前向保密"——状态与输出是确定性关系，此处不作此声称。）
fn fill_from_drbg(buf: &mut [u8]) {
    use crate::digest::Digest;
    use std::cell::Cell;
    use std::collections::hash_map::RandomState;
    use std::hash::BuildHasher;

    if buf.is_empty() {
        return;
    }

    thread_local! {
        static STATE: Cell<[u8; 32]> = const { Cell::new([0u8; 32]) };
        static SEEDED: Cell<bool> = const { Cell::new(false) };
    }

    STATE.with(|cell| {
        let mut state = cell.get();

        SEEDED.with(|s| {
            if !s.get() {
                let mut material = Vec::with_capacity(64);
                for i in 0..4u64 {
                    // `hash_one` 每次调用独立播种 `RandomState`，因此
                    // 四个块彼此不同；这正是这里要的熵来源。
                    material.extend_from_slice(&RandomState::new().hash_one(i).to_le_bytes());
                }
                // 混入单调时间以降低跨进程重播风险
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                material.extend_from_slice(&now.to_le_bytes());
                state = crate::sha256::Sha256::digest(&material).0;
                cell.set(state);
                s.set(true);
            }
        });

        let snapshot = state;
        let mut counter: u64 = 0;
        let mut written = 0;
        while written < buf.len() {
            let mut input = [0u8; 40];
            input[..32].copy_from_slice(&snapshot);
            input[32..].copy_from_slice(&counter.to_le_bytes());
            let block = crate::sha256::Sha256::digest(&input);
            let n = block.0.len().min(buf.len() - written);
            buf[written..written + n].copy_from_slice(&block.0[..n]);
            written += n;
            counter = counter.wrapping_add(1);
        }
        // 推进状态，避免下次调用复用同一 (state, counter) 组合
        let mut advance = [0u8; 40];
        advance[..32].copy_from_slice(&snapshot);
        advance[32..].copy_from_slice(&counter.to_le_bytes());
        cell.set(crate::sha256::Sha256::digest(&advance).0);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_distinct_values() {
        use std::collections::HashSet;
        let mut seen: HashSet<[u8; 16]> = HashSet::new();
        for _ in 0..1000 {
            assert!(seen.insert(bytes::<16>()), "16-byte random collided");
        }
    }

    #[test]
    fn outputs_are_not_all_zero() {
        // 若熵源失效，fill 会返回全零；此测试守护该失效模式
        assert!(bytes::<32>().iter().any(|&b| b != 0));
    }

    #[test]
    fn key_material_refuses_degraded_source_unless_overridden() {
        let mut buf = [0xffu8; 32];
        match quality() {
            RngQuality::Kernel => {
                assert!(fill_key_material(&mut buf, false).is_ok());
                assert!(buf.iter().any(|&b| b != 0xff), "buffer must be filled");
            }
            RngQuality::Degraded | RngQuality::Unknown => {
                // 未确认内核熵：默认必须拒绝，且不得留下未初始化内容
                assert!(
                    matches!(fill_key_material(&mut buf, false), Err(RngError::DegradedForKeyMaterial)),
                    "must refuse key material without confirmed kernel entropy"
                );
                assert!(buf.iter().all(|&b| b == 0), "rejected buffer must be zeroed");
                // 显式放行后可用，但调用方已承担风险
                assert!(fill_key_material(&mut buf, true).is_ok());
                assert!(buf.iter().any(|&b| b != 0));
            }
        }
    }

    #[test]
    fn kernel_quality_requires_observed_urandom() {
        // 测试进程内其它用例可能已经调用过 fill()，因此这里只断言不变量。
        //
        // 注意是**单向**的：`Kernel` => 有观测记录且未降级。反向不成立，
        // 因为某台机器可能先成功读过 urandom、之后又失败，此时
        // `have_kernel_entropy()` 为 true 而 `quality()` 已是 `Degraded`
        // （降级是粘性的，不会因为后来偶然成功就改口）。
        if quality() == RngQuality::Kernel {
            assert!(have_kernel_entropy(), "Kernel must never be reported without an observed read");
            assert!(!is_degraded());
        }
        // 无论当前处于哪种状态，未确认就不放行密钥材料
        if !have_kernel_entropy() || is_degraded() {
            assert!(!quality().permits_key_material());
        }
    }

    #[test]
    fn zero_length_fill_is_a_noop() {
        // `Read::read_exact` 对空缓冲区本就返回 Ok(())，因此这不是修复过的
        // 缺陷，而是把"空请求"的行为钉死：既不应被当作无熵源，也不应 panic。
        assert!(fill_from_urandom(&mut []));
        let mut empty: [u8; 0] = [];
        fill(&mut empty);
        assert_eq!(bytes::<0>().len(), 0);
    }

    #[test]
    fn entropy_looks_uniform() {
        // 单次取 256 字节，重复取值 168/256 完全正常（生日碰撞）。
        // 均匀性检查必须用足够样本量，否则只是在测生日悖论。
        // 这里取 65536 字节做卡方直觉检查：任一字节值出现次数
        // 应接近 256，允许 6 sigma 偏差。
        let n = 65536usize;
        let s = bytes::<65536>();
        let mut counts = [0u32; 256];
        for &b in &s[..n] {
            counts[b as usize] += 1;
        }
        let expected = (n / 256) as f32;
        let sigma = (expected * (1.0 - 1.0 / 256.0)).sqrt();
        for (v, c) in counts.iter().enumerate() {
            let dev = (*c as f32 - expected).abs();
            assert!(
                dev < 6.0 * sigma,
                "byte {v:#04x} count {} deviates {:.1} from expected {expected:.0}",
                c,
                dev
            );
        }
    }

    #[test]
    fn drbg_output_does_not_repeat_across_calls() {
        // 状态推进错误会导致计数器复用
        let a = bytes::<32>();
        let b = bytes::<32>();
        assert_ne!(a, b);
    }

    #[test]
    fn uniform_in_range() {
        for v in uniform::<64>() {
            assert!((0.0..1.0).contains(&v), "uniform out of range: {v}");
        }
    }

    #[test]
    fn fill_respects_buffer_len() {
        for n in [0usize, 1, 7, 16, 33, 1024] {
            let mut b = vec![0u8; n];
            fill(&mut b);
            assert_eq!(b.len(), n);
        }
    }
}
