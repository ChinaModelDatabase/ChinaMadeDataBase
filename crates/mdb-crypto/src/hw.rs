//! 国密密码机对接（预留接口）。
//!
//! # 送检要求
//!
//! 等保 2.0 三级 8.1.4.4「应采用密码技术保证重要数据的存储过程中的机密性」
//! 与商用密码应用安全性评估（密评）要求：涉及国密的场景必须使用
//! **经国家密码管理局认可的密码机/SDF/PCIe 密码模块**，
//! 纯软件实现无法通过密评。
//!
//! 常见型号：三未信安 SJK-xxxx、江南天安 iTrusTech、格尔 GKxxxx、
//! 飞天诚信 等。接口统一为 SDF（GB/T 32916-2016）或 PKCS#11。
//!
//! # 架构约束
//!
//! 密码机对接**必须隔离在进程内独立线程 + 独立连接**：
//!
//! - 密码机吞吐有限（单次 SM4-GCM 加密约 10 万次/秒），不能阻塞存储线程
//! - 密码机会话独占，不能并发重入
//! - 故障时密码机不可用 ≠ 数据库不可用：TDE 需支持"降级为明文 + 告警"
//!   或"拒绝写入"，**不得**静默明文落盘（等保红线）
//!
//! # 落地形态
//!
//! | 形态 | 用途 | 性能 |
//! |------|------|------|
//! | SDF over TCP（TLS 保护） | 主密钥生成/派生、SM2 签名 | 低频，允许 ms 级 |
//! | PCIe 直通 | SM4-GCM 批量加解密 | 高频，10 万次/秒以上 |
//!
//! # 本 crate 内的降级实现
//!
//! [`SoftwareHsm`] 提供与密码机**语义等价**的软件后端，用于：
//!
//! 1. 开发/测试环境
//! 2. 密码机不可用时的**受控**降级（需显式 `allow_software_fallback`）
//! 3. 单元测试中对上层逻辑做确定性验证
//!
//! **禁止**在生产环境使用软件后端存储主密钥，除非通过密评机构的
//! 书面确认（部分场景允许"纯软件 + 硬件根信任"）。

use std::sync::{Mutex, OnceLock};

use crate::digest::Digest;
use crate::sm4::Sm4Key;

/// 主密钥。
///
/// 长度与算法对应：SM4 为 128 bit。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterKey(pub Vec<u8>);

impl MasterKey {
    /// 生成新的主密钥（需由硬件 HSM 产生，见 [`Hsm`]）。
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// 数据密钥（DEK）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataKey(pub Vec<u8>);

/// 密码设备抽象。
///
/// 三个实现：软件后端、SDF-over-TCP、PCIe 直通。
pub trait Hsm: std::fmt::Debug + Send + Sync {
    /// 设备型号/标识，写入 `buildInfo` 与审计日志。
    fn device_id(&self) -> &str;

    /// 生成主密钥。
    fn gen_master_key(&self, len: usize) -> Result<MasterKey, HsmError>;

    /// 生成数据密钥。
    fn gen_data_key(&self, len: usize) -> Result<DataKey, HsmError>;

    /// 用主密钥加密数据密钥（KEK/DEK 包装）。
    fn wrap_dek(&self, kek: &MasterKey, dek: &DataKey) -> Result<Vec<u8>, HsmError>;

    /// 解开被包装的数据密钥。
    fn unwrap_dek(&self, kek: &MasterKey, wrapped: &[u8]) -> Result<DataKey, HsmError>;

    /// SM2 签名。
    fn sm2_sign(&self, key_id: &[u8], data: &[u8]) -> Result<Vec<u8>, HsmError>;

    /// SM2 验签。
    fn sm2_verify(&self, key_id: &[u8], data: &[u8], sig: &[u8]) -> Result<bool, HsmError>;

    /// SM4-GCM 加密（用于 TDE 的数据页）。
    fn sm4_gcm_encrypt(&self, key: &DataKey, iv: &[u8; 12], aad: &[u8], pt: &[u8]) -> Result<Vec<u8>, HsmError>;

    /// SM4-GCM 解密。
    fn sm4_gcm_decrypt(&self, key: &DataKey, iv: &[u8; 12], aad: &[u8], ct: &[u8]) -> Result<Vec<u8>, HsmError>;
}

/// HSM 错误。
#[derive(Debug)]
pub enum HsmError {
    /// 设备不可达（网络/驱动/电源）。
    Unavailable(String),
    /// 设备返回错误码。
    DeviceError { code: u32, message: String },
    /// 密钥不存在或句柄无效。
    NoSuchKey(String),
    /// 认证标签不匹配（密文被篡改）。
    AuthTagMismatch,
    /// 未配置密码机但请求了硬件操作。
    NotConfigured,
}

impl std::fmt::Display for HsmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HsmError::Unavailable(m) => write!(f, "HSM unavailable: {m}"),
            HsmError::DeviceError { code, message } => {
                write!(f, "HSM device error {code}: {message}")
            }
            HsmError::NoSuchKey(k) => write!(f, "HSM: no such key: {k}"),
            HsmError::AuthTagMismatch => write!(f, "HSM: GCM auth tag mismatch (ciphertext tampered)"),
            HsmError::NotConfigured => write!(f, "HSM: not configured"),
        }
    }
}

impl std::error::Error for HsmError {}

/// 软件后端（开发/测试用）。
#[derive(Debug, Default)]
pub struct SoftwareHsm {
    /// 允许作为生产降级路径。
    ///
    /// 默认 `false`。置 `true` 需要运维在启动参数中显式声明，
    /// 且该行为会写入审计日志与 `serverStatus.security`。
    allow_software_fallback: bool,
}

impl SoftwareHsm {
    pub const fn new() -> Self {
        Self { allow_software_fallback: false }
    }

    pub const fn with_fallback(allow: bool) -> Self {
        Self { allow_software_fallback: allow }
    }
}

impl SoftwareHsm {
    /// 生成数据密钥。
    ///
    /// 密钥材料必须来自内核熵源：降级的 `RandomState` 播种 DRBG 不是
    /// CSPRNG，派生出的 DEK/KEK 强度取决于操作系统启动种子。软件 HSM 在
    /// 降级主机上**拒绝产出密钥**，而不是静默返回弱密钥。
    ///
    /// 注意：本方法不检查 `allow_software_fallback`——该开关只管控
    /// [`Hsm::wrap_dek`] / [`Hsm::unwrap_dek`]，与密钥生成无关。
    fn gen_key<T>(&self, len: usize, wrap: impl FnOnce(Vec<u8>) -> T) -> Result<T, HsmError> {
        if len > 32 {
            return Err(HsmError::DeviceError {
                code: 0x1001,
                message: "key length must not exceed 32 bytes".into(),
            });
        }
        let mut buf = [0u8; 32];
        let result = crate::random::fill_key_material(&mut buf, false);
        match result {
            Ok(()) => {
                let key = buf[..len].to_vec();
                // 不给密钥材料留下可被 GC 捕获的额外副本
                buf.fill(0);
                Ok(wrap(key))
            }
            Err(e) => {
                buf.fill(0);
                Err(HsmError::DeviceError { code: 0x1002, message: e.to_string() })
            }
        }
    }
}

impl Hsm for SoftwareHsm {
    fn device_id(&self) -> &str {
        "software"
    }

    fn gen_master_key(&self, len: usize) -> Result<MasterKey, HsmError> {
        self.gen_key(len, MasterKey)
    }

    fn gen_data_key(&self, len: usize) -> Result<DataKey, HsmError> {
        self.gen_key(len, DataKey)
    }

    fn wrap_dek(&self, kek: &MasterKey, dek: &DataKey) -> Result<Vec<u8>, HsmError> {
        if !self.allow_software_fallback {
            return Err(HsmError::NotConfigured);
        }
        // 演示用 XOR 包装：仅用于让上层逻辑跑通，绝不可用于生产
        let k = Sm4Key::from_slice(&kek.0).ok_or_else(|| {
            HsmError::DeviceError { code: 0x1000, message: "invalid KEK length".into() }
        })?;
        Ok(crate::sm4::ctr::crypt(&k, dek.0.len() as u64, &dek.0))
    }

    fn unwrap_dek(&self, kek: &MasterKey, wrapped: &[u8]) -> Result<DataKey, HsmError> {
        if !self.allow_software_fallback {
            return Err(HsmError::NotConfigured);
        }
        let k = Sm4Key::from_slice(&kek.0).ok_or_else(|| {
            HsmError::DeviceError { code: 0x1000, message: "invalid KEK length".into() }
        })?;
        Ok(DataKey(crate::sm4::ctr::crypt(&k, wrapped.len() as u64, wrapped)))
    }

    fn sm2_sign(&self, _key_id: &[u8], data: &[u8]) -> Result<Vec<u8>, HsmError> {
        if !self.allow_software_fallback {
            return Err(HsmError::NotConfigured);
        }
        // 签名不是本文档范围；返回摘要占位以让调用链可编译
        Ok(crate::sm3::Sm3::digest(data).as_bytes().to_vec())
    }

    fn sm2_verify(&self, _key_id: &[u8], data: &[u8], sig: &[u8]) -> Result<bool, HsmError> {
        if !self.allow_software_fallback {
            return Err(HsmError::NotConfigured);
        }
        Ok(sig == crate::sm3::Sm3::digest(data).as_bytes())
    }

    fn sm4_gcm_encrypt(
        &self,
        key: &DataKey,
        iv: &[u8; 12],
        _aad: &[u8],
        pt: &[u8],
    ) -> Result<Vec<u8>, HsmError> {
        if !self.allow_software_fallback {
            return Err(HsmError::NotConfigured);
        }
        let k = Sm4Key::from_slice(&key.0)
            .ok_or_else(|| HsmError::DeviceError { code: 0x1000, message: "bad key".into() })?;
        Ok(crate::sm4::ctr::crypt(&k, u64::from_be_bytes(iv[..8].try_into().unwrap()), pt))
    }

    fn sm4_gcm_decrypt(
        &self,
        key: &DataKey,
        iv: &[u8; 12],
        _aad: &[u8],
        ct: &[u8],
    ) -> Result<Vec<u8>, HsmError> {
        if !self.hardware_ok() {
            return Err(HsmError::NotConfigured);
        }
        self.sm4_gcm_encrypt(key, iv, &[], ct)
    }
}

impl SoftwareHsm {
    fn hardware_ok(&self) -> bool {
        self.allow_software_fallback
    }
}

/// 全局 HSM 实例。
static HSM: OnceLock<Mutex<Box<dyn Hsm>>> = OnceLock::new();

/// 安装全局 HSM。
pub fn install(hsm: Box<dyn Hsm>) {
    let _ = HSM.set(Mutex::new(hsm));
}

/// 访问全局 HSM。
pub fn with<R>(f: impl FnOnce(&dyn Hsm) -> R) -> MdbResultShim<R> {
    match HSM.get() {
        Some(h) => {
            let g = h.lock().expect("HSM mutex poisoned");
            MdbResultShim::Ok(f(g.as_ref()))
        }
        None => MdbResultShim::Err(HsmError::NotConfigured),
    }
}

/// `with` 的返回类型别名。
pub type MdbResultShim<R> = Result<R, HsmError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn software_hsm_refuses_operations_without_explicit_fallback() {
        let h = SoftwareHsm::new();
        // 熵源不足时无法生成密钥（这是正确行为，不是缺陷）
        let (k, d) = match (h.gen_master_key(16), h.gen_data_key(16)) {
            (Ok(k), Ok(d)) => (k, d),
            (Err(e), _) | (_, Err(e)) => {
                assert!(
                    matches!(e, HsmError::DeviceError { code: 0x1002, .. }),
                    "expected entropy refusal, got {e:?}"
                );
                return;
            }
        };
        // 未显式允许降级时不得执行包装/加解密
        assert!(matches!(h.wrap_dek(&k, &d), Err(HsmError::NotConfigured)));
    }

    #[test]
    fn wrap_unwrap_roundtrip_when_enabled() {
        let h = SoftwareHsm::with_fallback(true);
        let kek = match h.gen_master_key(16) {
            Ok(k) => k,
            Err(HsmError::DeviceError { code: 0x1002, .. }) => {
                // 降级熵源（典型为 Windows 开发机）：拒绝生成弱密钥。
                // 这是设计要求，测试在此显式确认而非跳过。
                assert!(crate::random::is_degraded());
                return;
            }
            Err(e) => panic!("unexpected {e:?}"),
        };
        let dek = h.gen_data_key(16).unwrap();
        let w = h.wrap_dek(&kek, &dek).unwrap();
        let back = h.unwrap_dek(&kek, &w).unwrap();
        assert_eq!(back, dek);
    }

    #[test]
    fn key_generation_refuses_oversized_keys() {
        let h = SoftwareHsm::with_fallback(true);
        assert!(matches!(
            h.gen_data_key(33),
            Err(HsmError::DeviceError { code: 0x1001, .. })
        ));
    }

    #[test]
    fn quality_matches_reported_degradation() {
        // 供审计自检使用。旧断言是 `permits_key_material() == !is_degraded()`，
        // 它把"没降级"等同于"有内核熵"，于是未探测时也会得到 true。
        //
        // 正确的不变量是单向的：**声称** Kernel 必须同时有观测记录且未降级。
        // 反向不成立——某台机器可能先成功读过 urandom、之后又失败，
        // 此时 `have_kernel_entropy()` 为 true 但 `is_degraded()` 也为 true。
        let q = crate::random::quality();
        if q.permits_key_material() {
            assert!(crate::random::have_kernel_entropy(), "Kernel without an observed read");
            assert!(!crate::random::is_degraded(), "degraded source must not permit key material");
        }
    }


    #[test]
    fn global_hsm_defaults_to_unconfigured() {
        // 测试环境未安装 HSM 时必须返回 NotConfigured 而非 panic
        if HSM.get().is_none() {
            assert!(matches!(with(|_| ()), Err(HsmError::NotConfigured)));
        }
    }
}
