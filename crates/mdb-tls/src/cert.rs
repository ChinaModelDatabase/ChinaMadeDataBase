//! 证书处理（简化版）。
//!
//! TLCP 1.1 使用 SM2 数字证书（GM/T 0015-2012）。
//! 本模块提供简化的证书结构和自签名证书生成，
//! 用于测试和开发环境。

use mdb_common::{ErrorCode, MdbError, MdbResult};
use mdb_crypto::digest::Digest;
use mdb_crypto::sm3::Sm3;

/// 简化的 X.509 证书结构。
#[derive(Debug, Clone)]
pub struct TlsCertificate {
    /// 证书主题（CN）。
    pub subject: String,
    /// SM3 指纹（用于快速验证）。
    pub fingerprint: [u8; 32],
    /// 公钥数据（32 字节）。
    pub public_key: Vec<u8>,
    /// 签名数据（自签名证书：对 subject + public_key 的签名）。
    pub signature: Vec<u8>,
    /// 生效时间（Unix 时间戳秒）。
    pub not_before: u64,
    /// 失效时间（Unix 时间戳秒）。
    pub not_after: u64,
}

impl TlsCertificate {
    /// 验证证书是否在有效期内。
    pub fn is_valid_now(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        now >= self.not_before && now <= self.not_after
    }

    /// 验证证书指纹。
    pub fn verify_fingerprint(&self, data: &[u8]) -> bool {
        let computed = Sm3::digest(data);
        mdb_crypto::constant_time_eq(computed.as_bytes(), &self.fingerprint)
    }

    /// 序列化证书为 DER 格式（简化：TLV 编码）。
    pub fn to_der(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Version (tag=0x01)
        buf.push(0x01);
        buf.push(0x01);
        buf.push(0x00); // v0

        // Subject (tag=0x02)
        let subject_bytes = self.subject.as_bytes();
        buf.push(0x02);
        buf.push(subject_bytes.len() as u8);
        buf.extend_from_slice(subject_bytes);

        // Public Key (tag=0x03)
        buf.push(0x03);
        buf.push(self.public_key.len() as u8);
        buf.extend_from_slice(&self.public_key);

        // Signature (tag=0x04)
        buf.push(0x04);
        buf.push(self.signature.len() as u8);
        buf.extend_from_slice(&self.signature);

        // Validity (tag=0x05)
        buf.push(0x05);
        buf.push(0x10);
        buf.extend_from_slice(&self.not_before.to_be_bytes());
        buf.extend_from_slice(&self.not_after.to_be_bytes());

        buf
    }

    /// 从 DER 格式解析证书。
    pub fn from_der(data: &[u8]) -> MdbResult<Self> {
        let mut pos = 0;
        let mut subject = String::new();
        let mut public_key = Vec::new();
        let mut signature = Vec::new();
        let mut not_before = 0u64;
        let mut not_after = 0u64;

        while pos < data.len() {
            if pos + 2 > data.len() {
                break;
            }
            let tag = data[pos];
            let len = data[pos + 1] as usize;
            pos += 2;
            if pos + len > data.len() {
                return Err(MdbError::new(ErrorCode::FailedToParse, "certificate DER truncated"));
            }
            match tag {
                0x01 => { pos += len; } // version, skip
                0x02 => {
                    subject = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                    pos += len;
                }
                0x03 => {
                    public_key = data[pos..pos + len].to_vec();
                    pos += len;
                }
                0x04 => {
                    signature = data[pos..pos + len].to_vec();
                    pos += len;
                }
                0x05 => {
                    if len == 16 {
                        not_before = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
                        not_after = u64::from_be_bytes(data[pos + 8..pos + 16].try_into().unwrap());
                    }
                    pos += len;
                }
                _ => {
                    pos += len;
                }
            }
        }

        if public_key.is_empty() {
            return Err(MdbError::new(ErrorCode::FailedToParse, "certificate missing public key"));
        }

        let fingerprint = Sm3::digest(&data).0;

        Ok(Self { subject, fingerprint, public_key, signature, not_before, not_after })
    }
}

/// 生成自签名测试证书。
///
/// 使用给定的私钥材料生成 SM3 指纹自签名证书。
/// 注意：这是开发/测试用途，不包含真实的 SM2 签名。
pub fn generate_self_signed(subject: &str, private_key: &[u8; 32]) -> TlsCertificate {
    let public_key = {
        let mut hasher = Sm3::new();
        hasher.update(b"sm2-public:");
        hasher.update(private_key);
        let hash = hasher.finish().0;
        hash.to_vec()
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // 签名 = SM3(subject || public_key || private_key 的一部分)
    let signature = {
        let mut hasher = Sm3::new();
        hasher.update(subject.as_bytes());
        hasher.update(&public_key);
        hasher.update(&private_key[..16]);
        hasher.finish().0.to_vec()
    };

    let fingerprint = {
        let mut hasher = Sm3::new();
        hasher.update(subject.as_bytes());
        hasher.update(&public_key);
        hasher.update(&signature);
        hasher.finish().0
    };

    TlsCertificate {
        subject: subject.to_owned(),
        fingerprint,
        public_key,
        signature,
        not_before: now,
        not_after: now + 365 * 24 * 3600, // 1 年有效
    }
}

/// 验证证书链（简化：仅验证自签名证书）。
pub fn verify_certificate(cert: &TlsCertificate, ca_fingerprint: Option<&[u8; 32]>) -> MdbResult<()> {
    if !cert.is_valid_now() {
        return Err(MdbError::new(
            ErrorCode::IllegalOperation,
            "certificate has expired or is not yet valid",
        ));
    }

    if let Some(expected) = ca_fingerprint {
        if !mdb_crypto::constant_time_eq(&cert.fingerprint, expected) {
            return Err(MdbError::new(
                ErrorCode::Unauthorized,
                "certificate fingerprint mismatch",
            ));
        }
    }

    Ok(())
}

/// PEM 编码证书（Base64 + 头尾标记）。
pub fn to_pem(cert: &TlsCertificate) -> String {
    let der = cert.to_der();
    let b64 = mdb_crypto::base64::encode(&der);
    let mut pem = String::new();
    pem.push_str("-----BEGIN CERTIFICATE-----\n");
    for line in b64.as_bytes().chunks(64) {
        pem.push_str(&String::from_utf8_lossy(line));
        pem.push('\n');
    }
    pem.push_str("-----END CERTIFICATE-----\n");
    pem
}

/// PEM 解码证书。
pub fn from_pem(pem: &str) -> MdbResult<TlsCertificate> {
    let mut b64 = String::new();
    let mut in_cert = false;
    for line in pem.lines() {
        if line.contains("BEGIN CERTIFICATE") {
            in_cert = true;
            continue;
        }
        if line.contains("END CERTIFICATE") {
            break;
        }
        if in_cert {
            b64.push_str(line.trim());
        }
    }
    let der = mdb_crypto::base64::decode(&b64)?;
    TlsCertificate::from_der(&der)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_signed_certificate_roundtrip() {
        let key = mdb_crypto::random::bytes::<32>();
        let cert = generate_self_signed("test.mdb.local", &key);
        assert!(cert.is_valid_now());

        let der = cert.to_der();
        let decoded = TlsCertificate::from_der(&der).unwrap();
        assert_eq!(decoded.subject, "test.mdb.local");
        assert_eq!(decoded.public_key, cert.public_key);
    }

    #[test]
    fn pem_roundtrip() {
        let key = mdb_crypto::random::bytes::<32>();
        let cert = generate_self_signed("pem-test.mdb.local", &key);
        let pem = to_pem(&cert);
        assert!(pem.contains("BEGIN CERTIFICATE"));
        let decoded = from_pem(&pem).unwrap();
        assert_eq!(decoded.subject, cert.subject);
    }

    #[test]
    fn fingerprint_verification() {
        let key = mdb_crypto::random::bytes::<32>();
        let cert = generate_self_signed("verify.mdb.local", &key);
        // verify_fingerprint checks SM3(data) == stored fingerprint
        // The stored fingerprint = SM3(subject + pubkey + sig)
        // So we should verify with the same combined data
        let mut input = Vec::new();
        input.extend_from_slice(cert.subject.as_bytes());
        input.extend_from_slice(&cert.public_key);
        input.extend_from_slice(&cert.signature);
        assert!(cert.verify_fingerprint(&input));

        let wrong_data = b"wrong data";
        assert!(!cert.verify_fingerprint(wrong_data));
    }

    #[test]
    fn expired_certificate_rejected() {
        let cert = TlsCertificate {
            subject: "expired.mdb.local".into(),
            fingerprint: [0u8; 32],
            public_key: vec![0u8; 32],
            signature: vec![0u8; 32],
            not_before: 0,
            not_after: 100, // long expired
        };
        assert!(!cert.is_valid_now());
        assert!(verify_certificate(&cert, None).is_err());
    }
}
