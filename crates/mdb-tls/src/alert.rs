//! TLS 告警协议（RFC 5246 §7.2）。
//!
//! 告警消息是 TLS 协议中最短的消息（2 字节），
//! 用于通知对端错误或关闭连接。

/// 告警级别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AlertLevel {
    Warning = 1,
    Fatal = 2,
}

impl AlertLevel {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(AlertLevel::Warning),
            2 => Some(AlertLevel::Fatal),
            _ => None,
        }
    }
}

/// 告警类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AlertDescription {
    CloseNotify = 0,
    UnexpectedMessage = 10,
    BadRecordMac = 20,
    DecryptionFailed = 21,
    RecordOverflow = 22,
    DecompressionFailure = 30,
    HandshakeFailure = 40,
    BadCertificate = 42,
    UnsupportedCertificate = 43,
    CertificateRevoked = 44,
    CertificateExpired = 45,
    CertificateUnknown = 46,
    IllegalParameter = 47,
    UnknownCa = 48,
    AccessDenied = 49,
    DecodeError = 50,
    DecryptError = 51,
    ProtocolVersion = 70,
    InsufficientSecurity = 71,
    InternalError = 80,
    InappropriateFallback = 86,
    UserCanceled = 90,
    NoRenegotiation = 100,
    MissingExtension = 109,
    UnsupportedExtension = 110,
    UnknownPskIdentity = 115,
}

impl AlertDescription {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(AlertDescription::CloseNotify),
            10 => Some(AlertDescription::UnexpectedMessage),
            20 => Some(AlertDescription::BadRecordMac),
            21 => Some(AlertDescription::DecryptionFailed),
            22 => Some(AlertDescription::RecordOverflow),
            30 => Some(AlertDescription::DecompressionFailure),
            40 => Some(AlertDescription::HandshakeFailure),
            42 => Some(AlertDescription::BadCertificate),
            43 => Some(AlertDescription::UnsupportedCertificate),
            44 => Some(AlertDescription::CertificateRevoked),
            45 => Some(AlertDescription::CertificateExpired),
            46 => Some(AlertDescription::CertificateUnknown),
            47 => Some(AlertDescription::IllegalParameter),
            48 => Some(AlertDescription::UnknownCa),
            49 => Some(AlertDescription::AccessDenied),
            50 => Some(AlertDescription::DecodeError),
            51 => Some(AlertDescription::DecryptError),
            70 => Some(AlertDescription::ProtocolVersion),
            71 => Some(AlertDescription::InsufficientSecurity),
            80 => Some(AlertDescription::InternalError),
            86 => Some(AlertDescription::InappropriateFallback),
            90 => Some(AlertDescription::UserCanceled),
            100 => Some(AlertDescription::NoRenegotiation),
            109 => Some(AlertDescription::MissingExtension),
            110 => Some(AlertDescription::UnsupportedExtension),
            115 => Some(AlertDescription::UnknownPskIdentity),
            _ => None,
        }
    }
}

/// TLS 告警消息。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Alert {
    pub level: AlertLevel,
    pub description: AlertDescription,
}

impl Alert {
    pub fn new(level: AlertLevel, description: AlertDescription) -> Self {
        Self { level, description }
    }

    pub fn fatal(description: AlertDescription) -> Self {
        Self { level: AlertLevel::Fatal, description }
    }

    pub fn warning(description: AlertDescription) -> Self {
        Self { level: AlertLevel::Warning, description }
    }

    pub fn close_notify() -> Self {
        Self::warning(AlertDescription::CloseNotify)
    }

    pub fn encode(&self) -> [u8; 2] {
        [self.level as u8, self.description as u8]
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 2 {
            return None;
        }
        let level = AlertLevel::from_u8(data[0])?;
        let description = AlertDescription::from_u8(data[1])?;
        Some(Self { level, description })
    }
}

impl std::fmt::Display for Alert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Alert({:?}, {:?})", self.level, self.description)
    }
}

impl std::error::Error for Alert {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alert_roundtrip() {
        let alert = Alert::fatal(AlertDescription::HandshakeFailure);
        let encoded = alert.encode();
        let decoded = Alert::decode(&encoded).unwrap();
        assert_eq!(alert, decoded);
    }

    #[test]
    fn close_notify() {
        let alert = Alert::close_notify();
        assert_eq!(alert.level, AlertLevel::Warning);
        assert_eq!(alert.description, AlertDescription::CloseNotify);
    }

    #[test]
    fn decode_short_data() {
        assert!(Alert::decode(&[1]).is_none());
        assert!(Alert::decode(&[]).is_none());
    }
}
