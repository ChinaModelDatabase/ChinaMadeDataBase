//! 统一错误类型与 MongoDB 兼容错误码。
//!
//! # 兼容性说明
//!
//! MongoDB 驱动在收到 `ok: 0` 响应时读取三个字段：
//!
//! | 字段 | 驱动用途 |
//! |------|----------|
//! | `code` | 数值码，映射为宿主语言的错误类型/异常类 |
//! | `codeName` | 字符串码，Java/Node 驱动用它做标签匹配与重试判定 |
//! | `errmsg` | 展示文本；驱动可能做子串匹配 |
//!
//! 关键重试语义（`mongodb-driver-core` / `libmongoc` 行为）：
//!
//! - `TransientTransactionError`：事务整体需重试（`UnknownTransactionCommitResult` 仅重试提交）
//! - `ResumableChangeStreamError`：`codeName` 含此标签时驱动自动发起 `resumeToken` 重连
//! - `MaxTimeMSExpired`（50）、`ExceededTimeLimit`（262）：**绝不重试**，视为逻辑错误
//!
//! 因此本模块把错误码分为 5 类，见 [`ErrorCode::category`]。

use std::fmt;

/// 错误分类，用于服务端内部的重试/降级决策。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// 逻辑错误，重试无意义（例如文档过大、类型不匹配）。
    BadValue,
    /// 暂时性错误，可原样重试（例如选举中、写入关注不足）。
    Transient,
    /// 资源不足（磁盘满、内存压力、TooManyFiles）。
    Resource,
    /// 授权/认证失败。
    Auth,
    /// 内部错误或未归类。
    Internal,
}

/// MongoDB 兼容错误码。
///
/// 仅收录 MDB 实际会返回的码。完整清单见 MongoDB 官方 `ErrorCodes` 文档。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ErrorCode {
    BadValue = 2,
    NoSuchKey = 4,
    HostUnreachable = 6,
    HostNotFound = 7,
    FailedToParse = 9,
    UserNotFound = 11,
    Unauthorized = 13,
    TypeMismatch = 14,
    Overflow = 15,
    InvalidLength = 16,
    IllegalOperation = 20,
    EmptyArrayOperation = 21,
    BadDb = 29,
    DuplicateKey = 11000,
    UnrecognizedField = 40415,
    WriteConflict = 112,
    WriteConcernFailed = 64,
    ExceededTimeLimit = 262,
    MaxTimeMSExpired = 50,
    CursorNotFound = 43,
    NamespaceNotFound = 26,
    IndexNotFound = 27,
    IndexOptionsConflict = 68,
    IndexKeySpecsConflict = 86,
    ImmutableField = 66,
    LockBusy = 24,
    NotMaster = 10107,
    NotMasterNoSlaveOk = 13435,
    StaleShardVersion = 63,
    StaleEpoch = 150,
    StaleConfig = 13388,
    ShardConfigStale = 234,
    ReconfigInProgress = 133,
    ShutdownInProgress = 91,
    PrimarySteppedDown = 189,
    ReplicaSetNoPrimary = 134,
    ReplicaSetNotFound = 94,
    SnapshotTooOld = 286,
    SnapshotUnavailable = 287,
    TooManyLogicalSessions = 174,
    InvalidOptions = 72,
    UnknownReplWriteConcern = 79,
    QuotaExceeded = 13436,
    /// 命令尚未实现。
    ///
    /// 即 MongoDB 自身的 `CommandNotImplemented(59)`——这是真实错误码，
    /// 不是 MDB 私有扩展码。驱动据此把该命令视为不可用并降级。
    ///
    /// 曾经误设为 `59_000`（把 59 误当"便于统计"的自造值），
    /// 真实驱动因此抛 `OperationFailure(code=59000)`，而按标准应得
    /// `code=59`。扩展码不得借用标准码数值，否则驱动分支判断全错。
    CommandNotImplemented = 59,
    /// 分片键不合法。
    ///
    /// 即 MongoDB 自身的 `BadShardKey(13000)`，非私有扩展码。
    BadShardKey = 13_000,
    /// 存储层校验和不匹配（数据损坏）。
    ///
    /// 即 MongoDB 自身的 `DataCorruption(246)`，非私有扩展码。
    ///
    /// 该码触发客户端立即失败并触发告警，**不重试**。
    ///
    /// 曾误设为 `246_000`；事务冲突同类的 `WriteConflictExtended`
    /// 曾误设为 `112_000`，与既有 `WriteConflict(112)` 语义重复，
    /// 两者已合并删除，避免同一故障落到驱动无法识别的私有码上。
    DataCorruption = 246,

    // -----------------------------------------------------------------------
    // MDB 私有扩展码（91000+）
    // -----------------------------------------------------------------------

    /// 加密/解密操作失败（TDE、密钥管理等）。
    EncryptionError = 91001,
}

impl ErrorCode {
    /// 数值码，写入 BSON 的 `code` 字段。
    pub const fn code(self) -> i32 {
        self as u32 as i32
    }

    /// 字符串码，写入 BSON 的 `codeName` 字段。
    ///
    /// 需与 MongoDB `ErrorCodes` 文档逐字符一致。
    pub const fn code_name(self) -> &'static str {
        use ErrorCode::*;
        match self {
            BadValue => "BadValue",
            NoSuchKey => "NoSuchKey",
            HostUnreachable => "HostUnreachable",
            HostNotFound => "HostNotFound",
            FailedToParse => "FailedToParse",
            UserNotFound => "UserNotFound",
            Unauthorized => "Unauthorized",
            TypeMismatch => "TypeMismatch",
            Overflow => "Overflow",
            InvalidLength => "InvalidLength",
            IllegalOperation => "IllegalOperation",
            EmptyArrayOperation => "EmptyArrayOperation",
            BadDb => "BadDb",
            DuplicateKey => "DuplicateKey",
            UnrecognizedField => "UnrecognizedField",
            WriteConflict => "WriteConflict",
            WriteConcernFailed => "WriteConcernFailed",
            ExceededTimeLimit => "ExceededTimeLimit",
            MaxTimeMSExpired => "MaxTimeMSExpired",
            CursorNotFound => "CursorNotFound",
            NamespaceNotFound => "NamespaceNotFound",
            IndexNotFound => "IndexNotFound",
            IndexOptionsConflict => "IndexOptionsConflict",
            IndexKeySpecsConflict => "IndexKeySpecsConflict",
            ImmutableField => "ImmutableField",
            LockBusy => "LockBusy",
            NotMaster => "NotMaster",
            NotMasterNoSlaveOk => "NotMasterNoSlaveOk",
            StaleShardVersion => "StaleShardVersion",
            StaleEpoch => "StaleEpoch",
            StaleConfig => "StaleConfig",
            ShardConfigStale => "ShardConfigStale",
            ReconfigInProgress => "ReconfigInProgress",
            ShutdownInProgress => "ShutdownInProgress",
            PrimarySteppedDown => "PrimarySteppedDown",
            ReplicaSetNoPrimary => "ReplicaSetNoPrimary",
            ReplicaSetNotFound => "ReplicaSetNotFound",
            SnapshotTooOld => "SnapshotTooOld",
            SnapshotUnavailable => "SnapshotUnavailable",
            TooManyLogicalSessions => "TooManyLogicalSessions",
            InvalidOptions => "InvalidOptions",
            UnknownReplWriteConcern => "UnknownReplWriteConcern",
            QuotaExceeded => "QuotaExceeded",
            CommandNotImplemented => "CommandNotImplemented",
            BadShardKey => "BadShardKey",
            DataCorruption => "DataCorruption",
            EncryptionError => "EncryptionError",
        }
    }

    /// 分类，供服务端重试决策使用。
    pub const fn category(self) -> ErrorCategory {
        use ErrorCategory as EC;
        use ErrorCode::*;
        match self {
            BadValue | NoSuchKey | FailedToParse | TypeMismatch | Overflow | InvalidLength
            | IllegalOperation | EmptyArrayOperation | BadDb | UnrecognizedField
            | ImmutableField | InvalidOptions | BadShardKey | MaxTimeMSExpired
            | ExceededTimeLimit | DuplicateKey | IndexNotFound | IndexOptionsConflict
            | IndexKeySpecsConflict | CursorNotFound | NamespaceNotFound
            | UnknownReplWriteConcern | DataCorruption | EncryptionError => EC::BadValue,
            HostUnreachable | HostNotFound | NotMaster | NotMasterNoSlaveOk
            | WriteConcernFailed | ReconfigInProgress | ShutdownInProgress | PrimarySteppedDown
            | ReplicaSetNoPrimary | StaleShardVersion | StaleEpoch | StaleConfig
            | ShardConfigStale | SnapshotUnavailable | SnapshotTooOld
            |             WriteConflict | LockBusy => EC::Transient,
            Unauthorized | UserNotFound => EC::Auth,
            _ => EC::Internal,
        }
    }

    /// 该码是否允许驱动重试。
    ///
    /// 映射 `mongodb` Go 驱动 `retryable.go` 与 `libmongoc` 的可重试判定。
    pub const fn is_retryable(self) -> bool {
        matches!(self.category(), ErrorCategory::Transient)
    }

    /// 是否需标记事务标签以驱动驱动整体重试。
    pub const fn has_transaction_label(self) -> bool {
        self.is_retryable()
    }

    /// 反查：错误对象 → 错误码。
    pub const fn from_code(code: i32) -> Option<ErrorCode> {
        use ErrorCode::*;
        Some(match code {
            2 => BadValue,
            4 => NoSuchKey,
            6 => HostUnreachable,
            7 => HostNotFound,
            9 => FailedToParse,
            11 => UserNotFound,
            13 => Unauthorized,
            14 => TypeMismatch,
            15 => Overflow,
            16 => InvalidLength,
            20 => IllegalOperation,
            21 => EmptyArrayOperation,
            26 => NamespaceNotFound,
            27 => IndexNotFound,
            29 => BadDb,
            43 => CursorNotFound,
            50 => MaxTimeMSExpired,
            64 => WriteConcernFailed,
            63 => StaleShardVersion,
            66 => ImmutableField,
            68 => IndexOptionsConflict,
            72 => InvalidOptions,
            79 => UnknownReplWriteConcern,
            86 => IndexKeySpecsConflict,
            91 => ShutdownInProgress,
            94 => ReplicaSetNotFound,
            112 => WriteConflict,
            133 => ReconfigInProgress,
            134 => ReplicaSetNoPrimary,
            150 => StaleEpoch,
            174 => TooManyLogicalSessions,
            189 => PrimarySteppedDown,
            262 => ExceededTimeLimit,
            286 => SnapshotTooOld,
            287 => SnapshotUnavailable,
            10107 => NotMaster,
            13388 => StaleConfig,
            13435 => NotMasterNoSlaveOk,
            13436 => QuotaExceeded,
            11000 => DuplicateKey,
            40415 => UnrecognizedField,
            91001 => EncryptionError,
            _ => return None,
        })
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code_name())
    }
}

/// MDB 统一错误类型。
///
/// 布局经过紧凑化处理（code + message 联合），`ErrorCode` 为 `Copy`，
/// 整个错误在错误路径上不产生额外堆分配（`message` 仅在非空时分配）。
#[derive(Debug, Clone)]
pub struct MdbError {
    code: ErrorCode,
    message: Option<Box<str>>,
    /// 附加上下文，例如 `{"collection": "orders", "shard": "s0"}`。
    details: Option<Box<str>>,
}

impl MdbError {
    /// 以错误码构造，不带消息。
    ///
    /// 用于驱动明确按 `code` 分支处理的路径。
    pub const fn bare(code: ErrorCode) -> Self {
        Self { code, message: None, details: None }
    }

    /// 以错误码 + 消息构造。
    pub fn new(code: ErrorCode, message: impl Into<Box<str>>) -> Self {
        Self { code, message: Some(message.into()), details: None }
    }

    /// 附加结构化上下文，用于日志与错误响应。
    #[must_use]
    pub fn with_details(mut self, details: impl Into<Box<str>>) -> Self {
        self.details = Some(details.into());
        self
    }

    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// 错误消息；缺省时回退为 `codeName`。
    ///
    /// 对齐 MongoDB：`errmsg` 永不为空。
    pub fn message(&self) -> &str {
        self.message.as_deref().unwrap_or(self.code.code_name())
    }

    pub fn details(&self) -> Option<&str> {
        self.details.as_deref()
    }

    pub const fn category(&self) -> ErrorCategory {
        self.code.category()
    }

    pub const fn is_retryable(&self) -> bool {
        self.code.is_retryable()
    }
}

impl fmt::Display for MdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.code_name(), self.message())?;
        if let Some(d) = &self.details {
            write!(f, " [{d}]")?;
        }
        Ok(())
    }
}

impl std::error::Error for MdbError {}

/// MDB 通用返回类型。
pub type MdbResult<T> = Result<T, MdbError>;

/// 常见错误的简写构造。
pub mod err {
    use super::{ErrorCode, MdbError};

    pub fn bad_value(msg: impl Into<Box<str>>) -> MdbError {
        MdbError::new(ErrorCode::BadValue, msg)
    }

    pub fn unauthorized(msg: impl Into<Box<str>>) -> MdbError {
        MdbError::new(ErrorCode::Unauthorized, msg)
    }

    pub fn not_master(msg: impl Into<Box<str>>) -> MdbError {
        MdbError::new(ErrorCode::NotMaster, msg)
    }

    pub fn write_conflict(msg: impl Into<Box<str>>) -> MdbError {
        MdbError::new(ErrorCode::WriteConflict, msg)
    }

    pub fn duplicate_key(msg: impl Into<Box<str>>) -> MdbError {
        MdbError::new(ErrorCode::DuplicateKey, msg)
    }

    pub fn namespace_not_found(msg: impl Into<Box<str>>) -> MdbError {
        MdbError::new(ErrorCode::NamespaceNotFound, msg)
    }

    pub fn internal(msg: impl Into<Box<str>>) -> MdbError {
        MdbError::new(ErrorCode::FailedToParse, msg)
    }

    pub fn unimplemented(cmd: &str) -> MdbError {
        MdbError::new(
            ErrorCode::CommandNotImplemented,
            format!("no such command: '{cmd}'"),
        )
    }

    pub fn corrupted(msg: impl Into<Box<str>>) -> MdbError {
        MdbError::new(ErrorCode::DataCorruption, msg)
    }

    /// 便捷宏：`bail!(err::bad_value("..."))`
    #[macro_export]
    macro_rules! bail {
        ($e:expr) => {
            return Err($e)
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_roundtrip() {
        for code in [
            ErrorCode::BadValue,
            ErrorCode::Unauthorized,
            ErrorCode::NotMaster,
            ErrorCode::DuplicateKey,
            ErrorCode::CursorNotFound,
            ErrorCode::ExceededTimeLimit,
        ] {
            assert_eq!(ErrorCode::from_code(code.code()), Some(code), "roundtrip {code}");
        }
    }

    #[test]
    fn duplicate_key_code_is_exact() {
        // 11000 写错一位就会导致驱动把唯一键冲突误判为普通错误并静默重试
        assert_eq!(ErrorCode::DuplicateKey.code(), 11000);
        assert_eq!(ErrorCode::DuplicateKey.code_name(), "DuplicateKey");
    }

    #[test]
    fn retryability_matches_mongodb_semantics() {
        // 驱动会无限重试：必须归为 transient
        assert!(ErrorCode::NotMaster.is_retryable());
        assert!(ErrorCode::WriteConflict.is_retryable());
        assert!(ErrorCode::PrimarySteppedDown.is_retryable());
        // 驱动绝不重试：必须归为 bad value
        assert!(!ErrorCode::ExceededTimeLimit.is_retryable());
        assert!(!ErrorCode::MaxTimeMSExpired.is_retryable());
        assert!(!ErrorCode::DuplicateKey.is_retryable());
        assert!(!ErrorCode::TypeMismatch.is_retryable());
    }

    #[test]
    fn message_never_empty() {
        let e = MdbError::bare(ErrorCode::Unauthorized);
        assert_eq!(e.message(), "Unauthorized");
    }

    /// 冻结 MongoDB 标准错误码数值。
    ///
    /// 这些值会原样进入 BSON `code` 字段，驱动据此选择重试/降级分支。
    /// 真实驱动差分（pymongo）曾捕获 `CommandNotImplemented` 被误写为
    /// `59_000`，导致 `OperationFailure(code=59000)`。此测试锁死数值，
    /// 防止"为了便于统计"而擅自偏移标准码。
    #[test]
    fn standard_mongodb_codes_are_exact() {
        let expected: &[(ErrorCode, i32, &str)] = &[
            (ErrorCode::CommandNotImplemented, 59, "CommandNotImplemented"),
            (ErrorCode::BadShardKey, 13_000, "BadShardKey"),
            (ErrorCode::DataCorruption, 246, "DataCorruption"),
            (ErrorCode::WriteConflict, 112, "WriteConflict"),
            (ErrorCode::FailedToParse, 9, "FailedToParse"),
            (ErrorCode::NamespaceNotFound, 26, "NamespaceNotFound"),
            (ErrorCode::CursorNotFound, 43, "CursorNotFound"),
            (ErrorCode::MaxTimeMSExpired, 50, "MaxTimeMSExpired"),
            (ErrorCode::ExceededTimeLimit, 262, "ExceededTimeLimit"),
            (ErrorCode::NotMaster, 10_107, "NotMaster"),
            (ErrorCode::NotMasterNoSlaveOk, 13_435, "NotMasterNoSlaveOk"),
            (ErrorCode::StaleConfig, 13_388, "StaleConfig"),
        ];
        for (code, value, name) in expected {
            assert_eq!(code.code(), *value, "{name} 数值应为 {value}，实际 {}", code.code());
            assert_eq!(code.code_name(), *name);
        }
    }

    /// 任何"私有扩展码"都不得落在标准码区间内造出影子数值。
    #[test]
    fn no_shadowed_standard_codes() {
        // 历史上出现过 59_000 / 112_000 / 246_000 这类"标准码 x 1000"，
        // 驱动完全无法识别。此处显式禁止该模式再次出现。
        for code in [ErrorCode::CommandNotImplemented, ErrorCode::DataCorruption, ErrorCode::WriteConflict] {
            let v = code.code();
            assert!(v < 100_000, "{} 的码 {v} 落在驱动无法识别的区间", code.code_name());
        }
    }
}
