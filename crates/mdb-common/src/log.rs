//! 结构化日志。
//!
//! # 输出格式
//!
//! 输出到 stderr，单行 JSON（信创环境下的日志采集器普遍要求结构化输出，
//! 例如麒麟环境的 `rsyslog` + 审计插件、欧拉的 `auditd` 规则）。
//!
//! # 审计日志
//!
//! [`is_audit`] 区分普通日志与审计日志。审计日志由 `mdb-wire` 在
//! **认证/授权/权限变更/DDL** 时写入，写入独立的 `mdb-audit` 日志文件，
//! 用于等保 2.0 三级"审计记录"条款取证。审计日志**禁止**被普通
//! `logLevel` 配置降级，且必须包含 `uid`/`client`/`result` 三个字段。
//!
//! 见 `docs/03-xinchuang-mlps-compliance.md` 的等保条款对照。

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use crate::time::unix_millis;

/// 日志级别，与 MongoDB `logLevel` 取值兼容。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Level {
    Fatal = 0,
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl Level {
    pub const fn as_str(self) -> &'static str {
        match self {
            Level::Fatal => "F",
            Level::Error => "E",
            Level::Warn => "W",
            Level::Info => "I",
            Level::Debug => "D",
            Level::Trace => "D2",
        }
    }

    /// `logLevel` 配置字符串 → Level。
    ///
    /// 兼容 MongoDB 的 `verbose` / `debug` / `trace` 别名。
    pub fn parse(s: &str) -> Option<Level> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "fatal" => Level::Fatal,
            "error" => Level::Error,
            "warn" | "warning" => Level::Warn,
            "info" => Level::Info,
            "debug" | "verbose" => Level::Debug,
            "trace" => Level::Trace,
            _ => return None,
        })
    }
}

static GLOBAL_LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);
static AUDIT_ENABLED: AtomicBool = AtomicBool::new(true);

/// 设置全局日志级别。
pub fn set_level(level: Level) {
    GLOBAL_LEVEL.store(level as u8, Ordering::Relaxed);
}

pub fn level() -> Level {
    match GLOBAL_LEVEL.load(Ordering::Relaxed) {
        0 => Level::Fatal,
        1 => Level::Error,
        2 => Level::Warn,
        3 => Level::Info,
        4 => Level::Debug,
        _ => Level::Trace,
    }
}

/// 是否启用审计日志。生产环境**禁止**关闭（等保取证要求）。
pub fn set_audit_enabled(on: bool) {
    AUDIT_ENABLED.store(on, Ordering::Relaxed);
}

pub fn audit_enabled() -> bool {
    AUDIT_ENABLED.load(Ordering::Relaxed)
}

/// 单条日志。
#[derive(Debug, Clone)]
pub struct LogRecord<'a> {
    pub level: Level,
    pub component: &'a str,
    pub message: &'a str,
    /// 附加键值对，渲染为 JSON 字段。
    pub fields: &'a [(&'a str, String)],
    /// true 表示该条为审计记录，强制输出且不带级别过滤。
    pub audit: bool,
}

impl<'a> LogRecord<'a> {
    pub fn new(level: Level, component: &'a str, message: &'a str) -> Self {
        Self { level, component, message, fields: &[], audit: false }
    }

    /// 标记为审计记录。
    ///
    /// 审计记录**始终**输出（不受 `logLevel` 影响），这是等保 2.0
    /// 8.1.4.3 "应能够记录…并保存…审计记录" 的实现基础。
    #[must_use]
    pub fn audit(mut self) -> Self {
        self.audit = true;
        self
    }

    #[must_use]
    pub fn field(mut self, key: &'a str, value: impl AsRef<str>) -> Self {
        let mut v: Vec<(&'a str, String)> = self.fields.to_vec();
        v.push((key, value.as_ref().to_owned()));
        self.fields = Box::leak(v.into_boxed_slice());
        self
    }

    /// 写为单行 JSON 到 stderr。
    pub fn emit(&self) {
        if !self.audit && self.level > level() {
            return;
        }
        let mut out = String::with_capacity(128 + self.message.len());
        out.push('{');
        out.push_str("\"t\":{\"$date\":");
        out.push_str(&unix_millis().to_string());
        out.push('}');
        out.push_str(",\"s\":");
        crate::json::escape_into(self.level.as_str(), &mut out);
        out.push_str(",\"c\":");
        crate::json::escape_into(self.component, &mut out);
        out.push_str(",\"id\":");
        out.push_str(&self.component.len().to_string());
        out.push_str(",\"msg\":");
        crate::json::escape_into(self.message, &mut out);
        for (k, v) in self.fields {
            out.push(',');
            crate::json::escape_into(k, &mut out);
            out.push(':');
            crate::json::escape_into(v, &mut out);
        }
        out.push_str("}\n");
        let _ = std::io::stderr().write_all(out.as_bytes());
    }
}

/// 生成一条日志记录。
#[macro_export]
macro_rules! log_record {
    ($level:expr, $comp:expr, $($arg:tt)*) => {{
        $crate::log::LogRecord::new($level, $comp, &format!($($arg)*))
    }};
}

/// 信息级日志。
#[macro_export]
macro_rules! log_info {
    ($comp:expr, $($arg:tt)*) => {
        $crate::log_record!($crate::log::Level::Info, $comp, $($arg)*).emit()
    };
}

/// 警告级日志。
#[macro_export]
macro_rules! log_warn {
    ($comp:expr, $($arg:tt)*) => {
        $crate::log_record!($crate::log::Level::Warn, $comp, $($arg)*).emit()
    };
}

/// 错误级日志。
#[macro_export]
macro_rules! log_error {
    ($comp:expr, $($arg:tt)*) => {
        $crate::log_record!($crate::log::Level::Error, $comp, $($arg)*).emit()
    };
}

/// 调试级日志。
#[macro_export]
macro_rules! log_debug {
    ($comp:expr, $($arg:tt)*) => {
        $crate::log_record!($crate::log::Level::Debug, $comp, $($arg)*).emit()
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_parse_accepts_mongodb_aliases() {
        assert_eq!(Level::parse("verbose"), Some(Level::Debug));
        assert_eq!(Level::parse("WARNING"), Some(Level::Warn));
        assert_eq!(Level::parse("nope"), None);
    }

    #[test]
    fn level_ordering_is_mongodb_compatible() {
        // Fatal 最低、Trace 最高，与 logLevel 语义一致
        assert!(Level::Fatal < Level::Error);
        assert!(Level::Error < Level::Warn);
        assert!(Level::Warn < Level::Info);
        assert!(Level::Info < Level::Debug);
        assert!(Level::Debug < Level::Trace);
    }
}
