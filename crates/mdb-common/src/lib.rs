//! MDB 公共基础库。
//!
//! 本 crate 不依赖任何第三方 crate，同时被 MDB 全部组件依赖，禁止反向依赖
//! 任何业务 crate。
//!
//! # 稳定性契约
//!
//! [`code::ErrorCode`] 的**数值**与 MongoDB 保持一致，这是 wire 兼容的硬要求：
//! 官方驱动会把 `errmsg` 映射为宿主语言的异常类型并据此决定重试策略
//! （例如 `TransientTransactionError`、`UnknownTransactionCommitResult`），
//! 因此数值变更等同于协议破坏，需走 RFC。
//!
//! [`code::ErrorCode`] 的 `code_name` 字符串同样被驱动用于标签匹配，不得随意改写。

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod code;
pub mod config;
pub mod context;
pub mod json;
pub mod log;
pub mod metrics;
pub mod time;

pub use code::{ErrorCode, MdbError, MdbResult};
pub use config::Config;
pub use context::Context;
pub use log::{Level, LogRecord};
