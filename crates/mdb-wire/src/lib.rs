//! MongoDB Wire Protocol 实现（`OP_MSG` / 消息头 / `hello` 握手）。
//!
//! # 当前进度
//!
//! | 组件 | 状态 |
//! |------|------|
//! | 消息头 + 操作码 | ✅ 已实现 |
//! | `OP_MSG`（含 document sequence / 校验和） | ✅ 已实现 |
//! | `hello` / `isMaster` 握手应答 | ✅ 已实现 |
//! | `OP_QUERY` / `OP_REPLY`（旧版兼容） | ⬜ 未实现 |
//! | `OP_COMPRESSED` | ⬜ 未实现 |
//! | 实际 socket 收发循环 | ⬜ 未实现 |
//!
//! **尚未与任何真实驱动互通。** 详见 `docs/01-wire-protocol-compat-matrix.md`。
//!
//! # 依赖层次
//!
//! ```text
//! mdb-common <- mdb-crypto <- mdb-doc <- mdb-wire <- mdb-server
//! ```
//!
//! 本 crate 依赖 [`mdb_doc`] 做 BSON 编解码、[`mdb_crypto`] 做 CRC32C，
//! 错误统一收敛为 [`mdb_common::MdbError`]——`mdb-doc` 已为 `Error` 实现
//! `From`，因此 `?` 可直接跨 crate 传播，无需手写转换。

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod conn;
pub mod handshake;
pub mod header;
pub mod op_msg;

pub use conn::{Dispatched, Framed};
pub use handshake::{hello_reply, hello_reply_msg, is_handshake, MAX_WIRE_VERSION};
pub use header::{MessageHeader, OpCode, MAX_BSON_OBJECT_SIZE, MAX_MESSAGE_SIZE_BYTES};
pub use op_msg::{DocumentSequence, OpMsg};
