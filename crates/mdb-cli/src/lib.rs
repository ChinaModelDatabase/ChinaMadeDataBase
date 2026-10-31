//! mdb 客户端：替代 mongosh
//!
//! **阶段 0 骨架**：crate 边界、依赖方向与 lint 策略已固化，
//! 具体实现按 docs/02-storage-engine-design.md 的阶段计划推进。
//!
//! ## 依赖方向（单向，禁止反向依赖）
//!
//! `  ext
//! mdb-common <- mdb-crypto <- mdb-doc <- mdb-wire <- mdb-server
//!                     \                     \             \
//!                      \                     \             +- mdb-cli
//!                       \- mdb-storage <- mdb-index <- mdb-query
//!                                            \            \        /
//!                                             \- mdb-txn --+- mdb-repl
//!                                                          \- mdb-router
//! `

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
