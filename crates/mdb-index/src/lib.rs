//! 索引：B-Tree、TTL、Text 倒排、2dsphere、唯一索引
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

pub mod btree;
pub mod geospatial;
pub mod manager;
pub mod text;
pub mod ttl;
pub mod types;
pub mod unique;

pub use btree::BTree;
pub use geospatial::{BBox, GeoIndex, GeoPoint, GeoShape};
pub use manager::{extract_compound_key, extract_field, IndexManager};
pub use text::TextIndex;
pub use ttl::{TtlConfig, TtlIndex, TtlCleanupStats};
pub use types::{IndexEntry, IndexOptions, IndexType, RecordId, SortOrder};
pub use unique::UniqueIndex;
