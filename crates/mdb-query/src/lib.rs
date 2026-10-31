//! 查询引擎：优化器、算子、聚合 Pipeline、Explain
//!
//! ## 依赖方向（单向，禁止反向依赖）
//!
//! ```text
//! mdb-common <- mdb-crypto <- mdb-doc <- mdb-wire <- mdb-server
//!                     \                     \             \
//!                      \                     \             +- mdb-cli
//!                       \- mdb-storage <- mdb-index <- mdb-query
//!                                            \            \        /
//!                                             \- mdb-txn --+- mdb-repl
//!                                                          \- mdb-router
//! ```

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod aggregation;
pub mod cursor;
pub mod executor;
pub mod explain;
pub mod operators;
pub mod parser;
pub mod profiler;
pub mod sorter;
pub mod types;

pub use aggregation::{GroupId, Pipeline, PipelineStage};
pub use cursor::{Cursor, CursorManager, CursorOptions, SharedCursorManager};
pub use executor::{
    CollectionScanExecutor, FilterEvaluator, InMemoryCollection, ProjectionApplier,
    QueryExecutor, QueryPlanner, sort_documents,
};
pub use explain::{ExplainPlan, ExplainStage};
pub use operators::{
    All, And, ElemMatch, Eq, Exists, Gt, Gte, In, Lt, Lte, Ne, Nin, Nor, Not, Or, Size, Type,
    QueryOperator, RegexOp, Mod as ModOp,
};
pub use parser::{ComparisonOp, Filter, LogicalOp, filter_to_operator, parse_filter};
pub use profiler::{ProfilerStats, QueryProfiler, QueryTimer, SlowQueryLog};
pub use sorter::Sorter;
pub use types::{CollectionIndexes, IndexStats, QueryPlan, QueryResult, SortDirection, parse_sort_spec};
