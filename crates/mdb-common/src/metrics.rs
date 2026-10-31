//! 指标与运行上下文。
//!
//! 信创环境的监控体系（麒麟 `telemetry-agent`、欧拉 `node_exporter`、
//! 华为 AOM、Prometheus）要求暴露标准指标。MDB 暴露：
//!
//! - Prometheus 文本格式 `/metrics`（由 `mdb-server` 提供 HTTP 端点）
//! - 与 MongoDB 同名的 serverStatus 计数器（供现有监控系统零改造对接）
//!
//! 本模块实现一个**无锁近似计数**（非全局严格精确）：用 `AtomicU64` 自增，
//! 高基数下代价可控。金融级对账场景必须改用 `mdb-storage` 事务提交时的
//! 精确计数，不可依赖本模块。

use std::sync::atomic::{AtomicU64, Ordering};

/// 单调计数器。
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub const fn new() -> Self {
        Counter(AtomicU64::new(0))
    }
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// 简单 gauge（用于连接数、队列深度等可增可减量）。
#[derive(Debug, Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
    pub const fn new() -> Self {
        Gauge(AtomicU64::new(0))
    }
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn set(&self, v: u64) {
        self.0.store(v, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Prometheus 直方图，追踪观测值的分布。
///
/// 默认桶边界（毫秒）：0.1, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000
/// 适配命令延迟、查询耗时等数据库典型延迟分布。
#[derive(Debug)]
pub struct Histogram {
    buckets: Vec<f64>,
    counts: Vec<AtomicU64>,
    sum: AtomicU64,
    count: AtomicU64,
}

/// 将 f64 值乘以 1000 存储为 u64 微秒精度，以便原子操作。
fn f64_to_atomic_u64(v: f64) -> u64 {
    (v * 1000.0).round().max(0.0) as u64
}

fn atomic_u64_to_f64(v: u64) -> f64 {
    v as f64 / 1000.0
}

impl Histogram {
    pub fn new(buckets: Vec<f64>) -> Self {
        let count = buckets.len() + 1; // +1 for +Inf bucket
        let counts = (0..count).map(|_| AtomicU64::new(0)).collect();
        Self {
            buckets,
            counts,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn with_default_buckets() -> Self {
        Self::new(vec![
            0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
            1000.0, 2500.0, 5000.0, 10000.0,
        ])
    }

    pub fn observe(&self, value: f64) {
        let micro = f64_to_atomic_u64(value);
        self.sum.fetch_add(micro, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        for (i, &le) in self.buckets.iter().enumerate() {
            if value <= le {
                self.counts[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        // +Inf bucket always incremented
        self.counts
            .last()
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn bucket_le(&self) -> &[(f64, u64)] {
        // Return raw data for rendering; caller must not hold this across observe()
        &[]
    }

    pub fn render(&self, name: &str, help: &str) -> String {
        use std::fmt::Write as _;
        let mut s = String::with_capacity(1024);
        let _ = writeln!(s, "# HELP {} {}", name, help);
        let _ = writeln!(s, "# TYPE {} histogram", name);
        for (i, &le) in self.buckets.iter().enumerate() {
            let _ = writeln!(s, "{name}_bucket{{le=\"{le}\"}} {}", self.counts[i].load(Ordering::Relaxed));
        }
        let _ = writeln!(
            s,
            "{name}_bucket{{le=\"+Inf\"}} {}",
            self.counts.last().unwrap().load(Ordering::Relaxed)
        );
        let _ = writeln!(
            s,
            "{name}_sum {}",
            atomic_u64_to_f64(self.sum.load(Ordering::Relaxed))
        );
        let _ = writeln!(s, "{name}_count {}", self.count.load(Ordering::Relaxed));
        s
    }
}

/// 全局指标集合。
///
/// 字段名与 MongoDB `serverStatus.metrics` 对齐，便于现有监控面板复用。
#[derive(Debug)]
pub struct Metrics {
    // -- Connections --
    pub connections_total: Counter,
    pub connections_open: Gauge,
    pub connections_authenticated: Gauge,
    pub connection_errors_total: Counter,

    // -- Commands --
    pub commands_total: Counter,
    pub command_duration_ms: Histogram,
    pub command_errors_total: Counter,

    // -- CRUD --
    pub inserts_total: Counter,
    pub inserts_duration_ms: Histogram,
    pub finds_total: Counter,
    pub finds_duration_ms: Histogram,
    pub updates_total: Counter,
    pub updates_duration_ms: Histogram,
    pub deletes_total: Counter,
    pub deletes_duration_ms: Histogram,

    // -- Queries --
    pub query_plans_total: Counter,
    pub query_slow_total: Counter,
    pub query_memory_bytes: Histogram,

    // -- Cursors --
    pub cursors_open: Gauge,
    pub cursors_total: Counter,
    pub getmore_total: Counter,

    // -- Transactions --
    pub transactions_started: Counter,
    pub transactions_committed: Counter,
    pub transactions_aborted: Counter,
    pub transaction_duration_ms: Histogram,

    // -- Storage --
    pub storage_data_size_bytes: Gauge,
    pub storage_index_size_bytes: Gauge,
    pub storage_wal_bytes: Counter,
    pub storage_compactions: Counter,
    pub storage_compaction_bytes: Counter,
    pub storage_sst_files: Gauge,
    pub storage_bloom_filter_usage: Gauge,

    // -- Replication --
    pub replication_lag_ms: Gauge,
    pub oplog_size_bytes: Gauge,
    pub oplog_entries_total: Counter,

    // -- Index --
    pub index_hits_total: Counter,
    pub index_misses_total: Counter,
    pub index_size_bytes: Gauge,

    // -- GridFS --
    pub gridfs_uploads_total: Counter,
    pub gridfs_downloads_total: Counter,
    pub gridfs_size_bytes: Gauge,

    // -- Audit --
    pub audit_events_total: Counter,
    pub audit_buffer_size: Gauge,

    // -- Security --
    pub auth_failures_total: Counter,
    pub auth_successes_total: Counter,
    pub unauthorized_attempts_total: Counter,

    // -- TLS --
    pub tls_connections_total: Counter,
    pub tls_handshake_errors: Counter,

    // -- Backup --
    pub backup_total: Counter,
    pub backup_duration_ms: Histogram,
    pub backup_size_bytes: Histogram,

    // -- System --
    pub uptime_seconds: Gauge,
    pub memory_usage_bytes: Gauge,
    pub cpu_usage_percent: Gauge,

    // -- Legacy compatibility --
    pub write_conflict_retries: Counter,
    pub sstable_flushes: Counter,
    pub wal_bytes_written: Counter,
    pub checksum_mismatches: Counter,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            connections_total: Counter::new(),
            connections_open: Gauge::new(),
            connections_authenticated: Gauge::new(),
            connection_errors_total: Counter::new(),

            commands_total: Counter::new(),
            command_duration_ms: Histogram::with_default_buckets(),
            command_errors_total: Counter::new(),

            inserts_total: Counter::new(),
            inserts_duration_ms: Histogram::with_default_buckets(),
            finds_total: Counter::new(),
            finds_duration_ms: Histogram::with_default_buckets(),
            updates_total: Counter::new(),
            updates_duration_ms: Histogram::with_default_buckets(),
            deletes_total: Counter::new(),
            deletes_duration_ms: Histogram::with_default_buckets(),

            query_plans_total: Counter::new(),
            query_slow_total: Counter::new(),
            query_memory_bytes: Histogram::new(vec![
                256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0,
            ]),

            cursors_open: Gauge::new(),
            cursors_total: Counter::new(),
            getmore_total: Counter::new(),

            transactions_started: Counter::new(),
            transactions_committed: Counter::new(),
            transactions_aborted: Counter::new(),
            transaction_duration_ms: Histogram::with_default_buckets(),

            storage_data_size_bytes: Gauge::new(),
            storage_index_size_bytes: Gauge::new(),
            storage_wal_bytes: Counter::new(),
            storage_compactions: Counter::new(),
            storage_compaction_bytes: Counter::new(),
            storage_sst_files: Gauge::new(),
            storage_bloom_filter_usage: Gauge::new(),

            replication_lag_ms: Gauge::new(),
            oplog_size_bytes: Gauge::new(),
            oplog_entries_total: Counter::new(),

            index_hits_total: Counter::new(),
            index_misses_total: Counter::new(),
            index_size_bytes: Gauge::new(),

            gridfs_uploads_total: Counter::new(),
            gridfs_downloads_total: Counter::new(),
            gridfs_size_bytes: Gauge::new(),

            audit_events_total: Counter::new(),
            audit_buffer_size: Gauge::new(),

            auth_failures_total: Counter::new(),
            auth_successes_total: Counter::new(),
            unauthorized_attempts_total: Counter::new(),

            tls_connections_total: Counter::new(),
            tls_handshake_errors: Counter::new(),

            backup_total: Counter::new(),
            backup_duration_ms: Histogram::with_default_buckets(),
            backup_size_bytes: Histogram::new(vec![
                1024.0, 65536.0, 1048576.0, 16777216.0, 268435456.0, 1073741824.0,
            ]),

            uptime_seconds: Gauge::new(),
            memory_usage_bytes: Gauge::new(),
            cpu_usage_percent: Gauge::new(),

            write_conflict_retries: Counter::new(),
            sstable_flushes: Counter::new(),
            wal_bytes_written: Counter::new(),
            checksum_mismatches: Counter::new(),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// 进程级单例。
pub fn global() -> &'static Metrics {
    static G: std::sync::OnceLock<Metrics> = std::sync::OnceLock::new();
    G.get_or_init(Metrics::new)
}

/// 渲染为 Prometheus 文本格式。
pub fn render_prometheus(m: &Metrics) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(8192);

    macro_rules! emit_counter {
        ($name:literal, $help:literal, $v:expr) => {
            let _ = writeln!(s, "# HELP {} {}", $name, $help);
            let _ = writeln!(s, "# TYPE {} counter", $name);
            let _ = writeln!(s, "{}_total {}", $name, $v);
        };
    }

    macro_rules! emit_gauge {
        ($name:literal, $help:literal, $v:expr) => {
            let _ = writeln!(s, "# HELP {} {}", $name, $help);
            let _ = writeln!(s, "# TYPE {} gauge", $name);
            let _ = writeln!(s, "{} {}", $name, $v);
        };
    }

    // -- Connections --
    emit_counter!("mdb_connections", "Total connections accepted", m.connections_total.get());
    emit_gauge!("mdb_connections_open", "Currently open connections", m.connections_open.get());
    emit_gauge!("mdb_connections_authenticated", "Authenticated connections", m.connections_authenticated.get());
    emit_counter!("mdb_connection_errors", "Connection errors", m.connection_errors_total.get());

    // -- Commands --
    emit_counter!("mdb_commands", "Commands executed", m.commands_total.get());
    emit_counter!("mdb_command_errors", "Commands failed", m.command_errors_total.get());
    s.push_str(&m.command_duration_ms.render("mdb_command_duration_ms", "Command execution duration in milliseconds"));

    // -- CRUD --
    emit_counter!("mdb_inserts", "Documents inserted", m.inserts_total.get());
    s.push_str(&m.inserts_duration_ms.render("mdb_inserts_duration_ms", "Insert operation duration in milliseconds"));
    emit_counter!("mdb_finds", "Documents found", m.finds_total.get());
    s.push_str(&m.finds_duration_ms.render("mdb_finds_duration_ms", "Find operation duration in milliseconds"));
    emit_counter!("mdb_updates", "Documents updated", m.updates_total.get());
    s.push_str(&m.updates_duration_ms.render("mdb_updates_duration_ms", "Update operation duration in milliseconds"));
    emit_counter!("mdb_deletes", "Documents deleted", m.deletes_total.get());
    s.push_str(&m.deletes_duration_ms.render("mdb_deletes_duration_ms", "Delete operation duration in milliseconds"));

    // -- Queries --
    emit_counter!("mdb_query_plans", "Query plans created", m.query_plans_total.get());
    emit_counter!("mdb_query_slow", "Slow queries detected", m.query_slow_total.get());
    s.push_str(&m.query_memory_bytes.render("mdb_query_memory_bytes", "Query memory usage in bytes"));

    // -- Cursors --
    emit_gauge!("mdb_cursors_open", "Currently open cursors", m.cursors_open.get());
    emit_counter!("mdb_cursors_total", "Cursors created", m.cursors_total.get());
    emit_counter!("mdb_getmore", "getMore batches", m.getmore_total.get());

    // -- Transactions --
    emit_counter!("mdb_transactions_started", "Transactions started", m.transactions_started.get());
    emit_counter!("mdb_transactions_committed", "Transactions committed", m.transactions_committed.get());
    emit_counter!("mdb_transactions_aborted", "Transactions aborted", m.transactions_aborted.get());
    s.push_str(&m.transaction_duration_ms.render("mdb_transaction_duration_ms", "Transaction duration in milliseconds"));

    // -- Storage --
    emit_gauge!("mdb_storage_data_size_bytes", "Storage data size in bytes", m.storage_data_size_bytes.get());
    emit_gauge!("mdb_storage_index_size_bytes", "Storage index size in bytes", m.storage_index_size_bytes.get());
    emit_counter!("mdb_storage_wal_bytes", "WAL bytes written", m.storage_wal_bytes.get());
    emit_counter!("mdb_storage_compactions", "Storage compactions", m.storage_compactions.get());
    emit_counter!("mdb_storage_compaction_bytes", "Storage compaction bytes", m.storage_compaction_bytes.get());
    emit_gauge!("mdb_storage_sst_files", "SSTable file count", m.storage_sst_files.get());
    emit_gauge!("mdb_storage_bloom_filter_usage", "Bloom filter usage ratio", m.storage_bloom_filter_usage.get());

    // -- Replication --
    emit_gauge!("mdb_replication_lag_ms", "Replication lag in milliseconds", m.replication_lag_ms.get());
    emit_gauge!("mdb_oplog_size_bytes", "Oplog size in bytes", m.oplog_size_bytes.get());
    emit_counter!("mdb_oplog_entries", "Oplog entries", m.oplog_entries_total.get());

    // -- Index --
    emit_counter!("mdb_index_hits", "Index hits", m.index_hits_total.get());
    emit_counter!("mdb_index_misses", "Index misses", m.index_misses_total.get());
    emit_gauge!("mdb_index_size_bytes", "Index size in bytes", m.index_size_bytes.get());

    // -- GridFS --
    emit_counter!("mdb_gridfs_uploads", "GridFS uploads", m.gridfs_uploads_total.get());
    emit_counter!("mdb_gridfs_downloads", "GridFS downloads", m.gridfs_downloads_total.get());
    emit_gauge!("mdb_gridfs_size_bytes", "GridFS storage size in bytes", m.gridfs_size_bytes.get());

    // -- Audit --
    emit_counter!("mdb_audit_events", "Audit events", m.audit_events_total.get());
    emit_gauge!("mdb_audit_buffer_size", "Audit buffer size", m.audit_buffer_size.get());

    // -- Security --
    emit_counter!("mdb_auth_failures", "Authentication failures", m.auth_failures_total.get());
    emit_counter!("mdb_auth_successes", "Authentication successes", m.auth_successes_total.get());
    emit_counter!("mdb_unauthorized_attempts", "Unauthorized access attempts", m.unauthorized_attempts_total.get());

    // -- TLS --
    emit_counter!("mdb_tls_connections", "TLS connections", m.tls_connections_total.get());
    emit_counter!("mdb_tls_handshake_errors", "TLS handshake errors", m.tls_handshake_errors.get());

    // -- Backup --
    emit_counter!("mdb_backups", "Backup operations", m.backup_total.get());
    s.push_str(&m.backup_duration_ms.render("mdb_backup_duration_ms", "Backup duration in milliseconds"));
    s.push_str(&m.backup_size_bytes.render("mdb_backup_size_bytes", "Backup size in bytes"));

    // -- System --
    emit_gauge!("mdb_uptime_seconds", "Server uptime in seconds", m.uptime_seconds.get());
    emit_gauge!("mdb_memory_usage_bytes", "Memory usage in bytes", m.memory_usage_bytes.get());
    emit_gauge!("mdb_cpu_usage_percent", "CPU usage percent", m.cpu_usage_percent.get());

    // -- Legacy --
    emit_counter!("mdb_write_conflict_retries", "Write conflict retries", m.write_conflict_retries.get());
    emit_counter!("mdb_sstable_flushes", "SSTable flushes", m.sstable_flushes.get());
    emit_counter!("mdb_wal_bytes_written", "WAL bytes written (legacy)", m.wal_bytes_written.get());
    emit_counter!("mdb_checksum_mismatches", "Checksum mismatches", m.checksum_mismatches.get());

    s
}

/// 进程上下文：datapath / threadpool 名称、实例身份。
///
/// MongoDB 的日志 `id` 字段是 5 字符十六进制组件 id（如 `NETWORK`、`ACCESS`）。
/// MDB 保持该格式，使运维工具的日志过滤器无需修改。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Context {
    /// 组件 5 字符大写名。
    pub component: &'static str,
}

impl Context {
    pub const fn new(component: &'static str) -> Self {
        Self { component }
    }

    /// MongoDB 内置组件。
    pub const ACCESS: Context = Context::new("ACCESS");
    pub const NETWORK: Context = Context::new("NETWORK");
    pub const COMMAND: Context = Context::new("COMMAND");
    pub const STORAGE: Context = Context::new("STORAGE");
    pub const REPL: Context = Context::new("REPL");
    pub const TXN: Context = Context::new("TXN");
    pub const QUERY: Context = Context::new("QUERY");
    pub const SHARDING: Context = Context::new("SHARDING");
    pub const AUDIT: Context = Context::new("AUDIT");
    pub const STARTUP: Context = Context::new("STARTUP");
}

impl std::fmt::Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.component)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_are_monotonic_under_concurrency() {
        let c = Counter::new();
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for _ in 0..1000 {
                        c.inc();
                    }
                });
            }
        });
        assert_eq!(c.get(), 8000);
    }

    #[test]
    fn prometheus_output_has_help_and_type() {
        let s = render_prometheus(&Metrics::new());
        assert!(s.contains("# TYPE mdb_commands counter"));
        assert!(s.contains("mdb_checksum_mismatches_total 0"));
    }

    #[test]
    fn prometheus_output_contains_all_sections() {
        let s = render_prometheus(&Metrics::new());
        // Connections
        assert!(s.contains("mdb_connections_total"));
        assert!(s.contains("mdb_connections_open"));
        assert!(s.contains("mdb_connections_authenticated"));
        assert!(s.contains("mdb_connection_errors_total"));
        // Commands
        assert!(s.contains("mdb_commands_total"));
        assert!(s.contains("mdb_command_errors_total"));
        assert!(s.contains("mdb_command_duration_ms_count"));
        assert!(s.contains("mdb_command_duration_ms_sum"));
        // CRUD
        assert!(s.contains("mdb_inserts_total"));
        assert!(s.contains("mdb_finds_total"));
        assert!(s.contains("mdb_updates_total"));
        assert!(s.contains("mdb_deletes_total"));
        assert!(s.contains("mdb_inserts_duration_ms_count"));
        assert!(s.contains("mdb_finds_duration_ms_count"));
        // Queries
        assert!(s.contains("mdb_query_plans_total"));
        assert!(s.contains("mdb_query_slow_total"));
        // Cursors
        assert!(s.contains("mdb_cursors_open"));
        assert!(s.contains("mdb_getmore_total"));
        // Transactions
        assert!(s.contains("mdb_transactions_started_total"));
        assert!(s.contains("mdb_transactions_committed_total"));
        assert!(s.contains("mdb_transactions_aborted_total"));
        assert!(s.contains("mdb_transaction_duration_ms_count"));
        // Storage
        assert!(s.contains("mdb_storage_data_size_bytes"));
        assert!(s.contains("mdb_storage_wal_bytes_total"));
        assert!(s.contains("mdb_storage_compactions_total"));
        // Replication
        assert!(s.contains("mdb_replication_lag_ms"));
        assert!(s.contains("mdb_oplog_entries_total"));
        // Index
        assert!(s.contains("mdb_index_hits_total"));
        assert!(s.contains("mdb_index_misses_total"));
        // GridFS
        assert!(s.contains("mdb_gridfs_uploads_total"));
        assert!(s.contains("mdb_gridfs_downloads_total"));
        // Security
        assert!(s.contains("mdb_auth_failures_total"));
        assert!(s.contains("mdb_auth_successes_total"));
        assert!(s.contains("mdb_unauthorized_attempts_total"));
        // TLS
        assert!(s.contains("mdb_tls_connections_total"));
        assert!(s.contains("mdb_tls_handshake_errors_total"));
        // Backup
        assert!(s.contains("mdb_backups_total"));
        assert!(s.contains("mdb_backup_duration_ms_count"));
        // System
        assert!(s.contains("mdb_uptime_seconds"));
        assert!(s.contains("mdb_memory_usage_bytes"));
        assert!(s.contains("mdb_cpu_usage_percent"));
    }

    #[test]
    fn histogram_buckets_render_correctly() {
        let h = Histogram::new(vec![1.0, 2.0, 5.0]);
        h.observe(0.5);
        h.observe(1.5);
        h.observe(3.0);
        h.observe(10.0);
        let rendered = h.render("test_bucket", "test help");
        assert!(rendered.contains("test_bucket_bucket{le=\"1\"} 1"));
        assert!(rendered.contains("test_bucket_bucket{le=\"2\"} 2"));
        assert!(rendered.contains("test_bucket_bucket{le=\"5\"} 3"));
        assert!(rendered.contains("test_bucket_bucket{le=\"+Inf\"} 4"));
        assert!(rendered.contains("test_bucket_count 4"));
        assert!(rendered.contains("test_bucket_sum"));
    }

    #[test]
    fn histogram_tracks_sum() {
        let h = Histogram::new(vec![1.0, 10.0]);
        h.observe(2.5);
        h.observe(7.5);
        assert_eq!(h.count.load(Ordering::Relaxed), 2);
        // sum = 10000 microseconds = 10.0 f64 (2500 + 7500)
        assert_eq!(h.sum.load(Ordering::Relaxed), 10000);
    }

    #[test]
    fn gauge_inc_dec() {
        let g = Gauge::new();
        g.inc();
        g.inc();
        assert_eq!(g.get(), 2);
        g.dec();
        assert_eq!(g.get(), 1);
    }

    #[test]
    fn counter_add() {
        let c = Counter::new();
        c.add(5);
        c.add(3);
        assert_eq!(c.get(), 8);
    }

    #[test]
    fn metrics_new_initializes_all_fields() {
        let m = Metrics::new();
        assert_eq!(m.connections_total.get(), 0);
        assert_eq!(m.connections_open.get(), 0);
        assert_eq!(m.inserts_total.get(), 0);
        assert_eq!(m.finds_total.get(), 0);
        assert_eq!(m.updates_total.get(), 0);
        assert_eq!(m.deletes_total.get(), 0);
        assert_eq!(m.transactions_committed.get(), 0);
        assert_eq!(m.transactions_aborted.get(), 0);
        assert_eq!(m.auth_failures_total.get(), 0);
        assert_eq!(m.auth_successes_total.get(), 0);
        assert_eq!(m.backup_total.get(), 0);
    }

    #[test]
    fn context_ids_are_distinct_uppercase_ascii() {
        let all = [
            Context::ACCESS,
            Context::NETWORK,
            Context::COMMAND,
            Context::STORAGE,
            Context::REPL,
            Context::TXN,
            Context::QUERY,
            Context::SHARDING,
            Context::AUDIT,
            Context::STARTUP,
        ];
        for c in all {
            assert!(!c.component.is_empty());
            assert!(
                c.component.bytes().all(|b| b.is_ascii_uppercase()),
                "context id must be ASCII uppercase: {}",
                c.component
            );
        }
        let mut names: Vec<&str> = all.iter().map(|c| c.component).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(names.len(), before, "duplicate context id");
    }
}
