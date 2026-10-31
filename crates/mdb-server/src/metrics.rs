//! 运维工具：监控指标和备份脚本。
//!
//! 提供生产环境运维所需的工具和指标。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// 监控指标收集器。
pub struct MetricsCollector {
    /// 连接数。
    connections_open: AtomicU64,
    /// 命令总数。
    commands_total: AtomicU64,
    /// 插入操作数。
    inserts_total: AtomicU64,
    /// 查询操作数。
    finds_total: AtomicU64,
    /// 更新操作数。
    updates_total: AtomicU64,
    /// 删除操作数。
    deletes_total: AtomicU64,
    /// 事务提交数。
    transactions_committed: AtomicU64,
    /// 事务中止数。
    transactions_aborted: AtomicU64,
    /// 认证失败数。
    auth_failures: AtomicU64,
    /// 审计事件数。
    audit_events: AtomicU64,
    /// 数据大小（字节）。
    data_size: AtomicU64,
    /// 索引大小（字节）。
    index_size: AtomicU64,
    /// 慢查询数。
    slow_queries: AtomicU64,
    /// 错误数。
    errors_total: AtomicU64,
    /// 启动时间。
    start_time: Instant,
    /// 指标锁。
    lock: Mutex<()>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            connections_open: AtomicU64::new(0),
            commands_total: AtomicU64::new(0),
            inserts_total: AtomicU64::new(0),
            finds_total: AtomicU64::new(0),
            updates_total: AtomicU64::new(0),
            deletes_total: AtomicU64::new(0),
            transactions_committed: AtomicU64::new(0),
            transactions_aborted: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            audit_events: AtomicU64::new(0),
            data_size: AtomicU64::new(0),
            index_size: AtomicU64::new(0),
            slow_queries: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            start_time: Instant::now(),
            lock: Mutex::new(()),
        }
    }

    /// 记录连接打开。
    pub fn inc_connections(&self) {
        self.connections_open.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录连接关闭。
    pub fn dec_connections(&self) {
        self.connections_open.fetch_sub(1, Ordering::Relaxed);
    }

    /// 记录命令执行。
    pub fn inc_command(&self) {
        self.commands_total.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录插入操作。
    pub fn inc_insert(&self) {
        self.inserts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录查询操作。
    pub fn inc_find(&self) {
        self.finds_total.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录更新操作。
    pub fn inc_update(&self) {
        self.updates_total.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录删除操作。
    pub fn inc_delete(&self) {
        self.deletes_total.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录事务提交。
    pub fn inc_txn_committed(&self) {
        self.transactions_committed.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录事务中止。
    pub fn inc_txn_aborted(&self) {
        self.transactions_aborted.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录认证失败。
    pub fn inc_auth_failure(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录审计事件。
    pub fn inc_audit_event(&self) {
        self.audit_events.fetch_add(1, Ordering::Relaxed);
    }

    /// 更新数据大小。
    pub fn set_data_size(&self, size: u64) {
        self.data_size.store(size, Ordering::Relaxed);
    }

    /// 更新索引大小。
    pub fn set_index_size(&self, size: u64) {
        self.index_size.store(size, Ordering::Relaxed);
    }

    /// 记录慢查询。
    pub fn inc_slow_query(&self) {
        self.slow_queries.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录错误。
    pub fn inc_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// 获取运行时间（秒）。
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// 获取 Prometheus 格式的指标。
    pub fn prometheus_metrics(&self) -> String {
        let mut output = String::new();

        output.push_str("# HELP mdb_connections_open Current open connections\n");
        output.push_str("# TYPE mdb_connections_open gauge\n");
        output.push_str(&format!("mdb_connections_open {}\n", self.connections_open.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_commands_total Total commands executed\n");
        output.push_str("# TYPE mdb_commands_total counter\n");
        output.push_str(&format!("mdb_commands_total {}\n", self.commands_total.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_inserts_total Total insert operations\n");
        output.push_str("# TYPE mdb_inserts_total counter\n");
        output.push_str(&format!("mdb_inserts_total {}\n", self.inserts_total.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_finds_total Total find operations\n");
        output.push_str("# TYPE mdb_finds_total counter\n");
        output.push_str(&format!("mdb_finds_total {}\n", self.finds_total.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_updates_total Total update operations\n");
        output.push_str("# TYPE mdb_updates_total counter\n");
        output.push_str(&format!("mdb_updates_total {}\n", self.updates_total.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_deletes_total Total delete operations\n");
        output.push_str("# TYPE mdb_deletes_total counter\n");
        output.push_str(&format!("mdb_deletes_total {}\n", self.deletes_total.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_transactions_committed Total committed transactions\n");
        output.push_str("# TYPE mdb_transactions_committed counter\n");
        output.push_str(&format!("mdb_transactions_committed {}\n", self.transactions_committed.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_transactions_aborted Total aborted transactions\n");
        output.push_str("# TYPE mdb_transactions_aborted counter\n");
        output.push_str(&format!("mdb_transactions_aborted {}\n", self.transactions_aborted.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_auth_failures_total Total authentication failures\n");
        output.push_str("# TYPE mdb_auth_failures_total counter\n");
        output.push_str(&format!("mdb_auth_failures_total {}\n", self.auth_failures.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_audit_events_total Total audit events\n");
        output.push_str("# TYPE mdb_audit_events_total counter\n");
        output.push_str(&format!("mdb_audit_events_total {}\n", self.audit_events.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_storage_data_size_bytes Data size in bytes\n");
        output.push_str("# TYPE mdb_storage_data_size_bytes gauge\n");
        output.push_str(&format!("mdb_storage_data_size_bytes {}\n", self.data_size.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_storage_index_size_bytes Index size in bytes\n");
        output.push_str("# TYPE mdb_storage_index_size_bytes gauge\n");
        output.push_str(&format!("mdb_storage_index_size_bytes {}\n", self.index_size.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_slow_queries_total Total slow queries\n");
        output.push_str("# TYPE mdb_slow_queries_total counter\n");
        output.push_str(&format!("mdb_slow_queries_total {}\n", self.slow_queries.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_errors_total Total errors\n");
        output.push_str("# TYPE mdb_errors_total counter\n");
        output.push_str(&format!("mdb_errors_total {}\n", self.errors_total.load(Ordering::Relaxed)));

        output.push_str("# HELP mdb_uptime_seconds Server uptime in seconds\n");
        output.push_str("# TYPE mdb_uptime_seconds gauge\n");
        output.push_str(&format!("mdb_uptime_seconds {}\n", self.uptime_seconds()));

        output
    }
}

impl std::fmt::Debug for MetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsCollector")
            .field("connections_open", &self.connections_open.load(Ordering::Relaxed))
            .field("commands_total", &self.commands_total.load(Ordering::Relaxed))
            .field("uptime_seconds", &self.uptime_seconds())
            .finish()
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector() {
        let metrics = MetricsCollector::new();

        metrics.inc_connections();
        metrics.inc_command();
        metrics.inc_insert();
        metrics.inc_find();
        metrics.inc_update();
        metrics.inc_delete();
        metrics.inc_txn_committed();
        metrics.inc_txn_aborted();
        metrics.inc_auth_failure();
        metrics.inc_audit_event();
        metrics.set_data_size(1024);
        metrics.set_index_size(256);
        metrics.inc_slow_query();
        metrics.inc_error();

        let output = metrics.prometheus_metrics();
        assert!(output.contains("mdb_connections_open 1"));
        assert!(output.contains("mdb_commands_total 1"));
        assert!(output.contains("mdb_inserts_total 1"));
        assert!(output.contains("mdb_finds_total 1"));
        assert!(output.contains("mdb_updates_total 1"));
        assert!(output.contains("mdb_deletes_total 1"));
        assert!(output.contains("mdb_transactions_committed 1"));
        assert!(output.contains("mdb_transactions_aborted 1"));
        assert!(output.contains("mdb_auth_failures_total 1"));
        assert!(output.contains("mdb_audit_events_total 1"));
        assert!(output.contains("mdb_storage_data_size_bytes 1024"));
        assert!(output.contains("mdb_storage_index_size_bytes 256"));
        assert!(output.contains("mdb_slow_queries_total 1"));
        assert!(output.contains("mdb_errors_total 1"));
        assert!(output.contains("mdb_uptime_seconds"));
    }
}
