//! 查询性能分析器：记录慢查询、统计执行时间。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use mdb_doc::Document;

use crate::explain::ExplainPlan;

/// 查询性能分析器。
///
/// 记录所有超过阈值的慢查询，并提供统计信息。
/// 线程安全，可被多个执行器并发使用。
#[derive(Debug, Clone)]
pub struct QueryProfiler {
    /// 是否启用分析。
    enabled: Arc<AtomicBool>,
    /// 慢查询阈值（毫秒）。
    slow_ms: Arc<AtomicU64>,
    /// 慢查询日志。
    slow_queries: Arc<Mutex<Vec<SlowQueryLog>>>,
    /// 统计信息。
    stats: Arc<Mutex<ProfilerStats>>,
    /// 命名空间。
    namespace: Arc<String>,
}

/// 慢查询日志条目。
#[derive(Debug, Clone)]
pub struct SlowQueryLog {
    /// 时间戳（毫秒，自纪元起）。
    pub timestamp: i64,
    /// 命名空间（如 "test.users"）。
    pub namespace: String,
    /// 操作类型（"query", "find", "aggregate" 等）。
    pub operation: String,
    /// 查询命令文档。
    pub command: Document,
    /// 执行时间（毫秒）。
    pub duration_ms: u64,
    /// 返回的文档数。
    pub n_returned: u64,
    /// 执行计划（可选）。
    pub plan: Option<ExplainPlan>,
}

/// 分析器统计信息。
#[derive(Debug, Clone, Default)]
pub struct ProfilerStats {
    /// 总查询数。
    pub total_queries: u64,
    /// 慢查询数。
    pub slow_queries: u64,
    /// 总执行时间（毫秒）。
    pub total_time_ms: u64,
    /// 按操作类型统计计数。
    pub op_counts: HashMap<String, u64>,
}

impl QueryProfiler {
    /// 创建新的分析器，默认禁用。
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            enabled: Arc::new(AtomicBool::new(false)),
            slow_ms: Arc::new(AtomicU64::new(100)),
            slow_queries: Arc::new(Mutex::new(Vec::new())),
            stats: Arc::new(Mutex::new(ProfilerStats::default())),
            namespace: Arc::new(namespace.into()),
        }
    }

    /// 创建并启用分析器。
    pub fn enabled(namespace: impl Into<String>, slow_ms: u64) -> Self {
        Self {
            enabled: Arc::new(AtomicBool::new(true)),
            slow_ms: Arc::new(AtomicU64::new(slow_ms)),
            slow_queries: Arc::new(Mutex::new(Vec::new())),
            stats: Arc::new(Mutex::new(ProfilerStats::default())),
            namespace: Arc::new(namespace.into()),
        }
    }

    /// 启用分析。
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    /// 禁用分析。
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    /// 检查是否启用。
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// 设置慢查询阈值。
    pub fn set_slow_ms(&self, ms: u64) {
        self.slow_ms.store(ms, Ordering::Relaxed);
    }

    /// 获取慢查询阈值。
    pub fn slow_ms(&self) -> u64 {
        self.slow_ms.load(Ordering::Relaxed)
    }

    /// 记录查询执行。
    pub fn record_query(
        &self,
        operation: &str,
        command: Document,
        duration: Instant,
        n_returned: u64,
        plan: Option<ExplainPlan>,
    ) {
        let duration_ms = duration.elapsed().as_millis() as u64;

        // 更新统计
        if let Ok(mut stats) = self.stats.lock() {
            stats.total_queries += 1;
            stats.total_time_ms += duration_ms;
            *stats.op_counts.entry(operation.to_owned()).or_insert(0) += 1;
        }

        // 检查是否为慢查询
        if self.is_enabled() && duration_ms >= self.slow_ms() {
            let log = SlowQueryLog {
                timestamp: chrono_timestamp(),
                namespace: (*self.namespace).clone(),
                operation: operation.to_owned(),
                command,
                duration_ms,
                n_returned,
                plan,
            };

            if let Ok(mut slow) = self.slow_queries.lock() {
                slow.push(log);
                if let Ok(mut stats) = self.stats.lock() {
                    stats.slow_queries += 1;
                }
            }
        }
    }

    /// 获取慢查询日志的副本。
    pub fn slow_queries(&self) -> Vec<SlowQueryLog> {
        self.slow_queries.lock().map(|q| q.clone()).unwrap_or_default()
    }

    /// 获取统计信息的副本。
    pub fn stats(&self) -> ProfilerStats {
        self.stats.lock().map(|s| s.clone()).unwrap_or_default()
    }

    /// 清空慢查询日志。
    pub fn clear_slow_queries(&self) {
        if let Ok(mut q) = self.slow_queries.lock() {
            q.clear();
        }
    }

    /// 重置统计信息。
    pub fn reset_stats(&self) {
        if let Ok(mut s) = self.stats.lock() {
            *s = ProfilerStats::default();
        }
    }
}

/// 获取当前时间戳（毫秒）。
fn chrono_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// 查询执行计时器。
#[derive(Debug)]
pub struct QueryTimer {
    start: Instant,
}

impl QueryTimer {
    /// 开始计时。
    pub fn start() -> Self {
        Self { start: Instant::now() }
    }

    /// 获取已用时间（毫秒）。
    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// 获取 Instant 用于传给分析器。
    pub fn instant(&self) -> Instant {
        self.start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiler_new_disabled() {
        let p = QueryProfiler::new("test.users");
        assert!(!p.is_enabled());
        assert_eq!(p.slow_ms(), 100);
    }

    #[test]
    fn profiler_enabled() {
        let p = QueryProfiler::enabled("test.users", 50);
        assert!(p.is_enabled());
        assert_eq!(p.slow_ms(), 50);
    }

    #[test]
    fn profiler_enable_disable() {
        let p = QueryProfiler::new("test.users");
        assert!(!p.is_enabled());
        p.enable();
        assert!(p.is_enabled());
        p.disable();
        assert!(!p.is_enabled());
    }

    #[test]
    fn profiler_set_slow_ms() {
        let p = QueryProfiler::new("test.users");
        p.set_slow_ms(200);
        assert_eq!(p.slow_ms(), 200);
    }

    #[test]
    fn profiler_record_query_no_log_when_disabled() {
        let p = QueryProfiler::new("test.users");
        let mut cmd = Document::new();
        cmd.insert("find", mdb_doc::Bson::String("users".into()));

        p.record_query("query", cmd, Instant::now(), 10, None);
        assert!(p.slow_queries().is_empty());

        let stats = p.stats();
        assert_eq!(stats.total_queries, 1);
        assert_eq!(stats.slow_queries, 0);
        assert_eq!(*stats.op_counts.get("query").unwrap(), 1);
    }

    #[test]
    fn profiler_record_query_logs_slow_when_enabled() {
        let p = QueryProfiler::enabled("test.users", 0);
        let mut cmd = Document::new();
        cmd.insert("find", mdb_doc::Bson::String("users".into()));

        // Use a start time from the past to ensure duration > 0
        let start = Instant::now() - std::time::Duration::from_millis(1);
        p.record_query("find", cmd, start, 5, None);

        let slow = p.slow_queries();
        assert_eq!(slow.len(), 1);
        assert_eq!(slow[0].operation, "find");
        assert_eq!(slow[0].n_returned, 5);
        assert!(slow[0].duration_ms >= 1);

        let stats = p.stats();
        assert_eq!(stats.total_queries, 1);
        assert_eq!(stats.slow_queries, 1);
    }

    #[test]
    fn profiler_clear_slow_queries() {
        let p = QueryProfiler::enabled("test.users", 0);
        let mut cmd = Document::new();
        cmd.insert("find", mdb_doc::Bson::String("users".into()));
        std::thread::sleep(std::time::Duration::from_millis(1));
        p.record_query("find", cmd, Instant::now(), 0, None);
        assert!(!p.slow_queries().is_empty());

        p.clear_slow_queries();
        assert!(p.slow_queries().is_empty());
    }

    #[test]
    fn profiler_reset_stats() {
        let p = QueryProfiler::new("test.users");
        let mut cmd = Document::new();
        cmd.insert("find", mdb_doc::Bson::String("users".into()));
        p.record_query("find", cmd, Instant::now(), 0, None);

        assert_eq!(p.stats().total_queries, 1);
        p.reset_stats();
        let stats = p.stats();
        assert_eq!(stats.total_queries, 0);
        assert_eq!(stats.slow_queries, 0);
    }

    #[test]
    fn profiler_op_counts() {
        let p = QueryProfiler::new("test.users");
        let cmd = Document::new();

        p.record_query("find", cmd.clone(), Instant::now(), 0, None);
        p.record_query("find", cmd.clone(), Instant::now(), 0, None);
        p.record_query("aggregate", cmd, Instant::now(), 0, None);

        let stats = p.stats();
        assert_eq!(stats.total_queries, 3);
        assert_eq!(*stats.op_counts.get("find").unwrap(), 2);
        assert_eq!(*stats.op_counts.get("aggregate").unwrap(), 1);
    }

    #[test]
    fn profiler_clone() {
        let p1 = QueryProfiler::enabled("test.users", 50);
        let p2 = p1.clone();
        assert!(p2.is_enabled());
        assert_eq!(p2.slow_ms(), 50);
    }

    #[test]
    fn query_timer() {
        let timer = QueryTimer::start();
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(timer.elapsed_ms() >= 1);
    }
}
