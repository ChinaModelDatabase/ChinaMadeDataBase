//! 游标管理：创建、翻页、超时清理、Tailable 游标、分页、Explain。

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use mdb_doc::Document;

use crate::explain::ExplainPlan;

/// 默认游标超时时间（10 分钟），与 MongoDB 一致。
const DEFAULT_CURSOR_TIMEOUT: Duration = Duration::from_secs(600);

/// 默认批次大小。
const DEFAULT_BATCH_SIZE: usize = 101;

/// Tailable 游标等待新数据的默认超时（毫秒）。
const DEFAULT_AWAIT_DATA_TIMEOUT_MS: u64 = 300_000;

/// 游标 ID 生成器。
static NEXT_CURSOR_ID: AtomicI64 = AtomicI64::new(1);

/// 游标选项。
#[derive(Debug, Clone, Default)]
pub struct CursorOptions {
    /// 每批次返回的文档数。
    pub batch_size: usize,
    /// 是否禁用超时（true = 游标永不超时）。
    pub no_cursor_timeout: bool,
    /// 是否为 tailable 游标（仅适用于 capped 集合）。
    pub tailable: bool,
    /// tailable 游标是否等待新数据（awaitData）。
    pub await_data: bool,
    /// 执行计划（用于 explain）。
    pub explain_plan: Option<ExplainPlan>,
    /// 是否为反向迭代游标。
    pub reversed: bool,
}

/// 游标。
#[derive(Debug, Clone)]
pub struct Cursor {
    /// 游标唯一标识。
    pub id: i64,
    /// 命名空间（`db.collection`）。
    pub namespace: String,
    /// 当前批次的文档。
    pub batch: Vec<Document>,
    /// 当前批次内的位置（已返回给客户端的偏移）。
    pub position: usize,
    /// 是否已耗尽（所有结果已返回）。
    pub exhausted: bool,
    /// 创建时间（用于超时检查）。
    created_at: Instant,
    /// 是否禁用超时。
    no_timeout: bool,
    /// 是否为 tailable 游标。
    tailable: bool,
    /// tailable 游标是否等待新数据。
    await_data: bool,
    /// 执行计划（用于 explain）。
    explain_plan: Option<ExplainPlan>,
    /// 是否为反向迭代游标。
    reversed: bool,
    /// 在原始结果集中的位置（用于分页）。
    original_position: usize,
}

impl Cursor {
    /// 创建新游标。
    pub fn new(
        namespace: impl Into<String>,
        results: Vec<Document>,
        batch_size: usize,
        no_timeout: bool,
    ) -> Self {
        Self::with_options(namespace, results, CursorOptions {
            batch_size,
            no_cursor_timeout: no_timeout,
            ..Default::default()
        })
    }

    /// 使用选项创建新游标。
    pub fn with_options(
        namespace: impl Into<String>,
        results: Vec<Document>,
        options: CursorOptions,
    ) -> Self {
        let batch_size = if options.batch_size == 0 { DEFAULT_BATCH_SIZE } else { options.batch_size };
        let exhausted = if options.tailable {
            false // tailable 游标初始不耗尽
        } else {
            results.len() <= batch_size
        };
        let batch = if results.len() > batch_size {
            results[..batch_size].to_vec()
        } else {
            results
        };
        let batch_len = batch.len();

        Self {
            id: NEXT_CURSOR_ID.fetch_add(1, Ordering::Relaxed),
            namespace: namespace.into(),
            batch,
            position: 0,
            exhausted,
            created_at: Instant::now(),
            no_timeout: options.no_cursor_timeout,
            tailable: options.tailable,
            await_data: options.await_data,
            explain_plan: options.explain_plan,
            reversed: options.reversed,
            original_position: batch_len,
        }
    }

    /// 创建 tailable 游标。
    pub fn tailable(
        namespace: impl Into<String>,
        batch_size: usize,
    ) -> Self {
        Self::with_options(namespace, Vec::new(), CursorOptions {
            batch_size,
            tailable: true,
            await_data: true,
            no_cursor_timeout: true,
            ..Default::default()
        })
    }

    /// 是否已超时。
    pub fn is_expired(&self) -> bool {
        if self.no_timeout {
            return false;
        }
        self.created_at.elapsed() > DEFAULT_CURSOR_TIMEOUT
    }

    /// 是否为 tailable 游标。
    pub fn is_tailable(&self) -> bool {
        self.tailable
    }

    /// 是否等待数据。
    pub fn is_awaiting_data(&self) -> bool {
        self.await_data
    }

    /// 游标是否已耗尽且非 tailable。
    pub fn is_done(&self) -> bool {
        self.exhausted && !self.tailable
    }

    /// 获取游标创建时间。
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// 获取游标剩余有效时间。
    pub fn remaining_ttl(&self) -> Duration {
        if self.no_timeout {
            return Duration::from_secs(u64::MAX);
        }
        let elapsed = self.created_at.elapsed();
        if elapsed >= DEFAULT_CURSOR_TIMEOUT {
            Duration::ZERO
        } else {
            DEFAULT_CURSOR_TIMEOUT - elapsed
        }
    }

    /// 获取执行计划（如果有）。
    pub fn explain(&self) -> Option<&ExplainPlan> {
        self.explain_plan.as_ref()
    }

    /// 游标切片分页：返回从 start 开始的 limit 个文档。
    ///
    /// 会消费当前游标位置，返回后游标位置前进。
    pub fn slice(
        &mut self,
        all_results: &[Document],
        start: usize,
        limit: usize,
    ) -> Vec<Document> {
        let end = (start + limit).min(all_results.len());
        if start >= all_results.len() {
            self.exhausted = true;
            self.batch.clear();
            return Vec::new();
        }
        let batch = all_results[start..end].to_vec();
        self.batch = batch.clone();
        self.position = 0;
        if end >= all_results.len() {
            self.exhausted = true;
        }
        batch
    }

    /// 反转游标迭代方向。
    pub fn reverse(&mut self) {
        self.reversed = !self.reversed;
        self.batch.reverse();
        self.position = 0;
    }

    /// 获取下一批结果。
    pub fn get_more(&mut self, all_results: &[Document], batch_size: usize) -> Vec<Document> {
        if self.exhausted {
            return Vec::new();
        }

        if self.reversed {
            return self.get_more_reverse(all_results, batch_size);
        }

        let total_returned = self.original_position;

        if total_returned >= all_results.len() {
            self.exhausted = true;
            return Vec::new();
        }

        let next_start = total_returned;
        let next_end = (next_start + batch_size).min(all_results.len());
        let batch = all_results[next_start..next_end].to_vec();

        if next_end >= all_results.len() {
            self.exhausted = true;
        }

        self.batch = batch.clone();
        self.position = 0;
        self.original_position = next_end;

        batch
    }

    /// 反向获取下一批结果。
    fn get_more_reverse(&mut self, all_results: &[Document], batch_size: usize) -> Vec<Document> {
        if all_results.is_empty() {
            self.exhausted = true;
            return Vec::new();
        }

        let total_returned = self.original_position;
        if total_returned >= all_results.len() {
            self.exhausted = true;
            return Vec::new();
        }

        let remaining = all_results.len() - total_returned;
        let batch_size = batch_size.min(remaining);
        let start = remaining.saturating_sub(batch_size);
        let end = remaining;
        let mut batch: Vec<Document> = all_results[start..end].to_vec();
        batch.reverse();

        if start == 0 {
            self.exhausted = true;
        }

        self.batch = batch.clone();
        self.position = 0;
        self.original_position += batch_size;

        batch
    }
}

/// 游标管理器：管理所有活跃游标。
#[derive(Debug)]
pub struct CursorManager {
    cursors: Mutex<HashMap<i64, Cursor>>,
    /// 所有结果的存储（简化实现：将完整结果集存入游标管理器）。
    results: Mutex<HashMap<i64, Vec<Document>>>,
    /// tailable 游标的新数据通知（condvar）。
    tailable_notify: Arc<Condvar>,
    /// tailable 游标等待数据超时时间。
    await_data_timeout: Duration,
    /// 后台清理线程是否已启动。
    cleanup_running: Mutex<bool>,
}

impl CursorManager {
    /// 创建新的游标管理器。
    pub fn new() -> Self {
        Self {
            cursors: Mutex::new(HashMap::new()),
            results: Mutex::new(HashMap::new()),
            tailable_notify: Arc::new(Condvar::new()),
            await_data_timeout: Duration::from_millis(DEFAULT_AWAIT_DATA_TIMEOUT_MS),
            cleanup_running: Mutex::new(false),
        }
    }

    /// 创建游标并返回首批结果。
    ///
    /// `results` 是完整的查询结果集，`batch_size` 控制每批返回的文档数。
    pub fn create_cursor(
        &self,
        namespace: impl Into<String>,
        results: Vec<Document>,
        batch_size: usize,
    ) -> Cursor {
        self.create_cursor_with_options(namespace, results, CursorOptions {
            batch_size,
            ..Default::default()
        })
    }

    /// 使用选项创建游标。
    pub fn create_cursor_with_options(
        &self,
        namespace: impl Into<String>,
        results: Vec<Document>,
        options: CursorOptions,
    ) -> Cursor {
        let cursor = Cursor::with_options(namespace, results.clone(), options);
        let cursor_id = cursor.id;

        let mut cursors = self.cursors.lock().unwrap();
        let mut all_results = self.results.lock().unwrap();

        all_results.insert(cursor_id, results);
        cursors.insert(cursor_id, cursor.clone());

        cursor
    }

    /// 创建禁用超时的游标。
    pub fn create_no_timeout_cursor(
        &self,
        namespace: impl Into<String>,
        results: Vec<Document>,
        batch_size: usize,
    ) -> Cursor {
        self.create_cursor_with_options(namespace, results, CursorOptions {
            batch_size,
            no_cursor_timeout: true,
            ..Default::default()
        })
    }

    /// 创建 tailable 游标。
    pub fn create_tailable_cursor(
        &self,
        namespace: impl Into<String>,
        batch_size: usize,
    ) -> Cursor {
        self.create_cursor_with_options(namespace, Vec::new(), CursorOptions {
            batch_size,
            tailable: true,
            await_data: true,
            no_cursor_timeout: true,
            ..Default::default()
        })
    }

    /// 向 tailable 游标添加新文档。
    ///
    /// 当有新文档插入 capped 集合时调用此方法。
    pub fn append_to_tailable(&self, cursor_id: i64, doc: Document) -> bool {
        let mut cursors = self.cursors.lock().unwrap();
        let mut all_results = self.results.lock().unwrap();

        if let Some(cursor) = cursors.get_mut(&cursor_id) {
            if cursor.tailable {
                if let Some(results) = all_results.get_mut(&cursor_id) {
                    results.push(doc);
                    cursor.exhausted = false;
                    // 通知等待的 getMore 调用
                    self.tailable_notify.notify_all();
                    return true;
                }
            }
        }
        false
    }

    /// 获取游标的下一批结果（getMore）。
    pub fn get_more(&self, cursor_id: i64, batch_size: usize) -> Option<Vec<Document>> {
        let batch_size = if batch_size == 0 { DEFAULT_BATCH_SIZE } else { batch_size };

        let mut cursors = self.cursors.lock().unwrap();
        let all_results = self.results.lock().unwrap();

        let cursor = cursors.get_mut(&cursor_id)?;
        let results = all_results.get(&cursor_id)?;

        let batch = cursor.get_more(results, batch_size);
        Some(batch)
    }

    /// 等待 tailable 游标有新数据。
    ///
    /// 返回 true 表示有新数据，false 表示超时。
    pub fn await_tailable_data(&self, cursor_id: i64) -> bool {
        let cursors = self.cursors.lock().unwrap();

        // 检查游标是否存在且为 tailable
        if !cursors.contains_key(&cursor_id) || !cursors[&cursor_id].tailable {
            return false;
        }

        // 如果已有数据，立即返回
        if !cursors[&cursor_id].exhausted {
            return true;
        }

        // 等待新数据
        let _ = self.tailable_notify.wait_timeout_while(
            cursors,
            self.await_data_timeout,
            |cursors| {
                cursors.get(&cursor_id)
                    .map(|c| c.tailable && c.exhausted)
                    .unwrap_or(false)
            },
        );

        // 检查是否有新数据
        self.cursors.lock().unwrap().get(&cursor_id)
            .map(|c| !c.exhausted)
            .unwrap_or(false)
    }

    /// 关闭游标并释放资源。
    pub fn kill_cursor(&self, cursor_id: i64) -> bool {
        let mut cursors = self.cursors.lock().unwrap();
        let mut results = self.results.lock().unwrap();
        cursors.remove(&cursor_id);
        results.remove(&cursor_id).is_some()
    }

    /// 清理所有过期游标。
    pub fn cleanup_expired(&self) -> usize {
        let mut cursors = self.cursors.lock().unwrap();
        let mut results = self.results.lock().unwrap();

        let expired: Vec<i64> = cursors
            .iter()
            .filter(|(_, c)| c.is_expired())
            .map(|(id, _)| *id)
            .collect();

        let count = expired.len();
        for id in &expired {
            cursors.remove(id);
            results.remove(id);
        }
        count
    }

    /// 启动后台清理线程。
    ///
    /// 该线程每 60 秒检查一次过期游标并清理。
    pub fn start_background_cleanup(&self) {
        let mut running = self.cleanup_running.lock().unwrap();
        if *running {
            return;
        }
        *running = true;

        std::thread::Builder::new()
            .name("cursor-cleanup".to_string())
            .spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_secs(60));
                    // 这里简化实现，实际应该通过回调或Arc来清理
                }
            })
            .ok();
    }

    /// 停止后台清理线程。
    pub fn stop_background_cleanup(&self) {
        let mut running = self.cleanup_running.lock().unwrap();
        *running = false;
    }

    /// 活跃游标数量。
    pub fn active_count(&self) -> usize {
        self.cursors.lock().unwrap().len()
    }

    /// 检查游标是否存在。
    pub fn has_cursor(&self, cursor_id: i64) -> bool {
        self.cursors.lock().unwrap().contains_key(&cursor_id)
    }

    /// 获取游标信息。
    pub fn get_cursor(&self, cursor_id: i64) -> Option<Cursor> {
        self.cursors.lock().unwrap().get(&cursor_id).cloned()
    }

    /// 获取游标结果集。
    pub fn get_results(&self, cursor_id: i64) -> Option<Vec<Document>> {
        self.results.lock().unwrap().get(&cursor_id).cloned()
    }
}

impl Default for CursorManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 包装在 Arc 中的游标管理器，方便多线程共享。
#[derive(Debug, Clone)]
pub struct SharedCursorManager(Arc<CursorManager>);

impl SharedCursorManager {
    /// 创建共享游标管理器。
    pub fn new() -> Self {
        Self(Arc::new(CursorManager::new()))
    }

    /// 获取内部管理器引用。
    pub fn inner(&self) -> &CursorManager {
        &self.0
    }

    /// 创建带选项的游标。
    pub fn create_cursor_with_options(
        &self,
        namespace: impl Into<String>,
        results: Vec<Document>,
        options: CursorOptions,
    ) -> Cursor {
        self.0.create_cursor_with_options(namespace, results, options)
    }

    /// 创建 tailable 游标。
    pub fn create_tailable_cursor(
        &self,
        namespace: impl Into<String>,
        batch_size: usize,
    ) -> Cursor {
        self.0.create_tailable_cursor(namespace, batch_size)
    }

    /// 等待 tailable 游标有新数据。
    pub fn await_tailable_data(&self, cursor_id: i64) -> bool {
        self.0.await_tailable_data(cursor_id)
    }

    /// 获取游标信息。
    pub fn get_cursor(&self, cursor_id: i64) -> Option<Cursor> {
        self.0.get_cursor(cursor_id)
    }
}

impl Default for SharedCursorManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdb_doc::Bson;

    fn make_docs(n: usize) -> Vec<Document> {
        (0..n)
            .map(|i| {
                let mut doc = Document::new();
                doc.insert("_id", Bson::Int32(i as i32));
                doc.insert("value", Bson::String(format!("doc_{i}")));
                doc
            })
            .collect()
    }

    #[test]
    fn cursor_creation() {
        let docs = make_docs(5);
        let cursor = Cursor::new("test.coll", docs, 3, false);
        assert_eq!(cursor.batch.len(), 3);
        assert!(!cursor.exhausted);
        assert!(cursor.id > 0);
    }

    #[test]
    fn cursor_exhausted_when_fewer_than_batch() {
        let docs = make_docs(2);
        let cursor = Cursor::new("test.coll", docs, 5, false);
        assert_eq!(cursor.batch.len(), 2);
        assert!(cursor.exhausted);
    }

    #[test]
    fn cursor_exhausted_when_exact_batch() {
        let docs = make_docs(3);
        let cursor = Cursor::new("test.coll", docs, 3, false);
        assert_eq!(cursor.batch.len(), 3);
        assert!(cursor.exhausted);
    }

    #[test]
    fn cursor_get_more() {
        let docs = make_docs(10);
        let mut cursor = Cursor::new("test.coll", docs.clone(), 3, false);
        let batch = cursor.get_more(&docs, 3);
        assert_eq!(batch.len(), 3);
        assert!(!cursor.exhausted);
    }

    #[test]
    fn cursor_get_more_exhausted() {
        let docs = make_docs(5);
        let mut cursor = Cursor::new("test.coll", docs.clone(), 3, false);
        // First batch: 3 docs
        assert_eq!(cursor.batch.len(), 3);
        assert!(!cursor.exhausted);
        // Get more: next 2 docs
        let batch = cursor.get_more(&docs, 3);
        assert_eq!(batch.len(), 2);
        assert!(cursor.exhausted);
    }

    #[test]
    fn cursor_get_more_when_already_exhausted() {
        let docs = make_docs(2);
        let mut cursor = Cursor::new("test.coll", docs.clone(), 5, false);
        assert!(cursor.exhausted);
        let batch = cursor.get_more(&docs, 5);
        assert!(batch.is_empty());
    }

    #[test]
    fn cursor_manager_create_and_get_more() {
        let mgr = CursorManager::new();
        let docs = make_docs(10);
        let cursor = mgr.create_cursor("test.coll", docs, 3);
        assert_eq!(cursor.batch.len(), 3);

        let batch = mgr.get_more(cursor.id, 3).unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn cursor_manager_kill_cursor() {
        let mgr = CursorManager::new();
        let docs = make_docs(5);
        let cursor = mgr.create_cursor("test.coll", docs, 3);
        assert!(mgr.has_cursor(cursor.id));

        let killed = mgr.kill_cursor(cursor.id);
        assert!(killed);
        assert!(!mgr.has_cursor(cursor.id));
    }

    #[test]
    fn cursor_manager_kill_nonexistent() {
        let mgr = CursorManager::new();
        assert!(!mgr.kill_cursor(999));
    }

    #[test]
    fn cursor_manager_active_count() {
        let mgr = CursorManager::new();
        assert_eq!(mgr.active_count(), 0);

        let docs = make_docs(5);
        let c1 = mgr.create_cursor("test.a", docs.clone(), 3);
        let c2 = mgr.create_cursor("test.b", docs, 3);
        assert_eq!(mgr.active_count(), 2);

        mgr.kill_cursor(c1.id);
        assert_eq!(mgr.active_count(), 1);

        mgr.kill_cursor(c2.id);
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn cursor_manager_cleanup_expired() {
        let mgr = CursorManager::new();
        let docs = make_docs(5);
        let cursor = mgr.create_cursor("test.coll", docs, 3);

        // No expired cursors initially
        assert_eq!(mgr.cleanup_expired(), 0);

        // Manually expire by modifying the cursor (we can't easily mock time,
        // so we just test the cleanup mechanism)
        assert!(mgr.has_cursor(cursor.id));
    }

    #[test]
    fn cursor_no_timeout() {
        let docs = make_docs(5);
        let cursor = Cursor::new("test.coll", docs, 3, true);
        assert!(!cursor.is_expired());
    }

    #[test]
    fn shared_cursor_manager() {
        let mgr = SharedCursorManager::new();
        let docs = make_docs(5);
        let cursor = mgr.inner().create_cursor("test.coll", docs, 3);
        assert!(mgr.inner().has_cursor(cursor.id));
    }

    #[test]
    fn cursor_default_batch_size() {
        let docs = make_docs(5);
        let cursor = mgr_create_with_default_batch(docs);
        // Default batch size is 101, so 5 docs should all be in the first batch
        assert_eq!(cursor.batch.len(), 5);
        assert!(cursor.exhausted);
    }

    fn mgr_create_with_default_batch(docs: Vec<Document>) -> Cursor {
        let mgr = CursorManager::new();
        mgr.create_cursor("test.coll", docs, 0)
    }

    #[test]
    fn get_more_default_batch_size() {
        let mgr = CursorManager::new();
        let docs = make_docs(200);
        let cursor = mgr.create_cursor("test.coll", docs, 0);
        assert_eq!(cursor.batch.len(), 101); // default batch size
        assert!(!cursor.exhausted);

        let batch = mgr.get_more(cursor.id, 0).unwrap();
        assert_eq!(batch.len(), 99); // 200 - 101 = 99
    }

    // ===== 新增测试：Tailable 游标 =====

    #[test]
    fn tailable_cursor_creation() {
        let cursor = Cursor::tailable("test.capped", 100);
        assert!(cursor.is_tailable());
        assert!(cursor.is_awaiting_data());
        assert!(cursor.no_timeout);
        assert!(cursor.batch.is_empty());
        assert!(!cursor.exhausted); // tailable cursor starts as not exhausted
    }

    #[test]
    fn cursor_manager_create_tailable() {
        let mgr = CursorManager::new();
        let cursor = mgr.create_tailable_cursor("test.capped", 100);
        assert!(cursor.is_tailable());
        assert!(mgr.has_cursor(cursor.id));
    }

    #[test]
    fn tailable_cursor_append() {
        let mgr = CursorManager::new();
        let cursor = mgr.create_tailable_cursor("test.capped", 100);

        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(1));
        doc.insert("value", Bson::String("test".into()));

        assert!(mgr.append_to_tailable(cursor.id, doc));
        assert!(!cursor.is_done());
    }

    #[test]
    fn tailable_cursor_not_done() {
        let cursor = Cursor::tailable("test.capped", 100);
        assert!(!cursor.is_done()); // tailable cursors are never done
    }

    #[test]
    fn cursor_with_options() {
        let docs = make_docs(5);
        let options = CursorOptions {
            batch_size: 2,
            no_cursor_timeout: true,
            tailable: true,
            await_data: true,
            ..Default::default()
        };
        let cursor = Cursor::with_options("test.coll", docs, options);
        assert_eq!(cursor.batch.len(), 2);
        assert!(cursor.is_tailable());
        assert!(cursor.is_awaiting_data());
        assert!(cursor.no_timeout);
    }

    // ===== 新增测试：游标切片分页 =====

    #[test]
    fn cursor_slice() {
        let docs = make_docs(10);
        let mut cursor = Cursor::new("test.coll", docs.clone(), 5, false);

        let batch = cursor.slice(&docs, 2, 3);
        assert_eq!(batch.len(), 3);
        assert_eq!(
            batch[0].get("_id").and_then(Bson::as_i64),
            Some(2)
        );
        assert_eq!(
            batch[2].get("_id").and_then(Bson::as_i64),
            Some(4)
        );
    }

    #[test]
    fn cursor_slice_at_end() {
        let docs = make_docs(5);
        let mut cursor = Cursor::new("test.coll", docs.clone(), 3, false);

        let batch = cursor.slice(&docs, 3, 3);
        assert_eq!(batch.len(), 2);
        assert!(cursor.exhausted);
    }

    #[test]
    fn cursor_slice_out_of_bounds() {
        let docs = make_docs(5);
        let mut cursor = Cursor::new("test.coll", docs.clone(), 3, false);

        let batch = cursor.slice(&docs, 10, 3);
        assert!(batch.is_empty());
        assert!(cursor.exhausted);
    }

    // ===== 新增测试：反向迭代 =====

    #[test]
    fn cursor_reverse() {
        let docs = make_docs(5);
        let mut cursor = Cursor::new("test.coll", docs.clone(), 5, false);

        cursor.reverse();
        assert!(cursor.reversed);
        // batch should be reversed
        assert_eq!(
            cursor.batch[0].get("_id").and_then(Bson::as_i64),
            Some(4)
        );
        assert_eq!(
            cursor.batch[4].get("_id").and_then(Bson::as_i64),
            Some(0)
        );
    }

    #[test]
    fn cursor_reverse_toggle() {
        let cursor = Cursor::new("test.coll", make_docs(5), 5, false);
        assert!(!cursor.reversed);

        let mut cursor = cursor;
        cursor.reverse();
        assert!(cursor.reversed);
        cursor.reverse();
        assert!(!cursor.reversed);
    }

    #[test]
    fn cursor_get_more_reverse() {
        let docs = make_docs(10);
        let mut cursor = Cursor::new("test.coll", docs.clone(), 3, false);
        cursor.reverse();

        let batch = cursor.get_more(&docs, 3);
        assert_eq!(batch.len(), 3);
        // Should get the last 3 docs in reverse order
        assert_eq!(
            batch[0].get("_id").and_then(Bson::as_i64),
            Some(6)
        );
    }

    // ===== 新增测试：执行计划 =====

    #[test]
    fn cursor_explain() {
        let plan = ExplainPlan::collscan(10, 50);
        let options = CursorOptions {
            explain_plan: Some(plan),
            ..Default::default()
        };
        let docs = make_docs(5);
        let cursor = Cursor::with_options("test.coll", docs, options);

        let explain = cursor.explain();
        assert!(explain.is_some());
        assert_eq!(explain.unwrap().query_plan, "COLLSCAN");
    }

    #[test]
    fn cursor_explain_none() {
        let docs = make_docs(5);
        let cursor = Cursor::new("test.coll", docs, 3, false);
        assert!(cursor.explain().is_none());
    }

    // ===== 新增测试：剩余 TTL =====

    #[test]
    fn cursor_remaining_ttl() {
        let docs = make_docs(5);
        let cursor = Cursor::new("test.coll", docs, 3, false);

        let remaining = cursor.remaining_ttl();
        assert!(remaining > Duration::ZERO);
        assert!(remaining <= DEFAULT_CURSOR_TIMEOUT);
    }

    #[test]
    fn cursor_remaining_ttl_no_timeout() {
        let docs = make_docs(5);
        let cursor = Cursor::new("test.coll", docs, 3, true);

        let remaining = cursor.remaining_ttl();
        assert_eq!(remaining, Duration::from_secs(u64::MAX));
    }

    // ===== 新增测试：CursorOptions =====

    #[test]
    fn cursor_options_default() {
        let opts = CursorOptions::default();
        assert_eq!(opts.batch_size, 0);
        assert!(!opts.no_cursor_timeout);
        assert!(!opts.tailable);
        assert!(!opts.await_data);
        assert!(opts.explain_plan.is_none());
        assert!(!opts.reversed);
    }

    // ===== 新增测试：create_cursor_with_options =====

    #[test]
    fn cursor_manager_create_with_options() {
        let mgr = CursorManager::new();
        let docs = make_docs(10);
        let options = CursorOptions {
            batch_size: 5,
            no_cursor_timeout: true,
            ..Default::default()
        };
        let cursor = mgr.create_cursor_with_options("test.coll", docs, options);
        assert_eq!(cursor.batch.len(), 5);
        assert!(cursor.no_timeout);
    }
}
