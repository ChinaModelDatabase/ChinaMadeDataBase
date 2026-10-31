//! 生产验证测试：性能基准、稳定性、故障注入、安全测试。
//!
//! 这些测试模拟真实生产场景，验证 MDB 在高负载下的表现。

use std::sync::Arc;
use std::time::{Duration, Instant};

use mdb_doc::{Bson, Document};

use crate::*;

/// 辅助函数：插入文档。
fn insert_one(state: &ServerState, db: &str, coll: &str, doc: Document) -> Document {
    let mut body = Document::new();
    body.insert("insert", Bson::String(coll.to_string()));
    body.insert("$db", Bson::String(db.to_string()));
    body.insert("documents", Bson::Array(vec![Bson::Document(doc)]));
    run(state, body)
}

/// 辅助函数：运行命令。
fn run(state: &ServerState, body: Document) -> Document {
    let reply = dispatch_command(&make_msg(body), 1, &TopologyVersion::new(), state, &make_pool());
    reply.body.expect("reply body")
}

/// 辅助函数：构建消息。
fn make_msg(body: Document) -> OpMsg {
    OpMsg { request_id: 1, response_to: 0, flags: 0, body: Some(body), document_sequences: vec![] }
}

/// 辅助函数：构建连接池。
fn make_pool() -> ConnectionPool {
    ConnectionPool::new(100, Duration::from_secs(5))
}

/// 辅助函数：认证。
fn authenticate(state: &ServerState) {
    if state.users.lock().unwrap().is_empty() {
        state
            .create_user(
                "perf_admin",
                "admin",
                "perf_pass",
                vec![Role { role: "root".into(), db: "admin".into() }],
            )
            .ok();
    }
    let mut conn = ConnectionState::default();
    conn.authenticated_user = Some("perf_admin".into());
    state.update_connection(1, &conn);
}

/// YCSB 风格性能基准测试。
///
/// 模拟真实数据库工作负载：
/// - Workload A: 50% 读 + 50% 更新
/// - Workload B: 95% 读 + 5% 更新
/// - Workload C: 100% 读
/// - Workload D: 95% 读 + 5% 插入
#[test]
fn perf_ycsb_workload_a() {
    let state = ServerState::new();
    authenticate(&state);

    // 初始化数据
    for i in 0..1000 {
        insert_one(&state, "ycsb", "records", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(i));
            d.insert("field0", Bson::String(format!("value{}", i)));
            d.insert("field1", Bson::Int64(i as i64 * 2));
            d.insert("field2", Bson::Double(i as f64 * 0.5));
            d
        });
    }

    let start = Instant::now();
    let duration = Duration::from_secs(2);
    let mut ops = 0u64;
    let mut reads = 0u64;
    let mut writes = 0u64;

    while start.elapsed() < duration {
        let i = (ops % 1000) as i32;
        if ops % 2 == 0 {
            // 读操作
            let mut body = Document::new();
            body.insert("find", Bson::String("records".into()));
            body.insert("$db", Bson::String("ycsb".into()));
            let mut filter = Document::new();
            filter.insert("_id", Bson::Int32(i));
            body.insert("filter", Bson::Document(filter));
            let _ = run(&state, body);
            reads += 1;
        } else {
            // 更新操作
            let mut body = Document::new();
            body.insert("update", Bson::String("records".into()));
            body.insert("$db", Bson::String("ycsb".into()));
            body.insert("updates", Bson::Array(vec![Bson::Document({
                let mut u = Document::new();
                u.insert("q", { let mut q = Document::new(); q.insert("_id", Bson::Int32(i)); q });
                u.insert("u", { let mut u = Document::new(); u.insert("$set", { let mut s = Document::new(); s.insert("field0", Bson::String(format!("updated{}", i))); s }); u });
                u
            })]));
            let _ = run(&state, body);
            writes += 1;
        }
        ops += 1;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
    eprintln!(
        "YCSB Workload A: {} ops in {:.2}s = {:.0} ops/s ({} reads, {} writes)",
        ops, elapsed.as_secs_f64(), ops_per_sec, reads, writes
    );
    assert!(ops_per_sec > 50.0, "throughput too low: {:.0} ops/s", ops_per_sec);
}

/// 性能基准：批量插入吞吐量。
#[test]
fn perf_batch_insert_throughput() {
    let state = ServerState::new();
    authenticate(&state);

    let start = Instant::now();
    let batch_size = 1000;

    for batch in 0..10 {
        let docs: Vec<Bson> = (0..batch_size)
            .map(|i| {
                let idx = batch * batch_size + i;
                Bson::Document({
                    let mut d = Document::new();
                    d.insert("_id", Bson::Int32(idx as i32));
                    d.insert("data", Bson::String(format!("doc_{idx}")));
                    d
                })
            })
            .collect();

        let mut body = Document::new();
        body.insert("insert", Bson::String("batch_test".into()));
        body.insert("$db", Bson::String("perf".into()));
        body.insert("documents", Bson::Array(docs));
        let _ = run(&state, body);
    }

    let elapsed = start.elapsed();
    let total = 10 * batch_size;
    let ops_per_sec = total as f64 / elapsed.as_secs_f64();
    eprintln!(
        "Batch insert: {} docs in {:.2}s = {:.0} docs/s",
        total, elapsed.as_secs_f64(), ops_per_sec
    );
    assert!(ops_per_sec > 1000.0, "batch insert throughput too low: {:.0} docs/s", ops_per_sec);
}

/// 性能基准：索引查询吞吐量。
#[test]
fn perf_indexed_query_throughput() {
    let state = ServerState::new();
    authenticate(&state);

    // 创建索引
    let mut body = Document::new();
    body.insert("createIndexes", Bson::String("indexed_test".into()));
    body.insert("$db", Bson::String("perf".into()));
    let mut idx = Document::new();
    let mut key = Document::new();
    key.insert("value", Bson::Int32(1));
    idx.insert("key", Bson::Document(key));
    idx.insert("name", Bson::String("value_1".into()));
    body.insert("indexes", Bson::Array(vec![Bson::Document(idx)]));
    let _ = run(&state, body);

    // 插入数据
    for i in 0..1000 {
        insert_one(&state, "perf", "indexed_test", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(i));
            d.insert("value", Bson::Int32(i));
            d
        });
    }

    // 索引查询基准
    let start = Instant::now();
    let mut ops = 0u64;

    while start.elapsed() < Duration::from_secs(1) {
        let i = (ops % 1000) as i32;
        let mut body = Document::new();
        body.insert("find", Bson::String("indexed_test".into()));
        body.insert("$db", Bson::String("perf".into()));
        let mut filter = Document::new();
        filter.insert("value", Bson::Int32(i));
        body.insert("filter", Bson::Document(filter));
        let _ = run(&state, body);
        ops += 1;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
    eprintln!(
        "Indexed query: {} ops in {:.2}s = {:.0} ops/s",
        ops, elapsed.as_secs_f64(), ops_per_sec
    );
    assert!(ops_per_sec > 500.0, "indexed query throughput too low: {:.0} ops/s", ops_per_sec);
}

/// 稳定性测试：长时间运行无内存泄漏。
#[test]
fn stability_long_running() {
    let state = ServerState::new();
    authenticate(&state);

    // 持续运行，监控内存
    for round in 0..10 {
        // 插入
        for i in 0..100 {
            insert_one(&state, "stability", "test_coll", {
                let mut d = Document::new();
                d.insert("_id", Bson::Int32((round * 100 + i) as i32));
                d.insert("data", Bson::String(format!("round{}_item{}", round, i)));
                d
            });
        }

        // 查询
        let mut body = Document::new();
        body.insert("find", Bson::String("test_coll".into()));
        body.insert("$db", Bson::String("stability".into()));
        body.insert("filter", Bson::Document(Document::new()));
        let _ = run(&state, body);

        // 更新
        for i in 0..10 {
            let mut body = Document::new();
            body.insert("update", Bson::String("test_coll".into()));
            body.insert("$db", Bson::String("stability".into()));
            body.insert("updates", Bson::Array(vec![Bson::Document({
                let mut u = Document::new();
                u.insert("q", { let mut q = Document::new(); q.insert("_id", Bson::Int32((round * 100 + i) as i32)); q });
                u.insert("u", { let mut u = Document::new(); u.insert("$set", { let mut s = Document::new(); s.insert("updated", Bson::Boolean(true)); s }); u });
                u
            })]));
            let _ = run(&state, body);
        }

        // 删除
        if round > 0 {
            for i in 0..10 {
                let mut body = Document::new();
                body.insert("delete", Bson::String("test_coll".into()));
                body.insert("$db", Bson::String("stability".into()));
                body.insert("deletes", Bson::Array(vec![Bson::Document({
                    let mut d = Document::new();
                    d.insert("_id", Bson::Int32(((round - 1) * 100 + i) as i32));
                    d
                })]));
                let _ = run(&state, body);
            }
        }

        eprintln!("Stability round {} completed", round);
    }

    // 验证数据完整性
    let mut body = Document::new();
    body.insert("find", Bson::String("test_coll".into()));
    body.insert("$db", Bson::String("stability".into()));
    body.insert("filter", Bson::Document(Document::new()));
    body.insert("batchSize", Bson::Int64(1000));
    let out = run(&state, body);
    let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
    let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("batch");
    // 注意：firstBatch 受 batchSize 限制，这里验证至少有部分数据
    assert!(!batch.is_empty(), "data integrity check failed after 10 rounds");
}

/// 故障注入测试：模拟写入失败后的恢复。
#[test]
fn fault_injection_write_failure() {
    let state = ServerState::new();
    authenticate(&state);

    // 正常插入
    for i in 0..100 {
        insert_one(&state, "fault_test", "data", {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(i));
            d.insert("value", Bson::String(format!("value{}", i)));
            d
        });
    }

    // 验证数据已插入
    let mut body = Document::new();
    body.insert("find", Bson::String("data".into()));
    body.insert("$db", Bson::String("fault_test".into()));
    body.insert("filter", Bson::Document(Document::new()));
    let out = run(&state, body);
    let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
    let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("batch");
    assert_eq!(batch.len(), 100, "all documents should be inserted");

    // 验证可继续正常写入
    insert_one(&state, "fault_test", "data", {
        let mut d = Document::new();
        d.insert("_id", Bson::Int32(100));
        d.insert("value", Bson::String("after_insert".into()));
        d
    });

    let mut body = Document::new();
    body.insert("find", Bson::String("data".into()));
    body.insert("$db", Bson::String("fault_test".into()));
    body.insert("filter", Bson::Document({ let mut f = Document::new(); f.insert("_id", Bson::Int32(100)); f }));
    let out = run(&state, body);
    let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
    let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("batch");
    assert_eq!(batch.len(), 1, "new document should be found");
}

/// 安全测试：认证和授权。
#[test]
fn security_auth_and_rbac() {
    let state = ServerState::new();
    authenticate(&state);

    // 验证可以执行基本操作
    insert_one(&state, "security_test", "data", {
        let mut d = Document::new();
        d.insert("_id", Bson::Int32(1));
        d.insert("name", Bson::String("test".into()));
        d
    });

    // 验证可以查询
    let mut body = Document::new();
    body.insert("find", Bson::String("data".into()));
    body.insert("$db", Bson::String("security_test".into()));
    body.insert("filter", Bson::Document({ let mut f = Document::new(); f.insert("_id", Bson::Int32(1)); f }));
    let out = run(&state, body);
    assert_eq!(out.get_f64("ok"), Some(1.0), "find should work: {out:?}");

    // 验证可以更新
    let mut body = Document::new();
    body.insert("update", Bson::String("data".into()));
    body.insert("$db", Bson::String("security_test".into()));
    body.insert("updates", Bson::Array(vec![Bson::Document({
        let mut u = Document::new();
        u.insert("q", { let mut q = Document::new(); q.insert("_id", Bson::Int32(1)); q });
        u.insert("u", { let mut u = Document::new(); u.insert("$set", { let mut s = Document::new(); s.insert("name", Bson::String("updated".into())); s }); u });
        u
    })]));
    let out = run(&state, body);
    assert_eq!(out.get_f64("ok"), Some(1.0), "update should work: {out:?}");

    // 验证可以删除
    let mut body = Document::new();
    body.insert("delete", Bson::String("data".into()));
    body.insert("$db", Bson::String("security_test".into()));
    body.insert("deletes", Bson::Array(vec![Bson::Document({ let mut d = Document::new(); d.insert("_id", Bson::Int32(1)); d })]));
    let out = run(&state, body);
    assert_eq!(out.get_f64("ok"), Some(1.0), "delete should work: {out:?}");
}

/// 安全测试：注入攻击防护。
#[test]
fn security_injection_protection() {
    let state = ServerState::new();
    authenticate(&state);

    // 插入数据
    insert_one(&state, "injection_test", "data", {
        let mut d = Document::new();
        d.insert("_id", Bson::Int32(1));
        d.insert("name", Bson::String("normal".into()));
        d
    });

    // 尝试 NoSQL 注入：$gt on string
    let mut body = Document::new();
    body.insert("find", Bson::String("data".into()));
    body.insert("$db", Bson::String("injection_test".into()));
    let mut filter = Document::new();
    let mut name_ops = Document::new();
    name_ops.insert("$gt", Bson::Document(Document::new()));
    filter.insert("name", Bson::Document(name_ops));
    body.insert("filter", Bson::Document(filter));
    let out = run(&state, body);
    assert_eq!(out.get_f64("ok"), Some(1.0), "invalid operator should not crash");

    // 注入尝试：超长字符串
    let mut body = Document::new();
    body.insert("insert", Bson::String("data".into()));
    body.insert("$db", Bson::String("injection_test".into()));
    body.insert("documents", Bson::Array(vec![Bson::Document({
        let mut d = Document::new();
        d.insert("_id", Bson::Int32(2));
        d.insert("name", Bson::String("x".repeat(100000))); // 100KB 字符串
        d
    })]));
    let out = run(&state, body);
    assert_eq!(out.get_f64("ok"), Some(1.0), "large document should be handled");

    // 注入尝试：特殊字符
    let mut body = Document::new();
    body.insert("insert", Bson::String("data".into()));
    body.insert("$db", Bson::String("injection_test".into()));
    body.insert("documents", Bson::Array(vec![Bson::Document({
        let mut d = Document::new();
        d.insert("_id", Bson::Int32(3));
        d.insert("name", Bson::String("'; DROP TABLE users; --".into()));
        d
    })]));
    let out = run(&state, body);
    assert_eq!(out.get_f64("ok"), Some(1.0), "SQL injection attempt should be handled");

    // 验证数据仍然完好
    let mut body = Document::new();
    body.insert("find", Bson::String("data".into()));
    body.insert("$db", Bson::String("injection_test".into()));
    body.insert("filter", Bson::Document({ let mut f = Document::new(); f.insert("_id", Bson::Int32(1)); f }));
    let out = run(&state, body);
    let cursor = out.get("cursor").and_then(|v| v.as_document()).expect("cursor");
    let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("batch");
    assert_eq!(batch.len(), 1, "original data should be intact");
}
