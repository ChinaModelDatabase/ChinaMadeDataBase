//! 写路径语义回归测试：替换文档、upsert、唯一索引、复合索引前缀查询、事务。
//!
//! 这些都是"客户端真的会这么发"的路径，而单元测试用的是内部 API，
//! 之前正是这些地方静默出错（upsert 丢了 `$set`、唯一索引不拦重复、
//! 复合索引的前缀查询一条都查不到）。

use std::net::SocketAddr;
use std::time::Duration;

use mdb_doc::{Bson, Document};
use mdb_server::Server;
use mdb_wire::conn::Framed;
use mdb_wire::op_msg::{DocumentSequence, OpMsg};

struct TestServer {
    addr: SocketAddr,
}

impl TestServer {
    fn start() -> Self {
        let server = Server::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("bind");
        let addr = server.local_addr().expect("local_addr");
        std::thread::spawn(move || {
            let _ = server.serve_forever();
        });
        Self { addr }
    }

    fn run(&self, body: Document) -> Document {
        self.run_with_seq(body, None)
    }

    fn run_with_seq(&self, body: Document, documents: Option<Vec<Document>>) -> Document {
        let stream = std::net::TcpStream::connect(self.addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut msg = OpMsg::request(1, body);
        if let Some(docs) = documents {
            // 每个用例一条连接：连接必须复用，否则 lsid 会话会换掉。
            let mut seq = DocumentSequence::new("documents");
            for d in docs {
                seq.push(d);
            }
            msg.document_sequences.push(seq);
        }
        let bytes = msg.to_bytes().expect("encode");
        let mut framed = Framed::new(stream);
        framed.write_message(&bytes).expect("write");
        let raw = framed
            .next_message()
            .expect("read")
            .expect("got a message");
        OpMsg::parse(&raw).expect("parse reply").body.expect("body")
    }
}

fn cmd(coll: &str, extra: &[(&str, Bson)], db: &str) -> Document {
    let mut d = Document::new();
    d.insert("insert", Bson::String(coll.to_owned()));
    for (k, v) in extra {
        d.insert(*k, v.clone());
    }
    d.insert("$db", Bson::String(db.to_owned()));
    d
}

#[test]
fn replacement_document_replaces_fields_and_keeps_id() {
    let s = TestServer::start();
    let mut doc = Document::new();
    doc.insert("_id", Bson::Int32(1));
    doc.insert("name", Bson::String("old".into()));
    doc.insert("value", Bson::Int32(1));
    let reply = s.run_with_seq(cmd("repl", &[], "t"), Some(vec![doc]));
    assert_eq!(reply.get("ok").and_then(|v| v.as_f64()), Some(1.0), "{reply:?}");

    let update = {
        let mut u = Document::new();
        u.insert("q", Bson::Document({
            let mut q = Document::new();
            q.insert("_id", Bson::Int32(1));
            q
        }));
        u.insert("u", Bson::Document({
            let mut r = Document::new();
            r.insert("name", Bson::String("new".into()));
            r.insert("value", Bson::Int32(2));
            r
        }));
        let mut d = Document::new();
        d.insert("update", Bson::String("repl".into()));
        d.insert("updates", Bson::Array(vec![Bson::Document(u)]));
        d.insert("$db", Bson::String("t".into()));
        d
    };
    let reply = s.run(update);
    assert_eq!(reply.get("nModified").and_then(|v| v.as_i64()), Some(1), "{reply:?}");

    let found = {
        let mut d = Document::new();
        d.insert("find", Bson::String("repl".into()));
        d.insert("filter", Bson::Document(Document::new()));
        d.insert("$db", Bson::String("t".into()));
        s.run(d)
    };
    let cursor = found.get("cursor").and_then(|v| v.as_document()).expect("cursor");
    let batch = cursor.get("firstBatch").and_then(|v| v.as_array()).expect("batch");
    assert_eq!(batch.len(), 1);
    let doc = batch[0].as_document().expect("doc");
    // 整篇替换：`_id` 保留，旧字段被替换文档完全覆盖。
    assert_eq!(doc.get("_id"), Some(&Bson::Int32(1)));
    assert_eq!(doc.get("name").and_then(|v| v.as_str()), Some("new"));
    assert_eq!(doc.get("value"), Some(&Bson::Int32(2)));
    assert_eq!(doc.len(), 3, "替换文档不该残留旧字段：{doc:?}");
}

#[test]
fn upsert_applies_operators_and_reports_upserted_id() {
    let s = TestServer::start();
    let update = {
        let mut u = Document::new();
        u.insert("q", Bson::Document({
            let mut q = Document::new();
            q.insert("name", Bson::String("upsert_test".into()));
            q
        }));
        u.insert("u", Bson::Document({
            let mut set = Document::new();
            set.insert("$set", Bson::Document({
                let mut v = Document::new();
                v.insert("value", Bson::Int32(1));
                v
            }));
            set
        }));
        u.insert("upsert", Bson::Boolean(true));
        let mut d = Document::new();
        d.insert("update", Bson::String("upserts".into()));
        d.insert("updates", Bson::Array(vec![Bson::Document(u)]));
        d.insert("$db", Bson::String("t".into()));
        d
    };
    let reply = s.run(update);
    assert_eq!(reply.get("ok").and_then(|v| v.as_f64()), Some(1.0), "{reply:?}");
    let upserted = reply.get("upserted").and_then(|v| v.as_array()).expect("upserted");
    assert_eq!(upserted.len(), 1);
    let entry = upserted[0].as_document().expect("entry");
    // 驱动靠 `_id` 回填 `upserted_id`：只给 index 会 KeyError。
    assert!(entry.get("_id").is_some(), "upserted entry needs _id: {entry:?}");

    let found = {
        let mut d = Document::new();
        d.insert("find", Bson::String("upserts".into()));
        d.insert("filter", Bson::Document(Document::new()));
        d.insert("$db", Bson::String("t".into()));
        s.run(d)
    };
    let batch = found
        .get("cursor")
        .and_then(|v| v.as_document())
        .and_then(|c| c.get("firstBatch"))
        .and_then(|v| v.as_array())
        .expect("batch");
    let doc = batch[0].as_document().expect("doc");
    // 过滤条件里的等值字段 + `$set` 的结果都必须落在文档里。
    assert_eq!(doc.get("name").and_then(|v| v.as_str()), Some("upsert_test"));
    assert_eq!(doc.get("value"), Some(&Bson::Int32(1)), "{doc:?}");
}

#[test]
fn unique_index_rejects_duplicate_insert() {
    let s = TestServer::start();
    let create = {
        let mut idx = Document::new();
        idx.insert("key", Bson::Document({
            let mut k = Document::new();
            k.insert("email", Bson::Int32(1));
            k
        }));
        idx.insert("name", Bson::String("email_1".into()));
        let mut d = Document::new();
        d.insert("createIndexes", Bson::String("uniq".into()));
        d.insert("indexes", Bson::Array(vec![Bson::Document({
            let mut opts = idx;
            opts.insert("unique", Bson::Boolean(true));
            opts
        })]));
        d.insert("$db", Bson::String("t".into()));
        d
    };
    assert_eq!(s.run(create).get("ok").and_then(|v| v.as_f64()), Some(1.0));

    let first = {
        let mut d = Document::new();
        d.insert("email", Bson::String("a@b.c".into()));
        d
    };
    assert_eq!(
        s.run_with_seq(cmd("uniq", &[], "t"), Some(vec![first]))
            .get("ok")
            .and_then(|v| v.as_f64()),
        Some(1.0)
    );

    let dup = {
        let mut d = Document::new();
        d.insert("email", Bson::String("a@b.c".into()));
        d
    };
    let reply = s.run_with_seq(cmd("uniq", &[], "t"), Some(vec![dup]));
    let errors = reply.get("writeErrors").and_then(|v| v.as_array()).expect("writeErrors");
    let code = errors[0]
        .as_document()
        .and_then(|e| e.get("code"))
        .and_then(|v| v.as_i64());
    assert_eq!(code, Some(11000), "重复键必须是 11000：{reply:?}");
}

#[test]
fn compound_index_prefix_query_finds_documents() {
    let s = TestServer::start();
    let create = {
        let mut idx = Document::new();
        idx.insert("key", Bson::Document({
            let mut k = Document::new();
            k.insert("a", Bson::Int32(1));
            k.insert("b", Bson::Int32(1));
            k
        }));
        idx.insert("name", Bson::String("a_1_b_1".into()));
        let mut d = Document::new();
        d.insert("createIndexes", Bson::String("comp".into()));
        d.insert("indexes", Bson::Array(vec![Bson::Document(idx)]));
        d.insert("$db", Bson::String("t".into()));
        d
    };
    assert_eq!(s.run(create).get("ok").and_then(|v| v.as_f64()), Some(1.0));

    let docs: Vec<Document> = (1..=3)
        .map(|i| {
            let mut d = Document::new();
            d.insert("_id", Bson::Int32(i));
            d.insert("a", Bson::Int32(if i == 3 { 2 } else { 1 }));
            d.insert("b", Bson::Int32(i));
            d
        })
        .collect();
    assert_eq!(
        s.run_with_seq(cmd("comp", &[], "t"), Some(docs))
            .get("ok")
            .and_then(|v| v.as_f64()),
        Some(1.0)
    );

    // 只给前缀字段：GridFS 查 chunks 就是这种形态，之前一条都查不到。
    let found = {
        let mut filter = Document::new();
        filter.insert("a", Bson::Int32(1));
        let mut d = Document::new();
        d.insert("find", Bson::String("comp".into()));
        d.insert("filter", Bson::Document(filter));
        d.insert("$db", Bson::String("t".into()));
        s.run(d)
    };
    let batch = found
        .get("cursor")
        .and_then(|v| v.as_document())
        .and_then(|c| c.get("firstBatch"))
        .and_then(|v| v.as_array())
        .expect("batch");
    assert_eq!(batch.len(), 2, "前缀查询应命中 2 条：{found:?}");
}

#[test]
fn transaction_buffered_until_commit_and_discarded_on_abort() {
    let s = TestServer::start();
    let lsid = {
        let mut id = Document::new();
        id.insert("id", Bson::Binary(mdb_doc::Binary::generic(vec![1u8; 16])));
        id
    };
    let mut insert = cmd("txn", &[], "t");
    insert.insert("lsid", Bson::Document(lsid.clone()));
    insert.insert("startTransaction", Bson::Boolean(true));
    insert.insert("txnNumber", Bson::Int64(1));
    insert.insert("autocommit", Bson::Boolean(false));
    let doc = {
        let mut d = Document::new();
        d.insert("_id", Bson::Int32(7));
        d.insert("name", Bson::String("txn_doc".into()));
        d
    };
    let reply = s.run_with_seq(insert, Some(vec![doc]));
    assert_eq!(reply.get("ok").and_then(|v| v.as_f64()), Some(1.0), "{reply:?}");

    // 提交前不可见（写的是缓冲区）。
    let count_before = {
        let mut d = Document::new();
        d.insert("count", Bson::String("txn".into()));
        d.insert("$db", Bson::String("t".into()));
        s.run(d)
    };
    assert_eq!(
        count_before.get("n").and_then(|v| v.as_i64()),
        Some(0),
        "事务提交前不应落盘：{count_before:?}"
    );

    let commit = {
        let mut d = Document::new();
        d.insert("commitTransaction", Bson::Int32(1));
        d.insert("lsid", Bson::Document(lsid.clone()));
        d.insert("txnNumber", Bson::Int64(1));
        d.insert("autocommit", Bson::Boolean(false));
        d.insert("$db", Bson::String("t".into()));
        d
    };
    assert_eq!(s.run(commit).get("ok").and_then(|v| v.as_f64()), Some(1.0));

    let count_after = {
        let mut d = Document::new();
        d.insert("count", Bson::String("txn".into()));
        d.insert("$db", Bson::String("t".into()));
        s.run(d)
    };
    assert_eq!(
        count_after.get("n").and_then(|v| v.as_i64()),
        Some(1),
        "commit 后应可见：{count_after:?}"
    );
}

#[test]
fn transaction_abort_discards_buffered_writes() {
    let s = TestServer::start();
    let lsid = {
        let mut id = Document::new();
        id.insert("id", Bson::Binary(mdb_doc::Binary::generic(vec![9u8; 16])));
        id
    };
    let mut insert = cmd("txn_abort", &[], "t");
    insert.insert("lsid", Bson::Document(lsid.clone()));
    insert.insert("startTransaction", Bson::Boolean(true));
    insert.insert("txnNumber", Bson::Int64(2));
    insert.insert("autocommit", Bson::Boolean(false));
    let doc = {
        let mut d = Document::new();
        d.insert("_id", Bson::Int32(8));
        d.insert("name", Bson::String("abort_doc".into()));
        d
    };
    assert_eq!(
        s.run_with_seq(insert, Some(vec![doc]))
            .get("ok")
            .and_then(|v| v.as_f64()),
        Some(1.0)
    );

    let abort = {
        let mut d = Document::new();
        d.insert("abortTransaction", Bson::Int32(1));
        d.insert("lsid", Bson::Document(lsid));
        d.insert("txnNumber", Bson::Int64(2));
        d.insert("autocommit", Bson::Boolean(false));
        d.insert("$db", Bson::String("t".into()));
        d
    };
    assert_eq!(s.run(abort).get("ok").and_then(|v| v.as_f64()), Some(1.0));

    let count = {
        let mut d = Document::new();
        d.insert("count", Bson::String("txn_abort".into()));
        d.insert("$db", Bson::String("t".into()));
        s.run(d)
    };
    assert_eq!(
        count.get("n").and_then(|v| v.as_i64()),
        Some(0),
        "abort 必须丢弃缓冲区：{count:?}"
    );
}
