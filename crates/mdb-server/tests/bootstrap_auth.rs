//! 全新实例的引导：localhost exception。
//!
//! # 为什么单独一个文件
//!
//! 用户表为空时服务端必须允许回环连接建第一个用户，否则
//! `createUser` 自身也要鉴权 → 死锁，实例除了 `hello`/`ping`
//! 什么也做不了。这条路径只在真实 TCP 连接上成立（需要 peer_addr
//! 判定回环），所以走集成测试而不是单测。

use std::net::SocketAddr;
use std::time::Duration;

use mdb_doc::{Bson, Document};
use mdb_server::Server;
use mdb_wire::conn::Framed;
use mdb_wire::op_msg::OpMsg;

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

    fn roundtrip(&self, body: Document) -> Document {
        let stream = std::net::TcpStream::connect(self.addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let bytes = OpMsg::request(1, body).to_bytes().expect("encode");
        let mut framed = Framed::new(stream);
        framed.write_message(&bytes).expect("write");
        let raw = framed
            .next_message()
            .expect("read")
            .expect("got a message");
        framed
            .into_inner()
            .shutdown(std::net::Shutdown::Both)
            .ok();
        OpMsg::parse(&raw).expect("parse reply").body.expect("body")
    }
}

/// 空用户表 + 回环连接：必须先能建出管理员用户，随后立即鉴权。
#[test]
fn localhost_exception_bootstraps_first_user_then_locks_down() {
    let server = TestServer::start();

    let mut body = Document::new();
    body.insert("createUser", Bson::String("root".into()));
    body.insert("pwd", Bson::String("secret".into()));
    body.insert(
        "roles",
        Bson::Array(vec![Bson::Document({
            let mut r = Document::new();
            r.insert("role", Bson::String("root".into()));
            r.insert("db", Bson::String("admin".into()));
            r
        })]),
    );
    body.insert("$db", Bson::String("admin".into()));
    let reply = server.roundtrip(body);
    assert_eq!(
        reply.get("ok"),
        Some(&Bson::Double(1.0)),
        "空用户表下回环连接必须能创建第一个用户，实际：{reply:?}"
    );

    // 用户表非空后 localhost exception 立即失效。
    let mut insert = Document::new();
    insert.insert("insert", Bson::String("users".into()));
    insert.insert("$db", Bson::String("test".into()));
    let reply = server.roundtrip(insert);
    assert_eq!(
        reply.get("ok"),
        Some(&Bson::Double(0.0)),
        "已有用户后未鉴权连接必须被拒，实际：{reply:?}"
    );
    assert_eq!(reply.get("code"), Some(&Bson::Int32(13)));
}
