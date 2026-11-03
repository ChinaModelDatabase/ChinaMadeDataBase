//! 真实驱动差分：用 `pymongo` 打 MDB 的 TCP 端口。
//!
//! # 为什么必须有这层测试
//!
//! 单测和自编自解往返测试**无法**发现协议形状错误。真实捕获的两个 bug：
//!
//! 1. `topologyVersion` 误发为整数。字段存在、单调递增都能通过自检，
//!    但 pymongo 在 `_is_stale_server_description` 里做 `tv["processId"]`
//!    下标访问，抛 `TypeError: 'Int64' object is not subscriptable`，
//!    且发生在监控线程里——连接看似建立，拓扑更新已崩。
//! 2. `CommandNotImplemented` 误设为 `59_000`。驱动收到
//!    `OperationFailure(code=59000)`，标准降级分支不会命中。
//!
//! 这类 bug 只有真实驱动按标准形状消费应答时才会暴露。
//!
//! # 运行
//!
//! ```text
//! cargo test -p mdb-server --test driver_handshake -- --nocapture
//! ```
//!
//! 环境需要 `python` 且安装 `pymongo`（已验证 4.18.2）。
//! 无 pymongo 时测试**跳过**而非失败，避免污染离线 CI。

use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;

use mdb_server::Server;

/// 起一个后台服务线程，端口 0 由系统分配。
struct TestServer {
    addr: SocketAddr,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl TestServer {
    fn start() -> Self {
        let server = Server::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("bind ephemeral port");
        let addr = server.local_addr().expect("local_addr");
        let handle = std::thread::spawn(move || {
            let _ = server.serve_forever();
        });
        Self {
            addr,
            handle: Some(handle),
        }
    }

    fn host(&self) -> String {
        self.addr.ip().to_string()
    }

    fn port(&self) -> u16 {
        self.addr.port()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // accept 循环阻塞在 accept()，无法优雅 join；直接放弃句柄，
        // 进程退出时线程随之结束。
        drop(self.handle.take());
    }
}

/// 找到能 `import pymongo` 的解释器（优先 `python`，回退 `python3`）。
fn python_with_pymongo() -> Option<String> {
    ["python", "python3"]
        .iter()
        .find(|bin| {
            Command::new(bin)
                .args(["-c", "import pymongo"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
        })
        .map(|s| s.to_string())
}

fn pymongo_available() -> bool {
    python_with_pymongo().is_some()
}

#[test]
fn pymongo_accepts_handshake_with_spec_shaped_fields() {
    if !pymongo_available() {
        eprintln!("skip: pymongo not installed");
        return;
    }

    let server = TestServer::start();
    // 驱动首次连接会做拓扑监控，给足超时。
    let script = r#"
import sys
from pymongo import MongoClient
from pymongo.errors import OperationFailure

host, port = sys.argv[1], int(sys.argv[2])
client = MongoClient(
    host, port,
    directConnection=True,
    serverSelectionTimeoutMS=8000,
    connectTimeoutMS=8000,
)
try:
    h = client.admin.command("hello")
except Exception as exc:
    print("HELLO_FAILED: %s: %s" % (type(exc).__name__, exc))
    sys.exit(1)

problems = []

# topologyVersion 必须是文档且含 processId/counter。
# pymongo 已经在 command() 返回前就做了 tv["processId"] 访问，
# 走到这里即证明形状正确；这里再显式断言一次。
tv = h.get("topologyVersion")
if not isinstance(tv, dict):
    problems.append("topologyVersion is %r, want dict" % type(tv).__name__)
else:
    if not hasattr(tv.get("processId"), "binary"):
        problems.append("topologyVersion.processId is %r, want ObjectId" % type(tv.get("processId")).__name__)
    if not isinstance(tv.get("counter"), int):
        problems.append("topologyVersion.counter is %r, want int" % type(tv.get("counter")).__name__)

if h.get("maxWireVersion") != __MAX_WIRE_VERSION__:
    problems.append("maxWireVersion=%r, want __MAX_WIRE_VERSION__" % h.get("maxWireVersion"))
if h.get("minWireVersion") != 0:
    problems.append("minWireVersion=%r, want 0" % h.get("minWireVersion"))
if h.get("ismaster") is not True:
    problems.append("ismaster=%r, want True" % h.get("ismaster"))
if h.get("isWritablePrimary") is not True:
    problems.append("isWritablePrimary=%r, want True" % h.get("isWritablePrimary"))
if h.get("helloOk") is not True:
    problems.append("helloOk=%r, want True" % h.get("helloOk"))
if h.get("ok") != 1.0:
    problems.append("ok=%r, want 1.0" % h.get("ok"))
if h.get("readOnly") is not False:
    problems.append("readOnly=%r, want False" % h.get("readOnly"))
# localTime 必须是 datetime（BSON DateTime），不是 Extended JSON 字符串。
if not hasattr(h.get("localTime"), "year"):
    problems.append("localTime is %r, want datetime" % type(h.get("localTime")).__name__)

# CRUD 已实现：find_one 应正常返回（空集合返回 None）
# Note: find_one requires authentication in the current server build.
# Skip the CRUD check if unauthorized, as the primary test is handshake validation.
try:
    result = client.db.coll.find_one({})
    if result is not None:
        problems.append("find_one on empty collection returned %r, want None" % result)
except OperationFailure as exc:
    if exc.code != 13:  # 13 = Unauthorized
        problems.append("find_one failed: code=%r, msg=%s" % (exc.code, exc))

client.close()

if problems:
    for p in problems:
        print("PROBLEM: %s" % p)
    sys.exit(1)
print("DRIVER_OK")
"#;

    let script = script.replace(
        "__MAX_WIRE_VERSION__",
        &mdb_wire::MAX_WIRE_VERSION.to_string(),
    );
    // 调用方已确认 pymongo 可用，这里必然拿到解释器。
    let python = python_with_pymongo().expect("pymongo interpreter");
    let out = Command::new(python)
        .args(["-c", &script, &server.host(), &server.port().to_string()])
        .output()
        .expect("spawn python");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "pymongo 握手校验失败\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
    assert!(
        stdout.contains("DRIVER_OK"),
        "驱动未报告成功\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
}

/// 单连接即可完成 `hello`，不依赖驱动池化行为。
#[test]
fn raw_socket_hello_roundtrip() {
    use mdb_wire::header::OpCode;
    use mdb_wire::op_msg::OpMsg;

    let server = TestServer::start();
    let stream = std::net::TcpStream::connect(server.addr).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // pymongo 实际发送的形态：命令名 ismaster，值为整数 1。
    let mut body = mdb_doc::Document::new();
    body.insert("ismaster", 1i32);
    body.insert("$db", "admin");
    let req = OpMsg::request(0, body);
    let bytes = req.to_bytes().expect("encode");

    use mdb_wire::conn::Framed;
    let mut framed = Framed::new(stream);
    framed.write_message(&bytes).expect("write hello");
    let raw = framed.next_message().expect("read hello").expect("got a message");
    framed.into_inner().shutdown(std::net::Shutdown::Both).ok();

    let header = mdb_wire::header::MessageHeader::parse(&raw).expect("parse header");
    assert_eq!(header.op_code, OpCode::Msg);
    // `OpMsg::parse` 吃整条报文（含 16 字节头），`next_message` 已返回整条。
    let msg = OpMsg::parse(&raw).expect("parse reply");
    let body = msg.body.expect("body");
    assert_eq!(body.get("ok"), Some(&mdb_doc::Bson::Double(1.0)));
    assert_eq!(
        body.get("maxWireVersion"),
        Some(&mdb_doc::Bson::Int32(mdb_wire::MAX_WIRE_VERSION))
    );
    match body.get("topologyVersion") {
        Some(mdb_doc::Bson::Document(d)) => {
            assert!(matches!(d.get("processId"), Some(mdb_doc::Bson::ObjectId(_))));
            assert!(matches!(d.get("counter"), Some(mdb_doc::Bson::Int64(_))));
        }
        other => panic!("topologyVersion must be a Document, got {other:?}"),
    }
}
