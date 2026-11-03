//! MDB 交互式 shell（mongosh 替代）。
//!
//! ```text
//! # 连接到本地服务器
//! cargo run -p mdb-cli -- 127.0.0.1 27018
//!
//! # 或者使用连接字符串
//! cargo run -p mdb-cli -- "mongodb://127.0.0.1:27018"
//! ```
//!
//! # 为什么必须走 mdb-wire
//!
//! 服务端只认 OP_MSG。早先版本把 JSON 文本加个换行直接写进 socket，
//! 服务端解析出的 `messageLength` 是 `'{"in'` 的 ASCII 字节，立刻
//! `FailedToParse` 并断连。这里统一用 `mdb_wire` 编解码。

#![forbid(unsafe_code)]

use std::io::{self, BufRead, Write};
use std::net::TcpStream;

use mdb_doc::{Bson, Document};
use mdb_wire::conn::Framed;
use mdb_wire::op_msg::DocumentSequence;
use mdb_wire::OpMsg;

/// 未显式指定库时的默认库，与 mongosh 一致。
const DEFAULT_DB: &str = "test";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (host, port) = parse_connection_args(&args);

    println!("MDB Shell v0.1.0");
    println!("Connecting to {host}:{port}...");

    let addr = format!("{host}:{port}");
    match TcpStream::connect(&addr) {
        Ok(stream) => {
            println!("Connected to {addr}");
            println!("Type 'help' for commands, 'exit' to quit.");
            run_repl(stream);
        }
        Err(e) => {
            eprintln!("Failed to connect to {addr}: {e}");
            eprintln!("Make sure mdb-server is running:");
            eprintln!("  cargo run -p mdb-server --example serve -- {port}");
            std::process::exit(1);
        }
    }
}

/// 解析连接参数。
fn parse_connection_args(args: &[String]) -> (String, u16) {
    if args.is_empty() {
        return ("127.0.0.1".to_string(), 27018);
    }

    let arg = &args[0];
    if arg.starts_with("mongodb://") || arg.starts_with("mongodb+srv://") {
        // 解析连接字符串
        let stripped = arg.trim_start_matches("mongodb://").trim_start_matches("mongodb+srv://");
        let host_port = stripped.split('/').next().unwrap_or("127.0.0.1:27018");
        let parts: Vec<&str> = host_port.split(':').collect();
        let host = parts.first().unwrap_or(&"127.0.0.1").to_string();
        let port = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(27018);
        (host, port)
    } else {
        // 简单格式：host port 或 port
        if args.len() >= 2 {
            let port = args[1].parse().unwrap_or(27018);
            (arg.clone(), port)
        } else {
            let port = arg.parse().unwrap_or(27018);
            ("127.0.0.1".to_string(), port)
        }
    }
}

/// 运行交互式 REPL。
fn run_repl(stream: TcpStream) {
    let mut framed = Framed::new(stream);
    let mut request_id = 1i32;

    let stdin = io::stdin();
    let mut input = String::new();

    loop {
        print!("mdb> ");
        io::stdout().flush().unwrap();

        input.clear();
        match stdin.lock().read_line(&mut input) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error reading input: {e}");
                break;
            }
        }

        let line = input.trim();
        if line.is_empty() {
            continue;
        }

        match line {
            "exit" | "quit" | "\\q" => {
                println!("Goodbye!");
                break;
            }
            "help" | "\\?" => {
                print_help();
            }
            _ => execute_command(line, &mut framed, &mut request_id),
        }
    }
}

/// 执行一条命令并打印应答。
fn execute_command(line: &str, framed: &mut Framed<TcpStream>, request_id: &mut i32) {
    let body = match build_command(line) {
        Ok(body) => body,
        Err(e) => {
            println!("{{\"ok\": 0, \"errmsg\": {}}}", mdb_doc::json::quote(&e));
            return;
        }
    };

    // insert 的文档序列按 wire 惯例放在 document sequence 里。
    let documents = body
        .get("documents")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_document().cloned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut body = body;
    if !documents.is_empty() {
        body.remove("documents");
    }

    let mut msg = OpMsg::request(*request_id, body);
    if !documents.is_empty() {
        let mut seq = DocumentSequence::new("documents");
        for doc in documents {
            seq.push(doc);
        }
        msg.document_sequences.push(seq);
    }
    *request_id += 1;

    let bytes = match msg.to_bytes() {
        Ok(b) => b,
        Err(e) => {
            eprintln!("encode failed: {e}");
            return;
        }
    };
    if let Err(e) = framed.write_message(&bytes) {
        eprintln!("send failed: {e}");
        return;
    }

    let raw = match framed.next_message() {
        Ok(Some(raw)) => raw,
        Ok(None) => {
            println!("{{\"ok\": 0, \"errmsg\": \"connection closed\"}}");
            return;
        }
        Err(e) => {
            eprintln!("read failed: {e}");
            return;
        }
    };
    let reply = match OpMsg::parse(&raw) {
        Ok(m) => match m.body {
            Some(body) => body,
            None => {
                eprintln!("reply has no body");
                return;
            }
        },
        Err(e) => {
            eprintln!("decode reply failed: {e}");
            return;
        }
    };
    match Ok::<_, String>(reply) {
        Ok(reply) => {
            let unauthorized = reply.get("ok").and_then(|v| v.as_f64()) == Some(0.0)
                && reply.get("code").and_then(|v| v.as_i64()) == Some(13);
            if unauthorized {
                println!("{}", relaxed(&reply));
                println!("hint: 服务端要求鉴权。新实例可先用 `db.createUser(...)` 建用户（localhost exception），或直接连接尚未创建用户的实例。");
                return;
            }
            println!("{}", relaxed(&reply));
        }
        Err(e) => eprintln!("{e}"),
    }
}

/// 渲染应答；extended JSON 失败时退回 Debug，保证输出不为空。
fn relaxed(doc: &Document) -> String {
    mdb_doc::json::to_relaxed_json(&Bson::Document(doc.clone())).unwrap_or_else(|_| format!("{doc:?}"))
}

/// 把一行输入翻译成 OP_MSG 命令体。
///
/// 支持三种写法：
///
/// 1. 裸命令：`{"find": "users", "$db": "test"}`
/// 2. 简写命令名：`ping` / `buildInfo`（默认 `$db=admin`）
/// 3. mongosh 风格：`db.coll.insertOne({...})` 等
fn build_command(line: &str) -> Result<Document, String> {
    let line = line.trim().trim_end_matches(';');

    if let Some(rest) = line.strip_prefix('{') {
        let text = format!("{{{rest}");
        return json_to_doc(&text);
    }

    if let Some(rest) = line.strip_prefix("db.") {
        return build_db_command(rest);
    }

    let mut d = Document::new();
    d.insert(line, Bson::Int32(1));
    d.insert("$db", Bson::String("admin".into()));
    Ok(d)
}

fn json_to_doc(text: &str) -> Result<Document, String> {
    match mdb_doc::json::from_json(text) {
        Ok(Bson::Document(d)) => {
            let mut d = d;
            if !d.contains_key("$db") {
                d.insert("$db", Bson::String(DEFAULT_DB.into()));
            }
            Ok(d)
        }
        Ok(other) => Err(format!("expected a JSON object, got {other:?}")),
        Err(e) => Err(format!("invalid JSON: {e}")),
    }
}

/// 解析 `coll.method(args)`；不支持嵌套 `db`，集合名不含点号。
fn build_db_command(rest: &str) -> Result<Document, String> {
    // `db.createUser(...)` 建的是账号，不是集合里的文档；库必须用 admin。
    if let Some(args) = rest.strip_prefix("createUser") {
        let args = args.trim().trim_start_matches('(').trim_end().trim_end_matches(')');
        let spec = json_to_doc(args)?;
        let user = spec.get("user").and_then(Bson::as_str).unwrap_or_default().to_owned();
        let pwd = spec.get("pwd").and_then(Bson::as_str).unwrap_or_default().to_owned();
        if user.is_empty() || pwd.is_empty() {
            return Err("createUser requires {user, pwd, roles}".into());
        }
        let roles = spec.get("roles").cloned().unwrap_or(Bson::Array(vec![]));
        let mut d = Document::new();
        d.insert("createUser", Bson::String(user));
        d.insert("pwd", Bson::String(pwd));
        d.insert("roles", roles);
        d.insert("$db", Bson::String("admin".into()));
        return Ok(d);
    }

    let (coll, tail) = rest
        .split_once('.')
        .ok_or_else(|| "expected `db.<collection>.<method>(...)".to_string())?;
    if coll.is_empty() {
        return Err("missing collection name".into());
    }
    let (method, args) = tail
        .split_once('(')
        .ok_or_else(|| "expected `db.<collection>.<method>(...)`".to_string())?;
    let args = args.trim_end().strip_suffix(')').unwrap_or(args).trim();
    let method = method.trim();

    match method {
        "insertOne" => {
            if args.is_empty() {
                return Err("insertOne requires a document".into());
            }
            let doc = json_to_doc(args)?;
            let mut d = Document::new();
            d.insert("insert", Bson::String(coll.into()));
            d.insert("documents", Bson::Array(vec![Bson::Document(doc)]));
            d.insert("$db", Bson::String(DEFAULT_DB.into()));
            Ok(d)
        }
        "insertMany" => {
            let arr = json_to_doc(&format!("{{\"v\": {args}}}"))?
                .remove("v")
                .ok_or("insertMany requires an array")?;
            let docs = arr.as_array().ok_or("insertMany requires an array")?.clone();
            let mut d = Document::new();
            d.insert("insert", Bson::String(coll.into()));
            d.insert("documents", Bson::Array(docs.to_vec()));
            d.insert("$db", Bson::String(DEFAULT_DB.into()));
            Ok(d)
        }
        "find" | "findOne" => {
            let filter = if args.is_empty() {
                Document::new()
            } else {
                json_to_doc(args)?
            };
            let mut d = Document::new();
            d.insert("find", Bson::String(coll.into()));
            d.insert("filter", Bson::Document(filter));
            if method == "findOne" {
                d.insert("limit", Bson::Int32(1));
            }
            d.insert("$db", Bson::String(DEFAULT_DB.into()));
            Ok(d)
        }
        "updateOne" | "updateMany" => {
            let (filter, update) = split_args(args)?;
            let mut u = Document::new();
            u.insert("q", Bson::Document(filter));
            u.insert("u", Bson::Document(update));
            u.insert("multi", Bson::Boolean(method == "updateMany"));
            let mut d = Document::new();
            d.insert("update", Bson::String(coll.into()));
            d.insert("updates", Bson::Array(vec![Bson::Document(u)]));
            d.insert("$db", Bson::String(DEFAULT_DB.into()));
            Ok(d)
        }
        "deleteOne" | "deleteMany" => {
            let doc = if args.is_empty() {
                Document::new()
            } else {
                json_to_doc(args)?
            };
            let mut q = Document::new();
            q.insert("q", Bson::Document(doc));
            q.insert("limit", Bson::Int32(if method == "deleteOne" { 1 } else { 0 }));
            let mut d = Document::new();
            d.insert("delete", Bson::String(coll.into()));
            d.insert("deletes", Bson::Array(vec![Bson::Document(q)]));
            d.insert("$db", Bson::String(DEFAULT_DB.into()));
            Ok(d)
        }
        "countDocuments" => {
            let query = if args.is_empty() {
                Document::new()
            } else {
                json_to_doc(args)?
            };
            let mut d = Document::new();
            d.insert("count", Bson::String(coll.into()));
            d.insert("query", Bson::Document(query));
            d.insert("$db", Bson::String(DEFAULT_DB.into()));
            Ok(d)
        }
        "createIndex" => {
            let keys = json_to_doc(args)?;
            let name = keys
                .keys()
                .map(|k| format!("{k}_{}", keys.get(k).map(|v| v.to_string()).unwrap_or_default()))
                .collect::<Vec<_>>()
                .join("_");
            let mut index = Document::new();
            index.insert("key", Bson::Document(keys));
            index.insert("name", Bson::String(name));
            let mut d = Document::new();
            d.insert("createIndexes", Bson::String(coll.into()));
            d.insert("indexes", Bson::Array(vec![Bson::Document(index)]));
            d.insert("$db", Bson::String(DEFAULT_DB.into()));
            Ok(d)
        }
        other => Err(format!("unsupported method `db.{coll}.{other}`; try insertOne/find/updateOne/deleteOne/countDocuments/createIndex")),
    }
}

/// 按最外层逗号切两个 JSON 参数（用于 `updateOne(filter, update)`）。
fn split_args(args: &str) -> Result<(Document, Document), String> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escape = false;
    for (i, ch) in args.char_indices() {
        if in_string {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' | '[' => depth += 1,
            '}' | ']' => depth -= 1,
            ',' if depth == 0 => {
                let (a, b) = args.split_at(i);
                return Ok((json_to_doc(a)?, json_to_doc(&b[1..])?));
            }
            _ => {}
        }
    }
    Err("expected two arguments: (filter, update)".into())
}

/// 打印帮助信息。
fn print_help() {
    println!("MDB Shell Commands:");
    println!("  help              Show this help message");
    println!("  exit / quit       Exit the shell");
    println!();
    println!("MongoDB Commands (send as JSON):");
    println!("  db.coll.find()         Find documents");
    println!("  db.coll.insertOne()    Insert a document");
    println!("  db.coll.updateOne()    Update documents");
    println!("  db.coll.deleteOne()    Delete documents");
    println!("  db.coll.countDocuments() Count documents");
    println!("  db.coll.createIndex()  Create an index");
    println!();
    println!("Or send raw JSON commands:");
    println!("  {{\"find\": \"users\", \"$db\": \"test\"}}");
    println!("  {{\"insert\": \"users\", \"documents\": [{{\"_id\": 1}}], \"$db\": \"test\"}}");
}
