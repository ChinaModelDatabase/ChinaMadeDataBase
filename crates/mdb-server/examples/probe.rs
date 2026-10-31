//! 协议探针：把驱动发来的原始消息打印出来。
//!
//! 用途是**实测**而非猜测驱动行为——尤其是"连接后第一条消息用
//! `OP_MSG` 还是旧版 `OP_QUERY`"，这一点各驱动/各版本并不一致，
//! 凭记忆写实现极容易错。
//!
//! ```text
//! cargo run -p mdb-server --example probe
//! # 另开一个终端
//! python scripts/probe_client.py
//! ```

use std::net::TcpListener;

use mdb_wire::conn::Framed;

fn main() {
    let port: u16 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(27099);
    let listener = match TcpListener::bind(("127.0.0.1", port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("bind {port} failed: {e}");
            std::process::exit(1);
        }
    };
    println!("probe listening on 127.0.0.1:{port}");

    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        println!("--- connection ---");
        let mut framed = Framed::new(stream);
        loop {
            match framed.next_message() {
                Ok(Some(raw)) => {
                    let op = i32::from_le_bytes([raw[12], raw[13], raw[14], raw[15]]);
                    let req = i32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);
                    let total = raw.len();
                    println!("  opCode={op} requestID={req} len={total}");
                    if op == 2013 {
                        match mdb_wire::OpMsg::parse(&raw) {
                            Ok(msg) => {
                                println!("    flags={:#x}", msg.flags);
                                match &msg.body {
                                    Some(b) => {
                                        let json = mdb_doc::json::to_relaxed_json(&mdb_doc::Bson::Document(
                                            b.clone(),
                                        ))
                                        .unwrap_or_else(|e| format!("<{e}>"));
                                        println!("    body={json}")
                                    }
                                    None => println!("    body=<none>"),
                                }
                                for s in &msg.document_sequences {
                                    println!("    seq {} -> {} docs", s.identifier, s.documents.len());
                                }
                            }
                            Err(e) => println!("    OP_MSG parse error: {e}"),
                        }
                    } else {
                        // 非 OP_MSG：dump 头之后的载荷，便于手工解析
                        let payload = &raw[16..];
                        println!("    payload[0..{}] = {}", payload.len().min(64), hex(payload));
                    }
                }
                Ok(None) => {
                    println!("  peer closed");
                    break;
                }
                Err(e) => {
                    println!("  frame error: {e}");
                    break;
                }
            }
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}
