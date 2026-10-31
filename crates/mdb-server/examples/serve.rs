//! 启动 MDB 服务端。
//!
//! ```text
//! # 纯内存模式（无持久化）
//! cargo run -p mdb-server --example serve -- 27018
//!
//! # 持久化模式：数据落盘，重启后自动恢复
//! cargo run -p mdb-server --example serve -- 27018 /path/to/dbpath
//!
//! python scripts/handshake_check.py 127.0.0.1 27018
//! ```

use std::net::SocketAddr;

use mdb_server::Server;

fn main() {
    let mut args = std::env::args().skip(1);
    let port: u16 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(27018);
    let dbpath = args.next(); // 可选：第二个参数为数据目录
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    let server = match dbpath {
        Some(path) => match Server::bind_with_dbpath(addr, &path) {
            Ok(s) => {
                println!("mdb-server (persistent) listening on {addr}, dbpath={path}");
                s
            }
            Err(e) => {
                eprintln!("bind_with_dbpath({path}) failed: {e}");
                std::process::exit(1);
            }
        },
        None => match Server::bind(addr) {
            Ok(s) => {
                println!("mdb-server (in-memory) listening on {addr}");
                s
            }
            Err(e) => {
                eprintln!("bind failed: {e}");
                std::process::exit(1);
            }
        },
    };

    if let Err(e) = server.serve_forever() {
        eprintln!("serve failed: {e}");
        std::process::exit(1);
    }
}
