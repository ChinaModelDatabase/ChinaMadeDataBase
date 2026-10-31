//! MDB 交互式 shell（mongosh 替代）。
//!
//! ```text
//! # 连接到本地服务器
//! cargo run -p mdb-cli -- 127.0.0.1 27018
//!
//! # 或者使用连接字符串
//! cargo run -p mdb-cli -- "mongodb://127.0.0.1:27018"
//! ```

#![forbid(unsafe_code)]

use std::io::{self, BufRead, Write};
use std::net::TcpStream;

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
    let reader = io::BufReader::new(stream.try_clone().expect("clone stream"));
    let mut writer = stream;
    let mut reader = reader;

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

        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        match input {
            "exit" | "quit" | "\\q" => {
                println!("Goodbye!");
                break;
            }
            "help" | "\\?" => {
                print_help();
            }
            _ => {
                // 发送到服务器并打印响应
                execute_command(input, &mut writer, &mut reader);
            }
        }
    }
}

/// 执行命令。
fn execute_command(cmd: &str, writer: &mut TcpStream, reader: &mut io::BufReader<TcpStream>) {
    // 构建简单的 OP_MSG 命令
    let body = if cmd.starts_with('{') {
        cmd.to_string()
    } else {
        // 尝试解析为简单命令
        format!("{{\"{}\": 1}}", cmd)
    };

    // 发送命令
    let msg = format!("{}\n", body);
    if let Err(e) = writer.write_all(msg.as_bytes()) {
        eprintln!("Error sending command: {e}");
        return;
    }
    let _ = writer.flush();

    // 读取响应（简化：读取一行）
    let mut response = String::new();
    match reader.read_line(&mut response) {
        Ok(_) => {
            if !response.trim().is_empty() {
                println!("{}", response.trim());
            } else {
                println!("{{\"ok\": 0, \"errmsg\": \"no response\"}}");
            }
        }
        Err(e) => {
            eprintln!("Error reading response: {e}");
        }
    }
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
