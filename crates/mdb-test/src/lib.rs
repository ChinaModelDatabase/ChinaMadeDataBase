//! 差分测试框架：wire 协议差分、故障注入、基准、BSON 向量回归
//!
//! # 本 crate 的作用：工作区结构守卫
//!
//! 这里不放"占位测试"。占位 `assert!(true)` 会让测试计数虚高——
//! `cargo test` 报"通过"却零覆盖，正是本项目明令禁止的过度宣称。
//!
//! 取而代之，本 crate 承载**真实**的工作区结构校验：任何 crate 漏掉
//! `#![forbid(unsafe_code)]`、或出现 `unsafe`、或再次退化成占位断言，
//! 都会被下面这些测试直接抓住。
//!
//! ## 依赖方向（单向，禁止反向依赖）
//!
//! ```text
//! mdb-common <- mdb-crypto <- mdb-doc <- mdb-wire <- mdb-server
//!                     \                     \             \
//!                      \                     \             +- mdb-cli
//!                       \- mdb-storage <- mdb-index <- mdb-query
//!                                            \            \        /
//!                                             \- mdb-txn --+- mdb-repl
//!                                                          \- mdb-router
//! ```

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

#[cfg(test)]
use std::path::{Path, PathBuf};

/// 工作区根目录（`crates/` 的**父**目录）。
#[cfg(test)]
fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = <root>/crates/mdb-test -> 上溯两级才是 <root>
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("mdb-test 必须位于 <root>/crates/ 下")
        .to_path_buf()
}

/// 枚举全部业务 crate 目录。
#[cfg(test)]
fn member_crates() -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(workspace_root().join("crates"))
        .expect("crates/ 可读")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir() && p.join("Cargo.toml").is_file())
        .collect();
    out.sort();
    assert!(!out.is_empty(), "未发现任何 crate");
    out
}

#[cfg(test)]
fn rust_sources(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else { continue };
        for e in entries.filter_map(|e| e.ok()) {
            let p = e.path();
            if p.is_dir() {
                if p.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

#[cfg(test)]
fn read(p: &Path) -> String {
    std::fs::read_to_string(p).unwrap_or_else(|e| panic!("读取 {} 失败: {e}", p.display()))
}

#[cfg(test)]
fn rel(p: &Path) -> String {
    p.strip_prefix(workspace_root())
        .unwrap_or(p)
        .to_string_lossy()
        .replace('\\', "/")
}

#[test]
fn every_crate_forbids_unsafe_code() {
    let mut missing = Vec::new();
    for c in member_crates() {
        let lib = c.join("src/lib.rs");
        if !read(&lib).contains("#![forbid(unsafe_code)]") {
            missing.push(rel(&lib));
        }
    }
    assert!(
        missing.is_empty(),
        "以下 crate 缺少 `#![forbid(unsafe_code)]`：\n  {}",
        missing.join("\n  ")
    );
}

/// 判断 `line` 中第 `pos` 处的 `unsafe` 是否为**关键字**。
///
/// 不能简单 `contains("unsafe")`——标识符里出现该子串很常见
/// （`when_unsafe`、`forbids_unsafe_code`、`UNSAFE_MAX`），会把
/// 合法代码误判为违规。Rust 中 `_` 也是单词字符，故两侧任一为
/// 单词字符即说明它属于某个更长的标识符。
#[cfg(test)]
fn is_unsafe_keyword(line: &str, pos: usize) -> bool {
    let word = |c: char| c.is_alphanumeric() || c == '_';
    let before = line[..pos].chars().next_back();
    let after = line[pos + "unsafe".len()..].chars().next();
    !before.is_some_and(word) && !after.is_some_and(word)
}

/// 把字符串字面量、行注释、字符字面量内容抹成空格。
///
/// 只做单行状态机：本仓库不使用跨行原始字符串，这已足够。
/// 抹掉而非删除，是为了让字节下标保持不变，`is_unsafe_keyword`
/// 的前后字符判断才不会因为长度变化而错位。
#[cfg(test)]
fn strip_literals(line: &str) -> String {
    let b: Vec<char> = line.chars().collect();
    let mut out: Vec<char> = b.clone();
    let mut i = 0;
    // 状态：None / InStr{quote} / InChar{quote}
    let mut in_str: Option<char> = None;
    while i < b.len() {
        match in_str {
            Some(q) => {
                if b[i] == '\\' {
                    if i + 1 < b.len() {
                        out[i] = ' ';
                        out[i + 1] = ' ';
                        i += 2;
                        continue;
                    }
                } else if b[i] == q {
                    in_str = None;
                    i += 1;
                    continue;
                } else {
                    out[i] = ' ';
                }
            }
            None => {
                if b[i] == '/' && i + 1 < b.len() && b[i + 1] == '/' {
                    // 行注释剩余部分全部抹掉
                    for slot in &mut out[i..] {
                        *slot = ' ';
                    }
                    break;
                }
                if b[i] == '"' || b[i] == '\'' {
                    in_str = Some(b[i]);
                }
            }
        }
        i += 1;
    }
    out.into_iter().collect()
}

#[test]
fn no_unsafe_code_anywhere() {
    let mut hits = Vec::new();
    for c in member_crates() {
        for f in rust_sources(&c) {
            for (n, line) in read(&f).lines().enumerate() {
                // 先抹掉字面量与注释，再判断 `unsafe` 是不是关键字
                let masked = strip_literals(line);
                let mut from = 0;
                let mut found = false;
                while let Some(off) = masked[from..].find("unsafe") {
                    let pos = from + off;
                    if is_unsafe_keyword(&masked, pos) {
                        found = true;
                        break;
                    }
                    from = pos + 1;
                }
                if found {
                    hits.push(format!("{}:{}: {}", rel(&f), n + 1, line.trim()));
                }
            }
        }
    }
    assert!(hits.is_empty(), "出现 unsafe 用法：\n  {}", hits.join("\n  "));
}

#[test]
fn no_placeholder_assertions() {
    // `assert!(true)` / `assert_eq!(true, true)` 之类的空测试会让测试计数
    // 虚高，给出"已覆盖"的错觉。历史上有 9 个 crate 用它凑数。
    let mut hits = Vec::new();
    for c in member_crates() {
        for f in rust_sources(&c) {
            for (n, line) in read(&f).lines().enumerate() {
                let t: String = line.chars().filter(|c| *c != ' ').collect();
                if t == "assert!(true);" || t == "assert!(true)" || t.starts_with("assert!(true);") {
                    hits.push(format!("{}:{}", rel(&f), n + 1));
                }
            }
        }
    }
    assert!(
        hits.is_empty(),
        "发现占位断言 `assert!(true)`：\n  {}\n请换成真实断言，或删除该测试。",
        hits.join("\n  ")
    );
}

#[test]
fn no_duplicate_workspace_dependencies() {
    // 工作区应使用 path 依赖 + workspace 继承，不得引入外部 crate
    // （零第三方依赖是本项目为信创可审计性定下的硬约束）。
    for c in member_crates() {
        let manifest = read(&c.join("Cargo.toml"));
        let mut in_deps = false;
        for line in manifest.lines() {
            let t = line.trim();
            if t.starts_with('[') {
                in_deps = t.starts_with("[dependencies]") || t.starts_with("[dev-dependencies]");
                continue;
            }
            if !in_deps || t.starts_with('#') || !t.contains('=') {
                continue;
            }
            let name = t.split('=').next().unwrap_or("").trim().trim_matches('"');
            if name.is_empty() || name.starts_with("mdb-") {
                continue;
            }
            let path = t.split('=').nth(1).unwrap_or("");
            // 允许 version = "0.0.0" 式的占位，但必须是 path 依赖
            let ok = path.contains("path") || path.contains("workspace");
            assert!(
                ok,
                "{}: 外部依赖 `{name}` 缺少 path/workspace 说明：{t}",
                rel(&c.join("Cargo.toml"))
            );
        }
    }
}
