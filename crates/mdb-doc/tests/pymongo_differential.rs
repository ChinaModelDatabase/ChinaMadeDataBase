//! 与官方驱动 `pymongo` 的差分测试。
//!
//! `tests/vectors/pymongo-4.18.2.jsonl` 是用 `pymongo 4.18.2` 的
//! `bson.encode` 与 `bson.json_util` 生成并冻结的语料，每行形如
//! `{"label": ..., "hex": ..., "canonical": ..., "relaxed": ...}`。
//! 重新生成的脚本见同目录 `generate.py`（仅开发期使用，需要 Python 环境）。
//!
//! # 为什么要有这层测试
//!
//! 手写"符合规范"的编解码器几乎总能通过自己写的单元测试，却和真实驱动
//! 有细微出入。已经靠这层测试抓到的三个真实缺陷：
//!
//! 1. `string` 的 `int32` 长度**包含**结尾 NUL。BSON 规范原文是
//!    "plus one for the trailing null byte"。按"长度 = 数据字节数"实现时，
//!    编解码双方自洽、自测全绿，但**没有任何真实驱动读得回来**。
//! 2. `code_with_scope` 的 `total_size` **包含自身 4 字节**。
//! 3. relaxed EJSON 不能把 `NaN` / `Infinity` 写成裸数字——那不是合法 JSON。
//!
//! 另外还纠正了两处此前凭记忆写错的东西：
//!
//! - `Timestamp` 的 64 位布局是高 32 位 `inc`、低 32 位 `secs`。
//! - Binary subtype 0x02 的历史布局只重复长度、不重复子类型。
//!
//! 这类问题只有拿第三方实现做基准才暴露得出来。

use mdb_doc::{codec, json, Bson, Document};

const CORPUS: &str = include_str!("vectors/pymongo-4.18.2.jsonl");

struct Vector {
    label: String,
    hex: String,
    canonical: String,
    relaxed: String,
}

// 语料行形如 `{"canonical": "...", "hex": "...", "label": "...", "relaxed": "..."}`，
// 字段值本身是含转义的 JSON 字符串。这里用 `from_json` 解析该行再取字段，
// 这样字符串字段的转义交给已测试的解析器，不引入第二套手写 JSON 解析。
fn load() -> Vec<Vector> {
    let mut out = Vec::new();
    for (n, line) in CORPUS.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let doc =
            json::from_json(line).unwrap_or_else(|e| panic!("corpus line {}: bad JSON: {e}", n + 1));
        let d = doc.as_document().expect("corpus line must be a document");
        let field = |k: &str| -> String {
            d.get(k)
                .and_then(Bson::as_str)
                .unwrap_or_else(|| panic!("corpus line {} missing {k}", n + 1))
                .to_owned()
        };
        out.push(Vector {
            label: field("label"),
            hex: field("hex"),
            canonical: field("canonical"),
            relaxed: field("relaxed"),
        });
    }
    assert!(out.len() >= 40, "corpus should be substantial, got {}", out.len());
    out
}

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    assert!(hex.len() % 2 == 0, "odd-length hex");
    hex.as_bytes()
        .chunks(2)
        .map(|p| {
            let s = std::str::from_utf8(p).expect("hex is ascii");
            u8::from_str_radix(s, 16).unwrap_or_else(|_| panic!("bad hex {s}"))
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decimal128 是刻意不实现的类型：它只按字节搬运，不参与十进制换算。
fn has_decimal128(v: &Bson) -> bool {
    match v {
        Bson::Decimal128(_) => true,
        Bson::Document(d) => d.iter().any(|(_, v)| has_decimal128(v)),
        Bson::Array(a) => a.iter().any(has_decimal128),
        _ => false,
    }
}

/// 深度相等，但把 `NaN` 当作等于自身。
///
/// `Bson` 的 `PartialEq` 派生自 `f64`，而 `NaN != NaN`。语料里含 NaN 向量，
/// 直接用 `assert_eq!` 会得到"看起来一样却不相等"的失败，掩盖真正的差异。
fn same(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(x), Bson::Double(y)) => x == y || (x.is_nan() && y.is_nan()),
        (Bson::Document(x), Bson::Document(y)) => {
            x.keys().eq(y.keys()) && x.iter().zip(y.iter()).all(|((_, l), (_, r))| same(l, r))
        }
        (Bson::Array(x), Bson::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(l, r)| same(l, r))
        }
        _ => a == b,
    }
}

fn from_bytes_or_panic(label: &str, bytes: &[u8]) -> Document {
    match codec::from_bytes(bytes) {
        Ok(d) => d,
        Err(e) => panic!("[{label}] failed to decode pymongo bytes: {e}"),
    }
}

fn find(label: &str) -> Vector {
    load().into_iter().find(|v| v.label == label).unwrap_or_else(|| panic!("no vector {label}"))
}

#[test]
fn corpus_is_not_empty() {
    assert!(load().len() >= 40);
}

#[test]
fn decodes_pymongo_bytes() {
    for v in load() {
        from_bytes_or_panic(&v.label, &hex_to_bytes(&v.hex));
    }
}

/// 最强的一条：我的编码器对 pymongo 的每一个字节都能逐字节重现。
#[test]
fn re_encoding_reproduces_pymongo_bytes_exactly() {
    for v in load() {
        let doc = from_bytes_or_panic(&v.label, &hex_to_bytes(&v.hex));
        let again = codec::to_bytes(&doc)
            .unwrap_or_else(|e| panic!("[{}] re-encode failed: {e}", v.label));
        assert_eq!(hex(&again), v.hex, "[{}] re-encoded bytes differ", v.label);
    }
}

#[test]
fn canonical_ejson_agrees_with_pymongo() {
    for v in load() {
        let doc = from_bytes_or_panic(&v.label, &hex_to_bytes(&v.hex));
        if has_decimal128(&Bson::Document(doc.clone())) {
            assert!(
                json::to_canonical_json(&Bson::Document(doc)).is_err(),
                "[{}] decimal128 must be refused, not emitted",
                v.label
            );
            continue;
        }
        let mine = json::to_canonical_json(&Bson::Document(doc))
            .unwrap_or_else(|e| panic!("[{}] canonical output failed: {e}", v.label));
        let theirs = json::from_json(&v.canonical)
            .unwrap_or_else(|e| panic!("[{}] pymongo canonical EJSON unreadable: {e}", v.label));
        let mine_value = json::from_json(&mine)
            .unwrap_or_else(|e| panic!("[{}] my canonical EJSON must re-parse: {e}", v.label));
        assert!(
            same(&mine_value, &theirs),
            "[{}] canonical EJSON mismatch\n  mine:    {mine}\n  pymongo: {}",
            v.label,
            v.canonical
        );
    }
}

#[test]
fn relaxed_ejson_agrees_with_pymongo() {
    for v in load() {
        let doc = from_bytes_or_panic(&v.label, &hex_to_bytes(&v.hex));
        if has_decimal128(&Bson::Document(doc.clone())) {
            continue;
        }
        let mine = json::to_relaxed_json(&Bson::Document(doc))
            .unwrap_or_else(|e| panic!("[{}] relaxed output failed: {e}", v.label));
        let theirs = json::from_json(&v.relaxed)
            .unwrap_or_else(|e| panic!("[{}] pymongo relaxed EJSON unreadable: {e}", v.label));
        let mine_value = json::from_json(&mine)
            .unwrap_or_else(|e| panic!("[{}] my relaxed EJSON must re-parse: {e}", v.label));
        assert!(
            same(&mine_value, &theirs),
            "[{}] relaxed EJSON mismatch\n  mine:    {mine}\n  pymongo: {}",
            v.label,
            v.relaxed
        );
    }
}

/// 解析 pymongo 的 canonical EJSON，结果必须与解析其字节得到的一致。
///
/// 这条链路同时检验两个独立实现：我的 BSON 解码器与我的 EJSON 解析器。
#[test]
fn parsing_pymongo_ejson_matches_parsing_pymongo_bytes() {
    for v in load() {
        let doc = from_bytes_or_panic(&v.label, &hex_to_bytes(&v.hex));
        if has_decimal128(&Bson::Document(doc.clone())) {
            continue;
        }
        let from_canonical =
            json::from_json(&v.canonical).unwrap_or_else(|e| panic!("[{}] {e}", v.label));
        assert!(
            same(&from_canonical, &Bson::Document(doc.clone())),
            "[{}] EJSON/BSON disagreement",
            v.label
        );
    }
}

#[test]
fn string_length_includes_the_trailing_nul() {
    // pymongo: {"a": "hello"} -> 12 00 00 00 02 61 00 06 00 00 00 68 65 6c 6c 6f 00 00
    // 长度字段是 6（5 字节数据 + NUL），不是 5。按 5 写出去没有驱动读得回。
    let v = find("string_ascii");
    assert_eq!(v.hex, "120000000261000600000068656c6c6f0000");
    let doc = codec::from_bytes(&hex_to_bytes(&v.hex)).unwrap();
    assert_eq!(doc.get("a").and_then(Bson::as_str), Some("hello"));
}

#[test]
fn code_with_scope_total_includes_its_own_four_bytes() {
    let v = find("code_w_scope");
    let doc = codec::from_bytes(&hex_to_bytes(&v.hex)).unwrap();
    match doc.get("a") {
        Some(Bson::JavaScriptWithScope { code, scope }) => {
            assert_eq!(code, "x = y");
            assert_eq!(scope.get("y").and_then(Bson::as_i64), Some(3));
        }
        other => panic!("expected code with scope, got {other:?}"),
    }
}

#[test]
fn timestamp_uses_secs_high_inc_low() {
    // pymongo Timestamp(t, i)：低 32 位是 i，高 32 位是 t
    //
    // 注意 BSON 规范正文写的是反的（"most significant 32 bits are the
    // increment"）。权威依据是 bson-corpus/tests/timestamp.json 的
    // (123456789, 42) => 2A000000 15CD5B0700。
    let v = find("timestamp");
    let bytes = hex_to_bytes(&v.hex);
    let inc = u32::from_le_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);
    let secs = u32::from_le_bytes([bytes[11], bytes[12], bytes[13], bytes[14]]);
    assert_eq!((secs, inc), (1, 2), "secs=1 in the high word, inc=2 in the low word");
    let doc = codec::from_bytes(&bytes).unwrap();
    match doc.get("a") {
        Some(Bson::Timestamp(ts)) => {
            assert_eq!(ts.secs, 1);
            assert_eq!(ts.inc, 2);
        }
        other => panic!("expected timestamp, got {other:?}"),
    }
}

#[test]
fn long_above_2_53_survives_every_path() {
    // relaxed 形式会用裸数字，任何中转 f64 的实现都会把它变成 ...92
    let mut doc = Document::new();
    doc.insert("n", 9_007_199_254_740_993i64);
    let bytes = codec::to_bytes(&doc).unwrap();
    let back = codec::from_bytes(&bytes).unwrap();
    let relaxed = json::to_relaxed_json(&Bson::Document(back.clone())).unwrap();
    assert_eq!(relaxed, r#"{"n":{"$numberLong":"9007199254740993"}}"#);
    assert!(same(&json::from_json(&relaxed).unwrap(), &Bson::Document(back)));
}

#[test]
fn field_order_matches_pymongo() {
    // BSON 字节序即插入序；哈希化会毁掉 `_id` 首字段约定与命令名识别
    let v = find("order_preserved");
    let doc = codec::from_bytes(&hex_to_bytes(&v.hex)).unwrap();
    assert_eq!(doc.keys().collect::<Vec<_>>(), ["z", "a", "m"]);
}

#[test]
fn non_finite_doubles_are_byte_stable() {
    for label in ["double_nan", "double_inf", "double_ninf"] {
        let v = find(label);
        let doc = codec::from_bytes(&hex_to_bytes(&v.hex)).unwrap();
        let again = codec::to_bytes(&doc).unwrap();
        assert_eq!(hex(&again), v.hex, "[{label}] must be byte-stable");
        // relaxed 也必须可被 JSON 解析器读回：裸 NaN 不是合法 JSON
        let relaxed = json::to_relaxed_json(&Bson::Document(doc)).unwrap();
        assert!(
            json::from_json(&relaxed).is_ok(),
            "[{label}] relaxed output is not parseable: {relaxed}"
        );
    }
}

#[test]
fn deep_and_wide_documents_survive() {
    for label in ["deep_20", "wide_100"] {
        let v = find(label);
        let doc = from_bytes_or_panic(label, &hex_to_bytes(&v.hex));
        assert_eq!(hex(&codec::to_bytes(&doc).unwrap()), v.hex);
    }
}
