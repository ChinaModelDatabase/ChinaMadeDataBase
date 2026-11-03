//! 线格式端到端往返测试（只走公开 API）。
//!
//! 单元测试直接调 `OpMsg::to_bytes` / `parse`，这里额外覆盖：
//! 1. 公开导出面（`mdb_wire::OpMsg` 等）是否可用；
//! 2. 一次**完整**的握手往返，模拟驱动连上后的第一个消息；
//! 3. 线上字节布局与 `docs/01` 记录的一致。

use mdb_doc::{Bson, Document};
fn tv() -> mdb_wire::handshake::TopologyVersionSnapshot {
    mdb_wire::handshake::TopologyVersionSnapshot {
        process_id: mdb_doc::ObjectId::from_bytes([3u8; 12]),
        counter: 0,
    }
}

use mdb_wire::{
    hello_reply_msg, is_handshake, MessageHeader, OpCode, OpMsg, MAX_BSON_OBJECT_SIZE,
    MAX_MESSAGE_SIZE_BYTES, MAX_WIRE_VERSION,
};

/// 构造驱动会发出的 `hello` 命令。
fn driver_hello(request_id: i32) -> OpMsg {
    let mut body = Document::new();
    body.insert("hello", 1i32);
    body.insert("client", Document::new());
    body.insert("$db", "admin");
    OpMsg::request(request_id, body)
}

#[test]
fn full_handshake_roundtrip() {
    // --- 客户端：发 hello ---
    let request = driver_hello(1000);
    let wire = request.to_bytes().expect("encode request");

    // 线上第一层必须是合法头，且 opCode 为 2013
    let header = MessageHeader::parse(&wire).expect("parse header");
    assert_eq!(header.op_code, OpCode::Msg);
    assert_eq!(header.request_id, 1000);
    assert_eq!(header.response_to, 0);
    assert_eq!(header.message_length as usize, wire.len());

    // --- 服务端：解析并识别 ---
    let parsed = OpMsg::parse(&wire).expect("parse request");
    let body = parsed.body.as_ref().expect("hello must have a body");
    assert!(is_handshake(body));
    assert_eq!(body.get("$db").and_then(Bson::as_str), Some("admin"));

    // --- 服务端：应答 ---
    let reply = hello_reply_msg(parsed.request_id, 1, tv());
    let reply_wire = reply.to_bytes().expect("encode reply");
    assert_eq!(
        MessageHeader::parse(&reply_wire).unwrap().response_to,
        1000,
        "reply must point back at the request id"
    );

    // --- 客户端：解析应答 ---
    let got = OpMsg::parse(&reply_wire).expect("parse reply");
    let rbody = got.body.as_ref().expect("hello reply must have a body");
    assert_eq!(
        rbody.get("maxWireVersion"),
        Some(&Bson::Int32(MAX_WIRE_VERSION))
    );
    assert_eq!(rbody.get("ok"), Some(&Bson::Double(1.0)));
    assert_eq!(rbody.get("ismaster"), Some(&Bson::Boolean(true)));
    assert!(matches!(rbody.get("localTime"), Some(Bson::DateTime(_))));
}

#[test]
fn advertised_limits_match_headers() {
    // 头里的上限与 hello 应答里的能力声明必须一致，否则驱动按其中一个
    // 分配缓冲、另一个校验长度时会越界或误拒
    let h = MessageHeader::request(1, OpCode::Msg, 0);
    assert_eq!(h.op_code, OpCode::Msg);

    let reply = hello_reply_msg(1, 1, tv());
    let body = reply.body.unwrap();
    assert_eq!(
        body.get("maxMessageSizeBytes"),
        Some(&Bson::Int32(MAX_MESSAGE_SIZE_BYTES))
    );
    assert_eq!(
        body.get("maxBsonObjectSize"),
        Some(&Bson::Int32(MAX_BSON_OBJECT_SIZE))
    );
}

/// 编译期不变式：`maxBsonObjectSize` 必须严格小于 `maxMessageSizeBytes`，
/// 否则单个文档加上头与段标记就超限，驱动按前者分配缓冲会在写入时越界。
const _: () = assert!(MAX_BSON_OBJECT_SIZE < MAX_MESSAGE_SIZE_BYTES);

#[test]
fn driver_style_insert_survives_the_wire() {
    // 真实 insert 形态：body + document sequence("documents")
    let mut body = Document::new();
    body.insert("insert", "users");
    body.insert("ordered", true);
    body.insert("$db", "test");
    let mut msg = OpMsg::request(7, body);

    let mut seq = mdb_wire::DocumentSequence::new("documents");
    for i in 0..3i32 {
        let mut doc = Document::new();
        doc.insert("_id", i);
        doc.insert("name", format!("user{i}"));
        seq.push(doc);
    }
    msg.document_sequences.push(seq);

    let wire = msg.to_bytes().unwrap();
    let back = OpMsg::parse(&wire).unwrap();
    assert_eq!(back.body_str("insert"), Some("users"));
    assert_eq!(back.document_sequences.len(), 1);
    let docs = &back.document_sequences[0];
    assert_eq!(docs.identifier, "documents");
    assert_eq!(docs.documents.len(), 3);
    assert_eq!(docs.documents[2].get("name").and_then(Bson::as_str), Some("user2"));
}

#[test]
fn checksummed_message_roundtrips_through_public_api() {
    let mut msg = driver_hello(55);
    msg.flags |= mdb_wire::op_msg::FLAG_CHECKSUM_PRESENT;
    let wire = msg.to_bytes().unwrap();
    let back = OpMsg::parse(&wire).unwrap();
    assert!(back.checksum_present());
    assert!(is_handshake(back.body.as_ref().unwrap()));
}

#[test]
fn two_messages_do_not_merge() {
    // 粘包场景：一次读到两条消息，必须能按 messageLength 正确切分
    let a = driver_hello(1).to_bytes().unwrap();
    let b = hello_reply_msg(1, 1, tv()).to_bytes().unwrap();
    let mut stream = a.clone();
    stream.extend_from_slice(&b);

    let split = MessageHeader::parse(&stream).unwrap().message_length as usize;
    assert_eq!(split, a.len());
    assert_eq!(OpMsg::parse(&stream[..split]).unwrap().request_id, 1);
    assert_eq!(OpMsg::parse(&stream[split..]).unwrap().response_to, 1);
}
