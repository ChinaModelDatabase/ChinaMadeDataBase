import struct
import sys
import os
import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from mdb_native.bson import ObjectId, BsonEncoder, BsonDecoder, BSON_DOUBLE, BSON_STRING, BSON_DOCUMENT, BSON_ARRAY, BSON_BOOLEAN, BSON_NULL, BSON_INT32, BSON_INT64, BSON_OBJECTID, BSON_DATETIME, BSON_BINARY
from mdb_native.wire_protocol import WireProtocol, OpMsg, MSG_HEADER_SIZE, OP_MSG
from mdb_native.crc32c import crc32c, crc32c_update


def test_objectid():
    oid1 = ObjectId()
    assert len(str(oid1)) == 24
    oid2 = ObjectId(str(oid1))
    assert oid1 == oid2
    assert hash(oid1) == hash(oid2)
    oid3 = ObjectId(bytes(oid1))
    assert oid1 == oid3
    print("  [PASS] ObjectId generation and equality")

    try:
        ObjectId("abc")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass
    print("  [PASS] ObjectId invalid input rejection")


def test_bson_double():
    doc = {"pi": 3.14159}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["pi"] == 3.14159
    assert isinstance(decoded["pi"], float)
    print("  [PASS] BSON double encode/decode")


def test_bson_string():
    doc = {"name": "hello world"}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["name"] == "hello world"
    print("  [PASS] BSON string encode/decode")

    doc_utf = {"text": "\u00e9\u00e8\u00ea\u00eb\u2603"}
    encoded = BsonEncoder.encode(doc_utf)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["text"] == "\u00e9\u00e8\u00ea\u00eb\u2603"
    print("  [PASS] BSON string UTF-8 encode/decode")


def test_bson_int32():
    doc = {"count": 42}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["count"] == 42
    assert isinstance(decoded["count"], int)
    print("  [PASS] BSON int32 encode/decode")


def test_bson_int64():
    doc = {"big": 2**40}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["big"] == 2**40
    print("  [PASS] BSON int64 encode/decode")


def test_bson_bool():
    for val in [True, False]:
        doc = {"flag": val}
        encoded = BsonEncoder.encode(doc)
        decoded = BsonDecoder.decode(encoded)
        assert decoded["flag"] == val
        assert isinstance(decoded["flag"], bool)
    print("  [PASS] BSON boolean encode/decode")


def test_bson_null():
    doc = {"nothing": None}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["nothing"] is None
    print("  [PASS] BSON null encode/decode")


def test_bson_objectid():
    oid = ObjectId()
    doc = {"_id": oid}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["_id"] == oid
    print("  [PASS] BSON ObjectId encode/decode")


def test_bson_datetime():
    dt = datetime.datetime(2024, 1, 15, 12, 30, 45, 123000, tzinfo=datetime.timezone.utc)
    doc = {"ts": dt}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["ts"].year == dt.year
    assert decoded["ts"].month == dt.month
    assert decoded["ts"].day == dt.day
    print("  [PASS] BSON datetime encode/decode")


def test_bson_nested_doc():
    doc = {"outer": {"inner": {"value": 123}}}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["outer"]["inner"]["value"] == 123
    print("  [PASS] BSON nested document encode/decode")


def test_bson_array():
    doc = {"arr": [1, "two", 3.0, None, True]}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["arr"][0] == 1
    assert decoded["arr"][1] == "two"
    assert decoded["arr"][2] == 3.0
    assert decoded["arr"][3] is None
    assert decoded["arr"][4] is True
    assert len(decoded["arr"]) == 5
    print("  [PASS] BSON array encode/decode")


def test_bson_binary():
    data = b"\x00\x01\x02\x03\xff"
    doc = {"bin": data}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["bin"] == data
    print("  [PASS] BSON binary encode/decode")


def test_bson_empty_doc():
    doc = {}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded == {}
    assert len(encoded) == 5
    print("  [PASS] BSON empty document")


def test_bson_complex():
    oid = ObjectId()
    dt = datetime.datetime(2024, 6, 1, tzinfo=datetime.timezone.utc)
    doc = {
        "_id": oid,
        "name": "test",
        "count": 42,
        "score": 98.6,
        "active": True,
        "tags": ["a", "b", "c"],
        "nested": {"x": 1},
        "created": dt,
        "data": b"\x01\x02",
        "nothing": None,
        "big": 2**50,
    }
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["_id"] == oid
    assert decoded["name"] == "test"
    assert decoded["count"] == 42
    assert decoded["score"] == 98.6
    assert decoded["active"] is True
    assert decoded["tags"] == ["a", "b", "c"]
    assert decoded["nested"]["x"] == 1
    assert decoded["created"] == dt
    assert decoded["data"] == b"\x01\x02"
    assert decoded["nothing"] is None
    assert decoded["big"] == 2**50
    print("  [PASS] BSON complex document roundtrip")


def test_bson_binary_header():
    doc = {"x": 1}
    encoded = BsonEncoder.encode(doc)
    size = struct.unpack_from("<i", encoded, 0)[0]
    assert size == len(encoded)
    assert encoded[-1] == 0x00
    print("  [PASS] BSON binary format validation")


def test_bson_many_fields():
    doc = {f"field_{i}": i for i in range(100)}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    for i in range(100):
        assert decoded[f"field_{i}"] == i
    print("  [PASS] BSON document with 100 fields")


def test_wire_protocol_build_request():
    msg = WireProtocol.build_request(
        req_id=42,
        command="ping",
    )
    assert len(msg) >= MSG_HEADER_SIZE
    msg_len, req_id, resp_to, op_code = struct.unpack_from("<iiii", msg, 0)
    assert msg_len == len(msg)
    assert req_id == 42
    assert resp_to == 0
    assert op_code == OP_MSG
    print("  [PASS] WireProtocol build_request header")


def test_wire_protocol_build_with_docs():
    docs = [{"x": 1}, {"y": 2}]
    msg = WireProtocol.build_request(
        req_id=1,
        command="insert",
        docs=docs,
        seq_name="documents",
    )
    msg_len = struct.unpack_from("<i", msg, 0)[0]
    assert msg_len == len(msg)
    assert len(msg) > MSG_HEADER_SIZE + 4
    print("  [PASS] WireProtocol build_request with document sequence")


def test_wire_protocol_parse_response():
    msg = WireProtocol.build_request(req_id=1, command="ping")
    resp = WireProtocol.parse_response(msg)
    assert isinstance(resp, OpMsg)
    assert resp.request_id == 1
    assert resp.response_to == 0
    assert resp.more_to_come is False
    print("  [PASS] WireProtocol parse_response basic")


def test_wire_protocol_parse_response_with_docs():
    docs = [{"a": 1}, {"b": 2}]
    msg = WireProtocol.build_request(
        req_id=5,
        command="insert",
        docs=docs,
        seq_name="docs",
    )
    resp = WireProtocol.parse_response(msg)
    assert resp.request_id == 5
    assert len(resp.documents) == 2
    assert resp.documents[0]["a"] == 1
    assert resp.documents[1]["b"] == 2
    print("  [PASS] WireProtocol parse_response with documents")


def test_wire_protocol_roundtrip():
    for cmd in ["ping", "hello", "isMaster"]:
        msg = WireProtocol.build_request(req_id=100, command=cmd)
        resp = WireProtocol.parse_response(msg)
        assert resp.request_id == 100
    print("  [PASS] WireProtocol multiple command roundtrips")


def test_crc32c_basic():
    data = b"123456789"
    result = crc32c(data)
    assert isinstance(result, int)
    assert result != 0
    print("  [PASS] CRC32C basic computation")


def test_crc32c_empty():
    result = crc32c(b"")
    assert result == 0
    print("  [PASS] CRC32C empty input")


def test_crc32c_update():
    data = b"hello world"
    full = crc32c(data)
    partial1 = crc32c_update(0, b"hello ")
    partial2 = crc32c_update(partial1, b"world")
    assert partial2 == full
    print("  [PASS] CRC32C incremental update")


def test_crc32c_known_vectors():
    assert crc32c(b"123456789") == 0xE3069283
    print("  [PASS] CRC32C known test vectors")


def test_wire_protocol_empty_body():
    msg = WireProtocol.build_request(req_id=7, command="ping")
    resp = WireProtocol.parse_response(msg)
    assert "ping" in resp.body or "ok" in resp.body
    print("  [PASS] WireProtocol empty body")


def test_bson_negative_int32():
    doc = {"val": -42}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["val"] == -42
    print("  [PASS] BSON negative int32")


def test_bson_negative_float():
    doc = {"val": -3.14}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["val"] == -3.14
    print("  [PASS] BSON negative float")


def test_bson_empty_string():
    doc = {"val": ""}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["val"] == ""
    print("  [PASS] BSON empty string")


def test_bson_empty_array():
    doc = {"val": []}
    encoded = BsonEncoder.encode(doc)
    decoded = BsonDecoder.decode(encoded)
    assert decoded["val"] == []
    print("  [PASS] BSON empty array")


if __name__ == "__main__":
    print("=== BSON Tests ===")
    test_objectid()
    test_bson_double()
    test_bson_string()
    test_bson_int32()
    test_bson_int64()
    test_bson_bool()
    test_bson_null()
    test_bson_objectid()
    test_bson_datetime()
    test_bson_nested_doc()
    test_bson_array()
    test_bson_binary()
    test_bson_empty_doc()
    test_bson_complex()
    test_bson_binary_header()
    test_bson_many_fields()
    test_bson_negative_int32()
    test_bson_negative_float()
    test_bson_empty_string()
    test_bson_empty_array()

    print("\n=== Wire Protocol Tests ===")
    test_wire_protocol_build_request()
    test_wire_protocol_build_with_docs()
    test_wire_protocol_parse_response()
    test_wire_protocol_parse_response_with_docs()
    test_wire_protocol_roundtrip()
    test_wire_protocol_empty_body()

    print("\n=== CRC32C Tests ===")
    test_crc32c_basic()
    test_crc32c_empty()
    test_crc32c_update()
    test_crc32c_known_vectors()

    print("\n=== All tests passed ===")
