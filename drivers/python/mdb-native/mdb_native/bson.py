import struct
import datetime
import os
import time
from typing import Any, Dict, List, Optional, Tuple

# BSON element types
BSON_DOUBLE = 1
BSON_STRING = 2
BSON_DOCUMENT = 3
BSON_ARRAY = 4
BSON_BINARY = 5
BSON_OBJECTID = 7
BSON_BOOLEAN = 8
BSON_DATETIME = 9
BSON_NULL = 10
BSON_INT32 = 16
BSON_INT64 = 18

class ObjectId:
    _random_bytes = os.urandom(5)
    _counter = int.from_bytes(os.urandom(3), "big")
    _pid = os.getpid() & 0xFFFF

    def __init__(self, oid: Optional[bytes] = None) -> None:
        if oid is None:
            self._id = self._generate()
        elif isinstance(oid, bytes) and len(oid) == 12:
            self._id = oid
        elif isinstance(oid, str) and len(oid) == 24:
            self._id = bytes.fromhex(oid)
        else:
            raise ValueError(f"Invalid ObjectId: {oid!r}")

    @classmethod
    def _generate(cls) -> bytes:
        ts = int(time.time())
        cls._counter = (cls._counter + 1) & 0xFFFFFF
        counter_bytes = cls._counter.to_bytes(3, "big")
        return (
            ts.to_bytes(4, "big")
            + cls._random_bytes
            + counter_bytes
        )

    def __str__(self) -> str:
        return self._id.hex()

    def __repr__(self) -> str:
        return f"ObjectId('{self}')"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ObjectId):
            return self._id == other._id
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._id)

    def __bytes__(self) -> bytes:
        return self._id

    @property
    def binary(self) -> bytes:
        return self._id

class BsonEncoder:
    @staticmethod
    def encode(doc: Dict[str, Any]) -> bytes:
        buf = bytearray()
        buf.extend(b"\x00\x00\x00\x00")
        for key, value in doc.items():
            _encode_element(buf, key, value)
        buf.append(0x00)
        struct.pack_into("<i", buf, 0, len(buf))
        return bytes(buf)

    @staticmethod
    def encode_value(value: Any) -> Tuple[int, bytes]:
        buf = bytearray()
        _encode_value_inner(buf, value)
        return bytes(buf)

def _encode_value_inner(buf: bytearray, value: Any) -> None:
    if isinstance(value, float):
        buf.extend(struct.pack("<d", value))
    elif isinstance(value, bool):
        buf.append(1 if value else 0)
    elif isinstance(value, int):
        if -0x80000000 <= value <= 0x7FFFFFFF:
            buf.extend(struct.pack("<i", value))
        else:
            buf.extend(struct.pack("<q", value))
    elif isinstance(value, str):
        encoded = value.encode("utf-8")
        buf.extend(struct.pack("<i", len(encoded) + 1))
        buf.extend(encoded)
        buf.append(0x00)
    elif isinstance(value, bytes):
        buf.extend(struct.pack("<i", len(value) + 5))
        buf.append(0x00)
        buf.extend(struct.pack("<i", len(value)))
        buf.extend(value)
    elif value is None:
        pass
    elif isinstance(value, ObjectId):
        buf.extend(value.binary)
    elif isinstance(value, datetime.datetime):
        ts = int(value.timestamp() * 1000)
        buf.extend(struct.pack("<q", ts))
    elif isinstance(value, dict):
        inner = BsonEncoder.encode(value)
        buf.extend(inner)
    elif isinstance(value, (list, tuple)):
        arr_doc = {}
        for i, item in enumerate(value):
            arr_doc[str(i)] = item
        inner = BsonEncoder.encode(arr_doc)
        buf.extend(inner)
    else:
        raise TypeError(f"Cannot encode type {type(value)}")

def _encode_type_byte(buf: bytearray, value: Any) -> None:
    if isinstance(value, float):
        buf.append(BSON_DOUBLE)
    elif isinstance(value, bool):
        buf.append(BSON_BOOLEAN)
    elif isinstance(value, int):
        if -0x80000000 <= value <= 0x7FFFFFFF:
            buf.append(BSON_INT32)
        else:
            buf.append(BSON_INT64)
    elif isinstance(value, str):
        buf.append(BSON_STRING)
    elif isinstance(value, bytes):
        buf.append(BSON_BINARY)
    elif value is None:
        buf.append(BSON_NULL)
    elif isinstance(value, ObjectId):
        buf.append(BSON_OBJECTID)
    elif isinstance(value, datetime.datetime):
        buf.append(BSON_DATETIME)
    elif isinstance(value, dict):
        buf.append(BSON_DOCUMENT)
    elif isinstance(value, (list, tuple)):
        buf.append(BSON_ARRAY)
    else:
        raise TypeError(f"Cannot encode type {type(value)}")

def _encode_element(buf: bytearray, name: str, value: Any) -> None:
    _encode_type_byte(buf, value)
    name_bytes = name.encode("utf-8")
    buf.extend(name_bytes)
    buf.append(0x00)
    _encode_value_inner(buf, value)

class BsonDecoder:
    @staticmethod
    def decode(data: bytes) -> Dict[str, Any]:
        if len(data) < 5:
            raise ValueError("BSON document too short")
        size = struct.unpack_from("<i", data, 0)[0]
        if size != len(data):
            raise ValueError(f"Size mismatch: header={size}, actual={len(data)}")
        doc, _ = _decode_document(data, 4)
        return doc

def _decode_document(data: bytes, offset: int) -> Tuple[Dict[str, Any], int]:
    doc: Dict[str, Any] = {}
    while offset < len(data):
        elem_type = data[offset]
        offset += 1
        if elem_type == 0x00:
            break
        name, offset = _read_cstring(data, offset)
        value, offset = _decode_value(data, offset, elem_type)
        doc[name] = value
    return doc, offset

def _read_cstring(data: bytes, offset: int) -> Tuple[str, int]:
    end = data.index(0x00, offset)
    return data[offset:end].decode("utf-8"), end + 1

def _decode_value(data: bytes, offset: int, elem_type: int) -> Tuple[Any, int]:
    if elem_type == BSON_DOUBLE:
        value = struct.unpack_from("<d", data, offset)[0]
        return value, offset + 8
    elif elem_type == BSON_STRING:
        str_len = struct.unpack_from("<i", data, offset)[0]
        offset += 4
        value = data[offset:offset + str_len - 1].decode("utf-8")
        return value, offset + str_len
    elif elem_type == BSON_DOCUMENT:
        doc_len = struct.unpack_from("<i", data, offset)[0]
        doc, _ = _decode_document(data, offset + 4)
        return doc, offset + doc_len
    elif elem_type == BSON_ARRAY:
        doc_len = struct.unpack_from("<i", data, offset)[0]
        arr_doc, _ = _decode_document(data, offset + 4)
        arr = [arr_doc[str(i)] for i in range(len(arr_doc))]
        return arr, offset + doc_len
    elif elem_type == BSON_BINARY:
        total_len = struct.unpack_from("<i", data, offset)[0]
        offset += 4
        subtype = data[offset]
        offset += 1
        bin_len = struct.unpack_from("<i", data, offset)[0]
        offset += 4
        value = data[offset:offset + bin_len]
        return value, offset + bin_len
    elif elem_type == BSON_OBJECTID:
        oid = ObjectId(data[offset:offset + 12])
        return oid, offset + 12
    elif elem_type == BSON_BOOLEAN:
        value = data[offset] != 0
        return value, offset + 1
    elif elem_type == BSON_DATETIME:
        ts_ms = struct.unpack_from("<q", data, offset)[0]
        dt = datetime.datetime.fromtimestamp(ts_ms / 1000, tz=datetime.timezone.utc)
        return dt, offset + 8
    elif elem_type == BSON_NULL:
        return None, offset
    elif elem_type == BSON_INT32:
        value = struct.unpack_from("<i", data, offset)[0]
        return value, offset + 4
    elif elem_type == BSON_INT64:
        value = struct.unpack_from("<q", data, offset)[0]
        return value, offset + 8
    else:
        raise ValueError(f"Unknown BSON type: {elem_type}")
