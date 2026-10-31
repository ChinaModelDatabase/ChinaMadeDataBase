import os
import struct
import time
import threading


class ObjectId:
    _counter_lock = threading.Lock()
    _counter = int.from_bytes(os.urandom(4), "big") & 0xFFFFFF
    _random_bytes = os.urandom(5)

    def __init__(self, oid: bytes = None):
        if oid is None:
            oid = self._generate()
        if len(oid) != 12:
            raise ValueError("ObjectId must be exactly 12 bytes")
        self._id = oid

    @classmethod
    def _generate(cls) -> bytes:
        timestamp = int(time.time())
        with cls._counter_lock:
            counter = cls._counter
            cls._counter = (cls._counter + 1) & 0xFFFFFF
        return (
            struct.pack(">I", timestamp)
            + cls._random_bytes
            + struct.pack(">I", counter)[1:4]
        )

    @classmethod
    def from_hex(cls, hex_str: str) -> "ObjectId":
        return cls(bytes.fromhex(hex_str))

    @property
    def binary(self) -> bytes:
        return self._id

    @property
    def generation_time(self) -> float:
        return float(struct.unpack(">I", self._id[0:4])[0])

    def __str__(self) -> str:
        return self._id.hex()

    def __repr__(self) -> str:
        return f"ObjectId('{self}')"

    def __eq__(self, other) -> bool:
        if isinstance(other, ObjectId):
            return self._id == other._id
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._id)
