"""驱动握手实测：让 pymongo 连上 MDB 并校验 hello 应答。

这是 MDB 目前**唯一**的外部驱动验证。单测只能证明自洽，
证明不了 pymongo 真的能连上。

    python scripts/handshake_check.py [host] [port]

返回码：0 = 通过，1 = 失败。
"""

import sys

from pymongo import MongoClient
from pymongo.errors import PyMongoError

# 期望的握手字段：(字段名, 期望值, 期望 BSON 承载类型)
EXPECTED = [
    ("maxWireVersion", 21, int),
    ("minWireVersion", 0, int),
    ("maxBsonObjectSize", 16777216, int),
    ("maxMessageSizeBytes", 48000000, int),
    ("maxWriteBatchSize", 100000, int),
    ("logicalSessionTimeoutMinutes", 30, int),
    ("ismaster", True, bool),
    ("isWritablePrimary", True, bool),
    ("helloOk", True, bool),
    ("readOnly", False, bool),
    ("ok", 1.0, float),
]


def main() -> int:
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 27098

    failures = []
    try:
        client = MongoClient(
            host,
            port,
            serverSelectionTimeoutMS=6000,
            connectTimeoutMS=6000,
            directConnection=True,
        )
        info = client.admin.command("hello")
    except PyMongoError as exc:
        print(f"FAIL connect/hello: {type(exc).__name__}: {exc}")
        return 1

    print("hello reply:")
    for key in sorted(info):
        print(f"  {key} = {info[key]!r} ({type(info[key]).__name__})")
    print()

    for key, want, want_type in EXPECTED:
        if key not in info:
            failures.append(f"missing field {key!r}")
            continue
        got = info[key]
        if type(got) is not want_type:
            failures.append(
                f"{key}: type {type(got).__name__} != expected {want_type.__name__} (got {got!r})"
            )
            continue
        if got != want:
            failures.append(f"{key}: {got!r} != expected {want!r}")

    # localTime 必须是 datetime，不能是 int
    import datetime

    if not isinstance(info.get("localTime"), datetime.datetime):
        failures.append(
            f"localTime: type {type(info.get('localTime')).__name__} != expected datetime"
        )

    # 未实现的命令应报 CommandNotImplemented，而不是连接失败
    try:
        client.db.coll.find_one({})
        failures.append("find_one unexpectedly succeeded (not implemented yet)")
    except PyMongoError as exc:
        code = getattr(exc, "code", None)
        print(f"find_one correctly rejected: code={code} ({type(exc).__name__})")
        if code != 59:
            failures.append(f"find_one error code {code}, expected 59 CommandNotImplemented")

    client.close()

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nPASS: pymongo accepted the handshake and all fields match")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
