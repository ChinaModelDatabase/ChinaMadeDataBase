"""用 pymongo 连到 MDB 探针，观察驱动发出的第一批消息。

只做观察，不做任何断言——目的是拿到真实驱动行为的第一手证据，
而不是验证我们自己（可能一致地错）的实现。

    python scripts/probe_client.py [host] [port]
"""

import sys

import pymongo
from pymongo import MongoClient
from pymongo.errors import PyMongoError


def main() -> int:
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 27099

    print(f"pymongo {pymongo.version} -> {host}:{port}")
    client = MongoClient(
        host,
        port,
        serverSelectionTimeoutMS=4000,
        connectTimeoutMS=4000,
        directConnection=True,
    )
    try:
        # admin.command("hello") 走与握手同一条路径
        print("--- issuing hello ---")
        info = client.admin.command("hello")
        print("--- got reply ---")
        for key in sorted(info):
            print(f"  {key} = {info[key]!r}")
    except PyMongoError as exc:
        print(f"PyMongoError: {type(exc).__name__}: {exc}")
        return 1
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
