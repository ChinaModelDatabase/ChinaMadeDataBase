from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pymongo import MongoClient as _PyMongoClient
from pymongo.uri_parser import parse_uri

from .database import MdbDatabase
from .transaction import MdbTransaction


class MdbClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_source: str = "admin",
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("serverSelectionTimeoutMS", 5000)

        if username and password:
            kwargs.setdefault("username", username)
            kwargs.setdefault("password", password)
            kwargs.setdefault("authSource", auth_source)

        self._client = _PyMongoClient(host, port, **kwargs)
        self._host = host
        self._port = port

    @classmethod
    def from_uri(cls, uri: str, **kwargs: Any) -> "MdbClient":
        parsed = parse_uri(uri)
        # pymongo 的 parse_uri 把主机列表放在 `nodelist`（不是 `hosts`）。
        # 取错键会让 URI 里的地址被静默换成 localhost:27017 —— 连到别的实例
        # 而不是报错，非常难查。
        hosts = parsed.get("nodelist") or parsed.get("hosts") or [("localhost", 27017)]
        host = hosts[0][0]
        port = hosts[0][1] if len(hosts[0]) > 1 else 27017
        username = parsed.get("username")
        password = parsed.get("password")
        options = parsed.get("options", {}) or {}
        auth_source = options.get("authSource") or options.get("authsource") or "admin"
        database = parsed.get("database")

        client = cls(
            host=host,
            port=port,
            username=username,
            password=password,
            auth_source=auth_source,
            **kwargs,
        )

        if database:
            client._default_database = database

        return client

    def database(self, name: Optional[str] = None) -> MdbDatabase:
        if name is None:
            name = getattr(self, "_default_database", "test")
        pymongo_db = self._client[name]
        return MdbDatabase(pymongo_db)

    def __getitem__(self, name: str) -> MdbDatabase:
        return self.database(name)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def list_databases(self) -> List[Dict[str, Any]]:
        return list(self._client.list_databases())

    def server_status(self) -> Dict[str, Any]:
        return self._client.admin.command("serverStatus")

    def replica_set_status(self) -> Dict[str, Any]:
        return self._client.admin.command("replSetGetStatus")

    def is_connected(self) -> bool:
        try:
            self._client.admin.command("ping")
            return True
        except Exception:
            return False

    def close(self) -> None:
        self._client.close()

    def start_transaction(self) -> MdbTransaction:
        return MdbTransaction(self._client)

    def start_session(self, **kwargs: Any) -> Any:
        return self._client.start_session(**kwargs)

    def __enter__(self) -> "MdbClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"MdbClient(host={self._host!r}, port={self._port})"
