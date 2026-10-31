from __future__ import annotations
from typing import Any, Dict, List, Optional

from mdb_native.connection import MdbConnection
from mdb_native.collection import MdbCollection

class MdbDatabase:
    def __init__(self, conn: MdbConnection, name: str) -> None:
        self._conn = conn
        self.name = name

    def collection(self, name: str) -> MdbDatabase:
        return MdbCollection(self._conn, self.name, name)

    def list_collections(self) -> List[Dict[str, Any]]:
        resp = self._conn.send_command(
            "listCollections",
            filter={},
        )
        return resp.get("cursor", {}).get("firstBatch", [])

    def create_collection(
        self,
        name: str,
        capped: bool = False,
        size: Optional[int] = None,
        max: Optional[int] = None,
    ) -> Dict[str, Any]:
        options: Dict[str, Any] = {}
        if capped:
            options["capped"] = True
            if size is not None:
                options["size"] = size
            if max is not None:
                options["max"] = max
        resp = self._conn.send_command("create", **{name: 1, **options})
        return resp

    def drop_collection(self, name: str) -> Dict[str, Any]:
        resp = self._conn.send_command("drop", **{name: 1})
        return resp

    def command(self, cmd: str, **kwargs: Any) -> Dict[str, Any]:
        return self._conn.send_command(cmd, **kwargs)

    def stats(self, scale: int = 1) -> Dict[str, Any]:
        return self._conn.send_command("dbStats", scale=scale)

    def create_user(
        self,
        username: str,
        password: str,
        roles: Optional[List[Dict[str, str]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        user_doc: Dict[str, Any] = {
            "createUser": username,
            "pwd": password,
            **kwargs,
        }
        if roles is not None:
            user_doc["roles"] = roles
        else:
            user_doc["roles"] = [{"role": "readWrite", "db": self.name}]
        return self._conn.send_command(**user_doc)
