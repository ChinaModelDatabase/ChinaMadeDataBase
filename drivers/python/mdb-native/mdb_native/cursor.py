from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple

from mdb_native.connection import MdbConnection

class MdbCursor:
    def __init__(
        self,
        conn: MdbConnection,
        db_name: str,
        coll_name: str,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._conn = conn
        self._db_name = db_name
        self._coll_name = coll_name
        self._filter = filter
        self._projection = projection
        self._batch: List[Dict[str, Any]] = []
        self._cursor_id: int = 0
        self._ns: Optional[str] = None
        self._retrieved: int = 0
        self._limit: int = 0
        self._skip: int = 0
        self._sort: Optional[Dict[str, Any]] = None
        self._batch_size: int = 101
        self._closed = False

    def __iter__(self) -> "MdbCursor":
        return self

    def __next__(self) -> Dict[str, Any]:
        while True:
            if self._batch:
                doc = self._batch.pop(0)
                self._retrieved += 1
                if self._limit > 0 and self._retrieved > self._limit:
                    self.close()
                    raise StopIteration
                return doc

            if self._cursor_id == 0 and not self._batch:
                if self._retrieved == 0:
                    self._execute_query()
                    if self._batch:
                        continue
                raise StopIteration

            if self._cursor_id != 0:
                self._get_more()
                continue

            raise StopIteration

    def _execute_query(self) -> None:
        find_cmd: Dict[str, Any] = {
            "find": self._coll_name,
            "filter": self._filter or {},
        }
        if self._projection is not None:
            find_cmd["projection"] = self._projection
        if self._limit != 0:
            find_cmd["limit"] = abs(self._limit)
        if self._skip:
            find_cmd["skip"] = self._skip
        if self._sort:
            find_cmd["sort"] = self._sort
        find_cmd["batchSize"] = self._batch_size

        resp = self._conn.send_command(**find_cmd)
        self._cursor_id = resp.get("cursor", {}).get("id", 0)
        self._ns = resp.get("cursor", {}).get("ns")
        self._batch = resp.get("cursor", {}).get("firstBatch", [])

    def _get_more(self) -> None:
        if not self._ns:
            raise StopIteration
        ns_parts = self._ns.split(".", 1)
        db_name = ns_parts[0] if ns_parts else self._db_name

        resp = self._conn.send_command(
            "getMore",
            **{
                "getMore": self._cursor_id,
                "collection": self._coll_name,
                "batchSize": self._batch_size,
            },
        )
        self._cursor_id = resp.get("cursor", {}).get("id", 0)
        self._batch = resp.get("cursor", {}).get("nextBatch", [])

    def sort(self, key_or_list: Any, direction: int = 1) -> "MdbCursor":
        if isinstance(key_or_list, str):
            self._sort = {key_or_list: direction}
        elif isinstance(key_or_list, list):
            self._sort = dict(key_or_list)
        elif isinstance(key_or_list, dict):
            self._sort = key_or_list
        return self

    def limit(self, count: int) -> "MdbCursor":
        self._limit = count
        return self

    def skip(self, count: int) -> "MdbCursor":
        self._skip = count
        return self

    def projection(self, spec: Dict[str, Any]) -> "MdbCursor":
        self._projection = spec
        return self

    def batch_size(self, size: int) -> "MdbCursor":
        self._batch_size = size
        return self

    def to_list(self, length: Optional[int] = None) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for doc in self:
            result.append(doc)
            if length is not None and len(result) >= length:
                break
        return result

    def explain(self) -> Dict[str, Any]:
        find_cmd: Dict[str, Any] = {
            "find": self._coll_name,
            "filter": self._filter or {},
        }
        if self._projection is not None:
            find_cmd["projection"] = self._projection
        return self._conn.send_command(
            "explain",
            **{self._coll_name: 1},
        )

    def close(self) -> None:
        if self._cursor_id != 0 and self._ns:
            try:
                self._conn.send_command(
                    "killCursors",
                    **{self._coll_name: 1, "cursors": [self._cursor_id]},
                )
            except Exception:
                pass
        self._cursor_id = 0
        self._batch.clear()
        self._closed = True
