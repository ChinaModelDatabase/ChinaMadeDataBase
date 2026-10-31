from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from pymongo.cursor import Cursor as _PyMongoCursor
from pymongo.collation import Collation


class MdbCursor:
    def __init__(self, pymongo_cursor: _PyMongoCursor) -> None:
        self._cursor = pymongo_cursor

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return self

    def __next__(self) -> Dict[str, Any]:
        return next(self._cursor)

    def sort(
        self,
        key_or_list: Union[str, List[Tuple[str, int]]],
        direction: int = 1,
    ) -> "MdbCursor":
        self._cursor.sort(key_or_list, direction)
        return self

    def limit(self, n: int) -> "MdbCursor":
        self._cursor.limit(n)
        return self

    def skip(self, n: int) -> "MdbCursor":
        self._cursor.skip(n)
        return self

    def batch_size(self, n: int) -> "MdbCursor":
        self._cursor.batch_size(n)
        return self

    def projection(self, spec: Dict[str, int]) -> "MdbCursor":
        self._cursor.projection(spec)
        return self

    def explain(self) -> Dict[str, Any]:
        return self._cursor.explain()

    def to_list(self, length: Optional[int] = None) -> List[Dict[str, Any]]:
        if length is None:
            return list(self._cursor)
        result: List[Dict[str, Any]] = []
        for _ in range(length):
            try:
                result.append(next(self._cursor))
            except StopIteration:
                break
        return result

    def next(self) -> Dict[str, Any]:
        return next(self._cursor)

    def close(self) -> None:
        self._cursor.close()

    @property
    def alive(self) -> bool:
        return self._cursor.alive

    @property
    def collection(self) -> Any:
        return self._cursor.collection

    def __enter__(self) -> "MdbCursor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
