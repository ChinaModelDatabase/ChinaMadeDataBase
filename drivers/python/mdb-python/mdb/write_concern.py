from __future__ import annotations

from enum import Enum
from typing import Any, Optional


class WValue(Enum):
    MAJORITY = "majority"
    ONE = 1
    ZERO = 0


class WriteConcern:
    def __init__(
        self,
        w: Optional[Union[int, str, WValue]] = None,
        wtimeout_ms: Optional[int] = None,
        j: Optional[bool] = None,
        fsync: Optional[bool] = None,
    ) -> None:
        if w is not None:
            if isinstance(w, WValue):
                self._w: Any = w.value
            else:
                self._w = w
        else:
            self._w = None
        self._wtimeout_ms = wtimeout_ms
        self._j = j
        self._fsync = fsync

    @property
    def w(self) -> Any:
        return self._w

    @property
    def wtimeout_ms(self) -> Optional[int]:
        return self._wtimeout_ms

    @property
    def j(self) -> Optional[bool]:
        return self._j

    @property
    def fsync(self) -> Optional[bool]:
        return self._fsync

    def document(self) -> Dict[str, Any]:
        doc: Dict[str, Any] = {}
        if self._w is not None:
            doc["w"] = self._w
        if self._wtimeout_ms is not None:
            doc["wtimeout"] = self._wtimeout_ms
        if self._j is not None:
            doc["j"] = self._j
        if self._fsync is not None:
            doc["fsync"] = self._fsync
        return doc

    def __repr__(self) -> str:
        parts = []
        if self._w is not None:
            parts.append(f"w={self._w!r}")
        if self._wtimeout_ms is not None:
            parts.append(f"wtimeout_ms={self._wtimeout_ms}")
        if self._j is not None:
            parts.append(f"j={self._j}")
        if self._fsync is not None:
            parts.append(f"fsync={self._fsync}")
        return f"WriteConcern({', '.join(parts)})"


DEFAULT_WRITE_CONCERN = WriteConcern(w=WValue.MAJORITY)

from typing import Dict

W_CONCERN_MAJORITY = WriteConcern(w=WValue.MAJORITY)
W_CONCERN_ONE = WriteConcern(w=1)
W_CONCERN_ZERO = WriteConcern(w=0)
