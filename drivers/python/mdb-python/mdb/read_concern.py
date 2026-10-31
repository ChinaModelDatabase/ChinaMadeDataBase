from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional


class ReadConcernLevel(Enum):
    LOCAL = "local"
    AVAILABLE = "available"
    MAJORITY = "majority"
    LINEARIZABLE = "linearizable"
    SNAPSHOT = "snapshot"


class ReadConcern:
    def __init__(
        self,
        level: Optional[Union[str, ReadConcernLevel]] = None,
        after_cluster_time: Optional[int] = None,
    ) -> None:
        if level is not None:
            if isinstance(level, ReadConcernLevel):
                self._level: Optional[str] = level.value
            else:
                self._level = level
        else:
            self._level = None
        self._after_cluster_time = after_cluster_time

    @property
    def level(self) -> Optional[str]:
        return self._level

    @property
    def after_cluster_time(self) -> Optional[int]:
        return self._after_cluster_time

    def document(self) -> Dict[str, Any]:
        doc: Dict[str, Any] = {}
        if self._level is not None:
            doc["level"] = self._level
        if self._after_cluster_time is not None:
            doc["afterClusterTime"] = self._after_cluster_time
        return doc

    def __repr__(self) -> str:
        parts = []
        if self._level is not None:
            parts.append(f"level={self._level!r}")
        if self._after_cluster_time is not None:
            parts.append(f"after_cluster_time={self._after_cluster_time}")
        return f"ReadConcern({', '.join(parts)})"


DEFAULT_READ_CONCERN = ReadConcern(level=ReadConcernLevel.MAJORITY)

RC_LOCAL = ReadConcern(level=ReadConcernLevel.LOCAL)
RC_AVAILABLE = ReadConcern(level=ReadConcernLevel.AVAILABLE)
RC_MAJORITY = ReadConcern(level=ReadConcernLevel.MAJORITY)
RC_LINEARIZABLE = ReadConcern(level=ReadConcernLevel.LINEARIZABLE)
RC_SNAPSHOT = ReadConcern(level=ReadConcernLevel.SNAPSHOT)
