from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional


class MdbChangeStreamEvent:
    def __init__(self, raw_event: Dict[str, Any]) -> None:
        self._raw = raw_event

    @property
    def operation_type(self) -> str:
        return self._raw.get("operationType", "")

    @property
    def full_document(self) -> Optional[Dict[str, Any]]:
        return self._raw.get("fullDocument")

    @property
    def document_key(self) -> Dict[str, Any]:
        return self._raw.get("documentKey", {})

    @property
    def update_description(self) -> Optional[Dict[str, Any]]:
        return self._raw.get("updateDescription")

    @property
    def ns(self) -> Dict[str, Any]:
        return self._raw.get("ns", {})

    @property
    def cluster_time(self) -> Any:
        return self._raw.get("clusterTime")

    @property
    def resume_token(self) -> Any:
        return self._raw.get("_id")

    def __repr__(self) -> str:
        return (
            f"MdbChangeStreamEvent(operation_type={self.operation_type!r}, "
            f"ns={self.ns!r})"
        )


class MdbChangeStream:
    def __init__(self, pymongo_change_stream: Any) -> None:
        self._stream = pymongo_change_stream

    def __iter__(self) -> Iterator[MdbChangeStreamEvent]:
        return self

    def __next__(self) -> MdbChangeStreamEvent:
        raw = next(self._stream)
        return MdbChangeStreamEvent(raw)

    def next(self) -> MdbChangeStreamEvent:
        raw = next(self._stream)
        return MdbChangeStreamEvent(raw)

    def try_next(self) -> Optional[MdbChangeStreamEvent]:
        raw = self._stream.try_next()
        if raw is None:
            return None
        return MdbChangeStreamEvent(raw)

    def resume_after(self, token: Any) -> "MdbChangeStream":
        self._stream.resume_after(token)
        return self

    def start_after(self, token: Any) -> "MdbChangeStream":
        self._stream.start_after(token)
        return self

    def start_at_operation_time(self, time: Any) -> "MdbChangeStream":
        self._stream.start_at_operation_time(time)
        return self

    def max_await_time_ms(self, ms: int) -> "MdbChangeStream":
        self._stream.max_await_time_ms(ms)
        return self

    def pipeline(self, stages: List[Dict[str, Any]]) -> "MdbChangeStream":
        self._stream.pipeline(stages)
        return self

    def close(self) -> None:
        self._stream.close()

    def __enter__(self) -> "MdbChangeStream":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
