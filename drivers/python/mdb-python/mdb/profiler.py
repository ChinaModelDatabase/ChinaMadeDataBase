from __future__ import annotations

from typing import Any, Dict, List


class MdbProfiler:
    def __init__(self, database: Any) -> None:
        self._database = database

    def enable(self, slow_ms: int = 100, sample_rate: float = 1.0) -> None:
        self._database.command({
            "profile": 1,
            "slowms": slow_ms,
            "sampleRate": sample_rate,
        })

    def disable(self) -> None:
        self._database.command({"profile": 0})

    def slow_queries(self, limit: int = 100) -> List[Dict[str, Any]]:
        system_profiler = self._database["system.profile"]
        cursor = system_profiler.find().sort("ts", -1).limit(limit)
        return list(cursor)

    def status(self) -> Dict[str, Any]:
        return self._database.command("profile", 2)

    def stats(self) -> Dict[str, Any]:
        status = self.status()
        level = status.get("was", 0)
        slow_ms = status.get("slowms", 100)
        return {
            "enabled": level > 0,
            "level": level,
            "slow_ms": slow_ms,
            "sample_rate": status.get("sampleRate", 1.0),
        }

    def set_level(self, level: int) -> None:
        if level not in (0, 1, 2):
            raise ValueError(f"Invalid profiling level: {level}. Must be 0, 1, or 2.")
        self._database.command({"profile": level})

    def reset(self) -> None:
        self.disable()
        system_profiler = self._database["system.profile"]
        system_profiler.drop()

    def __repr__(self) -> str:
        return "MdbProfiler()"
