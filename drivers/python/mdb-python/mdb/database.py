from __future__ import annotations

from typing import Any, Dict, List, Optional

from .collection import MdbCollection
from .profiler import MdbProfiler


class MdbDatabase:
    def __init__(self, pymongo_database: Any) -> None:
        self._database = pymongo_database

    @property
    def name(self) -> str:
        return self._database.name

    @property
    def client(self) -> Any:
        return self._database.client

    def collection(self, name: str) -> MdbCollection:
        pymongo_coll = self._database[name]
        return MdbCollection(pymongo_coll)

    def __getitem__(self, name: str) -> MdbCollection:
        return self.collection(name)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._database, name)

    def list_collections(self) -> List[Dict[str, Any]]:
        return list(self._database.list_collections())

    def create_collection(self, name: str, **options: Any) -> Any:
        return self._database.create_collection(name, **options)

    def drop_collection(self, name: str) -> None:
        self._database.drop_collection(name)

    def command(self, command: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        return self._database.command(command, **kwargs)

    def stats(self) -> Dict[str, Any]:
        return self._database.command("dbStats")

    def profiler(self) -> MdbProfiler:
        return MdbProfiler(self._database)

    def create_user(
        self,
        username: str,
        password: str,
        roles: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if roles is None:
            roles = [{"role": "readWrite", "db": self.name}]
        user_doc = {
            "createUser": username,
            "pwd": password,
            "roles": roles,
        }
        return self._database.command(user_doc)

    def drop_user(self, username: str) -> Dict[str, Any]:
        return self._database.command({"dropUser": username})

    def get_users(self) -> List[Dict[str, Any]]:
        result = self._database.command({"usersInfo": 1})
        return result.get("users", [])

    def __repr__(self) -> str:
        return f"MdbDatabase({self.name!r})"
