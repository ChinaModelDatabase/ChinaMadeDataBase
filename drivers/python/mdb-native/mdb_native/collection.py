from __future__ import annotations
from typing import Any, Dict, List, Optional, Sequence, Union

from mdb_native.connection import MdbConnection
from mdb_native.cursor import MdbCursor

class MdbCollection:
    def __init__(self, conn: MdbConnection, db_name: str, coll_name: str) -> None:
        self._conn = conn
        self._db_name = db_name
        self._coll_name = coll_name

    @property
    def full_name(self) -> str:
        return f"{self._db_name}.{self._coll_name}"

    def _ns(self) -> str:
        return self.full_name

    def insert_one(self, document: Dict[str, Any]) -> Dict[str, Any]:
        from mdb_native.bson import ObjectId
        if "_id" not in document:
            document["_id"] = ObjectId()
        resp = self._conn.send_command_with_docs(
            "insert",
            [document],
            seq_name="documents",
            **{self._coll_name: 1},
        )
        return {
            "acknowledged": True,
            "inserted_id": document["_id"],
        }

    def insert_many(
        self, documents: List[Dict[str, Any]], ordered: bool = True
    ) -> Dict[str, Any]:
        from mdb_native.bson import ObjectId
        inserted_ids = []
        for doc in documents:
            if "_id" not in doc:
                doc["_id"] = ObjectId()
            inserted_ids.append(doc["_id"])

        resp = self._conn.send_command_with_docs(
            "insert",
            documents,
            seq_name="documents",
            ordered=ordered,
            **{self._coll_name: 1},
        )
        return {
            "acknowledged": True,
            "inserted_ids": inserted_ids,
        }

    def find(
        self,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
    ) -> MdbCursor:
        return MdbCursor(self._conn, self._db_name, self._coll_name, filter, projection)

    def find_one(
        self,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        cursor = self.find(filter, projection).limit(1)
        for doc in cursor:
            return doc
        return None

    def update_one(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> Dict[str, Any]:
        cmd_args: Dict[str, Any] = {
            "q": filter,
            "u": update,
            "upsert": upsert,
        }
        resp = self._conn.send_command(
            "update",
            **{self._coll_name: [cmd_args]},
        )
        return {
            "acknowledged": True,
            "matched_count": resp.get("n", 0),
            "modified_count": resp.get("nModified", 0),
            "upserted_id": resp.get("upserted"),
        }

    def update_many(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> Dict[str, Any]:
        cmd_args: Dict[str, Any] = {
            "q": filter,
            "u": update,
            "upsert": upsert,
            "multi": True,
        }
        resp = self._conn.send_command(
            "update",
            **{self._coll_name: [cmd_args]},
        )
        return {
            "acknowledged": True,
            "matched_count": resp.get("n", 0),
            "modified_count": resp.get("nModified", 0),
            "upserted_id": resp.get("upserted"),
        }

    def delete_one(self, filter: Dict[str, Any]) -> Dict[str, Any]:
        cmd_args: Dict[str, Any] = {"q": filter, "limit": 1}
        resp = self._conn.send_command(
            "delete",
            **{self._coll_name: [cmd_args]},
        )
        return {
            "acknowledged": True,
            "deleted_count": resp.get("n", 0),
        }

    def delete_many(self, filter: Dict[str, Any]) -> Dict[str, Any]:
        cmd_args: Dict[str, Any] = {"q": filter, "limit": 0}
        resp = self._conn.send_command(
            "delete",
            **{self._coll_name: [cmd_args]},
        )
        return {
            "acknowledged": True,
            "deleted_count": resp.get("n", 0),
        }

    def aggregate(
        self,
        pipeline: List[Dict[str, Any]],
        batch_size: Optional[int] = None,
    ) -> MdbCursor:
        cursor_opt: Dict[str, Any] = {}
        if batch_size is not None:
            cursor_opt["batchSize"] = batch_size
        resp = self._conn.send_command(
            "aggregate",
            **{self._coll_name: 1, "pipeline": pipeline, "cursor": cursor_opt},
        )
        first_batch = resp.get("cursor", {}).get("firstBatch", [])
        cursor_id = resp.get("cursor", {}).get("id", 0)
        ns = resp.get("cursor", {}).get("ns", self._ns())

        cursor = MdbCursor.__new__(MdbCursor)
        cursor._conn = self._conn
        cursor._db_name = self._db_name
        cursor._coll_name = self._coll_name
        cursor._filter = None
        cursor._projection = None
        cursor._batch = first_batch
        cursor._cursor_id = cursor_id
        cursor._ns = ns
        cursor._retrieved = 0
        cursor._limit = 0
        cursor._skip = 0
        cursor._sort: Optional[Dict[str, Any]] = None
        return cursor

    def distinct(
        self,
        key: str,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        args: Dict[str, Any] = {
            "distinct": self._coll_name,
            "key": key,
        }
        if filter:
            args["query"] = filter
        resp = self._conn.send_command(**args)
        return resp.get("values", [])

    def count_documents(
        self,
        filter: Optional[Dict[str, Any]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> int:
        pipeline: List[Dict[str, Any]] = []
        if filter:
            pipeline.append({"$match": filter})
        if skip is not None:
            pipeline.append({"$skip": skip})
        if limit is not None:
            pipeline.append({"$limit": limit})
        pipeline.append({"$count": "count"})

        resp = self._conn.send_command(
            "count",
            **{self._coll_name: 1},
        )
        if "n" in resp:
            return resp["n"]

        cursor_resp = self._conn.send_command(
            "aggregate",
            **{self._coll_name: 1, "pipeline": pipeline, "cursor": {}},
        )
        batch = cursor_resp.get("cursor", {}).get("firstBatch", [])
        if batch:
            return batch[0].get("count", 0)
        return 0

    def create_index(
        self,
        keys: Union[str, List[tuple]],
        unique: bool = False,
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        if isinstance(keys, str):
            key_pairs = [(keys, 1)]
        else:
            key_pairs = keys

        index_doc: Dict[str, Any] = {
            "key": dict(key_pairs),
        }
        if name:
            index_doc["name"] = name
        else:
            parts = [f"{k}_{v}" for k, v in key_pairs]
            index_doc["name"] = "_".join(parts)
        if unique:
            index_doc["unique"] = True
        index_doc.update(kwargs)

        resp = self._conn.send_command(
            "createIndexes",
            **{self._coll_name: 1, "indexes": [index_doc]},
        )
        return index_doc["name"]

    def drop_index(self, name_or_keys: Union[str, List[tuple]]) -> Dict[str, Any]:
        if isinstance(name_or_keys, list):
            parts = [f"{k}_{v}" for k, v in name_or_keys]
            name = "_".join(parts)
        else:
            name = name_or_keys

        resp = self._conn.send_command(
            "dropIndexes",
            **{self._coll_name: 1, "index": name},
        )
        return resp

    def list_indexes(self) -> List[Dict[str, Any]]:
        resp = self._conn.send_command(
            "listIndexes",
            **{self._coll_name: 1},
        )
        return resp.get("cursor", {}).get("firstBatch", [])

    def stats(self, scale: int = 1) -> Dict[str, Any]:
        return self._conn.send_command(
            "collStats",
            **{self._coll_name: 1, "scale": scale},
        )

    def explain(self) -> Dict[str, Any]:
        return self._conn.send_command(
            "explain",
            **{self._coll_name: 1},
        )
