from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from bson import ObjectId
from pymongo import ASCENDING as _ASC, DESCENDING as _DESC
from pymongo.collation import Collation

from .change_stream import MdbChangeStream
from .cursor import MdbCursor
from .gridfs import MdbGridFSBucket
from .index import MdbIndex


class MdbCollection:
    def __init__(self, pymongo_collection: Any) -> None:
        self._collection = pymongo_collection

    @property
    def name(self) -> str:
        return self._collection.name

    @property
    def full_name(self) -> str:
        return self._collection.full_name

    @property
    def database(self) -> Any:
        return self._collection.database

    def find(
        self,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
        **kwargs: Any,
    ) -> MdbCursor:
        pymongo_cursor = self._collection.find(filter or {}, projection=projection, **kwargs)
        return MdbCursor(pymongo_cursor)

    def find_one(
        self,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
        **kwargs: Any,
    ) -> Optional[Dict[str, Any]]:
        return self._collection.find_one(filter or {}, projection=projection, **kwargs)

    def insert_one(
        self,
        document: Dict[str, Any],
        **kwargs: Any,
    ) -> Any:
        return self._collection.insert_one(document, **kwargs)

    def insert_many(
        self,
        documents: List[Dict[str, Any]],
        ordered: bool = True,
        **kwargs: Any,
    ) -> Any:
        return self._collection.insert_many(documents, ordered=ordered, **kwargs)

    def update_one(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
        **kwargs: Any,
    ) -> Any:
        return self._collection.update_one(filter, update, upsert=upsert, **kwargs)

    def update_many(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
        **kwargs: Any,
    ) -> Any:
        return self._collection.update_many(filter, update, upsert=upsert, **kwargs)

    def delete_one(self, filter: Dict[str, Any], **kwargs: Any) -> Any:
        return self._collection.delete_one(filter, **kwargs)

    def delete_many(self, filter: Dict[str, Any], **kwargs: Any) -> Any:
        return self._collection.delete_many(filter, **kwargs)

    def replace_one(
        self,
        filter: Dict[str, Any],
        replacement: Dict[str, Any],
        upsert: bool = False,
        **kwargs: Any,
    ) -> Any:
        return self._collection.replace_one(filter, replacement, upsert=upsert, **kwargs)

    def find_one_and_update(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        projection: Optional[Dict[str, int]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        upsert: bool = False,
        return_document: str = "before",
        **kwargs: Any,
    ) -> Optional[Dict[str, Any]]:
        from pymongo import ReturnDocument

        if return_document == "after":
            return_document_value = ReturnDocument.AFTER
        else:
            return_document_value = ReturnDocument.BEFORE
        return self._collection.find_one_and_update(
            filter,
            update,
            projection=projection,
            sort=sort,
            upsert=upsert,
            return_document=return_document_value,
            **kwargs,
        )

    def find_one_and_replace(
        self,
        filter: Dict[str, Any],
        replacement: Dict[str, Any],
        projection: Optional[Dict[str, int]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        upsert: bool = False,
        return_document: str = "before",
        **kwargs: Any,
    ) -> Optional[Dict[str, Any]]:
        from pymongo import ReturnDocument

        if return_document == "after":
            return_document_value = ReturnDocument.AFTER
        else:
            return_document_value = ReturnDocument.BEFORE
        return self._collection.find_one_and_replace(
            filter,
            replacement,
            projection=projection,
            sort=sort,
            upsert=upsert,
            return_document=return_document_value,
            **kwargs,
        )

    def find_one_and_delete(
        self,
        filter: Dict[str, Any],
        projection: Optional[Dict[str, int]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        **kwargs: Any,
    ) -> Optional[Dict[str, Any]]:
        return self._collection.find_one_and_delete(
            filter, projection=projection, sort=sort, **kwargs
        )

    def aggregate(
        self,
        pipeline: List[Dict[str, Any]],
        **kwargs: Any,
    ) -> MdbCursor:
        pymongo_cursor = self._collection.aggregate(pipeline, **kwargs)
        return MdbCursor(pymongo_cursor)

    def distinct(
        self,
        key: str,
        filter: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[Any]:
        return self._collection.distinct(key, filter or {}, **kwargs)

    def count_documents(
        self,
        filter: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> int:
        return self._collection.count_documents(filter or {}, **kwargs)

    def create_index(
        self,
        keys: Union[str, List[Tuple[str, int]]],
        **kwargs: Any,
    ) -> str:
        pymongo_index = MdbIndex(keys, **kwargs)
        key_pairs = pymongo_index.keys
        doc = pymongo_index.document()
        doc.pop("key", None)
        unique = doc.pop("unique", False)
        sparse = doc.pop("sparse", False)
        background = doc.pop("background", False)
        expire_after_seconds = doc.pop("expireAfterSeconds", None)
        partial_filter_expression = doc.pop("partialFilterExpression", None)
        collation = doc.pop("collation", None)
        index_name = doc.pop("name", None)

        return self._collection.create_index(
            key_pairs,
            unique=unique,
            sparse=sparse,
            background=background,
            expireAfterSeconds=expire_after_seconds,
            partialFilterExpression=partial_filter_expression,
            collation=collation,
            name=index_name,
            **doc,
        )

    def drop_index(self, name_or_keys: Union[str, List[Tuple[str, int]]]) -> None:
        if isinstance(name_or_keys, list):
            name = self._gen_index_name(name_or_keys)
        else:
            name = name_or_keys
        self._collection.drop_index(name)

    def list_indexes(self) -> List[Dict[str, Any]]:
        return list(self._collection.list_indexes())

    def index_information(self) -> Dict[str, Any]:
        return self._collection.index_information()

    def drop_indexes(self) -> None:
        self._collection.drop_indexes()

    def stats(self) -> Dict[str, Any]:
        return self._collection.database.command("collStats", self.name)

    def validate(self) -> Dict[str, Any]:
        return self._collection.database.command("validate", self.name)

    def compact(self) -> Dict[str, Any]:
        return self._collection.database.command("compact", self.name)

    def re_index(self) -> Dict[str, Any]:
        return self._collection.database.command("reIndex", self.name)

    def watch(
        self,
        pipeline: Optional[List[Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> MdbChangeStream:
        pymongo_stream = self._collection.watch(pipeline or [], **kwargs)
        return MdbChangeStream(pymongo_stream)

    def gridfs_bucket(self, **kwargs: Any) -> MdbGridFSBucket:
        return MdbGridFSBucket(self._collection.database, **kwargs)

    def _gen_index_name(self, keys: List[Tuple[str, Union[int, str]]]) -> str:
        parts = []
        for key, direction in keys:
            if isinstance(direction, str):
                parts.append(f"{key}_{direction}")
            else:
                parts.append(f"{key}_{'1' if direction > 0 else '-1'}")
        return "_".join(parts)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._collection, name)

    def __repr__(self) -> str:
        return f"MdbCollection({self.full_name!r})"
