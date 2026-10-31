from __future__ import annotations

import os
from typing import Any, BinaryIO, Dict, List, Optional, Union

from bson import ObjectId
from gridfs import GridFSBucket as _PyMongoGridFSBucket


class MdbGridFSBucket:
    def __init__(self, database: Any, bucket_name: str = "fs", **kwargs: Any) -> None:
        self._bucket = _PyMongoGridFSBucket(database, bucket_name=bucket_name, **kwargs)
        self._database = database
        self._bucket_name = bucket_name

    def upload_from_bytes(
        self,
        filename: str,
        data: Union[bytes, bytearray],
        *,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> ObjectId:
        kwargs.setdefault("content_type", content_type)
        if metadata is not None:
            kwargs.setdefault("metadata", metadata)
        file_id = self._bucket.upload_from_stream(filename, data, **kwargs)
        return file_id

    def upload_from_file(
        self,
        filename: str,
        file_obj: BinaryIO,
        *,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> ObjectId:
        kwargs.setdefault("content_type", content_type)
        if metadata is not None:
            kwargs.setdefault("metadata", metadata)
        file_id = self._bucket.upload_from_stream(filename, file_obj, **kwargs)
        return file_id

    def download_to_bytes(self, file_id: Union[ObjectId, Any]) -> bytes:
        download_stream = self._bucket.open_download_stream(file_id)
        try:
            return download_stream.read()
        finally:
            download_stream.close()

    def download_to_file(
        self,
        file_id: Union[ObjectId, Any],
        file_obj: BinaryIO,
    ) -> None:
        download_stream = self._bucket.open_download_stream(file_id)
        try:
            file_obj.write(download_stream.read())
        finally:
            download_stream.close()

    def find(
        self,
        filter: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        collection = self._database[self._bucket_name + ".files"]
        cursor = collection.find(filter or {}, **kwargs)
        return list(cursor)

    def delete(self, file_id: Union[ObjectId, Any]) -> None:
        self._bucket.delete(file_id)

    def rename(self, file_id: Union[ObjectId, Any], new_filename: str) -> None:
        collection = self._database[self._bucket_name + ".files"]
        collection.update_one(
            {"_id": file_id},
            {"$set": {"filename": new_filename}},
        )

    def __repr__(self) -> str:
        return f"MdbGridFSBucket(bucket_name={self._bucket_name!r})"
