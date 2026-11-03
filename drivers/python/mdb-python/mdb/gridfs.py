from __future__ import annotations

import os
from typing import Any, BinaryIO, Dict, List, Optional, Union

from bson import ObjectId
from gridfs import GridFSBucket as _PyMongoGridFSBucket


def _merge_content_type(
    content_type: Optional[str], metadata: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """GridFS 没有 `content_type` 参数：内容类型按约定放在 metadata 里。

    pymongo 的 `upload_from_stream` 不接受 `content_type`，直接透传会 TypeError。
    """
    if content_type is None:
        return metadata
    merged = dict(metadata or {})
    merged.setdefault("contentType", content_type)
    return merged


class MdbGridFSBucket:
    def __init__(self, database: Any, bucket_name: str = "fs", **kwargs: Any) -> None:
        # pymongo 的 GridFSBucket 只接受 pymongo 自己的 Database（内部有
        # isinstance 校验），传包装类进去会直接 TypeError。
        # 延迟导入：database 模块也会用到这里，顶层 import 会形成循环依赖。
        from .database import MdbDatabase

        pymongo_db = database._database if isinstance(database, MdbDatabase) else database
        self._bucket = _PyMongoGridFSBucket(pymongo_db, bucket_name=bucket_name, **kwargs)
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
        file_id = self._bucket.upload_from_stream(
            filename, data, metadata=_merge_content_type(content_type, metadata), **kwargs
        )
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
        file_id = self._bucket.upload_from_stream(
            filename, file_obj, metadata=_merge_content_type(content_type, metadata), **kwargs
        )
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
