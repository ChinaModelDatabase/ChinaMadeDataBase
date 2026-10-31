import hashlib
import os
from typing import Any, BinaryIO, Dict, List, Optional, Union

from mdb_native.bson import ObjectId
from mdb_native.collection import MdbCollection
from mdb_native.connection import MdbConnection

DEFAULT_CHUNK_SIZE = 255 * 1024

class GridFS:
    def __init__(
        self,
        conn: MdbConnection,
        db_name: str,
        bucket_name: str = "fs",
    ) -> None:
        self._conn = conn
        self._db_name = db_name
        self._bucket_name = bucket_name
        self._files = MdbCollection(conn, db_name, f"{bucket_name}.files")
        self._chunks = MdbCollection(conn, db_name, f"{bucket_name}.chunks")

    def upload(
        self,
        filename: str,
        source: Union[bytes, BinaryIO],
        content_type: Optional[str] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ObjectId:
        file_id = ObjectId()
        md5 = hashlib.md5()
        length = 0
        n = 0

        if isinstance(source, bytes):
            data = source
            length = len(data)
            md5.update(data)

            for i in range(0, length, chunk_size):
                chunk_data = data[i:i + chunk_size]
                self._chunks.insert_one({
                    "files_id": file_id,
                    "n": n,
                    "data": chunk_data,
                })
                n += 1
        else:
            chunks: List[bytes] = []
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)
                md5.update(chunk)
                length += len(chunk)

            for i, chunk_data in enumerate(chunks):
                self._chunks.insert_one({
                    "files_id": file_id,
                    "n": i,
                    "data": chunk_data,
                })

        file_doc: Dict[str, Any] = {
            "_id": file_id,
            "filename": filename,
            "length": length,
            "chunkSize": chunk_size,
            "uploadDate": ObjectId(),
            "md5": md5.hexdigest(),
        }
        if content_type:
            file_doc["contentType"] = content_type
        if metadata:
            file_doc["metadata"] = metadata

        self._files.insert_one(file_doc)
        return file_id

    def download(
        self,
        file_id: Optional[ObjectId] = None,
        filename: Optional[str] = None,
    ) -> bytes:
        if file_id:
            file_doc = self._files.find_one({"_id": file_id})
        elif filename:
            file_doc = self._files.find_one({"filename": filename})
        else:
            raise ValueError("Provide file_id or filename")

        if file_doc is None:
            raise FileNotFoundError("File not found")

        chunks = self._chunks.find({"files_id": file_doc["_id"]})
        chunks_list = sorted(chunks, key=lambda c: c["n"])
        result = b"".join(c["data"] for c in chunks_list)
        return result

    def download_to_stream(
        self,
        destination: BinaryIO,
        file_id: Optional[ObjectId] = None,
        filename: Optional[str] = None,
    ) -> None:
        data = self.download(file_id=file_id, filename=filename)
        destination.write(data)

    def list_files(
        self,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        return self._files.find(filter or {}).to_list()

    def delete(self, file_id: ObjectId) -> None:
        self._files.delete_one({"_id": file_id})
        self._chunks.delete_many({"files_id": file_id})

    def find(self, filename: str) -> Optional[Dict[str, Any]]:
        return self._files.find_one({"filename": filename})
