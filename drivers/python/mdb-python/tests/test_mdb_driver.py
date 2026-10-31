from __future__ import annotations

import os
from typing import Any, Dict, Generator
from unittest.mock import MagicMock, patch

import pytest

from mdb import (
    ASCENDING,
    DESCENDING,
    GEOSPHERE,
    HASHED,
    TEXT,
    MdbChangeStream,
    MdbChangeStreamEvent,
    MdbClient,
    MdbCollection,
    MdbDatabase,
    MdbCursor,
    MdbGridFSBucket,
    MdbIndex,
    MdbProfiler,
    MdbTransaction,
    ReadConcern,
    ReadConcernLevel,
    WriteConcern,
    WValue,
)

MDB_HOST = os.environ.get("MDB_HOST", "localhost")
MDB_PORT = int(os.environ.get("MDB_PORT", "27017"))
MDB_URI = os.environ.get("MDB_URI", f"mongodb://{MDB_HOST}:{MDB_PORT}")
MDB_DATABASE = os.environ.get("MDB_DATABASE", "test_mdb_driver")


@pytest.fixture(scope="module")
def client() -> Generator[MdbClient, None, None]:
    c = MdbClient(MDB_HOST, MDB_PORT)
    yield c
    c.close()


@pytest.fixture(scope="module")
def database(client: MdbClient) -> MdbDatabase:
    return client.database(MDB_DATABASE)


@pytest.fixture(autouse=True)
def cleanup(database: MdbDatabase) -> Generator[None, None, None]:
    yield
    for coll_info in database.list_collections():
        coll_name = coll_info["name"]
        if not coll_name.startswith("system."):
            database.drop_collection(coll_name)


class TestMdbClient:
    def test_connect(self, client: MdbClient) -> None:
        assert client.is_connected()

    def test_close(self) -> None:
        c = MdbClient(MDB_HOST, MDB_PORT)
        c.close()
        assert not c.is_connected()

    def test_context_manager(self) -> None:
        with MdbClient(MDB_HOST, MDB_PORT) as c:
            assert c.is_connected()

    def test_database_property(self, client: MdbClient) -> None:
        db = client.database("test")
        assert db.name == "test"

    def test_getitem(self, client: MdbClient) -> None:
        db = client["test"]
        assert isinstance(db, MdbDatabase)

    def test_list_databases(self, client: MdbClient) -> None:
        dbs = client.list_databases()
        assert isinstance(dbs, list)

    def test_server_status(self, client: MdbClient) -> None:
        status = client.server_status()
        assert "host" in status
        assert "version" in status

    def test_start_transaction(self, client: MdbClient) -> None:
        txn = client.start_transaction()
        assert isinstance(txn, MdbTransaction)

    def test_from_uri(self) -> None:
        c = MdbClient.from_uri(f"mongodb://{MDB_HOST}:{MDB_PORT}")
        assert c.is_connected()
        c.close()


class TestMdbDatabase:
    def test_collection(self, database: MdbDatabase) -> None:
        coll = database.collection("test_coll")
        assert isinstance(coll, MdbCollection)
        assert coll.name == "test_coll"

    def test_getitem(self, database: MdbDatabase) -> None:
        coll = database["test_coll"]
        assert isinstance(coll, MdbCollection)

    def test_create_and_drop_collection(self, database: MdbDatabase) -> None:
        database.create_collection("temp_coll")
        colls = [c["name"] for c in database.list_collections()]
        assert "temp_coll" in colls
        database.drop_collection("temp_coll")
        colls = [c["name"] for c in database.list_collections()]
        assert "temp_coll" not in colls

    def test_stats(self, database: MdbDatabase) -> None:
        stats = database.stats()
        assert "db" in stats or "dataSize" in stats

    def test_profiler(self, database: MdbDatabase) -> None:
        profiler = database.profiler()
        assert isinstance(profiler, MdbProfiler)

    def test_user_management(self, database: MdbDatabase) -> None:
        try:
            database.create_user("test_user", "test_password")
            users = database.get_users()
            usernames = [u["user"] for u in users]
            assert "test_user" in usernames
            database.drop_user("test_user")
            users = database.get_users()
            usernames = [u["user"] for u in users]
            assert "test_user" not in usernames
        except Exception as e:
            if "not authorized" in str(e).lower() or "auth" in str(e).lower():
                pytest.skip("Not authorized for user management")
            raise


class TestMdbCollection:
    def test_insert_one(self, database: MdbDatabase) -> None:
        coll = database.collection("test_insert")
        result = coll.insert_one({"name": "test", "value": 42})
        assert result.inserted_id is not None

    def test_insert_many(self, database: MdbDatabase) -> None:
        coll = database.collection("test_insert_many")
        docs = [{"name": f"doc_{i}", "value": i} for i in range(5)]
        result = coll.insert_many(docs)
        assert len(result.inserted_ids) == 5

    def test_find_one(self, database: MdbDatabase) -> None:
        coll = database.collection("test_find_one")
        coll.insert_one({"name": "find_me", "value": 100})
        doc = coll.find_one({"name": "find_me"})
        assert doc is not None
        assert doc["value"] == 100

    def test_find(self, database: MdbDatabase) -> None:
        coll = database.collection("test_find")
        for i in range(10):
            coll.insert_one({"index": i, "value": f"val_{i}"})
        cursor = coll.find({"index": {"$gte": 5}})
        results = cursor.to_list()
        assert len(results) == 5

    def test_update_one(self, database: MdbDatabase) -> None:
        coll = database.collection("test_update")
        coll.insert_one({"name": "update_me", "value": 1})
        result = coll.update_one({"name": "update_me"}, {"$set": {"value": 2}})
        assert result.modified_count == 1
        doc = coll.find_one({"name": "update_me"})
        assert doc["value"] == 2

    def test_update_many(self, database: MdbDatabase) -> None:
        coll = database.collection("test_update_many")
        for i in range(5):
            coll.insert_one({"group": "a", "value": i})
        result = coll.update_many({"group": "a"}, {"$set": {"group": "b"}})
        assert result.modified_count == 5

    def test_delete_one(self, database: MdbDatabase) -> None:
        coll = database.collection("test_delete")
        coll.insert_one({"name": "delete_me"})
        result = coll.delete_one({"name": "delete_me"})
        assert result.deleted_count == 1
        assert coll.find_one({"name": "delete_me"}) is None

    def test_delete_many(self, database: MdbDatabase) -> None:
        coll = database.collection("test_delete_many")
        for i in range(3):
            coll.insert_one({"group": "delete_all"})
        result = coll.delete_many({"group": "delete_all"})
        assert result.deleted_count == 3

    def test_replace_one(self, database: MdbDatabase) -> None:
        coll = database.collection("test_replace")
        coll.insert_one({"name": "replace_me", "value": 1})
        result = coll.replace_one(
            {"name": "replace_me"}, {"name": "replaced", "value": 2}
        )
        assert result.modified_count == 1
        doc = coll.find_one({"name": "replaced"})
        assert doc["value"] == 2

    def test_find_one_and_update(self, database: MdbDatabase) -> None:
        coll = database.collection("test_foau")
        coll.insert_one({"name": "foau", "value": 1})
        old_doc = coll.find_one_and_update(
            {"name": "foau"}, {"$set": {"value": 2}}
        )
        assert old_doc["value"] == 1
        new_doc = coll.find_one({"name": "foau"})
        assert new_doc["value"] == 2

    def test_find_one_and_replace(self, database: MdbDatabase) -> None:
        coll = database.collection("test_foar")
        coll.insert_one({"name": "foar", "value": 1})
        old_doc = coll.find_one_and_replace(
            {"name": "foar"}, {"name": "foar", "value": 3}
        )
        assert old_doc["value"] == 1
        new_doc = coll.find_one({"name": "foar"})
        assert new_doc["value"] == 3

    def test_find_one_and_delete(self, database: MdbDatabase) -> None:
        coll = database.collection("test_foad")
        coll.insert_one({"name": "foad", "value": 1})
        deleted = coll.find_one_and_delete({"name": "foad"})
        assert deleted["value"] == 1
        assert coll.find_one({"name": "foad"}) is None

    def test_aggregate(self, database: MdbDatabase) -> None:
        coll = database.collection("test_aggregate")
        for i in range(5):
            coll.insert_one({"group": "x" if i < 3 else "y", "value": i})
        pipeline = [
            {"$group": {"_id": "$group", "total": {"$sum": "$value"}}},
            {"$sort": {"_id": 1}},
        ]
        results = coll.aggregate(pipeline).to_list()
        assert len(results) == 2

    def test_distinct(self, database: MdbDatabase) -> None:
        coll = database.collection("test_distinct")
        for i in range(5):
            coll.insert_one({"category": "A" if i < 3 else "B"})
        values = coll.distinct("category")
        assert set(values) == {"A", "B"}

    def test_count_documents(self, database: MdbDatabase) -> None:
        coll = database.collection("test_count")
        for i in range(10):
            coll.insert_one({"even": i % 2 == 0})
        count = coll.count_documents({"even": True})
        assert count == 5

    def test_create_and_drop_index(self, database: MdbDatabase) -> None:
        coll = database.collection("test_index")
        index_name = coll.create_index([("name", ASCENDING)])
        assert isinstance(index_name, str)
        indexes = coll.index_information()
        assert index_name in indexes
        coll.drop_index(index_name)
        indexes = coll.index_information()
        assert index_name not in indexes

    def test_unique_index(self, database: MdbDatabase) -> None:
        coll = database.collection("test_unique")
        coll.create_index([("email", ASCENDING)], unique=True)
        coll.insert_one({"email": "unique@test.com"})
        with pytest.raises(Exception):
            coll.insert_one({"email": "unique@test.com"})

    def test_list_indexes(self, database: MdbDatabase) -> None:
        coll = database.collection("test_list_idx")
        coll.create_index([("field", ASCENDING)])
        indexes = coll.list_indexes()
        assert len(list(indexes)) >= 2

    def test_stats(self, database: MdbDatabase) -> None:
        coll = database.collection("test_stats")
        coll.insert_one({"test": True})
        stats = coll.stats()
        assert "count" in stats or "ns" in stats

    def test_watch(self, database: MdbDatabase) -> None:
        coll = database.collection("test_watch")
        with coll.watch() as stream:
            coll.insert_one({"watched": True})
            event = stream.try_next()
            if event is not None:
                assert isinstance(event, MdbChangeStreamEvent)
                assert event.operation_type in (
                    "insert",
                    "update",
                    "replace",
                    "delete",
                )

    def test_gridfs_bucket(self, database: MdbDatabase) -> None:
        coll = database.collection("test_gridfs")
        bucket = coll.gridfs_bucket()
        assert isinstance(bucket, MdbGridFSBucket)

    def test_upsert(self, database: MdbDatabase) -> None:
        coll = database.collection("test_upsert")
        result = coll.update_one(
            {"name": "upsert_test"},
            {"$set": {"value": 1}},
            upsert=True,
        )
        assert result.upserted_id is not None

    def test_cursor_methods(self, database: MdbDatabase) -> None:
        coll = database.collection("test_cursor")
        for i in range(20):
            coll.insert_one({"index": i})
        results = coll.find().sort("index", DESCENDING).limit(5).skip(0).to_list()
        assert len(results) == 5
        assert results[0]["index"] == 19

    def test_projection(self, database: MdbDatabase) -> None:
        coll = database.collection("test_projection")
        coll.insert_one({"name": "test", "value": 1, "secret": "hidden"})
        doc = coll.find_one({"name": "test"}, projection={"name": 1, "value": 1})
        assert "secret" not in doc


class TestMdbCursor:
    def test_iterator(self, database: MdbDatabase) -> None:
        coll = database.collection("test_cursor_iter")
        for i in range(3):
            coll.insert_one({"index": i})
        cursor = coll.find()
        count = 0
        for doc in cursor:
            assert "index" in doc
            count += 1
        assert count == 3

    def test_explain(self, database: MdbDatabase) -> None:
        coll = database.collection("test_explain")
        coll.insert_one({"test": True})
        explanation = coll.find({"test": True}).explain()
        assert isinstance(explanation, dict)

    def test_alive(self, database: MdbDatabase) -> None:
        coll = database.collection("test_alive")
        coll.insert_one({"test": True})
        cursor = coll.find()
        assert cursor.alive
        cursor.to_list()
        assert not cursor.alive

    def test_context_manager(self, database: MdbDatabase) -> None:
        coll = database.collection("test_cursor_ctx")
        coll.insert_one({"test": True})
        with coll.find() as cursor:
            results = cursor.to_list()
            assert len(results) == 1


class TestMdbTransaction:
    def test_transaction_context(self, client: MdbClient) -> None:
        db = client.database(MDB_DATABASE)
        with client.start_transaction() as txn:
            coll = txn.collection(MDB_DATABASE, "txn_test")
            coll.insert_one({"name": "txn_doc", "value": 1})
        result = db["txn_test"].find_one({"name": "txn_doc"})
        assert result is not None

    def test_transaction_abort(self, client: MdbClient) -> None:
        db = client.database(MDB_DATABASE)
        try:
            with client.start_transaction() as txn:
                coll = txn.collection(MDB_DATABASE, "txn_abort")
                coll.insert_one({"name": "abort_doc"})
                raise Exception("force abort")
        except Exception:
            pass
        result = db["txn_abort"].find_one({"name": "abort_doc"})
        assert result is None


class TestMdbIndex:
    def test_index_document(self) -> None:
        idx = MdbIndex([("name", ASCENDING)], unique=True)
        doc = idx.document()
        assert doc["key"] == [("name", ASCENDING)]
        assert doc["unique"] is True

    def test_single_key(self) -> None:
        idx = MdbIndex("name")
        assert idx.keys == [("name", ASCENDING)]

    def test_repr(self) -> None:
        idx = MdbIndex([("name", ASCENDING)])
        assert "MdbIndex" in repr(idx)


class TestMdbChangeStreamEvent:
    def test_properties(self) -> None:
        event = MdbChangeStreamEvent(
            {
                "operationType": "insert",
                "fullDocument": {"name": "test"},
                "documentKey": {"_id": "123"},
                "ns": {"db": "test", "coll": "test"},
            }
        )
        assert event.operation_type == "insert"
        assert event.full_document == {"name": "test"}
        assert event.document_key == {"_id": "123"}
        assert event.ns == {"db": "test", "coll": "test"}

    def test_repr(self) -> None:
        event = MdbChangeStreamEvent(
            {"operationType": "insert", "ns": {"db": "test", "coll": "coll"}}
        )
        assert "MdbChangeStreamEvent" in repr(event)


class TestWriteConcern:
    def test_defaults(self) -> None:
        wc = WriteConcern()
        assert wc.w is None
        assert wc.wtimeout_ms is None

    def test_majority(self) -> None:
        wc = WriteConcern(w=WValue.MAJORITY)
        assert wc.w == "majority"

    def test_document(self) -> None:
        wc = WriteConcern(w=WValue.MAJORITY, wtimeout_ms=1000, j=True)
        doc = wc.document()
        assert doc["w"] == "majority"
        assert doc["wtimeout"] == 1000
        assert doc["j"] is True

    def test_repr(self) -> None:
        wc = WriteConcern(w=1, wtimeout_ms=500)
        assert "WriteConcern" in repr(wc)


class TestReadConcern:
    def test_defaults(self) -> None:
        rc = ReadConcern()
        assert rc.level is None

    def test_majority(self) -> None:
        rc = ReadConcern(level=ReadConcernLevel.MAJORITY)
        assert rc.level == "majority"

    def test_document(self) -> None:
        rc = ReadConcern(level=ReadConcernLevel.LINEARIZABLE)
        doc = rc.document()
        assert doc["level"] == "linearizable"

    def test_repr(self) -> None:
        rc = ReadConcern(level=ReadConcernLevel.MAJORITY)
        assert "ReadConcern" in repr(rc)


class TestMdbGridFSBucket:
    def test_upload_and_download(self, database: MdbDatabase) -> None:
        bucket = MdbGridFSBucket(database, bucket_name="test_gridfs")
        data = b"Hello, MDB GridFS!"
        file_id = bucket.upload_from_bytes("test.txt", data)
        downloaded = bucket.download_to_bytes(file_id)
        assert downloaded == data

    def test_upload_from_file(self, database: MdbDatabase) -> None:
        import io

        bucket = MdbGridFSBucket(database, bucket_name="test_gridfs_file")
        data = b"File content"
        file_obj = io.BytesIO(data)
        file_id = bucket.upload_from_file("file.txt", file_obj)
        assert file_id is not None
        downloaded = bucket.download_to_bytes(file_id)
        assert downloaded == data

    def test_find(self, database: MdbDatabase) -> None:
        bucket = MdbGridFSBucket(database, bucket_name="test_gridfs_find")
        bucket.upload_from_bytes("find_me.txt", b"content")
        files = bucket.find()
        assert len(files) >= 1

    def test_delete(self, database: MdbDatabase) -> None:
        bucket = MdbGridFSBucket(database, bucket_name="test_gridfs_del")
        file_id = bucket.upload_from_bytes("delete_me.txt", b"content")
        bucket.delete(file_id)
        files = bucket.find({"_id": file_id})
        assert len(files) == 0

    def test_rename(self, database: MdbDatabase) -> None:
        bucket = MdbGridFSBucket(database, bucket_name="test_gridfs_rename")
        file_id = bucket.upload_from_bytes("old_name.txt", b"content")
        bucket.rename(file_id, "new_name.txt")
        files = bucket.find({"_id": file_id})
        assert files[0]["filename"] == "new_name.txt"

    def test_repr(self) -> None:
        bucket = MdbGridFSBucket(MagicMock(), bucket_name="fs")
        assert "MdbGridFSBucket" in repr(bucket)


class TestMdbProfiler:
    def test_enable_disable(self, database: MdbDatabase) -> None:
        profiler = database.profiler()
        profiler.enable(slow_ms=50)
        stats = profiler.stats()
        assert stats["enabled"] is True
        profiler.disable()
        stats = profiler.stats()
        assert stats["enabled"] is False

    def test_set_level(self, database: MdbDatabase) -> None:
        profiler = database.profiler()
        profiler.set_level(2)
        stats = profiler.stats()
        assert stats["level"] == 2
        profiler.set_level(0)

    def test_invalid_level(self, database: MdbDatabase) -> None:
        profiler = database.profiler()
        with pytest.raises(ValueError):
            profiler.set_level(5)

    def test_slow_queries(self, database: MdbDatabase) -> None:
        profiler = database.profiler()
        profiler.enable(slow_ms=0)
        coll = database.collection("profiler_test")
        for i in range(5):
            coll.insert_one({"test": i})
        queries = profiler.slow_queries(limit=5)
        assert isinstance(queries, list)
        profiler.disable()


class TestMdbDatabaseUserManagement:
    def test_create_drop_user(self, database: MdbDatabase) -> None:
        try:
            database.create_user("driver_test_user", "password123")
            users = database.get_users()
            assert any(u["user"] == "driver_test_user" for u in users)
            database.drop_user("driver_test_user")
            users = database.get_users()
            assert not any(u["user"] == "driver_test_user" for u in users)
        except Exception as e:
            if "not authorized" in str(e).lower() or "auth" in str(e).lower():
                pytest.skip("Not authorized for user management")
            raise
