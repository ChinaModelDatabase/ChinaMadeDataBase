from __future__ import annotations

from typing import Any, Dict, Optional


class MdbTransaction:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._session: Any = None
        self._started = False

    def start(self) -> None:
        if self._session is None:
            self._session = self._client.start_session()
        self._session.start_transaction()
        self._started = True

    def commit(self) -> None:
        if self._session is not None and self._started:
            self._session.commit_transaction()
            self._started = False

    def abort(self) -> None:
        if self._session is not None and self._started:
            self._session.abort_transaction()
            self._started = False

    def database(self, name: str) -> Any:
        if self._session is None:
            raise RuntimeError("Transaction not started. Call start() first.")
        db = self._client[name]
        return _TransactionalDatabase(db, self._session)

    def collection(self, db_name: str, coll_name: str) -> Any:
        if self._session is None:
            raise RuntimeError("Transaction not started. Call start() first.")
        db = self._client[db_name]
        coll = db[coll_name]
        return _TransactionalCollection(coll, self._session)

    @property
    def session(self) -> Any:
        return self._session

    def __enter__(self) -> "MdbTransaction":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None:
            self.abort()
        else:
            self.commit()


class _TransactionalDatabase:
    def __init__(self, database: Any, session: Any) -> None:
        self._database = database
        self._session = session

    def __getattr__(self, name: str) -> Any:
        return getattr(self._database, name)

    def __getitem__(self, name: str) -> "_TransactionalCollection":
        coll = self._database[name]
        return _TransactionalCollection(coll, self._session)

    def collection(self, name: str) -> "_TransactionalCollection":
        coll = self._database[name]
        return _TransactionalCollection(coll, self._session)

    def command(self, command: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        return self._database.command(command, session=self._session, **kwargs)


class _TransactionalCollection:
    def __init__(self, collection: Any, session: Any) -> None:
        self._collection = collection
        self._session = session

    def __getattr__(self, name: str) -> Any:
        return getattr(self._collection, name)

    def find(self, filter: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Any:
        return self._collection.find(filter or {}, session=self._session, **kwargs)

    def find_one(
        self,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
    ) -> Optional[Dict[str, Any]]:
        return self._collection.find_one(
            filter or {}, projection=projection, session=self._session
        )

    def insert_one(self, document: Dict[str, Any], **kwargs: Any) -> Any:
        return self._collection.insert_one(document, session=self._session, **kwargs)

    def insert_many(self, documents, **kwargs: Any):
        return self._collection.insert_many(documents, session=self._session, **kwargs)

    def update_one(self, filter, update, upsert=False, **kwargs):
        return self._collection.update_one(
            filter, update, upsert=upsert, session=self._session, **kwargs
        )

    def update_many(self, filter, update, upsert=False, **kwargs):
        return self._collection.update_many(
            filter, update, upsert=upsert, session=self._session, **kwargs
        )

    def delete_one(self, filter, **kwargs):
        return self._collection.delete_one(filter, session=self._session, **kwargs)

    def delete_many(self, filter, **kwargs):
        return self._collection.delete_many(filter, session=self._session, **kwargs)
