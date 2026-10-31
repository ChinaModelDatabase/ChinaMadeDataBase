from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

ASCENDING = 1
DESCENDING = -1
GEOSPHERE = "2dsphere"
TEXT = "text"
HASHED = "hashed"

_KEY_TYPE = Union[str, Sequence[Tuple[str, Union[int, str]]]]


class MdbIndex:
    def __init__(
        self,
        keys: _KEY_TYPE,
        *,
        name: Optional[str] = None,
        unique: bool = False,
        sparse: bool = False,
        background: bool = False,
        expire_after_seconds: Optional[int] = None,
        partial_filter_expression: Optional[Mapping[str, Any]] = None,
        collation: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        if isinstance(keys, str):
            self._keys: List[Tuple[str, int]] = [(keys, ASCENDING)]
        elif isinstance(keys, list):
            self._keys = list(keys)
        else:
            raise TypeError(f"keys must be str or list of pairs, got {type(keys)}")

        self._name = name
        self._unique = unique
        self._sparse = sparse
        self._background = background
        self._expire_after_seconds = expire_after_seconds
        self._partial_filter_expression = partial_filter_expression
        self._collation = collation
        self._extra: Dict[str, Any] = dict(kwargs)

    @property
    def keys(self) -> List[Tuple[str, int]]:
        return list(self._keys)

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def unique(self) -> bool:
        return self._unique

    @property
    def sparse(self) -> bool:
        return self._sparse

    def document(self) -> Dict[str, Any]:
        doc: Dict[str, Any] = {"key": self._keys}
        if self._name is not None:
            doc["name"] = self._name
        if self._unique:
            doc["unique"] = True
        if self._sparse:
            doc["sparse"] = True
        if self._background:
            doc["background"] = True
        if self._expire_after_seconds is not None:
            doc["expireAfterSeconds"] = self._expire_after_seconds
        if self._partial_filter_expression is not None:
            doc["partialFilterExpression"] = self._partial_filter_expression
        if self._collation is not None:
            doc["collation"] = self._collation
        doc.update(self._extra)
        return doc

    def __repr__(self) -> str:
        return f"MdbIndex({self._keys!r}, name={self._name!r})"


def create_index(
    collection: Any,
    keys: _KEY_TYPE,
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

    return collection.create_index(
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
