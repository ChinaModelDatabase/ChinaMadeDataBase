# MDB C/C++ Driver

A C/C++ client library for MDB (MongoDB-compatible) built on top of [libmongoc](https://mongoc.org/) (the official MongoDB C driver).

Since MDB is wire-protocol compatible with MongoDB 7.0, this driver wraps libmongoc with MDB-specific extensions.

## Requirements

- CMake >= 3.16
- C11 / C++17 compiler
- libmongoc >= 1.20 (with pkg-config)
- MDB or MongoDB 7.0+ server

## Building

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

## Running Tests

```bash
cd build
ctest --output-on-failure
```

Ensure a MDB/MongoDB server is running on `localhost:27017` before running tests.

## Project Structure

```
mdb-c/
  CMakeLists.txt
  include/mdb/
    mdb.h          # C API
    mdb.hpp        # C++ API
  src/
    mdb.c          # C implementation
    mdb.cpp        # C++ implementation
  tests/
    test_mdb.c     # C tests
  examples/
    basic_usage.c  # Example program
```

## C API

### Client

```c
#include <mdb/mdb.h>

mdb_client_t* client = mdb_client_new("localhost", 27017);
mdb_database_t* db = mdb_client_get_database(client, "mydb");
mdb_collection_t* coll = mdb_database_get_collection(db, "mycoll");
```

### CRUD Operations

```c
/* Insert */
bson_t* doc = BCON_NEW("name", BCON_UTF8("Alice"), "age", BCON_INT32(30));
bson_t* reply = NULL;
mdb_collection_insert_one(coll, doc, &reply, NULL);
bson_destroy(doc);
bson_destroy(reply);

/* Find */
bson_t* filter = BCON_NEW("name", BCON_UTF8("Alice"));
bson_t* result = NULL;
mdb_collection_find_one(coll, filter, NULL, &result, NULL);
bson_destroy(filter);
bson_destroy(result);

/* Update */
bson_t* update = BCON_NEW("$set", "{", "age", BCON_INT32(31), "}");
mdb_collection_update_one(coll, filter, update, &reply, NULL);
bson_destroy(update);

/* Delete */
mdb_collection_delete_one(coll, filter, &reply, NULL);
bson_destroy(reply);
```

### Cursor Iteration

```c
mdb_cursor_t* cursor = mdb_collection_find(coll, NULL, NULL);
const bson_t* doc;
while (mdb_cursor_next(cursor, &doc)) {
    /* process doc */
}
mdb_cursor_destroy(cursor);
```

### Aggregation

```c
bson_t pipeline;
bson_init(&pipeline);
bson_t stage;
bson_init(&stage);
bson_t group;
bson_init(&group);
BSON_APPEND_UTF8(&group, "_id", "$status");
BSON_APPEND_DOCUMENT(&stage, "$group", &group);
bson_append_document(&pipeline, &stage);

mdb_cursor_t* cursor = mdb_collection_aggregate(coll, &pipeline, NULL);
/* iterate cursor */
bson_destroy(&pipeline);
```

## C++ API

```cpp
#include <mdb/mdb.hpp>
#include <iostream>

int main() {
    mdb::Client client("localhost", 27017);
    auto db = client.database("mydb");
    auto coll = db.collection("mycoll");

    /* Insert */
    bson_t doc;
    bson_init(&doc);
    BSON_APPEND_UTF8(&doc, "name", "Alice");
    BSON_APPEND_INT32(&doc, "age", 30);
    coll.insert_one(doc);
    bson_destroy(&doc);

    /* Find */
    auto result = coll.find_one();
    if (result) {
        char* str = bson_as_canonical_extended_json(&*result, NULL);
        std::cout << str << std::endl;
        bson_free(str);
    }

    /* Aggregation */
    bson_t pipeline;
    bson_init(&pipeline);
    bson_t stage;
    bson_init(&stage);
    bson_t group;
    bson_init(&group);
    BSON_APPEND_UTF8(&group, "_id", "$type");
    BSON_APPEND_DOCUMENT(&stage, "$group", &group);
    bson_append_document(&pipeline, &stage);

    auto cursor = coll.aggregate(pipeline);
    while (cursor.next()) {
        bson_t current = cursor.current();
        char* str = bson_as_canonical_extended_json(&current, NULL);
        std::cout << str << std::endl;
        bson_free(str);
        bson_destroy(&current);
    }
    bson_destroy(&pipeline);
}
```

## Error Handling

### C API

All functions that can fail accept an optional `bson_error_t*` parameter. When a function returns `false`, check the error for details:

```c
bson_error_t error;
if (!mdb_collection_insert_one(coll, doc, NULL, &error)) {
    fprintf(stderr, "Error %d: %s\n", error.code, error.message);
}
```

### C++ API

C++ methods throw `mdb::Error` on failure:

```cpp
try {
    coll.insert_one(doc);
} catch (const mdb::Error& e) {
    std::cerr << "Error " << e.code() << ": " << e.what() << std::endl;
}
```

## License

MIT
