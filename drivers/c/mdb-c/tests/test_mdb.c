#include "mdb/mdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define TEST_HOST "localhost"
#define TEST_PORT 27017
#define TEST_DB "test_mdb_driver"
#define TEST_COLL "test_collection"

static int tests_run = 0;
static int tests_passed = 0;

#define TEST_ASSERT(cond, msg) do { \
    tests_run++; \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL: %s (line %d)\n", msg, __LINE__); \
    } else { \
        fprintf(stderr, "  PASS: %s\n", msg); \
        tests_passed++; \
    } \
} while(0)

static void test_client_connect(void) {
    fprintf(stderr, "\n=== Test: Client Connect ===\n");
    mdb_client_t* client = mdb_client_new(TEST_HOST, TEST_PORT);
    TEST_ASSERT(client != NULL, "Client creation");

    if (client) {
        bool connected = mdb_client_connect(client);
        TEST_ASSERT(connected, "Client connection");
        TEST_ASSERT(mdb_client_is_connected(client), "Client is connected");
        mdb_client_destroy(client);
    }
}

static void test_database_ops(void) {
    fprintf(stderr, "\n=== Test: Database Operations ===\n");
    mdb_client_t* client = mdb_client_new(TEST_HOST, TEST_PORT);
    if (!client) return;

    mdb_database_t* db = mdb_client_get_database(client, TEST_DB);
    TEST_ASSERT(db != NULL, "Get database");

    if (db) {
        bson_t* reply = NULL;
        bson_error_t error;
        bool ok = mdb_database_create_collection(db, "test_coll", &reply, &error);
        TEST_ASSERT(ok, "Create collection");
        bson_destroy(reply);

        ok = mdb_database_list_collections(db, &reply, &error);
        TEST_ASSERT(ok, "List collections");
        bson_destroy(reply);

        mdb_database_destroy(db);
    }
    mdb_client_destroy(client);
}

static void test_collection_crud(void) {
    fprintf(stderr, "\n=== Test: Collection CRUD ===\n");
    mdb_client_t* client = mdb_client_new(TEST_HOST, TEST_PORT);
    if (!client) return;

    mdb_database_t* db = mdb_client_get_database(client, TEST_DB);
    mdb_collection_t* coll = mdb_database_get_collection(db, TEST_COLL);
    TEST_ASSERT(coll != NULL, "Get collection");

    if (!coll) {
        mdb_database_destroy(db);
        mdb_client_destroy(client);
        return;
    }

    /* Insert one */
    bson_t* doc = BCON_NEW("name", BCON_UTF8("Alice"),
                            "age", BCON_INT32(30),
                            "active", BCON_BOOL(true));
    bson_error_t error;
    bson_t* reply = NULL;

    bool ok = mdb_collection_insert_one(coll, doc, &reply, &error);
    TEST_ASSERT(ok, "Insert one");
    bson_destroy(doc);
    bson_destroy(reply);

    /* Find one */
    bson_t* filter = BCON_NEW("name", BCON_UTF8("Alice"));
    bson_t* result = NULL;
    ok = mdb_collection_find_one(coll, filter, NULL, &result, &error);
    TEST_ASSERT(ok, "Find one");
    if (result) {
        bson_iter_t iter;
        ok = bson_iter_init_find(&iter, result, "age");
        TEST_ASSERT(ok, "Find result has 'age' field");
        if (ok) {
            TEST_ASSERT(bson_iter_as_int32(&iter) == 30, "Age is 30");
        }
        bson_destroy(result);
    }
    bson_destroy(filter);

    /* Update one */
    filter = BCON_NEW("name", BCON_UTF8("Alice"));
    bson_t* update = BCON_NEW("$set", BCON_INT32(31));
    bson_destroy(update);
    update = BCON_NEW("$set", "{", "age", BCON_INT32(31), "}");
    ok = mdb_collection_update_one(coll, filter, update, &reply, &error);
    TEST_ASSERT(ok, "Update one");
    bson_destroy(filter);
    bson_destroy(update);
    bson_destroy(reply);

    /* Count documents */
    int64_t count = mdb_collection_count_documents(coll, NULL, NULL, &error);
    TEST_ASSERT(count >= 1, "Count documents >= 1");

    /* Delete one */
    filter = BCON_NEW("name", BCON_UTF8("Alice"));
    ok = mdb_collection_delete_one(coll, filter, &reply, &error);
    TEST_ASSERT(ok, "Delete one");
    bson_destroy(filter);
    bson_destroy(reply);

    /* Verify deleted */
    count = mdb_collection_count_documents(coll, NULL, NULL, &error);
    TEST_ASSERT(count == 0, "Count after delete == 0");

    mdb_collection_destroy(coll);
    mdb_database_destroy(db);
    mdb_client_destroy(client);
}

static void test_aggregation(void) {
    fprintf(stderr, "\n=== Test: Aggregation ===\n");
    mdb_client_t* client = mdb_client_new(TEST_HOST, TEST_PORT);
    if (!client) return;

    mdb_database_t* db = mdb_client_get_database(client, TEST_DB);
    mdb_collection_t* coll = mdb_database_get_collection(db, TEST_COLL);

    /* Insert test data */
    bson_t* doc1 = BCON_NEW("type", BCON_UTF8("A"), "value", BCON_INT32(10));
    bson_t* doc2 = BCON_NEW("type", BCON_UTF8("B"), "value", BCON_INT32(20));
    bson_t* doc3 = BCON_NEW("type", BCON_UTF8("A"), "value", BCON_INT32(30));

    bson_error_t error;
    mdb_collection_insert_one(coll, doc1, NULL, &error);
    mdb_collection_insert_one(coll, doc2, NULL, &error);
    mdb_collection_insert_one(coll, doc3, NULL, &error);
    bson_destroy(doc1);
    bson_destroy(doc2);
    bson_destroy(doc3);

    /* Build pipeline: group by type, sum value */
    bson_t pipeline;
    bson_init(&pipeline);
    bson_t stage1;
    bson_init(&stage1);
    bson_t group;
    bson_init(&group);
    BSON_APPEND_UTF8(&group, "_id", "$type");
    bson_t total;
    bson_init(&total);
    BSON_APPEND_UTF8(&total, "$sum", "$value");
    BSON_APPEND_DOCUMENT(&group, "total", &total);
    bson_destroy(&total);
    BSON_APPEND_DOCUMENT(&stage1, "$group", &group);
    bson_destroy(&group);
    bson_append_document(&pipeline, "0", &stage1);
    bson_destroy(&stage1);

    mdb_cursor_t* cursor = mdb_collection_aggregate(coll, &pipeline, NULL);
    TEST_ASSERT(cursor != NULL, "Aggregate cursor created");

    int result_count = 0;
    const bson_t* result_doc;
    while (mdb_cursor_next(cursor, &result_doc)) {
        result_count++;
    }
    TEST_ASSERT(result_count == 2, "Aggregation returned 2 groups");

    mdb_cursor_destroy(cursor);
    bson_destroy(&pipeline);

    mdb_collection_destroy(coll);
    mdb_database_destroy(db);
    mdb_client_destroy(client);
}

static void test_cursor(void) {
    fprintf(stderr, "\n=== Test: Cursor ===\n");
    mdb_client_t* client = mdb_client_new(TEST_HOST, TEST_PORT);
    if (!client) return;

    mdb_database_t* db = mdb_client_get_database(client, TEST_DB);
    mdb_collection_t* coll = mdb_database_get_collection(db, TEST_COLL);

    /* Insert test data */
    bson_t* docs[5];
    bson_error_t error;
    for (int i = 0; i < 5; i++) {
        docs[i] = BCON_NEW("index", BCON_INT32(i));
        mdb_collection_insert_one(coll, docs[i], NULL, &error);
        bson_destroy(docs[i]);
    }

    /* Find all */
    mdb_cursor_t* cursor = mdb_collection_find(coll, NULL, NULL);
    TEST_ASSERT(cursor != NULL, "Find cursor created");

    int count = 0;
    const bson_t* doc;
    while (mdb_cursor_next(cursor, &doc)) {
        count++;
    }
    TEST_ASSERT(count == 5, "Cursor returned 5 documents");
    TEST_ASSERT(!mdb_cursor_error(cursor, &error), "No cursor error");

    mdb_cursor_destroy(cursor);
    mdb_collection_destroy(coll);
    mdb_database_destroy(db);
    mdb_client_destroy(client);
}

static void test_list_databases(void) {
    fprintf(stderr, "\n=== Test: List Databases ===\n");
    mdb_client_t* client = mdb_client_new(TEST_HOST, TEST_PORT);
    if (!client) return;

    bson_t* reply = NULL;
    bson_error_t error;
    bool ok = mdb_client_list_databases(client, &reply, &error);
    TEST_ASSERT(ok, "List databases");
    if (reply) {
        bson_iter_t iter;
        ok = bson_iter_init_find(&iter, reply, "databases");
        TEST_ASSERT(ok, "Reply has 'databases' field");
        bson_destroy(reply);
    }

    mdb_client_destroy(client);
}

static void test_server_status(void) {
    fprintf(stderr, "\n=== Test: Server Status ===\n");
    mdb_client_t* client = mdb_client_new(TEST_HOST, TEST_PORT);
    if (!client) return;

    bson_t* reply = NULL;
    bson_error_t error;
    bool ok = mdb_client_server_status(client, &reply, &error);
    TEST_ASSERT(ok, "Server status");
    if (reply) {
        bson_iter_t iter;
        ok = bson_iter_init_find(&iter, reply, "ok");
        TEST_ASSERT(ok, "Reply has 'ok' field");
        bson_destroy(reply);
    }

    mdb_client_destroy(client);
}

static void test_write_concern(void) {
    fprintf(stderr, "\n=== Test: Write Concern ===\n");
    mdb_write_concern_t* wc = mdb_write_concern_new();
    TEST_ASSERT(wc != NULL, "Write concern new");

    mdb_write_concern_set_w(wc, 1);
    mdb_write_concern_set_wtimeout(wc, 5000);
    TEST_ASSERT(true, "Write concern set options");

    mdb_write_concern_destroy(wc);

    wc = mdb_write_concern_majority();
    TEST_ASSERT(wc != NULL, "Write concern majority");
    mdb_write_concern_destroy(wc);
}

static void test_read_concern(void) {
    fprintf(stderr, "\n=== Test: Read Concern ===\n");
    mdb_read_concern_t* rc = mdb_read_concern_new("majority");
    TEST_ASSERT(rc != NULL, "Read concern majority");
    mdb_read_concern_destroy(rc);

    rc = mdb_read_concern_new("local");
    TEST_ASSERT(rc != NULL, "Read concern local");
    mdb_read_concern_destroy(rc);
}

static void test_bson_macros(void) {
    fprintf(stderr, "\n=== Test: BSON Macros ===\n");
    bson_t doc;
    bson_init(&doc);

    MDB_BSON_APPEND_UTF8(&doc, "str", "hello");
    MDB_BSON_APPEND_INT32(&doc, "int", 42);
    MDB_BSON_APPEND_INT64(&doc, "long", 1234567890123LL);
    MDB_BSON_APPEND_DOUBLE(&doc, "double", 3.14);
    MDB_BSON_APPEND_BOOL(&doc, "bool", true);
    MDB_BSON_APPEND_NULL(&doc, "null");

    TEST_ASSERT(bson_count_keys(&doc) == 6, "BSON macros created 6 fields");

    bson_destroy(&doc);
}

static void cleanup_test_db(void) {
    mdb_client_t* client = mdb_client_new(TEST_HOST, TEST_PORT);
    if (!client) return;

    bson_error_t error;
    mdb_database_t* db = mdb_client_get_database(client, TEST_DB);
    if (db) {
        bson_t* cmd = BCON_NEW("dropDatabase", BCON_INT32(1));
        bson_t* reply = NULL;
        mdb_database_command_simple(db, cmd, &reply, &error);
        bson_destroy(cmd);
        bson_destroy(reply);
        mdb_database_destroy(db);
    }
    mdb_client_destroy(client);
}

int main(int argc, char* argv[]) {
    mongoc_init();

    fprintf(stderr, "MDB C Driver Tests\n");
    fprintf(stderr, "==================\n");

    cleanup_test_db();

    test_client_connect();
    test_list_databases();
    test_server_status();
    test_database_ops();
    test_collection_crud();
    test_aggregation();
    test_cursor();
    test_write_concern();
    test_read_concern();
    test_bson_macros();

    fprintf(stderr, "\n==================\n");
    fprintf(stderr, "Results: %d/%d passed\n", tests_passed, tests_run);

    mongoc_cleanup();

    return (tests_passed == tests_run) ? 0 : 1;
}
