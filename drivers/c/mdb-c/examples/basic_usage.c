#include "mdb/mdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void print_bson(const bson_t* bson) {
    char* str = bson_as_canonical_extended_json(bson, NULL);
    printf("%s\n", str);
    bson_free(str);
}

int main(int argc, char* argv[]) {
    const char* host = "localhost";
    uint16_t port = 27017;

    if (argc > 1) host = argv[1];
    if (argc > 2) port = (uint16_t)atoi(argv[2]);

    printf("Connecting to %s:%u...\n", host, port);

    mdb_client_t* client = mdb_client_new(host, port);
    if (!client) {
        fprintf(stderr, "Failed to create client\n");
        return 1;
    }

    mdb_database_t* db = mdb_client_get_database(client, "example_db");
    mdb_collection_t* coll = mdb_database_get_collection(db, "users");

    /* Create a document */
    bson_t* doc = BCON_NEW(
        "name", BCON_UTF8("John Doe"),
        "email", BCON_UTF8("john@example.com"),
        "age", BCON_INT32(30),
        "scores", "[", BCON_INT32(85), BCON_INT32(92), BCON_INT32(78), "]",
        "address", "{",
            "street", BCON_UTF8("123 Main St"),
            "city", BCON_UTF8("Springfield"),
            "zip", BCON_UTF8("12345"),
        "}"
    );

    bson_error_t error;
    bson_t* reply = NULL;

    printf("\nInserting document...\n");
    if (!mdb_collection_insert_one(coll, doc, &reply, &error)) {
        fprintf(stderr, "Insert failed: %s\n", error.message);
        bson_destroy(doc);
        mdb_collection_destroy(coll);
        mdb_database_destroy(db);
        mdb_client_destroy(client);
        return 1;
    }

    printf("Inserted. Response:\n");
    print_bson(reply);
    bson_destroy(reply);
    bson_destroy(doc);

    /* Find the document */
    printf("\nFinding document...\n");
    bson_t* filter = BCON_NEW("name", BCON_UTF8("John Doe"));
    bson_t* result = NULL;

    if (mdb_collection_find_one(coll, filter, NULL, &result, &error)) {
        printf("Found:\n");
        print_bson(result);
        bson_destroy(result);
    } else {
        fprintf(stderr, "Find failed: %s\n", error.message);
    }
    bson_destroy(filter);

    /* Update the document */
    printf("\nUpdating document...\n");
    filter = BCON_NEW("name", BCON_UTF8("John Doe"));
    bson_t* update = BCON_NEW("$set", "{", "age", BCON_INT32(31), "}");

    if (mdb_collection_update_one(coll, filter, update, &reply, &error)) {
        printf("Updated. Response:\n");
        print_bson(reply);
        bson_destroy(reply);
    } else {
        fprintf(stderr, "Update failed: %s\n", error.message);
    }
    bson_destroy(filter);
    bson_destroy(update);

    /* Insert many */
    printf("\nInserting multiple documents...\n");
    bson_t* docs[3];
    docs[0] = BCON_NEW("name", BCON_UTF8("Alice"), "age", BCON_INT32(25));
    docs[1] = BCON_NEW("name", BCON_UTF8("Bob"), "age", BCON_INT32(28));
    docs[2] = BCON_NEW("name", BCON_UTF8("Charlie"), "age", BCON_INT32(35));

    if (mdb_collection_insert_many(coll, (const bson_t**)docs, 3, &reply, &error)) {
        printf("Inserted %d documents.\n", 3);
        bson_destroy(reply);
    } else {
        fprintf(stderr, "Insert many failed: %s\n", error.message);
    }

    for (int i = 0; i < 3; i++) bson_destroy(docs[i]);

    /* Count documents */
    printf("\nCounting documents...\n");
    int64_t count = mdb_collection_count_documents(coll, NULL, NULL, &error);
    printf("Total documents: %lld\n", (long long)count);

    /* List all with cursor */
    printf("\nListing all documents:\n");
    mdb_cursor_t* cursor = mdb_collection_find(coll, NULL, NULL);
    const bson_t* cur_doc;
    int idx = 0;
    while (mdb_cursor_next(cursor, &cur_doc)) {
        printf("  [%d] ", idx++);
        print_bson(cur_doc);
    }
    mdb_cursor_destroy(cursor);

    /* Cleanup */
    printf("\nCleaning up...\n");
    mdb_database_drop_collection(db, "users", &error);
    mdb_collection_destroy(coll);
    mdb_database_destroy(db);
    mdb_client_destroy(client);

    printf("Done.\n");
    return 0;
}
