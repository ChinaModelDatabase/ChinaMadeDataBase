#include "mdb_native/database.h"
#include "mdb_native/collection.h"
#include "mdb_native/connection.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct mdb_database {
    mdb_client_t *client;
    char *name;
};

struct mdb_collection {
    mdb_database_t *database;
    char *name;
};

mdb_collection_t *mdb_database_collection(mdb_database_t *db, const char *name) {
    if (!db || !name) return NULL;
    mdb_collection_t *coll = calloc(1, sizeof(mdb_collection_t));
    if (!coll) return NULL;
    coll->database = db;
    coll->name = strdup(name);
    return coll;
}

mdb_bson_t *mdb_database_list_collections(mdb_database_t *db) {
    if (!db || !db->client) return NULL;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return NULL;
    mdb_bson_append_utf8(cmd, "listCollections", 1, 0);
    mdb_bson_append_utf8(cmd, "namePattern", 1, 0);

    mdb_bson_t *filter = mdb_bson_new();
    mdb_bson_append_utf8(filter, "name", 1, 0);
    mdb_bson_append_document(cmd, "filter", filter);
    mdb_bson_destroy(filter);

    mdb_bson_t *result = mdb_connection_send_command(db->client->conn, cmd);
    mdb_bson_destroy(cmd);
    return result;
}

mdb_bson_t *mdb_database_create_collection(mdb_database_t *db, const char *name) {
    if (!db || !db->client || !name) return NULL;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return NULL;
    mdb_bson_append_utf8(cmd, "create", (uint32_t)strlen(name), name);
    mdb_bson_t *result = mdb_connection_send_command(db->client->conn, cmd);
    mdb_bson_destroy(cmd);
    return result;
}

mdb_bson_t *mdb_database_drop_collection(mdb_database_t *db, const char *name) {
    if (!db || !db->client || !name) return NULL;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return NULL;
    mdb_bson_append_utf8(cmd, "drop", (uint32_t)strlen(name), name);
    mdb_bson_t *result = mdb_connection_send_command(db->client->conn, cmd);
    mdb_bson_destroy(cmd);
    return result;
}

mdb_bson_t *mdb_database_command(mdb_database_t *db, const mdb_bson_t *cmd) {
    if (!db || !db->client || !cmd) return NULL;
    return mdb_connection_send_command(db->client->conn, cmd);
}
