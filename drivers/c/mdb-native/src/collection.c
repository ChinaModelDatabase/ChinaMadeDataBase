#include "mdb_native/collection.h"
#include "mdb_native/connection.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct mdb_collection {
    mdb_database_t *database;
    char *name;
};

static mdb_connection_t *get_conn(mdb_collection_t *coll) {
    return coll->database->client->conn;
}

static const char *get_full_namespace(mdb_collection_t *coll) {
    return coll->name;
}

static void write_cstring(uint8_t **p, const char *s) {
    size_t len = strlen(s) + 1;
    memcpy(*p, s, len);
    *p += len;
}

bool mdb_collection_insert_one(mdb_collection_t *coll, const mdb_bson_t *doc, mdb_bson_t **result) {
    if (!coll || !doc) return false;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return false;
    mdb_bson_append_utf8(cmd, "insert", (uint32_t)strlen(coll->name), coll->name);
    mdb_bson_t *docs_array = mdb_bson_new();
    uint32_t dlen = 0;
    uint8_t *dbytes = mdb_bson_to_bytes(doc, &dlen);
    mdb_bson_t *copy = dbytes ? mdb_bson_from_bytes(dbytes, dlen) : NULL;
    free(dbytes);
    if (copy) {
        mdb_bson_append_document(docs_array, "0", copy);
        mdb_bson_destroy(copy);
    }
    mdb_bson_append_array(cmd, "documents", docs_array);
    mdb_bson_destroy(docs_array);

    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);
    if (result) *result = res;
    else mdb_bson_destroy(res);
    return res != NULL;
}

bool mdb_collection_insert_many(mdb_collection_t *coll, const mdb_bson_t **docs, size_t count, mdb_bson_t **result) {
    if (!coll || !docs || count == 0) return false;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return false;
    mdb_bson_append_utf8(cmd, "insert", (uint32_t)strlen(coll->name), coll->name);
    mdb_bson_t *docs_array = mdb_bson_new();
    for (size_t i = 0; i < count; i++) {
        char key[32];
        snprintf(key, sizeof(key), "%zu", i);
        uint32_t dlen = 0;
        uint8_t *dbytes = mdb_bson_to_bytes(docs[i], &dlen);
        mdb_bson_t *copy = dbytes ? mdb_bson_from_bytes(dbytes, dlen) : NULL;
        free(dbytes);
        if (copy) {
            mdb_bson_append_document(docs_array, key, copy);
            mdb_bson_destroy(copy);
        }
    }
    mdb_bson_append_array(cmd, "documents", docs_array);
    mdb_bson_destroy(docs_array);

    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);
    if (result) *result = res;
    else mdb_bson_destroy(res);
    return res != NULL;
}

mdb_cursor_t *mdb_collection_find(mdb_collection_t *coll, const mdb_bson_t *filter) {
    if (!coll) return NULL;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return NULL;
    mdb_bson_append_utf8(cmd, "find", 1, coll->name);
    if (filter) {
        uint32_t flen = 0;
        uint8_t *fbytes = mdb_bson_to_bytes(filter, &flen);
        mdb_bson_t *fcopy = fbytes ? mdb_bson_from_bytes(fbytes, flen) : NULL;
        free(fbytes);
        if (fcopy) {
            mdb_bson_append_document(cmd, "filter", fcopy);
            mdb_bson_destroy(fcopy);
        }
    }

    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);

    mdb_cursor_t *cursor = calloc(1, sizeof(mdb_cursor_t));
    if (!cursor) { mdb_bson_destroy(res); return NULL; }

    cursor->coll = coll;
    cursor->cursor_id = 0;
    cursor->batch = NULL;
    cursor->batch_count = 0;
    cursor->batch_capacity = 0;
    cursor->current_index = 0;
    cursor->first_batch = NULL;
    cursor->first_batch_count = 0;

    if (res) {
        uint32_t rlen = 0;
        uint8_t *rbytes = mdb_bson_to_bytes(res, &rlen);
        mdb_bson_t *rcopy = rbytes ? mdb_bson_from_bytes(rbytes, rlen) : NULL;
        free(rbytes);
        cursor->first_batch = rcopy;
        mdb_bson_destroy(res);
    }

    return cursor;
}

mdb_bson_t *mdb_collection_find_one(mdb_collection_t *coll, const mdb_bson_t *filter) {
    if (!coll) return NULL;
    mdb_cursor_t *cur = mdb_collection_find(coll, filter);
    if (!cur) return NULL;
    mdb_cursor_limit(cur, 1);
    mdb_bson_t *result = NULL;
    if (mdb_cursor_next(cur)) {
        mdb_bson_t *current = mdb_cursor_current(cur);
        if (current) {
            uint32_t clen = 0;
            uint8_t *cbytes = mdb_bson_to_bytes(current, &clen);
            result = cbytes ? mdb_bson_from_bytes(cbytes, clen) : NULL;
            free(cbytes);
        }
    }
    mdb_cursor_destroy(cur);
    return result;
}

bool mdb_collection_update_one(mdb_collection_t *coll, const mdb_bson_t *filter,
                               const mdb_bson_t *update, mdb_bson_t **result) {
    if (!coll || !filter || !update) return false;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return false;
    mdb_bson_append_utf8(cmd, "update", 1, coll->name);

    mdb_bson_t *updates = mdb_bson_new();
    mdb_bson_t *op = mdb_bson_new();

    uint32_t flen = 0;
    uint8_t *fbytes = mdb_bson_to_bytes(filter, &flen);
    mdb_bson_t *fcopy = fbytes ? mdb_bson_from_bytes(fbytes, flen) : NULL;
    free(fbytes);
    if (fcopy) {
        mdb_bson_append_document(op, "q", fcopy);
        mdb_bson_destroy(fcopy);
    }

    uint32_t ulen = 0;
    uint8_t *ubytes = mdb_bson_to_bytes(update, &ulen);
    mdb_bson_t *ucopy = ubytes ? mdb_bson_from_bytes(ubytes, ulen) : NULL;
    free(ubytes);
    if (ucopy) {
        mdb_bson_append_document(op, "u", ucopy);
        mdb_bson_destroy(ucopy);
    }

    mdb_bson_append_bool(op, "upsert", false);
    mdb_bson_append_document(updates, "0", op);
    mdb_bson_destroy(op);
    mdb_bson_append_array(cmd, "updates", updates);
    mdb_bson_destroy(updates);

    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);
    if (result) *result = res;
    else mdb_bson_destroy(res);
    return res != NULL;
}

bool mdb_collection_update_many(mdb_collection_t *coll, const mdb_bson_t *filter,
                                const mdb_bson_t *update, mdb_bson_t **result) {
    if (!coll || !filter || !update) return false;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return false;
    mdb_bson_append_utf8(cmd, "update", 1, coll->name);

    mdb_bson_t *updates = mdb_bson_new();
    mdb_bson_t *op = mdb_bson_new();

    uint32_t flen = 0;
    uint8_t *fbytes = mdb_bson_to_bytes(filter, &flen);
    mdb_bson_t *fcopy = fbytes ? mdb_bson_from_bytes(fbytes, flen) : NULL;
    free(fbytes);
    if (fcopy) {
        mdb_bson_append_document(op, "q", fcopy);
        mdb_bson_destroy(fcopy);
    }

    uint32_t ulen = 0;
    uint8_t *ubytes = mdb_bson_to_bytes(update, &ulen);
    mdb_bson_t *ucopy = ubytes ? mdb_bson_from_bytes(ubytes, ulen) : NULL;
    free(ubytes);
    if (ucopy) {
        mdb_bson_append_document(op, "u", ucopy);
        mdb_bson_destroy(ucopy);
    }

    mdb_bson_append_bool(op, "upsert", false);
    mdb_bson_append_document(updates, "0", op);
    mdb_bson_destroy(op);
    mdb_bson_append_array(cmd, "updates", updates);
    mdb_bson_destroy(updates);

    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);
    if (result) *result = res;
    else mdb_bson_destroy(res);
    return res != NULL;
}

bool mdb_collection_delete_one(mdb_collection_t *coll, const mdb_bson_t *filter, mdb_bson_t **result) {
    if (!coll || !filter) return false;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return false;
    mdb_bson_append_utf8(cmd, "delete", 1, coll->name);

    mdb_bson_t *deletes = mdb_bson_new();
    mdb_bson_t *op = mdb_bson_new();

    uint32_t flen = 0;
    uint8_t *fbytes = mdb_bson_to_bytes(filter, &flen);
    mdb_bson_t *fcopy = fbytes ? mdb_bson_from_bytes(fbytes, flen) : NULL;
    free(fbytes);
    if (fcopy) {
        mdb_bson_append_document(op, "q", fcopy);
        mdb_bson_destroy(fcopy);
    }
    mdb_bson_append_int32(op, "limit", 1);
    mdb_bson_append_document(deletes, "0", op);
    mdb_bson_destroy(op);
    mdb_bson_append_array(cmd, "deletes", deletes);
    mdb_bson_destroy(deletes);

    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);
    if (result) *result = res;
    else mdb_bson_destroy(res);
    return res != NULL;
}

bool mdb_collection_delete_many(mdb_collection_t *coll, const mdb_bson_t *filter, mdb_bson_t **result) {
    if (!coll || !filter) return false;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return false;
    mdb_bson_append_utf8(cmd, "delete", 1, coll->name);

    mdb_bson_t *deletes = mdb_bson_new();
    mdb_bson_t *op = mdb_bson_new();

    uint32_t flen = 0;
    uint8_t *fbytes = mdb_bson_to_bytes(filter, &flen);
    mdb_bson_t *fcopy = fbytes ? mdb_bson_from_bytes(fbytes, flen) : NULL;
    free(fbytes);
    if (fcopy) {
        mdb_bson_append_document(op, "q", fcopy);
        mdb_bson_destroy(fcopy);
    }
    mdb_bson_append_int32(op, "limit", 0);
    mdb_bson_append_document(deletes, "0", op);
    mdb_bson_destroy(op);
    mdb_bson_append_array(cmd, "deletes", deletes);
    mdb_bson_destroy(deletes);

    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);
    if (result) *result = res;
    else mdb_bson_destroy(res);
    return res != NULL;
}

mdb_cursor_t *mdb_collection_aggregate(mdb_collection_t *coll, const mdb_bson_t **stages, size_t stage_count) {
    if (!coll || !stages || stage_count == 0) return NULL;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return NULL;
    mdb_bson_append_utf8(cmd, "aggregate", 1, coll->name);

    mdb_bson_t *pipeline = mdb_bson_new();
    for (size_t i = 0; i < stage_count; i++) {
        char key[32];
        snprintf(key, sizeof(key), "%zu", i);
        uint32_t slen = 0;
        uint8_t *sbytes = mdb_bson_to_bytes(stages[i], &slen);
        mdb_bson_t *scopy = sbytes ? mdb_bson_from_bytes(sbytes, slen) : NULL;
        free(sbytes);
        if (scopy) {
            mdb_bson_append_document(pipeline, key, scopy);
            mdb_bson_destroy(scopy);
        }
    }
    mdb_bson_append_array(cmd, "pipeline", pipeline);
    mdb_bson_destroy(pipeline);

    mdb_bson_t *cursor_doc = mdb_bson_new();
    mdb_bson_append_int32(cursor_doc, "batchSize", 101);
    mdb_bson_append_document(cmd, "cursor", cursor_doc);
    mdb_bson_destroy(cursor_doc);

    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);

    mdb_cursor_t *cursor = calloc(1, sizeof(mdb_cursor_t));
    if (!cursor) { mdb_bson_destroy(res); return NULL; }
    cursor->coll = coll;
    cursor->first_batch = res;
    return cursor;
}

mdb_cursor_t *mdb_collection_distinct(mdb_collection_t *coll, const char *field, const mdb_bson_t *filter) {
    if (!coll || !field) return NULL;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return NULL;
    mdb_bson_append_utf8(cmd, "distinct", 1, coll->name);
    mdb_bson_append_utf8(cmd, "key", (uint32_t)strlen(field), field);

    if (filter) {
        uint32_t flen = 0;
        uint8_t *fbytes = mdb_bson_to_bytes(filter, &flen);
        mdb_bson_t *fcopy = fbytes ? mdb_bson_from_bytes(fbytes, flen) : NULL;
        free(fbytes);
        if (fcopy) {
            mdb_bson_append_document(cmd, "query", fcopy);
            mdb_bson_destroy(fcopy);
        }
    }

    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);

    mdb_cursor_t *cursor = calloc(1, sizeof(mdb_cursor_t));
    if (!cursor) { mdb_bson_destroy(res); return NULL; }
    cursor->coll = coll;
    cursor->first_batch = res;
    return cursor;
}

int64_t mdb_collection_count_documents(mdb_collection_t *coll, const mdb_bson_t *filter) {
    if (!coll) return -1;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return -1;
    mdb_bson_append_utf8(cmd, "count", 1, coll->name);
    if (filter) {
        uint32_t flen = 0;
        uint8_t *fbytes = mdb_bson_to_bytes(filter, &flen);
        mdb_bson_t *fcopy = fbytes ? mdb_bson_from_bytes(fbytes, flen) : NULL;
        free(fbytes);
        if (fcopy) {
            mdb_bson_append_document(cmd, "query", fcopy);
            mdb_bson_destroy(fcopy);
        }
    }

    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);
    mdb_bson_destroy(res);
    return 0;
}

bool mdb_collection_create_index(mdb_collection_t *coll, const mdb_bson_t *keys, bool unique, mdb_bson_t **result) {
    if (!coll || !keys) return false;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return false;
    mdb_bson_append_utf8(cmd, "createIndexes", 1, coll->name);

    mdb_bson_t *index = mdb_bson_new();
    uint32_t klen = 0;
    uint8_t *kbytes = mdb_bson_to_bytes(keys, &klen);
    mdb_bson_t *kcopy = kbytes ? mdb_bson_from_bytes(kbytes, klen) : NULL;
    free(kbytes);
    if (kcopy) {
        mdb_bson_append_document(index, "key", kcopy);
        mdb_bson_destroy(kcopy);
    }
    mdb_bson_append_bool(index, "unique", unique);

    mdb_bson_t *indexes = mdb_bson_new();
    mdb_bson_append_document(indexes, "0", index);
    mdb_bson_destroy(index);
    mdb_bson_append_array(cmd, "indexes", indexes);
    mdb_bson_destroy(indexes);

    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);
    if (result) *result = res;
    else mdb_bson_destroy(res);
    return res != NULL;
}

bool mdb_collection_drop_index(mdb_collection_t *coll, const char *index_name, mdb_bson_t **result) {
    if (!coll || !index_name) return false;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return false;
    mdb_bson_append_utf8(cmd, "dropIndexes", 1, coll->name);
    mdb_bson_append_utf8(cmd, "index", (uint32_t)strlen(index_name), index_name);
    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);
    if (result) *result = res;
    else mdb_bson_destroy(res);
    return res != NULL;
}

mdb_cursor_t *mdb_collection_list_indexes(mdb_collection_t *coll) {
    if (!coll) return NULL;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return NULL;
    mdb_bson_append_utf8(cmd, "listIndexes", 1, coll->name);
    mdb_bson_t *res = mdb_connection_send_command(get_conn(coll), cmd);
    mdb_bson_destroy(cmd);

    mdb_cursor_t *cursor = calloc(1, sizeof(mdb_cursor_t));
    if (!cursor) { mdb_bson_destroy(res); return NULL; }
    cursor->coll = coll;
    cursor->first_batch = res;
    return cursor;
}
