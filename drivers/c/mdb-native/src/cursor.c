#include "mdb_native/cursor.h"
#include "mdb_native/connection.h"
#include "mdb_native/wire.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct mdb_cursor {
    mdb_connection_t *conn;
    char *full_name;
    int64_t cursor_id;
    mdb_bson_t **batch;
    size_t batch_count;
    size_t batch_capacity;
    size_t current_index;
    mdb_bson_t *current;
    int64_t limit_count;
    int64_t skip_count;
    int64_t batch_size;
    bool exhausted;
};

static void cursor_clear_batch(mdb_cursor_t *cursor) {
    for (size_t i = 0; i < cursor->batch_count; i++) {
        mdb_bson_destroy(cursor->batch[i]);
        cursor->batch[i] = NULL;
    }
    cursor->batch_count = 0;
    cursor->current_index = 0;
}

static bool cursor_grow(mdb_cursor_t *cursor, size_t needed) {
    if (needed <= cursor->batch_capacity) return true;
    size_t new_cap = cursor->batch_capacity;
    while (new_cap < needed) new_cap *= 2;
    mdb_bson_t **new_batch = realloc(cursor->batch, new_cap * sizeof(mdb_bson_t *));
    if (!new_batch) return false;
    cursor->batch = new_batch;
    cursor->batch_capacity = new_cap;
    return true;
}

static bool cursor_do_get_more(mdb_cursor_t *cursor) {
    if (!cursor->conn || !cursor->full_name) return false;
    if (cursor->cursor_id == 0) return false;

    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return false;

    mdb_bson_append_utf8(cmd, "getMore", cursor->full_name, (uint32_t)strlen(cursor->full_name));
    mdb_bson_append_int64(cmd, "getMore", cursor->cursor_id);

    int64_t bs = cursor->batch_size > 0 ? cursor->batch_size : 101;
    mdb_bson_append_int32(cmd, "batchSize", (int32_t)bs);

    mdb_bson_t *result = mdb_connection_send_command(cursor->conn, cmd);
    mdb_bson_destroy(cmd);
    if (!result) return false;

    mdb_bson_t *cursor_doc = NULL;
    if (mdb_bson_find_document(result, "cursor", &cursor_doc) && cursor_doc) {
        int64_t new_id = 0;
        mdb_bson_find_int64(cursor_doc, "id", &new_id);
        cursor->cursor_id = new_id;

        mdb_bson_t *first_batch = NULL;
        if (mdb_bson_find_array(cursor_doc, "firstBatch", &first_batch) && first_batch) {
            uint32_t blen = 0;
            uint8_t *bbytes = mdb_bson_to_bytes(first_batch, &blen);
            mdb_bson_t *bcopy = bbytes ? mdb_bson_from_bytes(bbytes, blen) : NULL;
            free(bbytes);
            if (bcopy) {
                if (cursor_grow(cursor, cursor->batch_count + 1)) {
                    cursor->batch[cursor->batch_count++] = bcopy;
                }
            }
        }
        mdb_bson_destroy(cursor_doc);
    }

    mdb_bson_destroy(result);
    return cursor->batch_count > 0;
}

mdb_cursor_t *mdb_cursor_new(mdb_connection_t *conn, const char *full_name) {
    mdb_cursor_t *cursor = calloc(1, sizeof(mdb_cursor_t));
    if (!cursor) return NULL;
    cursor->conn = conn;
    cursor->full_name = full_name ? strdup(full_name) : NULL;
    cursor->cursor_id = 0;
    cursor->batch_capacity = 32;
    cursor->batch = calloc(cursor->batch_capacity, sizeof(mdb_bson_t *));
    cursor->batch_count = 0;
    cursor->current_index = 0;
    cursor->current = NULL;
    cursor->limit_count = 0;
    cursor->skip_count = 0;
    cursor->batch_size = 0;
    cursor->exhausted = false;
    return cursor;
}

bool mdb_cursor_load_batch(mdb_cursor_t *cursor, const uint8_t *data, uint32_t len) {
    if (!cursor || !data || len < 5) return false;

    mdb_bson_t *resp = mdb_bson_from_bytes(data, len);
    if (!resp) return false;

    mdb_bson_t *cursor_doc = NULL;
    if (mdb_bson_find_document(resp, "cursor", &cursor_doc) && cursor_doc) {
        int64_t id = 0;
        mdb_bson_find_int64(cursor_doc, "id", &id);
        cursor->cursor_id = id;

        mdb_bson_t *first_batch = NULL;
        if (mdb_bson_find_array(cursor_doc, "firstBatch", &first_batch) && first_batch) {
            uint32_t blen = 0;
            uint8_t *bbytes = mdb_bson_to_bytes(first_batch, &blen);
            mdb_bson_t *bcopy = bbytes ? mdb_bson_from_bytes(bbytes, blen) : NULL;
            free(bbytes);
            if (bcopy) {
                if (cursor_grow(cursor, cursor->batch_count + 1)) {
                    cursor->batch[cursor->batch_count++] = bcopy;
                }
            }
        }
        mdb_bson_destroy(cursor_doc);
    }

    mdb_bson_destroy(resp);
    return true;
}

bool mdb_cursor_next(mdb_cursor_t *cursor) {
    if (!cursor) return false;

    mdb_bson_destroy(cursor->current);
    cursor->current = NULL;

    if (cursor->limit_count > 0) {
        int64_t total_returned = (int64_t)cursor->current_index;
        if (total_returned >= cursor->limit_count) {
            return false;
        }
    }

    while (cursor->current_index >= cursor->batch_count) {
        if (cursor->cursor_id == 0) {
            cursor->exhausted = true;
            return false;
        }
        cursor_clear_batch(cursor);
        if (!cursor_do_get_more(cursor)) {
            cursor->exhausted = true;
            return false;
        }
    }

    cursor->current = mdb_bson_copy(cursor->batch[cursor->current_index]);
    cursor->current_index++;
    return cursor->current != NULL;
}

const mdb_bson_t *mdb_cursor_current(const mdb_cursor_t *cursor) {
    if (!cursor) return NULL;
    return cursor->current;
}

mdb_cursor_t *mdb_cursor_sort(mdb_cursor_t *cursor, const mdb_bson_t *sort) {
    if (cursor && sort) {
        mdb_bson_destroy(cursor->current);
        cursor->current = NULL;
        cursor_clear_batch(cursor);
    }
    return cursor;
}

mdb_cursor_t *mdb_cursor_limit(mdb_cursor_t *cursor, int64_t limit) {
    if (cursor) cursor->limit_count = limit;
    return cursor;
}

mdb_cursor_t *mdb_cursor_skip(mdb_cursor_t *cursor, int64_t skip) {
    if (cursor) cursor->skip_count = skip;
    return cursor;
}

void mdb_cursor_destroy(mdb_cursor_t *cursor) {
    if (!cursor) return;
    cursor_clear_batch(cursor);
    free(cursor->batch);
    mdb_bson_destroy(cursor->current);
    free(cursor->full_name);
    free(cursor);
}
