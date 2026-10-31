#include "mdb/mdb.h"
#include <stdlib.h>
#include <string.h>

/* ============ Internal structures ============ */

struct mdb_client_t {
    mongoc_client_t* client;
    mongoc_uri_t* uri;
    bool connected;
};

struct mdb_database_t {
    mdb_client_t* parent;
    mongoc_database_t* db;
    char* name;
};

struct mdb_collection_t {
    mdb_database_t* parent;
    mongoc_collection_t* coll;
    char* name;
};

struct mdb_cursor_t {
    mongoc_cursor_t* cursor;
};

struct mdb_change_stream_t {
    mdb_collection_t* parent;
    mongoc_change_stream_t* stream;
};

struct mdb_gridfs_bucket_t {
    mdb_database_t* parent;
    mongoc_gridfs_t* gridfs;
};

struct mdb_write_concern_t {
    mongoc_write_concern_t* wc;
};

struct mdb_read_concern_t {
    mongoc_read_concern_t* rc;
};

struct mdb_read_prefs_t {
    mongoc_read_prefs_t* prefs;
};

/* ============ Helper: build URI string ============ */

static char* _mdb_build_uri(const char* host, uint16_t port,
                             const char* username, const char* password,
                             const char* auth_db) {
    size_t len = strlen("mongodb://") + strlen(host) + 6 + 1;
    if (username && password) {
        len += strlen(username) + 1 + strlen(password) + 1 +
               strlen(auth_db) + strlen("?authSource=");
    }
    char* uri = (char*)malloc(len);
    if (!uri) return NULL;

    if (username && password) {
        snprintf(uri, len, "mongodb://%s:%s@%s:%u/?authSource=%s",
                 username, password, host, port, auth_db);
    } else {
        snprintf(uri, len, "mongodb://%s:%u", host, port);
    }
    return uri;
}

/* ============ Client ============ */

mdb_client_t* mdb_client_new(const char* host, uint16_t port) {
    return mdb_client_new_with_auth(host, port, NULL, NULL, NULL);
}

mdb_client_t* mdb_client_new_from_uri(const char* uri) {
    mdb_client_t* c = (mdb_client_t*)calloc(1, sizeof(mdb_client_t));
    if (!c) return NULL;

    c->uri = mongoc_uri_new(uri);
    if (!c->uri) {
        free(c);
        return NULL;
    }

    c->client = mongoc_client_new_from_uri(c->uri);
    if (!c->client) {
        mongoc_uri_destroy(c->uri);
        free(c);
        return NULL;
    }

    c->connected = true;
    return c;
}

mdb_client_t* mdb_client_new_with_auth(const char* host, uint16_t port,
    const char* username, const char* password, const char* auth_db) {
    mdb_client_t* c = (mdb_client_t*)calloc(1, sizeof(mdb_client_t));
    if (!c) return NULL;

    char* uri_str = _mdb_build_uri(host, port, username, password,
                                    auth_db ? auth_db : "admin");
    if (!uri_str) {
        free(c);
        return NULL;
    }

    c->uri = mongoc_uri_new(uri_str);
    free(uri_str);

    if (!c->uri) {
        free(c);
        return NULL;
    }

    c->client = mongoc_client_new_from_uri(c->uri);
    if (!c->client) {
        mongoc_uri_destroy(c->uri);
        free(c);
        return NULL;
    }

    c->connected = true;
    return c;
}

void mdb_client_destroy(mdb_client_t* client) {
    if (!client) return;
    if (client->client) {
        mongoc_client_destroy(client->client);
    }
    if (client->uri) {
        mongoc_uri_destroy(client->uri);
    }
    free(client);
}

bool mdb_client_connect(mdb_client_t* client) {
    if (!client || !client->client) return false;
    client->connected = true;
    return true;
}

bool mdb_client_is_connected(mdb_client_t* client) {
    return client && client->connected;
}

void mdb_client_close(mdb_client_t* client) {
    if (!client) return;
    client->connected = false;
}

mdb_database_t* mdb_client_get_database(mdb_client_t* client, const char* name) {
    if (!client || !client->client || !name) return NULL;

    mdb_database_t* db = (mdb_database_t*)calloc(1, sizeof(mdb_database_t));
    if (!db) return NULL;

    db->db = mongoc_client_get_database(client->client, name);
    if (!db->db) {
        free(db);
        return NULL;
    }

    db->parent = client;
    db->name = strdup(name);
    return db;
}

bool mdb_client_list_databases(mdb_client_t* client, bson_t** reply, bson_error_t* error) {
    if (!client || !client->client) return false;

    bson_t* cmd = BCON_NEW("listDatabases", BCON_INT32(1));
    bson_t* local_reply = NULL;
    bool ret = mongoc_client_command_simple(client->client, "admin", cmd, NULL,
                                            reply ? reply : &local_reply, error);
    bson_destroy(cmd);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_client_server_status(mdb_client_t* client, bson_t** reply, bson_error_t* error) {
    if (!client || !client->client) return false;

    bson_t* cmd = BCON_NEW("serverStatus", BCON_INT32(1));
    bson_t* local_reply = NULL;
    bool ret = mongoc_client_command_simple(client->client, "admin", cmd, NULL,
                                            reply ? reply : &local_reply, error);
    bson_destroy(cmd);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

/* ============ Database ============ */

mdb_collection_t* mdb_database_get_collection(mdb_database_t* db, const char* name) {
    if (!db || !db->db || !name) return NULL;

    mdb_collection_t* coll = (mdb_collection_t*)calloc(1, sizeof(mdb_collection_t));
    if (!coll) return NULL;

    coll->coll = mongoc_database_get_collection(db->db, name);
    if (!coll->coll) {
        free(coll);
        return NULL;
    }

    coll->parent = db;
    coll->name = strdup(name);
    return coll;
}

bool mdb_database_list_collections(mdb_database_t* db, bson_t** reply, bson_error_t* error) {
    if (!db || !db->db) return false;

    bson_t* cmd = BCON_NEW("listCollections", BCON_INT32(1));
    bson_t* local_reply = NULL;
    bool ret = mongoc_database_command_simple(db->db, cmd, NULL,
                                              reply ? reply : &local_reply, error);
    bson_destroy(cmd);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_database_create_collection(mdb_database_t* db, const char* name, bson_t** reply, bson_error_t* error) {
    if (!db || !db->db || !name) return false;

    bson_t* cmd = BCON_NEW("create", BCON_UTF8(name));
    bson_t* local_reply = NULL;
    bool ret = mongoc_database_command_simple(db->db, cmd, NULL,
                                              reply ? reply : &local_reply, error);
    bson_destroy(cmd);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_database_drop_collection(mdb_database_t* db, const char* name, bson_error_t* error) {
    if (!db || !db->db || !name) return false;

    bson_t* cmd = BCON_NEW("drop", BCON_UTF8(name));
    bson_t* reply = bson_new();
    bool ret = mongoc_database_command_simple(db->db, cmd, NULL, reply, error);
    bson_destroy(cmd);
    bson_destroy(reply);
    return ret;
}

bool mdb_database_command_simple(mdb_database_t* db, const bson_t* cmd, bson_t** reply, bson_error_t* error) {
    if (!db || !db->db || !cmd) return false;

    bson_t* local_reply = NULL;
    bool ret = mongoc_database_command_simple(db->db, cmd, NULL,
                                              reply ? reply : &local_reply, error);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_database_stats(mdb_database_t* db, bson_t** reply, bson_error_t* error) {
    if (!db || !db->db) return false;

    bson_t* cmd = BCON_NEW("dbStats", BCON_INT32(1));
    bson_t* local_reply = NULL;
    bool ret = mongoc_database_command_simple(db->db, cmd, NULL,
                                              reply ? reply : &local_reply, error);
    bson_destroy(cmd);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

void mdb_database_destroy(mdb_database_t* db) {
    if (!db) return;
    if (db->db) {
        mongoc_database_destroy(db->db);
    }
    free(db->name);
    free(db);
}

/* ============ Collection ============ */

mdb_cursor_t* mdb_collection_find(mdb_collection_t* coll, bson_t* filter, bson_t* opts) {
    if (!coll || !coll->coll) return NULL;

    mdb_cursor_t* c = (mdb_cursor_t*)calloc(1, sizeof(mdb_cursor_t));
    if (!c) return NULL;

    c->cursor = mongoc_collection_find_with_opts(coll->coll, filter, opts, NULL);
    if (!c->cursor) {
        free(c);
        return NULL;
    }
    return c;
}

bool mdb_collection_find_one(mdb_collection_t* coll, bson_t* filter, bson_t* opts,
                              bson_t** result, bson_error_t* error) {
    if (!coll || !coll->coll) return false;

    bson_t local_opts;
    bson_init(&local_opts);
    if (opts) {
        bson_copy_to(opts, &local_opts);
    }
    BSON_APPEND_INT32(&local_opts, "limit", 1);

    mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(coll->coll,
                                                                 filter,
                                                                 &local_opts,
                                                                 NULL);
    bson_destroy(&local_opts);

    if (!cursor) return false;

    const bson_t* doc;
    if (mongoc_cursor_next(cursor, &doc)) {
        *result = bson_copy(doc);
    } else {
        mongoc_cursor_error(cursor, error);
        mongoc_cursor_destroy(cursor);
        return false;
    }

    mongoc_cursor_destroy(cursor);
    return true;
}

bool mdb_collection_insert_one(mdb_collection_t* coll, bson_t* doc,
                                bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll || !doc) return false;

    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_insert_one(coll->coll, doc, NULL,
                                            reply ? reply : &local_reply, error);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_collection_insert_many(mdb_collection_t* coll, const bson_t** docs,
                                 size_t count, bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll || !docs || count == 0) return false;

    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_insert_many(coll->coll, docs, count, NULL,
                                             reply ? reply : &local_reply, error);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_collection_update_one(mdb_collection_t* coll, bson_t* filter,
                                bson_t* update, bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll || !filter || !update) return false;

    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_update_one(coll->coll, filter, update, NULL,
                                            reply ? reply : &local_reply, error);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_collection_update_many(mdb_collection_t* coll, bson_t* filter,
                                 bson_t* update, bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll || !filter || !update) return false;

    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_update_many(coll->coll, filter, update, NULL,
                                             reply ? reply : &local_reply, error);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_collection_delete_one(mdb_collection_t* coll, bson_t* filter,
                                bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll || !filter) return false;

    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_delete_one(coll->coll, filter, NULL,
                                            reply ? reply : &local_reply, error);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_collection_delete_many(mdb_collection_t* coll, bson_t* filter,
                                 bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll || !filter) return false;

    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_delete_many(coll->coll, filter, NULL,
                                             reply ? reply : &local_reply, error);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_collection_replace_one(mdb_collection_t* coll, bson_t* filter,
                                 bson_t* replacement, bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll || !filter || !replacement) return false;

    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_replace_one(coll->coll, filter, replacement, NULL,
                                             reply ? reply : &local_reply, error);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

mdb_cursor_t* mdb_collection_aggregate(mdb_collection_t* coll,
                                        bson_t* pipeline, bson_t* opts) {
    if (!coll || !coll->coll || !pipeline) return NULL;

    mdb_cursor_t* c = (mdb_cursor_t*)calloc(1, sizeof(mdb_cursor_t));
    if (!c) return NULL;

    c->cursor = mongoc_collection_aggregate(coll->coll, 0, pipeline, opts, NULL);
    if (!c->cursor) {
        free(c);
        return NULL;
    }
    return c;
}

bool mdb_collection_distinct(mdb_collection_t* coll, const char* key,
                              bson_t* filter, bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll || !key) return false;

    bson_t* cmd = BCON_NEW("distinct", BCON_UTF8(coll->name),
                            "key", BCON_UTF8(key));
    if (filter) {
        bson_t filter_copy;
        bson_init(&filter_copy);
        bson_copy_to(filter, &filter_copy);
        BSON_APPEND_DOCUMENT(cmd, "query", &filter_copy);
        bson_destroy(&filter_copy);
    }

    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_command_simple(coll->coll, cmd, NULL,
                                                reply ? reply : &local_reply, error);
    bson_destroy(cmd);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

int64_t mdb_collection_count_documents(mdb_collection_t* coll, bson_t* filter,
                                        bson_t* opts, bson_error_t* error) {
    if (!coll || !coll->coll) return -1;

    return mongoc_collection_count_documents(coll->coll, filter, opts, NULL, NULL, error);
}

bool mdb_collection_create_index(mdb_collection_t* coll, bson_t* keys,
                                  bson_t* opts, bson_error_t* error) {
    if (!coll || !coll->coll || !keys) return false;

    mongoc_index_opt_t index_opt;
    mongoc_index_opt_init(&index_opt);

    if (opts) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, opts, "unique") && bson_iter_as_bool(&iter)) {
            index_opt.is_unique = true;
        }
        if (bson_iter_init_find(&iter, opts, "name")) {
            const char* name = bson_iter_utf8(&iter, NULL);
            mongoc_index_opt_set_name(&index_opt, name);
        }
    }

    return mongoc_collection_create_index(coll->coll, keys, &index_opt, error);
}

bool mdb_collection_drop_index(mdb_collection_t* coll, const char* name, bson_error_t* error) {
    if (!coll || !coll->coll || !name) return false;

    bson_t* cmd = BCON_NEW("dropIndexes", BCON_UTF8(coll->name),
                            "index", BCON_UTF8(name));
    bson_t* reply = bson_new();
    bool ret = mongoc_collection_command_simple(coll->coll, cmd, NULL, reply, error);
    bson_destroy(cmd);
    bson_destroy(reply);
    return ret;
}

bool mdb_collection_list_indexes(mdb_collection_t* coll, bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll) return false;

    bson_t* cmd = BCON_NEW("listIndexes", BCON_UTF8(coll->name));
    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_command_simple(coll->coll, cmd, NULL,
                                                reply ? reply : &local_reply, error);
    bson_destroy(cmd);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_collection_stats(mdb_collection_t* coll, bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll) return false;

    bson_t* cmd = BCON_NEW("collStats", BCON_UTF8(coll->name));
    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_command_simple(coll->coll, cmd, NULL,
                                                reply ? reply : &local_reply, error);
    bson_destroy(cmd);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_collection_validate(mdb_collection_t* coll, bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll) return false;

    bson_t* cmd = BCON_NEW("validate", BCON_UTF8(coll->name));
    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_command_simple(coll->coll, cmd, NULL,
                                                reply ? reply : &local_reply, error);
    bson_destroy(cmd);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

bool mdb_collection_command_simple(mdb_collection_t* coll, const bson_t* cmd,
                                    bson_t** reply, bson_error_t* error) {
    if (!coll || !coll->coll || !cmd) return false;

    bson_t* local_reply = NULL;
    bool ret = mongoc_collection_command_simple(coll->coll, cmd, NULL,
                                                reply ? reply : &local_reply, error);
    if (!reply && local_reply) bson_destroy(local_reply);
    return ret;
}

void mdb_collection_destroy(mdb_collection_t* coll) {
    if (!coll) return;
    if (coll->coll) {
        mongoc_collection_destroy(coll->coll);
    }
    free(coll->name);
    free(coll);
}

/* ============ Cursor ============ */

bool mdb_cursor_next(mdb_cursor_t* cursor, const bson_t** doc) {
    if (!cursor || !cursor->cursor) return false;
    return mongoc_cursor_next(cursor->cursor, doc);
}

bool mdb_cursor_error(mdb_cursor_t* cursor, bson_error_t* error) {
    if (!cursor || !cursor->cursor) return false;
    return mongoc_cursor_error(cursor->cursor, error);
}

void mdb_cursor_destroy(mdb_cursor_t* cursor) {
    if (!cursor) return;
    if (cursor->cursor) {
        mongoc_cursor_destroy(cursor->cursor);
    }
    free(cursor);
}

/* ============ Change Stream ============ */

mdb_change_stream_t* mdb_collection_watch(mdb_collection_t* coll,
                                           bson_t* pipeline, bson_t* opts) {
    if (!coll || !coll->coll) return NULL;

    mdb_change_stream_t* cs = (mdb_change_stream_t*)calloc(1, sizeof(mdb_change_stream_t));
    if (!cs) return NULL;

    bson_t local_opts;
    bson_init(&local_opts);
    if (opts) {
        bson_copy_to(opts, &local_opts);
    }

    cs->stream = mongoc_collection_watch(coll->coll, pipeline, &local_opts);
    bson_destroy(&local_opts);

    if (!cs->stream) {
        free(cs);
        return NULL;
    }

    cs->parent = coll;
    return cs;
}

bool mdb_change_stream_next(mdb_change_stream_t* cs, bson_t** event) {
    if (!cs || !cs->stream) return false;

    const bson_t* doc;
    if (mongoc_change_stream_next(cs->stream, &doc)) {
        *event = bson_copy(doc);
        return true;
    }
    return false;
}

void mdb_change_stream_destroy(mdb_change_stream_t* cs) {
    if (!cs) return;
    if (cs->stream) {
        mongoc_change_stream_destroy(cs->stream);
    }
    free(cs);
}

/* ============ GridFS ============ */

mdb_gridfs_bucket_t* mdb_gridfs_bucket_new(mdb_database_t* db,
                                            const char* bucket_name,
                                            bson_t* opts) {
    if (!db || !db->db) return NULL;

    mdb_gridfs_bucket_t* bucket = (mdb_gridfs_bucket_t*)calloc(1, sizeof(mdb_gridfs_bucket_t));
    if (!bucket) return NULL;

    bson_error_t error;
    bucket->gridfs = mongoc_database_get_gridfs(db->db,
                                                 bucket_name ? bucket_name : "fs",
                                                 opts, &error);
    if (!bucket->gridfs) {
        free(bucket);
        return NULL;
    }

    bucket->parent = db;
    return bucket;
}

bool mdb_gridfs_bucket_upload_from_bytes(mdb_gridfs_bucket_t* bucket,
                                          const char* filename,
                                          const uint8_t* data, size_t len,
                                          bson_t** file_id, bson_error_t* error) {
    if (!bucket || !bucket->gridfs || !filename || !data) return false;

    mongoc_gridfs_file_opt_t file_opt = {0};
    file_opt.filename = filename;

    mongoc_gridfs_file_t* file = mongoc_gridfs_create_file(bucket->gridfs, &file_opt);
    if (!file) {
        if (error) {
            bson_set_error(error, MDB_ERROR_FAILED, 0, "Failed to create GridFS file");
        }
        return false;
    }

    mongoc_stream_t* stream = mongoc_gridfs_file_write_stream(file);
    if (!stream) {
        mongoc_gridfs_file_destroy(file);
        if (error) {
            bson_set_error(error, MDB_ERROR_FAILED, 0, "Failed to open GridFS write stream");
        }
        return false;
    }

    ssize_t written = mongoc_stream_write(stream, data, len, 1);
    mongoc_stream_destroy(stream);

    if ((size_t)written != len) {
        mongoc_gridfs_file_destroy(file);
        if (error) {
            bson_set_error(error, MDB_ERROR_FAILED, 0,
                           "Failed to write all data: wrote %zd of %zu", written, len);
        }
        return false;
    }

    if (!mongoc_gridfs_file_save(file)) {
        mongoc_gridfs_file_destroy(file);
        if (error) {
            bson_set_error(error, MDB_ERROR_FAILED, 0, "Failed to save GridFS file");
        }
        return false;
    }

    if (file_id) {
        const bson_t* id = mongoc_gridfs_file_get_id(file);
        *file_id = bson_copy(id);
    }

    mongoc_gridfs_file_destroy(file);
    return true;
}

bool mdb_gridfs_bucket_download_to_bytes(mdb_gridfs_bucket_t* bucket,
                                          bson_t* file_id,
                                          uint8_t** data, size_t* len,
                                          bson_error_t* error) {
    if (!bucket || !bucket->gridfs || !file_id || !data || !len) return false;

    bson_t* filter = bson_new();
    bson_copy_to(file_id, filter);

    mongoc_gridfs_file_t* file = mongoc_gridfs_find_one(bucket->gridfs, filter, error);
    bson_destroy(filter);

    if (!file) return false;

    mongoc_stream_t* stream = mongoc_gridfs_file_read_stream(file);
    if (!stream) {
        mongoc_gridfs_file_destroy(file);
        if (error) {
            bson_set_error(error, MDB_ERROR_FAILED, 0, "Failed to open GridFS read stream");
        }
        return false;
    }

    int32_t file_len = mongoc_gridfs_file_get_length(file);
    *data = (uint8_t*)malloc(file_len);
    if (!*data) {
        mongoc_stream_destroy(stream);
        mongoc_gridfs_file_destroy(file);
        if (error) {
            bson_set_error(error, MDB_ERROR_FAILED, 0, "Failed to allocate buffer");
        }
        return false;
    }

    ssize_t bytes_read = mongoc_stream_read(stream, *data, file_len, 1);
    mongoc_stream_destroy(stream);
    mongoc_gridfs_file_destroy(file);

    if (bytes_read < 0) {
        free(*data);
        *data = NULL;
        if (error) {
            bson_set_error(error, MDB_ERROR_FAILED, 0, "Failed to read GridFS data");
        }
        return false;
    }

    *len = (size_t)bytes_read;
    return true;
}

void mdb_gridfs_bucket_destroy(mdb_gridfs_bucket_t* bucket) {
    if (!bucket) return;
    if (bucket->gridfs) {
        mongoc_gridfs_destroy(bucket->gridfs, true);
    }
    free(bucket);
}

/* ============ Write Concern ============ */

mdb_write_concern_t* mdb_write_concern_new(void) {
    mdb_write_concern_t* wc = (mdb_write_concern_t*)calloc(1, sizeof(mdb_write_concern_t));
    if (!wc) return NULL;
    wc->wc = mongoc_write_concern_new();
    return wc;
}

mdb_write_concern_t* mdb_write_concern_majority(void) {
    mdb_write_concern_t* wc = mdb_write_concern_new();
    if (!wc) return NULL;
    mongoc_write_concern_set_wmajority(wc->wc, 0);
    return wc;
}

void mdb_write_concern_set_w(mdb_write_concern_t* wc, int32_t w) {
    if (!wc || !wc->wc) return;
    mongoc_write_concern_set_w(wc->wc, w);
}

void mdb_write_concern_set_wtimeout(mdb_write_concern_t* wc, int32_t ms) {
    if (!wc || !wc->wc) return;
    mongoc_write_concern_set_wtimeout(wc->wc, ms);
}

void mdb_write_concern_destroy(mdb_write_concern_t* wc) {
    if (!wc) return;
    if (wc->wc) {
        mongoc_write_concern_destroy(wc->wc);
    }
    free(wc);
}

/* ============ Read Concern ============ */

mdb_read_concern_t* mdb_read_concern_new(const char* level) {
    mdb_read_concern_t* rc = (mdb_read_concern_t*)calloc(1, sizeof(mdb_read_concern_t));
    if (!rc) return NULL;
    rc->rc = mongoc_read_concern_new();
    if (level) {
        mongoc_read_concern_set_level(rc->rc, level);
    }
    return rc;
}

void mdb_read_concern_destroy(mdb_read_concern_t* rc) {
    if (!rc) return;
    if (rc->rc) {
        mongoc_read_concern_destroy(rc->rc);
    }
    free(rc);
}
