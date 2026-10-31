#ifndef MDB_H
#define MDB_H

#include <bson/bson.h>
#include <mongoc/mongoc.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Error domain for MDB-specific errors */
#define MDB_ERROR_FAILED 1

/* Opaque types */
typedef struct mdb_client_t mdb_client_t;
typedef struct mdb_database_t mdb_database_t;
typedef struct mdb_collection_t mdb_collection_t;
typedef struct mdb_cursor_t mdb_cursor_t;
typedef struct mdb_change_stream_t mdb_change_stream_t;
typedef struct mdb_gridfs_bucket_t mdb_gridfs_bucket_t;
typedef struct mdb_write_concern_t mdb_write_concern_t;
typedef struct mdb_read_concern_t mdb_read_concern_t;
typedef struct mdb_read_prefs_t mdb_read_prefs_t;

/* ============ Client ============ */

mdb_client_t* mdb_client_new(const char* host, uint16_t port);
mdb_client_t* mdb_client_new_from_uri(const char* uri);
mdb_client_t* mdb_client_new_with_auth(const char* host, uint16_t port,
    const char* username, const char* password, const char* auth_db);
void mdb_client_destroy(mdb_client_t* client);
bool mdb_client_connect(mdb_client_t* client);
bool mdb_client_is_connected(mdb_client_t* client);
void mdb_client_close(mdb_client_t* client);

mdb_database_t* mdb_client_get_database(mdb_client_t* client, const char* name);
bool mdb_client_list_databases(mdb_client_t* client, bson_t** reply, bson_error_t* error);
bool mdb_client_server_status(mdb_client_t* client, bson_t** reply, bson_error_t* error);

/* ============ Database ============ */

mdb_collection_t* mdb_database_get_collection(mdb_database_t* db, const char* name);
bool mdb_database_list_collections(mdb_database_t* db, bson_t** reply, bson_error_t* error);
bool mdb_database_create_collection(mdb_database_t* db, const char* name, bson_t** reply, bson_error_t* error);
bool mdb_database_drop_collection(mdb_database_t* db, const char* name, bson_error_t* error);
bool mdb_database_command_simple(mdb_database_t* db, const bson_t* cmd, bson_t** reply, bson_error_t* error);
bool mdb_database_stats(mdb_database_t* db, bson_t** reply, bson_error_t* error);
void mdb_database_destroy(mdb_database_t* db);

/* ============ Collection ============ */

mdb_cursor_t* mdb_collection_find(mdb_collection_t* coll, bson_t* filter, bson_t* opts);
bool mdb_collection_find_one(mdb_collection_t* coll, bson_t* filter, bson_t* opts, bson_t** result, bson_error_t* error);
bool mdb_collection_insert_one(mdb_collection_t* coll, bson_t* doc, bson_t** reply, bson_error_t* error);
bool mdb_collection_insert_many(mdb_collection_t* coll, const bson_t** docs, size_t count, bson_t** reply, bson_error_t* error);
bool mdb_collection_update_one(mdb_collection_t* coll, bson_t* filter, bson_t* update, bson_t** reply, bson_error_t* error);
bool mdb_collection_update_many(mdb_collection_t* coll, bson_t* filter, bson_t* update, bson_t** reply, bson_error_t* error);
bool mdb_collection_delete_one(mdb_collection_t* coll, bson_t* filter, bson_t** reply, bson_error_t* error);
bool mdb_collection_delete_many(mdb_collection_t* coll, bson_t* filter, bson_t** reply, bson_error_t* error);
bool mdb_collection_replace_one(mdb_collection_t* coll, bson_t* filter, bson_t* replacement, bson_t** reply, bson_error_t* error);
mdb_cursor_t* mdb_collection_aggregate(mdb_collection_t* coll, bson_t* pipeline, bson_t* opts);
bool mdb_collection_distinct(mdb_collection_t* coll, const char* key, bson_t* filter, bson_t** reply, bson_error_t* error);
int64_t mdb_collection_count_documents(mdb_collection_t* coll, bson_t* filter, bson_t* opts, bson_error_t* error);
bool mdb_collection_create_index(mdb_collection_t* coll, bson_t* keys, bson_t* opts, bson_error_t* error);
bool mdb_collection_drop_index(mdb_collection_t* coll, const char* name, bson_error_t* error);
bool mdb_collection_list_indexes(mdb_collection_t* coll, bson_t** reply, bson_error_t* error);
bool mdb_collection_stats(mdb_collection_t* coll, bson_t** reply, bson_error_t* error);
bool mdb_collection_validate(mdb_collection_t* coll, bson_t** reply, bson_error_t* error);
bool mdb_collection_command_simple(mdb_collection_t* coll, const bson_t* cmd, bson_t** reply, bson_error_t* error);
void mdb_collection_destroy(mdb_collection_t* coll);

/* ============ Cursor ============ */

bool mdb_cursor_next(mdb_cursor_t* cursor, const bson_t** doc);
bool mdb_cursor_error(mdb_cursor_t* cursor, bson_error_t* error);
void mdb_cursor_destroy(mdb_cursor_t* cursor);

/* ============ Change Stream ============ */

mdb_change_stream_t* mdb_collection_watch(mdb_collection_t* coll, bson_t* pipeline, bson_t* opts);
bool mdb_change_stream_next(mdb_change_stream_t* cs, bson_t** event);
void mdb_change_stream_destroy(mdb_change_stream_t* cs);

/* ============ GridFS ============ */

mdb_gridfs_bucket_t* mdb_gridfs_bucket_new(mdb_database_t* db, const char* bucket_name, bson_t* opts);
bool mdb_gridfs_bucket_upload_from_bytes(mdb_gridfs_bucket_t* bucket, const char* filename,
    const uint8_t* data, size_t len, bson_t** file_id, bson_error_t* error);
bool mdb_gridfs_bucket_download_to_bytes(mdb_gridfs_bucket_t* bucket, bson_t* file_id,
    uint8_t** data, size_t* len, bson_error_t* error);
void mdb_gridfs_bucket_destroy(mdb_gridfs_bucket_t* bucket);

/* ============ Write/Read Concern ============ */

mdb_write_concern_t* mdb_write_concern_new(void);
mdb_write_concern_t* mdb_write_concern_majority(void);
void mdb_write_concern_set_w(mdb_write_concern_t* wc, int32_t w);
void mdb_write_concern_set_wtimeout(mdb_write_concern_t* wc, int32_t ms);
void mdb_write_concern_destroy(mdb_write_concern_t* wc);

mdb_read_concern_t* mdb_read_concern_new(const char* level);
void mdb_read_concern_destroy(mdb_read_concern_t* rc);

/* ============ Convenience macros ============ */

#define MDB_BSON_APPEND_UTF8(doc, key, val) bson_append_utf8(doc, key, -1, val, -1)
#define MDB_BSON_APPEND_INT32(doc, key, val) bson_append_int32(doc, key, -1, val)
#define MDB_BSON_APPEND_INT64(doc, key, val) bson_append_int64(doc, key, -1, val)
#define MDB_BSON_APPEND_DOUBLE(doc, key, val) bson_append_double(doc, key, -1, val)
#define MDB_BSON_APPEND_BOOL(doc, key, val) bson_append_bool(doc, key, -1, val)
#define MDB_BSON_APPEND_NULL(doc, key) bson_append_null(doc, key, -1)

#ifdef __cplusplus
}
#endif

#endif /* MDB_H */
