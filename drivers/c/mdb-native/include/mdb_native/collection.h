#ifndef MDB_COLLECTION_H
#define MDB_COLLECTION_H

#include "bson.h"
#include "cursor.h"
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mdb_collection mdb_collection_t;

bool mdb_collection_insert_one(mdb_collection_t *coll, const mdb_bson_t *doc, mdb_bson_t **result);
bool mdb_collection_insert_many(mdb_collection_t *coll, const mdb_bson_t **docs, size_t count, mdb_bson_t **result);

mdb_cursor_t *mdb_collection_find(mdb_collection_t *coll, const mdb_bson_t *filter);
mdb_bson_t *mdb_collection_find_one(mdb_collection_t *coll, const mdb_bson_t *filter);

bool mdb_collection_update_one(mdb_collection_t *coll, const mdb_bson_t *filter,
                               const mdb_bson_t *update, mdb_bson_t **result);
bool mdb_collection_update_many(mdb_collection_t *coll, const mdb_bson_t *filter,
                                const mdb_bson_t *update, mdb_bson_t **result);

bool mdb_collection_delete_one(mdb_collection_t *coll, const mdb_bson_t *filter, mdb_bson_t **result);
bool mdb_collection_delete_many(mdb_collection_t *coll, const mdb_bson_t *filter, mdb_bson_t **result);

mdb_cursor_t *mdb_collection_aggregate(mdb_collection_t *coll, const mdb_bson_t **stages, size_t stage_count);
mdb_cursor_t *mdb_collection_distinct(mdb_collection_t *coll, const char *field, const mdb_bson_t *filter);

int64_t mdb_collection_count_documents(mdb_collection_t *coll, const mdb_bson_t *filter);

bool mdb_collection_create_index(mdb_collection_t *coll, const mdb_bson_t *keys, bool unique, mdb_bson_t **result);
bool mdb_collection_drop_index(mdb_collection_t *coll, const char *index_name, mdb_bson_t **result);
mdb_cursor_t *mdb_collection_list_indexes(mdb_collection_t *coll);

#ifdef __cplusplus
}
#endif

#endif
