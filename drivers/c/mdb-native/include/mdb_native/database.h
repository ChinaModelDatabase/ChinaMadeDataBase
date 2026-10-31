#ifndef MDB_DATABASE_H
#define MDB_DATABASE_H

#include "bson.h"
#include "collection.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mdb_database mdb_database_t;

mdb_collection_t *mdb_database_collection(mdb_database_t *db, const char *name);
mdb_bson_t *mdb_database_list_collections(mdb_database_t *db);
mdb_bson_t *mdb_database_create_collection(mdb_database_t *db, const char *name);
mdb_bson_t *mdb_database_drop_collection(mdb_database_t *db, const char *name);
mdb_bson_t *mdb_database_command(mdb_database_t *db, const mdb_bson_t *cmd);

#ifdef __cplusplus
}
#endif

#endif
