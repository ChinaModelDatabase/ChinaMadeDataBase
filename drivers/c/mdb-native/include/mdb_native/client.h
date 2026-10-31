#ifndef MDB_CLIENT_H
#define MDB_CLIENT_H

#include "bson.h"
#include "connection.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mdb_client mdb_client_t;
typedef struct mdb_database mdb_database_t;

mdb_client_t *mdb_client_new(const char *uri);
void mdb_client_destroy(mdb_client_t *client);

bool mdb_client_connect(mdb_client_t *client);
void mdb_client_close(mdb_client_t *client);

mdb_database_t *mdb_client_database(mdb_client_t *client, const char *name);
mdb_bson_t *mdb_client_list_databases(mdb_client_t *client);
bool mdb_client_ping(mdb_client_t *client);

#ifdef __cplusplus
}
#endif

#endif
