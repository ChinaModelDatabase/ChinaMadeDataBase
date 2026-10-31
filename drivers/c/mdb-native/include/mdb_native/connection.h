#ifndef MDB_CONNECTION_H
#define MDB_CONNECTION_H

#include "bson.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mdb_connection mdb_connection_t;

mdb_connection_t *mdb_connection_new(const char *host, int port);
void mdb_connection_destroy(mdb_connection_t *conn);

bool mdb_connection_connect(mdb_connection_t *conn);
void mdb_connection_close(mdb_connection_t *conn);
bool mdb_connection_is_connected(const mdb_connection_t *conn);

mdb_bson_t *mdb_connection_send_command(mdb_connection_t *conn, const mdb_bson_t *cmd_doc);
mdb_bson_t *mdb_connection_send_command_with_docs(mdb_connection_t *conn, const mdb_bson_t *cmd_doc,
                                                   const mdb_bson_t **extra_docs, size_t extra_count);

#ifdef __cplusplus
}
#endif

#endif
