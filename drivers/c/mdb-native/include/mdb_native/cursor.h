#ifndef MDB_CURSOR_H
#define MDB_CURSOR_H

#include "bson.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mdb_cursor mdb_cursor_t;

bool mdb_cursor_next(mdb_cursor_t *cursor);
const mdb_bson_t *mdb_cursor_current(const mdb_cursor_t *cursor);

mdb_cursor_t *mdb_cursor_sort(mdb_cursor_t *cursor, const mdb_bson_t *sort);
mdb_cursor_t *mdb_cursor_limit(mdb_cursor_t *cursor, int64_t limit);
mdb_cursor_t *mdb_cursor_skip(mdb_cursor_t *cursor, int64_t skip);

void mdb_cursor_destroy(mdb_cursor_t *cursor);

#ifdef __cplusplus
}
#endif

#endif
