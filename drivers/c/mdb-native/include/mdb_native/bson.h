#ifndef MDB_BSON_H
#define MDB_BSON_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint8_t bytes[12];
} mdb_object_id_t;

void mdb_object_id_generate(mdb_object_id_t *oid);
char *mdb_object_id_to_string(const mdb_object_id_t *oid, char *buf, size_t buflen);

typedef struct mdb_bson mdb_bson_t;

mdb_bson_t *mdb_bson_new(void);
mdb_bson_t *mdb_bson_new_empty(void);
void mdb_bson_destroy(mdb_bson_t *bson);
mdb_bson_t *mdb_bson_copy(const mdb_bson_t *bson);

bool mdb_bson_append_int32(mdb_bson_t *bson, const char *name, int32_t value);
bool mdb_bson_append_int64(mdb_bson_t *bson, const char *name, int64_t value);
bool mdb_bson_append_double(mdb_bson_t *bson, const char *name, double value);
bool mdb_bson_append_utf8(mdb_bson_t *bson, const char *name, const char *value, uint32_t len);
bool mdb_bson_append_bool(mdb_bson_t *bson, const char *name, bool value);
bool mdb_bson_append_null(mdb_bson_t *bson, const char *name);
bool mdb_bson_append_document(mdb_bson_t *bson, const char *name, const mdb_bson_t *child);
bool mdb_bson_append_array(mdb_bson_t *bson, const char *name, const mdb_bson_t *child);
bool mdb_bson_append_object_id(mdb_bson_t *bson, const char *name, const mdb_object_id_t *oid);
bool mdb_bson_append_datetime(mdb_bson_t *bson, const char *name, int64_t ms_since_epoch);

uint8_t *mdb_bson_to_bytes(const mdb_bson_t *bson, uint32_t *out_len);
mdb_bson_t *mdb_bson_from_bytes(const uint8_t *data, uint32_t len);

bool mdb_bson_find_int32(const mdb_bson_t *bson, const char *name, int32_t *out);
bool mdb_bson_find_int64(const mdb_bson_t *bson, const char *name, int64_t *out);
bool mdb_bson_find_double(const mdb_bson_t *bson, const char *name, double *out);
bool mdb_bson_find_utf8(const mdb_bson_t *bson, const char *name, const char **out, uint32_t *out_len);
bool mdb_bson_find_bool(const mdb_bson_t *bson, const char *name, bool *out);
bool mdb_bson_find_document(const mdb_bson_t *bson, const char *name, mdb_bson_t **out);
bool mdb_bson_find_array(const mdb_bson_t *bson, const char *name, mdb_bson_t **out);
bool mdb_bson_find_object_id(const mdb_bson_t *bson, const char *name, mdb_object_id_t *out);
bool mdb_bson_find_datetime(const mdb_bson_t *bson, const char *name, int64_t *out);
bool mdb_bson_has_field(const mdb_bson_t *bson, const char *name);

#ifdef __cplusplus
}
#endif

#endif
