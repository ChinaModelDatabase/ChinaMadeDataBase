#ifndef MDB_WIRE_H
#define MDB_WIRE_H

#include "bson.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MDB_OP_MSG 2013

typedef struct {
    int32_t msg_len;
    int32_t req_id;
    int32_t resp_to;
    int32_t op_code;
} mdb_wire_header_t;

typedef struct mdb_opmsg mdb_opmsg_t;

mdb_opmsg_t *mdb_opmsg_new(int32_t req_id);
void mdb_opmsg_destroy(mdb_opmsg_t *msg);

bool mdb_opmsg_set_body(mdb_opmsg_t *msg, const mdb_bson_t *doc);
bool mdb_opmsg_add_document(mdb_opmsg_t *msg, const mdb_bson_t *doc);

uint8_t *mdb_opmsg_to_bytes(const mdb_opmsg_t *msg, uint32_t *out_len);
mdb_opmsg_t *mdb_opmsg_parse(const uint8_t *data, uint32_t len);

mdb_bson_t *mdb_opmsg_get_body(const mdb_opmsg_t *msg);
mdb_bson_t **mdb_opmsg_get_documents(const mdb_opmsg_t *msg, size_t *out_count);
int32_t mdb_opmsg_get_req_id(const mdb_opmsg_t *msg);
int32_t mdb_opmsg_get_resp_to(const mdb_opmsg_t *msg);
int32_t mdb_opmsg_get_op_code(const mdb_opmsg_t *msg);

#ifdef __cplusplus
}
#endif

#endif
