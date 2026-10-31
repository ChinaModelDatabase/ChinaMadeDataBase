#include "mdb_native/wire.h"
#include <stdlib.h>
#include <string.h>

#ifdef MDB_PLATFORM_WIN32
#include <windows.h>
#endif

#define MDB_SECTION_BODY      0
#define MDB_SECTION_DOCUMENT  1

struct mdb_opmsg {
    int32_t req_id;
    int32_t resp_to;
    mdb_bson_t *body;
    mdb_bson_t **documents;
    size_t doc_count;
    size_t doc_capacity;
};

static int32_t read_i32(const uint8_t *p) {
    return (int32_t)((uint32_t)p[0] | ((uint32_t)p[1] << 8) |
                     ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24));
}

static void write_i32(uint8_t *p, int32_t val) {
    p[0] = (uint8_t)(val & 0xFF);
    p[1] = (uint8_t)((val >> 8) & 0xFF);
    p[2] = (uint8_t)((val >> 16) & 0xFF);
    p[3] = (uint8_t)((val >> 24) & 0xFF);
}

static int32_t next_req_id(void) {
#ifdef MDB_PLATFORM_WIN32
    static LONG id = 0;
    return InterlockedIncrement(&id);
#else
    static int32_t id = 0;
    return __sync_fetch_and_add(&id, 1);
#endif
}

mdb_opmsg_t *mdb_opmsg_new(int32_t req_id) {
    mdb_opmsg_t *msg = calloc(1, sizeof(mdb_opmsg_t));
    if (!msg) return NULL;
    msg->req_id = req_id ? req_id : next_req_id();
    msg->resp_to = 0;
    msg->body = mdb_bson_new();
    if (!msg->body) { free(msg); return NULL; }
    msg->doc_capacity = 8;
    msg->documents = calloc(msg->doc_capacity, sizeof(mdb_bson_t *));
    if (!msg->documents) { mdb_bson_destroy(msg->body); free(msg); return NULL; }
    return msg;
}

void mdb_opmsg_destroy(mdb_opmsg_t *msg) {
    if (!msg) return;
    mdb_bson_destroy(msg->body);
    for (size_t i = 0; i < msg->doc_count; i++) {
        mdb_bson_destroy(msg->documents[i]);
    }
    free(msg->documents);
    free(msg);
}

bool mdb_opmsg_set_body(mdb_opmsg_t *msg, const mdb_bson_t *doc) {
    if (!msg || !doc) return false;
    mdb_bson_destroy(msg->body);
    uint32_t len = 0;
    uint8_t *bytes = mdb_bson_to_bytes(doc, &len);
    if (!bytes) return false;
    msg->body = mdb_bson_from_bytes(bytes, len);
    free(bytes);
    return msg->body != NULL;
}

bool mdb_opmsg_add_document(mdb_opmsg_t *msg, const mdb_bson_t *doc) {
    if (!msg || !doc) return false;
    if (msg->doc_count >= msg->doc_capacity) {
        msg->doc_capacity *= 2;
        mdb_bson_t **new_docs = realloc(msg->documents, msg->doc_capacity * sizeof(mdb_bson_t *));
        if (!new_docs) return false;
        msg->documents = new_docs;
    }
    uint32_t len = 0;
    uint8_t *bytes = mdb_bson_to_bytes(doc, &len);
    if (!bytes) return false;
    msg->documents[msg->doc_count] = mdb_bson_from_bytes(bytes, len);
    free(bytes);
    if (!msg->documents[msg->doc_count]) return false;
    msg->doc_count++;
    return true;
}

uint8_t *mdb_opmsg_to_bytes(const mdb_opmsg_t *msg, uint32_t *out_len) {
    if (!msg || !out_len) return NULL;

    uint32_t body_len = 0;
    uint8_t *body_bytes = mdb_bson_to_bytes(msg->body, &body_len);
    if (!body_bytes) return NULL;

    uint32_t total = 16 + 4 + 4 + 4 + body_len;

    for (size_t i = 0; i < msg->doc_count; i++) {
        uint32_t dl = 0;
        uint8_t *db = mdb_bson_to_bytes(msg->documents[i], &dl);
        total += 4 + dl;
        free(db);
    }

    uint8_t *buf = malloc(total);
    if (!buf) { free(body_bytes); return NULL; }

    uint8_t *p = buf;

    write_i32(p, (int32_t)total); p += 4;
    write_i32(p, msg->req_id); p += 4;
    write_i32(p, msg->resp_to); p += 4;
    write_i32(p, MDB_OP_MSG); p += 4;

    uint32_t flag_bits = 0;
    write_i32(p, (int32_t)flag_bits); p += 4;

    write_i32(p, MDB_SECTION_BODY); p += 4;
    memcpy(p, body_bytes, body_len);
    p += body_len;
    free(body_bytes);

    for (size_t i = 0; i < msg->doc_count; i++) {
        uint32_t dl = 0;
        uint8_t *db = mdb_bson_to_bytes(msg->documents[i], &dl);
        write_i32(p, MDB_SECTION_DOCUMENT); p += 4;
        memcpy(p, db, dl);
        p += dl;
        free(db);
    }

    *out_len = total;
    return buf;
}

mdb_opmsg_t *mdb_opmsg_parse(const uint8_t *data, uint32_t len) {
    if (!data || len < 16) return NULL;

    int32_t msg_len = read_i32(data);
    if (msg_len < 16 || (uint32_t)msg_len > len) return NULL;

    int32_t req_id = read_i32(data + 4);
    int32_t resp_to = read_i32(data + 8);
    int32_t op_code = read_i32(data + 12);

    if (op_code != MDB_OP_MSG) return NULL;

    mdb_opmsg_t *msg = calloc(1, sizeof(mdb_opmsg_t));
    if (!msg) return NULL;
    msg->req_id = req_id;
    msg->resp_to = resp_to;
    msg->doc_capacity = 8;
    msg->documents = calloc(msg->doc_capacity, sizeof(mdb_bson_t *));
    if (!msg->documents) { free(msg); return NULL; }

    const uint8_t *p = data + 16;
    const uint8_t *end = data + (uint32_t)msg_len;

    if (p + 4 > end) goto fail;
    p += 4;

    while (p < end) {
        if (p + 4 > end) break;
        int32_t section_kind = read_i32(p);
        p += 4;

        if (section_kind == MDB_SECTION_BODY) {
            if (p + 4 > end) break;
            int32_t doc_size = read_i32(p);
            if (doc_size < 5 || p + doc_size > end) break;
            msg->body = mdb_bson_from_bytes(p, (uint32_t)doc_size);
            p += doc_size;
        } else if (section_kind == MDB_SECTION_DOCUMENT) {
            if (p + 4 > end) break;
            int32_t doc_size = read_i32(p);
            if (doc_size < 5 || p + doc_size > end) break;
            if (msg->doc_count >= msg->doc_capacity) {
                msg->doc_capacity *= 2;
                msg->documents = realloc(msg->documents, msg->doc_capacity * sizeof(mdb_bson_t *));
            }
            msg->documents[msg->doc_count] = mdb_bson_from_bytes(p, (uint32_t)doc_size);
            msg->doc_count++;
            p += doc_size;
        } else {
            break;
        }
    }

    if (!msg->body) {
        msg->body = mdb_bson_new();
    }

    return msg;

fail:
    free(msg->documents);
    free(msg);
    return NULL;
}

mdb_bson_t *mdb_opmsg_get_body(const mdb_opmsg_t *msg) {
    if (!msg) return NULL;
    return msg->body;
}

mdb_bson_t **mdb_opmsg_get_documents(const mdb_opmsg_t *msg, size_t *out_count) {
    if (!msg || !out_count) return NULL;
    *out_count = msg->doc_count;
    return msg->documents;
}

int32_t mdb_opmsg_get_req_id(const mdb_opmsg_t *msg) {
    return msg ? msg->req_id : 0;
}

int32_t mdb_opmsg_get_resp_to(const mdb_opmsg_t *msg) {
    return msg ? msg->resp_to : 0;
}

int32_t mdb_opmsg_get_op_code(const mdb_opmsg_t *msg) {
    return msg ? MDB_OP_MSG : 0;
}
