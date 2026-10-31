#include "mdb_native/bson.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#define BSON_TYPE_DOUBLE    0x01
#define BSON_TYPE_UTF8      0x02
#define BSON_TYPE_DOCUMENT  0x03
#define BSON_TYPE_ARRAY     0x04
#define BSON_TYPE_OBJECTID  0x07
#define BSON_TYPE_BOOL      0x08
#define BSON_TYPE_DATETIME  0x09
#define BSON_TYPE_NULL      0x0A
#define BSON_TYPE_INT32     0x10
#define BSON_TYPE_INT64     0x12

struct mdb_bson {
    uint8_t *data;
    uint32_t len;
    uint32_t capacity;
};

static void bson_ensure_capacity(mdb_bson_t *bson, uint32_t additional) {
    if (bson->len + additional > bson->capacity) {
        uint32_t new_cap = bson->capacity ? bson->capacity * 2 : 256;
        while (new_cap < bson->len + additional) new_cap *= 2;
        bson->data = realloc(bson->data, new_cap);
        bson->capacity = new_cap;
    }
}

static bool bson_write_type(mdb_bson_t *bson, uint8_t type) {
    bson_ensure_capacity(bson, 1);
    bson->data[bson->len++] = type;
    return true;
}

static bool bson_write_cstring(mdb_bson_t *bson, const char *str) {
    size_t slen = strlen(str) + 1;
    bson_ensure_capacity(bson, (uint32_t)slen);
    memcpy(bson->data + bson->len, str, slen);
    bson->len += (uint32_t)slen;
    return true;
}

static bool bson_write_int32_le(mdb_bson_t *bson, int32_t val) {
    bson_ensure_capacity(bson, 4);
    uint8_t *p = bson->data + bson->len;
    p[0] = (uint8_t)(val & 0xFF);
    p[1] = (uint8_t)((val >> 8) & 0xFF);
    p[2] = (uint8_t)((val >> 16) & 0xFF);
    p[3] = (uint8_t)((val >> 24) & 0xFF);
    bson->len += 4;
    return true;
}

static bool bson_write_int64_le(mdb_bson_t *bson, int64_t val) {
    bson_ensure_capacity(bson, 8);
    uint8_t *p = bson->data + bson->len;
    for (int i = 0; i < 8; i++) {
        p[i] = (uint8_t)((val >> (i * 8)) & 0xFF);
    }
    bson->len += 8;
    return true;
}

static bool bson_write_double_le(mdb_bson_t *bson, double val) {
    int64_t ival;
    memcpy(&ival, &val, 8);
    return bson_write_int64_le(bson, ival);
}

static bool bson_write_type_int32(mdb_bson_t *bson, uint8_t type, const char *name, int32_t val) {
    bson_write_type(bson, type);
    bson_write_cstring(bson, name);
    bson_write_int32_le(bson, val);
    return true;
}

static bool bson_write_type_int64(mdb_bson_t *bson, uint8_t type, const char *name, int64_t val) {
    bson_write_type(bson, type);
    bson_write_cstring(bson, name);
    bson_write_int64_le(bson, val);
    return true;
}

static bool bson_write_type_double(mdb_bson_t *bson, uint8_t type, const char *name, double val) {
    bson_write_type(bson, type);
    bson_write_cstring(bson, name);
    bson_write_double_le(bson, val);
    return true;
}

mdb_bson_t *mdb_bson_new(void) {
    mdb_bson_t *bson = calloc(1, sizeof(mdb_bson_t));
    if (!bson) return NULL;
    bson->capacity = 256;
    bson->data = malloc(bson->capacity);
    if (!bson->data) { free(bson); return NULL; }
    bson->len = 4;
    return bson;
}

mdb_bson_t *mdb_bson_new_empty(void) {
    return mdb_bson_new();
}

void mdb_bson_destroy(mdb_bson_t *bson) {
    if (!bson) return;
    free(bson->data);
    free(bson);
}

mdb_bson_t *mdb_bson_copy(const mdb_bson_t *bson) {
    if (!bson) return NULL;
    uint32_t len = 0;
    uint8_t *bytes = mdb_bson_to_bytes(bson, &len);
    if (!bytes) return NULL;
    mdb_bson_t *copy = mdb_bson_from_bytes(bytes, len);
    free(bytes);
    return copy;
}

bool mdb_bson_append_int32(mdb_bson_t *bson, const char *name, int32_t value) {
    if (!bson || !name) return false;
    return bson_write_type_int32(bson, BSON_TYPE_INT32, name, value);
}

bool mdb_bson_append_int64(mdb_bson_t *bson, const char *name, int64_t value) {
    if (!bson || !name) return false;
    return bson_write_type_int64(bson, BSON_TYPE_INT64, name, value);
}

bool mdb_bson_append_double(mdb_bson_t *bson, const char *name, double value) {
    if (!bson || !name) return false;
    return bson_write_type_double(bson, BSON_TYPE_DOUBLE, name, value);
}

bool mdb_bson_append_utf8(mdb_bson_t *bson, const char *name, const char *value, uint32_t len) {
    if (!bson || !name || !value) return false;
    bson_write_type(bson, BSON_TYPE_UTF8);
    bson_write_cstring(bson, name);
    bson_write_int32_le(bson, (int32_t)(len + 1));
    bson_ensure_capacity(bson, len + 1);
    memcpy(bson->data + bson->len, value, len);
    bson->data[bson->len + len] = 0;
    bson->len += len + 1;
    return true;
}

bool mdb_bson_append_bool(mdb_bson_t *bson, const char *name, bool value) {
    if (!bson || !name) return false;
    bson_write_type(bson, BSON_TYPE_BOOL);
    bson_write_cstring(bson, name);
    bson_ensure_capacity(bson, 1);
    bson->data[bson->len++] = value ? 1 : 0;
    return true;
}

bool mdb_bson_append_null(mdb_bson_t *bson, const char *name) {
    if (!bson || !name) return false;
    bson_write_type(bson, BSON_TYPE_NULL);
    bson_write_cstring(bson, name);
    return true;
}

bool mdb_bson_append_document(mdb_bson_t *bson, const char *name, const mdb_bson_t *child) {
    if (!bson || !name || !child) return false;
    bson_write_type(bson, BSON_TYPE_DOCUMENT);
    bson_write_cstring(bson, name);
    uint32_t child_len = 0;
    uint8_t *child_bytes = mdb_bson_to_bytes(child, &child_len);
    if (!child_bytes) return false;
    bson_ensure_capacity(bson, child_len);
    memcpy(bson->data + bson->len, child_bytes, child_len);
    bson->len += child_len;
    free(child_bytes);
    return true;
}

bool mdb_bson_append_array(mdb_bson_t *bson, const char *name, const mdb_bson_t *child) {
    if (!bson || !name || !child) return false;
    bson_write_type(bson, BSON_TYPE_ARRAY);
    bson_write_cstring(bson, name);
    uint32_t child_len = 0;
    uint8_t *child_bytes = mdb_bson_to_bytes(child, &child_len);
    if (!child_bytes) return false;
    bson_ensure_capacity(bson, child_len);
    memcpy(bson->data + bson->len, child_bytes, child_len);
    bson->len += child_len;
    free(child_bytes);
    return true;
}

bool mdb_bson_append_object_id(mdb_bson_t *bson, const char *name, const mdb_object_id_t *oid) {
    if (!bson || !name || !oid) return false;
    bson_write_type(bson, BSON_TYPE_OBJECTID);
    bson_write_cstring(bson, name);
    bson_ensure_capacity(bson, 12);
    memcpy(bson->data + bson->len, oid->bytes, 12);
    bson->len += 12;
    return true;
}

bool mdb_bson_append_datetime(mdb_bson_t *bson, const char *name, int64_t ms_since_epoch) {
    if (!bson || !name) return false;
    return bson_write_type_int64(bson, BSON_TYPE_DATETIME, name, ms_since_epoch);
}

uint8_t *mdb_bson_to_bytes(const mdb_bson_t *bson, uint32_t *out_len) {
    if (!bson || !out_len) return NULL;
    uint32_t total = bson->len;
    if (total < 5) {
        uint8_t *doc = malloc(5);
        if (!doc) return NULL;
        doc[0] = 5; doc[1] = 0; doc[2] = 0; doc[3] = 0; doc[4] = 0;
        *out_len = 5;
        return doc;
    }
    uint8_t *copy = malloc(total);
    if (!copy) return NULL;
    memcpy(copy, bson->data, total);

    int32_t doc_size = (int32_t)total;
    copy[0] = (uint8_t)(doc_size & 0xFF);
    copy[1] = (uint8_t)((doc_size >> 8) & 0xFF);
    copy[2] = (uint8_t)((doc_size >> 16) & 0xFF);
    copy[3] = (uint8_t)((doc_size >> 24) & 0xFF);

    copy[total - 1] = 0x00;

    *out_len = total;
    return copy;
}

static int32_t read_int32_le(const uint8_t *p) {
    return (int32_t)((uint32_t)p[0] | ((uint32_t)p[1] << 8) |
                     ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24));
}

static int64_t read_int64_le(const uint8_t *p) {
    int64_t val = 0;
    for (int i = 0; i < 8; i++) {
        val |= ((int64_t)p[i]) << (i * 8);
    }
    return val;
}

mdb_bson_t *mdb_bson_from_bytes(const uint8_t *data, uint32_t len) {
    if (!data || len < 5) return NULL;
    int32_t doc_size = read_int32_le(data);
    if (doc_size < 5 || (uint32_t)doc_size > len) return NULL;

    mdb_bson_t *bson = calloc(1, sizeof(mdb_bson_t));
    if (!bson) return NULL;
    bson->capacity = (uint32_t)doc_size;
    bson->data = malloc(bson->capacity);
    if (!bson->data) { free(bson); return NULL; }
    memcpy(bson->data, data, (uint32_t)doc_size);
    bson->len = (uint32_t)doc_size;
    return bson;
}

void mdb_object_id_generate(mdb_object_id_t *oid) {
    if (!oid) return;

    uint32_t timestamp = (uint32_t)time(NULL);
    oid->bytes[0] = (uint8_t)((timestamp >> 24) & 0xFF);
    oid->bytes[1] = (uint8_t)((timestamp >> 16) & 0xFF);
    oid->bytes[2] = (uint8_t)((timestamp >> 8) & 0xFF);
    oid->bytes[3] = (uint8_t)(timestamp & 0xFF);

    static uint32_t counter = 0;
    counter++;

    oid->bytes[4] = (uint8_t)((counter >> 16) & 0xFF);
    oid->bytes[5] = (uint8_t)((counter >> 8) & 0xFF);
    oid->bytes[6] = (uint8_t)(counter & 0xFF);

    for (int i = 7; i < 12; i++) {
        oid->bytes[i] = (uint8_t)(rand() & 0xFF);
    }
}

char *mdb_object_id_to_string(const mdb_object_id_t *oid, char *buf, size_t buflen) {
    if (!oid || !buf || buflen < 25) return NULL;
    static const char hex[] = "0123456789abcdef";
    int pos = 0;
    for (int i = 0; i < 12 && pos + 3 <= (int)buflen; i++) {
        buf[pos++] = hex[(oid->bytes[i] >> 4) & 0x0F];
        buf[pos++] = hex[oid->bytes[i] & 0x0F];
    }
    buf[pos] = '\0';
    return buf;
}

static bool skip_element(const uint8_t *data, uint32_t len, uint32_t *pos) {
    if (*pos >= len) return false;
    uint8_t type = data[*pos];
    (*pos)++;

    while (*pos < len && data[*pos] != 0) (*pos)++;
    if (*pos >= len) return false;
    (*pos)++;

    switch (type) {
        case BSON_TYPE_DOUBLE:
            *pos += 8; break;
        case BSON_TYPE_UTF8: {
            if (*pos + 4 > len) return false;
            int32_t slen = read_int32_le(data + *pos);
            *pos += 4 + (uint32_t)slen; break;
        }
        case BSON_TYPE_DOCUMENT:
        case BSON_TYPE_ARRAY: {
            if (*pos + 4 > len) return false;
            int32_t dlen = read_int32_le(data + *pos);
            *pos += (uint32_t)dlen; break;
        }
        case BSON_TYPE_OBJECTID:
            *pos += 12; break;
        case BSON_TYPE_BOOL:
            *pos += 1; break;
        case BSON_TYPE_DATETIME:
            *pos += 8; break;
        case BSON_TYPE_NULL:
            break;
        case BSON_TYPE_INT32:
            *pos += 4; break;
        case BSON_TYPE_INT64:
            *pos += 8; break;
        default:
            return false;
    }
    return *pos <= len;
}

static bool find_element(const mdb_bson_t *bson, const char *name, uint8_t expected_type,
                          uint8_t *out_type, uint32_t *value_offset, uint32_t *value_len) {
    if (!bson || !name || bson->len < 5) return false;

    uint32_t pos = 4;
    while (pos < bson->len) {
        if (bson->data[pos] == 0) break;
        uint8_t type = bson->data[pos];
        pos++;

        const char *ename = (const char *)(bson->data + pos);
        while (pos < bson->len && bson->data[pos] != 0) pos++;
        if (pos >= bson->len) return false;
        pos++;

        uint32_t vstart = pos;
        if (strcmp(ename, name) == 0 && (expected_type == 0 || type == expected_type)) {
            if (out_type) *out_type = type;
            if (value_offset) *value_offset = vstart;
            if (value_len) *value_len = bson->len - vstart;
            return true;
        }

        switch (type) {
            case BSON_TYPE_DOUBLE: pos += 8; break;
            case BSON_TYPE_UTF8: {
                if (pos + 4 > bson->len) return false;
                int32_t slen = read_int32_le(bson->data + pos);
                pos += 4 + (uint32_t)slen; break;
            }
            case BSON_TYPE_DOCUMENT:
            case BSON_TYPE_ARRAY: {
                if (pos + 4 > bson->len) return false;
                int32_t dlen = read_int32_le(bson->data + pos);
                pos += (uint32_t)dlen; break;
            }
            case BSON_TYPE_OBJECTID: pos += 12; break;
            case BSON_TYPE_BOOL: pos += 1; break;
            case BSON_TYPE_DATETIME: pos += 8; break;
            case BSON_TYPE_NULL: break;
            case BSON_TYPE_INT32: pos += 4; break;
            case BSON_TYPE_INT64: pos += 8; break;
            default: return false;
        }
    }
    return false;
}

bool mdb_bson_has_field(const mdb_bson_t *bson, const char *name) {
    return find_element(bson, name, 0, NULL, NULL, NULL);
}

bool mdb_bson_find_int32(const mdb_bson_t *bson, const char *name, int32_t *out) {
    uint32_t off, vlen;
    if (!find_element(bson, name, BSON_TYPE_INT32, NULL, &off, &vlen)) return false;
    if (vlen < 4) return false;
    *out = read_int32_le(bson->data + off);
    return true;
}

bool mdb_bson_find_int64(const mdb_bson_t *bson, const char *name, int64_t *out) {
    uint32_t off, vlen;
    if (!find_element(bson, name, BSON_TYPE_INT64, NULL, &off, &vlen)) return false;
    if (vlen < 8) return false;
    *out = read_int64_le(bson->data + off);
    return true;
}

bool mdb_bson_find_double(const mdb_bson_t *bson, const char *name, double *out) {
    uint32_t off, vlen;
    if (!find_element(bson, name, BSON_TYPE_DOUBLE, NULL, &off, &vlen)) return false;
    if (vlen < 8) return false;
    int64_t ival = read_int64_le(bson->data + off);
    memcpy(out, &ival, 8);
    return true;
}

bool mdb_bson_find_utf8(const mdb_bson_t *bson, const char *name, const char **out, uint32_t *out_len) {
    uint32_t off, vlen;
    if (!find_element(bson, name, BSON_TYPE_UTF8, NULL, &off, &vlen)) return false;
    if (vlen < 4) return false;
    int32_t slen = read_int32_le(bson->data + off);
    if (slen < 1 || (uint32_t)(4 + slen) > vlen) return false;
    *out = (const char *)(bson->data + off + 4);
    *out_len = (uint32_t)(slen - 1);
    return true;
}

bool mdb_bson_find_bool(const mdb_bson_t *bson, const char *name, bool *out) {
    uint32_t off, vlen;
    if (!find_element(bson, name, BSON_TYPE_BOOL, NULL, &off, &vlen)) return false;
    if (vlen < 1) return false;
    *out = bson->data[off] != 0;
    return true;
}

bool mdb_bson_find_document(const mdb_bson_t *bson, const char *name, mdb_bson_t **out) {
    uint32_t off, vlen;
    if (!find_element(bson, name, BSON_TYPE_DOCUMENT, NULL, &off, &vlen)) return false;
    if (vlen < 4) return false;
    int32_t dlen = read_int32_le(bson->data + off);
    if (dlen < 5 || (uint32_t)dlen > vlen) return false;
    *out = mdb_bson_from_bytes(bson->data + off, (uint32_t)dlen);
    return *out != NULL;
}

bool mdb_bson_find_array(const mdb_bson_t *bson, const char *name, mdb_bson_t **out) {
    uint32_t off, vlen;
    if (!find_element(bson, name, BSON_TYPE_ARRAY, NULL, &off, &vlen)) return false;
    if (vlen < 4) return false;
    int32_t dlen = read_int32_le(bson->data + off);
    if (dlen < 5 || (uint32_t)dlen > vlen) return false;
    *out = mdb_bson_from_bytes(bson->data + off, (uint32_t)dlen);
    return *out != NULL;
}

bool mdb_bson_find_object_id(const mdb_bson_t *bson, const char *name, mdb_object_id_t *out) {
    uint32_t off, vlen;
    if (!find_element(bson, name, BSON_TYPE_OBJECTID, NULL, &off, &vlen)) return false;
    if (vlen < 12) return false;
    memcpy(out->bytes, bson->data + off, 12);
    return true;
}

bool mdb_bson_find_datetime(const mdb_bson_t *bson, const char *name, int64_t *out) {
    uint32_t off, vlen;
    if (!find_element(bson, name, BSON_TYPE_DATETIME, NULL, &off, &vlen)) return false;
    if (vlen < 8) return false;
    *out = read_int64_le(bson->data + off);
    return true;
}
