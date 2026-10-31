#include "mdb_native/bson.h"
#include "mdb_native/wire.h"
#include "mdb_native/connection.h"
#include "mdb_native/client.h"
#include "mdb_native/cursor.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

static int tests_run = 0;
static int tests_passed = 0;

#define ASSERT(expr) do { \
    tests_run++; \
    if (expr) { tests_passed++; } \
    else { fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); } \
} while(0)

#define ASSERT_EQ_INT32(a, b) do { \
    tests_run++; \
    if ((a) == (b)) { tests_passed++; } \
    else { fprintf(stderr, "FAIL: %s:%d: %d != %d\n", __FILE__, __LINE__, (a), (b)); } \
} while(0)

static void test_bson_int32(void) {
    mdb_bson_t *doc = mdb_bson_new();
    ASSERT(doc != NULL);
    ASSERT(mdb_bson_append_int32(doc, "x", 42));

    uint32_t len = 0;
    uint8_t *bytes = mdb_bson_to_bytes(doc, &len);
    ASSERT(bytes != NULL);
    ASSERT(len > 4);

    ASSERT_EQ_INT32(bytes[0], 11);
    ASSERT_EQ_INT32(bytes[1], 0);
    ASSERT_EQ_INT32(bytes[2], 0);
    ASSERT_EQ_INT32(bytes[3], 0);

    ASSERT_EQ_INT32(bytes[4], 0x10);
    ASSERT_EQ_INT32(bytes[5], 'x');
    ASSERT_EQ_INT32(bytes[6], 0);
    ASSERT_EQ_INT32(bytes[7], 42);
    ASSERT_EQ_INT32(bytes[8], 0);
    ASSERT_EQ_INT32(bytes[9], 0);
    ASSERT_EQ_INT32(bytes[10], 0);

    free(bytes);

    mdb_bson_t *parsed = mdb_bson_from_bytes((uint8_t []){11,0,0,0, 0x10,'x',0, 42,0,0,0}, 11);
    ASSERT(parsed != NULL);
    int32_t val = 0;
    ASSERT(mdb_bson_find_int32(parsed, "x", &val));
    ASSERT_EQ_INT32(val, 42);
    ASSERT(!mdb_bson_find_int32(parsed, "y", &val));

    mdb_bson_destroy(doc);
    mdb_bson_destroy(parsed);
}

static void test_bson_int64(void) {
    mdb_bson_t *doc = mdb_bson_new();
    ASSERT(doc != NULL);
    ASSERT(mdb_bson_append_int64(doc, "big", 123456789012345LL));

    int64_t val = 0;
    ASSERT(mdb_bson_find_int64(doc, "big", &val));
    ASSERT(val == 123456789012345LL);
    ASSERT(!mdb_bson_find_int64(doc, "missing", &val));

    mdb_bson_destroy(doc);
}

static void test_bson_double(void) {
    mdb_bson_t *doc = mdb_bson_new();
    ASSERT(doc != NULL);
    ASSERT(mdb_bson_append_double(doc, "pi", 3.14159));

    double val = 0;
    ASSERT(mdb_bson_find_double(doc, "pi", &val));
    ASSERT(val > 3.14 && val < 3.15);
    ASSERT(!mdb_bson_find_double(doc, "nope", &val));

    mdb_bson_destroy(doc);
}

static void test_bson_utf8(void) {
    mdb_bson_t *doc = mdb_bson_new();
    ASSERT(doc != NULL);
    ASSERT(mdb_bson_append_utf8(doc, "name", "hello", 5));

    const char *str = NULL;
    uint32_t slen = 0;
    ASSERT(mdb_bson_find_utf8(doc, "name", &str, &slen));
    ASSERT_EQ_INT32((int32_t)slen, 5);
    ASSERT(memcmp(str, "hello", 5) == 0);
    ASSERT(!mdb_bson_find_utf8(doc, "nope", &str, &slen));

    mdb_bson_destroy(doc);
}

static void test_bson_bool(void) {
    mdb_bson_t *doc = mdb_bson_new();
    ASSERT(doc != NULL);
    ASSERT(mdb_bson_append_bool(doc, "flag", true));
    ASSERT(mdb_bson_append_bool(doc, "off", false));

    bool val = false;
    ASSERT(mdb_bson_find_bool(doc, "flag", &val));
    ASSERT(val == true);
    ASSERT(mdb_bson_find_bool(doc, "off", &val));
    ASSERT(val == false);

    mdb_bson_destroy(doc);
}

static void test_bson_null(void) {
    mdb_bson_t *doc = mdb_bson_new();
    ASSERT(doc != NULL);
    ASSERT(mdb_bson_append_null(doc, "nothing"));
    ASSERT(mdb_bson_has_field(doc, "nothing"));
    ASSERT(!mdb_bson_has_field(doc, "something"));

    mdb_bson_destroy(doc);
}

static void test_bson_object_id(void) {
    mdb_bson_t *doc = mdb_bson_new();
    ASSERT(doc != NULL);
    mdb_object_id_t oid;
    mdb_object_id_generate(&oid);
    ASSERT(mdb_bson_append_object_id(doc, "_id", &oid));

    mdb_object_id_t out;
    ASSERT(mdb_bson_find_object_id(doc, "_id", &out));
    ASSERT(memcmp(oid.bytes, out.bytes, 12) == 0);

    char str[25];
    mdb_object_id_to_string(&oid, str, sizeof(str));
    ASSERT(strlen(str) == 24);

    mdb_bson_destroy(doc);
}

static void test_bson_datetime(void) {
    mdb_bson_t *doc = mdb_bson_new();
    ASSERT(doc != NULL);
    ASSERT(mdb_bson_append_datetime(doc, "ts", 1609459200000LL));

    int64_t val = 0;
    ASSERT(mdb_bson_find_datetime(doc, "ts", &val));
    ASSERT(val == 1609459200000LL);

    mdb_bson_destroy(doc);
}

static void test_bson_document(void) {
    mdb_bson_t *doc = mdb_bson_new();
    mdb_bson_t *inner = mdb_bson_new();
    mdb_bson_append_int32(inner, "a", 1);
    mdb_bson_append_int32(inner, "b", 2);
    ASSERT(mdb_bson_append_document(doc, "nested", inner));

    mdb_bson_t *out = NULL;
    ASSERT(mdb_bson_find_document(doc, "nested", &out));
    ASSERT(out != NULL);
    int32_t v = 0;
    ASSERT(mdb_bson_find_int32(out, "a", &v));
    ASSERT_EQ_INT32(v, 1);
    ASSERT(mdb_bson_find_int32(out, "b", &v));
    ASSERT_EQ_INT32(v, 2);

    mdb_bson_destroy(out);
    mdb_bson_destroy(inner);
    mdb_bson_destroy(doc);
}

static void test_bson_array(void) {
    mdb_bson_t *doc = mdb_bson_new();
    mdb_bson_t *arr = mdb_bson_new();
    mdb_bson_append_int32(arr, "0", 10);
    mdb_bson_append_int32(arr, "1", 20);
    ASSERT(mdb_bson_append_array(doc, "nums", arr));

    mdb_bson_t *out = NULL;
    ASSERT(mdb_bson_find_array(doc, "nums", &out));
    ASSERT(out != NULL);
    int32_t v = 0;
    ASSERT(mdb_bson_find_int32(out, "0", &v));
    ASSERT_EQ_INT32(v, 10);

    mdb_bson_destroy(out);
    mdb_bson_destroy(arr);
    mdb_bson_destroy(doc);
}

static void test_bson_copy(void) {
    mdb_bson_t *doc = mdb_bson_new();
    mdb_bson_append_int32(doc, "x", 99);
    mdb_bson_append_utf8(doc, "s", "test", 4);

    mdb_bson_t *copy = mdb_bson_copy(doc);
    ASSERT(copy != NULL);
    int32_t v = 0;
    ASSERT(mdb_bson_find_int32(copy, "x", &v));
    ASSERT_EQ_INT32(v, 99);

    mdb_bson_destroy(copy);
    mdb_bson_destroy(doc);
}

static void test_bson_roundtrip(void) {
    mdb_bson_t *doc = mdb_bson_new();
    mdb_bson_append_int32(doc, "i32", -42);
    mdb_bson_append_int64(doc, "i64", 9876543210LL);
    mdb_bson_append_double(doc, "dbl", 2.71828);
    mdb_bson_append_utf8(doc, "str", "hello world", 11);
    mdb_bson_append_bool(doc, "b", true);
    mdb_bson_append_null(doc, "n");

    uint32_t len = 0;
    uint8_t *bytes = mdb_bson_to_bytes(doc, &len);
    ASSERT(bytes != NULL);
    ASSERT(len > 0);

    mdb_bson_t *parsed = mdb_bson_from_bytes(bytes, len);
    ASSERT(parsed != NULL);

    int32_t i32v = 0;
    ASSERT(mdb_bson_find_int32(parsed, "i32", &i32v));
    ASSERT_EQ_INT32(i32v, -42);

    int64_t i64v = 0;
    ASSERT(mdb_bson_find_int64(parsed, "i64", &i64v));
    ASSERT(i64v == 9876543210LL);

    double dblv = 0;
    ASSERT(mdb_bson_find_double(parsed, "dbl", &dblv));
    ASSERT(dblv > 2.71 && dblv < 2.72);

    const char *str = NULL;
    uint32_t slen = 0;
    ASSERT(mdb_bson_find_utf8(parsed, "str", &str, &slen));
    ASSERT_EQ_INT32((int32_t)slen, 11);
    ASSERT(memcmp(str, "hello world", 11) == 0);

    bool bv = false;
    ASSERT(mdb_bson_find_bool(parsed, "b", &bv));
    ASSERT(bv == true);

    ASSERT(mdb_bson_has_field(parsed, "n"));
    ASSERT(!mdb_bson_has_field(parsed, "missing"));

    free(bytes);
    mdb_bson_destroy(parsed);
    mdb_bson_destroy(doc);
}

static void test_bson_empty(void) {
    mdb_bson_t *doc = mdb_bson_new();
    ASSERT(doc != NULL);

    uint32_t len = 0;
    uint8_t *bytes = mdb_bson_to_bytes(doc, &len);
    ASSERT(bytes != NULL);
    ASSERT_EQ_INT32((int32_t)len, 5);
    ASSERT_EQ_INT32(bytes[0], 5);
    ASSERT_EQ_INT32(bytes[4], 0);

    mdb_bson_t *parsed = mdb_bson_from_bytes(bytes, len);
    ASSERT(parsed != NULL);
    uint32_t parsed_len = 0;
    uint8_t *parsed_bytes = mdb_bson_to_bytes(parsed, &parsed_len);
    ASSERT_EQ_INT32((int32_t)parsed_len, 5);
    free(parsed_bytes);

    free(bytes);
    mdb_bson_destroy(parsed);
    mdb_bson_destroy(doc);
}

static void test_opmsg_build_parse(void) {
    mdb_bson_t *body = mdb_bson_new();
    mdb_bson_append_int32(body, "ping", 1);

    mdb_opmsg_t *msg = mdb_opmsg_new(42);
    ASSERT(msg != NULL);
    ASSERT(mdb_opmsg_set_body(msg, body));

    mdb_bson_t *extra = mdb_bson_new();
    mdb_bson_append_utf8(extra, "db", "admin", 5);
    ASSERT(mdb_opmsg_add_document(msg, extra));

    uint32_t len = 0;
    uint8_t *bytes = mdb_opmsg_to_bytes(msg, &len);
    ASSERT(bytes != NULL);
    ASSERT(len > 24);

    ASSERT_EQ_INT32(bytes[0], (int32_t)len);
    ASSERT_EQ_INT32(bytes[4], 42);
    ASSERT_EQ_INT32(bytes[8], 0);
    ASSERT_EQ_INT32(*(int32_t*)(bytes + 12), MDB_OP_MSG);

    mdb_opmsg_t *parsed = mdb_opmsg_parse(bytes, len);
    ASSERT(parsed != NULL);
    ASSERT_EQ_INT32(mdb_opmsg_get_req_id(parsed), 42);
    ASSERT_EQ_INT32(mdb_opmsg_get_resp_to(parsed), 0);
    ASSERT_EQ_INT32(mdb_opmsg_get_op_code(parsed), MDB_OP_MSG);

    mdb_bson_t *parsed_body = mdb_opmsg_get_body(parsed);
    ASSERT(parsed_body != NULL);
    int32_t v = 0;
    ASSERT(mdb_bson_find_int32(parsed_body, "ping", &v));
    ASSERT_EQ_INT32(v, 1);

    size_t doc_count = 0;
    mdb_bson_t **docs = mdb_opmsg_get_documents(parsed, &doc_count);
    ASSERT(docs != NULL);
    ASSERT(doc_count == 1);

    mdb_bson_destroy(extra);
    mdb_bson_destroy(body);
    mdb_opmsg_destroy(msg);
    mdb_opmsg_destroy(parsed);
    free(bytes);
}

static void test_opmsg_body_only(void) {
    mdb_bson_t *body = mdb_bson_new();
    mdb_bson_append_int32(body, "isMaster", 1);

    mdb_opmsg_t *msg = mdb_opmsg_new(100);
    ASSERT(mdb_opmsg_set_body(msg, body));

    uint32_t len = 0;
    uint8_t *bytes = mdb_opmsg_to_bytes(msg, &len);
    ASSERT(bytes != NULL);

    mdb_opmsg_t *parsed = mdb_opmsg_parse(bytes, len);
    ASSERT(parsed != NULL);
    ASSERT_EQ_INT32(mdb_opmsg_get_req_id(parsed), 100);

    mdb_bson_t *parsed_body = mdb_opmsg_get_body(parsed);
    ASSERT(parsed_body != NULL);
    int32_t v = 0;
    ASSERT(mdb_bson_find_int32(parsed_body, "isMaster", &v));
    ASSERT_EQ_INT32(v, 1);

    size_t dc = 0;
    mdb_bson_t **docs = mdb_opmsg_get_documents(parsed, &dc);
    ASSERT(dc == 0);

    mdb_bson_destroy(body);
    mdb_opmsg_destroy(msg);
    mdb_opmsg_destroy(parsed);
    free(bytes);
}

static void test_crc32c(void) {
    extern uint32_t mdb_crc32c(const uint8_t *data, size_t len);

    const uint8_t data1[] = "123456789";
    uint32_t crc = mdb_crc32c(data1, 9);
    ASSERT(crc == 0xE3069283);

    const uint8_t data2[] = {0x00, 0x00, 0x00, 0x00};
    crc = mdb_crc32c(data2, 4);
    ASSERT(crc != 0);

    crc = mdb_crc32c(NULL, 0);
    ASSERT(crc == 0);
}

static void test_bson_many_fields(void) {
    mdb_bson_t *doc = mdb_bson_new();
    char name[16];
    for (int i = 0; i < 100; i++) {
        snprintf(name, sizeof(name), "field%d", i);
        mdb_bson_append_int32(doc, name, i);
    }

    uint32_t len = 0;
    uint8_t *bytes = mdb_bson_to_bytes(doc, &len);
    ASSERT(bytes != NULL);

    mdb_bson_t *parsed = mdb_bson_from_bytes(bytes, len);
    ASSERT(parsed != NULL);

    for (int i = 0; i < 100; i++) {
        snprintf(name, sizeof(name), "field%d", i);
        int32_t v = -1;
        ASSERT(mdb_bson_find_int32(parsed, name, &v));
        ASSERT_EQ_INT32(v, i);
    }

    free(bytes);
    mdb_bson_destroy(parsed);
    mdb_bson_destroy(doc);
}

static void test_wire_parse_invalid(void) {
    ASSERT(mdb_opmsg_parse(NULL, 0) == NULL);
    ASSERT(mdb_opmsg_parse((uint8_t[]){0}, 1) == NULL);

    uint8_t bad[16] = {0};
    bad[0] = 16; bad[1] = 0; bad[2] = 0; bad[3] = 0;
    bad[12] = 99; bad[13] = 0; bad[14] = 0; bad[15] = 0;
    ASSERT(mdb_opmsg_parse(bad, 16) == NULL);
}

int main(void) {
    printf("Running bson tests...\n");
    test_bson_int32();
    test_bson_int64();
    test_bson_double();
    test_bson_utf8();
    test_bson_bool();
    test_bson_null();
    test_bson_object_id();
    test_bson_datetime();
    test_bson_document();
    test_bson_array();
    test_bson_copy();
    test_bson_roundtrip();
    test_bson_empty();
    test_bson_many_fields();

    printf("Running wire tests...\n");
    test_opmsg_build_parse();
    test_opmsg_body_only();
    test_wire_parse_invalid();

    printf("Running crc32c tests...\n");
    test_crc32c();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
