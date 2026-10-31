#include "mdb_native/bson.h"
#include "mdb_native/wire.h"
#include "mdb_native/connection.h"
#include "mdb_native/client.h"
#include "mdb_native/cursor.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static void print_bytes(const uint8_t *data, uint32_t len) {
    for (uint32_t i = 0; i < len; i++) {
        printf("%02x ", data[i]);
        if ((i + 1) % 16 == 0) printf("\n");
    }
    if (len % 16 != 0) printf("\n");
}

static void demo_bson(void) {
    printf("=== BSON Demo ===\n\n");

    mdb_bson_t *doc = mdb_bson_new();
    mdb_bson_append_int32(doc, "age", 30);
    mdb_bson_append_int64(doc, "big", 123456789012345LL);
    mdb_bson_append_double(doc, "score", 99.5);
    mdb_bson_append_utf8(doc, "name", "Alice", 5);
    mdb_bson_append_bool(doc, "active", true);
    mdb_bson_append_null(doc, "deleted");

    mdb_object_id_t oid;
    mdb_object_id_generate(&oid);
    mdb_bson_append_object_id(doc, "_id", &oid);

    mdb_bson_append_datetime(doc, "created", 1609459200000LL);

    mdb_bson_t *address = mdb_bson_new();
    mdb_bson_append_utf8(address, "city", "Beijing", 7);
    mdb_bson_append_int32(address, "zip", 100000);
    mdb_bson_append_document(doc, "address", address);

    uint32_t len = 0;
    uint8_t *bytes = mdb_bson_to_bytes(doc, &len);
    printf("BSON document (%u bytes):\n", len);
    print_bytes(bytes, len);

    printf("\nParsed fields:\n");
    int32_t age;
    mdb_bson_find_int32(doc, "age", &age);
    printf("  age = %d\n", age);

    int64_t big;
    mdb_bson_find_int64(doc, "big", &big);
    printf("  big = %lld\n", (long long)big);

    double score;
    mdb_bson_find_double(doc, "score", &score);
    printf("  score = %.1f\n", score);

    const char *name;
    uint32_t name_len;
    mdb_bson_find_utf8(doc, "name", &name, &name_len);
    printf("  name = %.*s\n", name_len, name);

    bool active;
    mdb_bson_find_bool(doc, "active", &active);
    printf("  active = %s\n", active ? "true" : "false");

    char oid_str[25];
    mdb_object_id_to_string(&oid, oid_str, sizeof(oid_str));
    printf("  _id = %s\n", oid_str);

    mdb_bson_t *addr_out = NULL;
    if (mdb_bson_find_document(doc, "address", &addr_out)) {
        const char *city;
        uint32_t city_len;
        mdb_bson_find_utf8(addr_out, "city", &city, &city_len);
        printf("  address.city = %.*s\n", city_len, city);
        mdb_bson_destroy(addr_out);
    }

    free(bytes);
    mdb_bson_destroy(address);
    mdb_bson_destroy(doc);
    printf("\n");
}

static void demo_wire(void) {
    printf("=== Wire Protocol Demo ===\n\n");

    mdb_bson_t *body = mdb_bson_new();
    mdb_bson_append_int32(body, "isMaster", 1);

    mdb_opmsg_t *msg = mdb_opmsg_new(1);
    mdb_opmsg_set_body(msg, body);

    mdb_bson_t *extra = mdb_bson_new();
    mdb_bson_append_utf8(extra, "db", "admin", 5);
    mdb_opmsg_add_document(msg, extra);

    uint32_t len = 0;
    uint8_t *bytes = mdb_opmsg_to_bytes(msg, &len);
    printf("OP_MSG wire message (%u bytes):\n", len);
    printf("  Header:\n");
    printf("    msgLen  = %d\n", *(int32_t*)(bytes + 0));
    printf("    reqId   = %d\n", *(int32_t*)(bytes + 4));
    printf("    respTo  = %d\n", *(int32_t*)(bytes + 8));
    printf("    opCode  = %d (OP_MSG=%d)\n", *(int32_t*)(bytes + 12), MDB_OP_MSG);
    printf("  Flag bits: %d\n", *(int32_t*)(bytes + 16));

    mdb_opmsg_t *parsed = mdb_opmsg_parse(bytes, len);
    if (parsed) {
        printf("\nParsed response:\n");
        printf("  reqId  = %d\n", mdb_opmsg_get_req_id(parsed));
        printf("  respTo = %d\n", mdb_opmsg_get_resp_to(parsed));
        printf("  opCode = %d\n", mdb_opmsg_get_op_code(parsed));

        mdb_bson_t *parsed_body = mdb_opmsg_get_body(parsed);
        int32_t v = 0;
        if (parsed_body && mdb_bson_find_int32(parsed_body, "isMaster", &v)) {
            printf("  body.isMaster = %d\n", v);
        }

        size_t dc = 0;
        mdb_opmsg_get_documents(parsed, &dc);
        printf("  extra documents: %zu\n", dc);
    }

    mdb_bson_destroy(extra);
    mdb_bson_destroy(body);
    mdb_opmsg_destroy(msg);
    mdb_opmsg_destroy(parsed);
    free(bytes);
    printf("\n");
}

static void demo_connection(const char *host, int port) {
    printf("=== Connection Demo ===\n\n");

    mdb_connection_t *conn = mdb_connection_new(host, port);
    printf("Connecting to %s:%d...\n", host, port);

    if (!mdb_connection_connect(conn)) {
        printf("Connection failed (server may not be running)\n\n");
        mdb_connection_destroy(conn);
        return;
    }
    printf("Connected!\n");

    mdb_bson_t *cmd = mdb_bson_new();
    mdb_bson_append_int32(cmd, "ping", 1);
    mdb_bson_t *result = mdb_connection_send_command(conn, cmd);
    mdb_bson_destroy(cmd);

    if (result) {
        printf("ping response OK\n");
        mdb_bson_destroy(result);
    } else {
        printf("ping failed\n");
    }

    mdb_connection_destroy(conn);
    printf("\n");
}

static void demo_client(const char *uri) {
    printf("=== Client Demo ===\n\n");

    mdb_client_t *client = mdb_client_new(uri);
    if (!mdb_client_connect(client)) {
        printf("Client connection failed (server may not be running)\n\n");
        mdb_client_destroy(client);
        return;
    }
    printf("Connected via client\n");

    if (mdb_client_ping(client)) {
        printf("ping: OK\n");
    }

    mdb_bson_t *dbs = mdb_client_list_databases(client);
    if (dbs) {
        printf("listDatabases: OK\n");
        mdb_bson_destroy(dbs);
    }

    mdb_client_destroy(client);
    printf("\n");
}

int main(int argc, char *argv[]) {
    const char *host = "127.0.0.1";
    int port = 27017;
    const char *uri = "mongodb://127.0.0.1:27017";

    if (argc > 1) host = argv[1];
    if (argc > 2) port = atoi(argv[2]);
    if (argc > 1) {
        char buf[256];
        snprintf(buf, sizeof(buf), "mongodb://%s:%d", host, port);
        uri = buf;
    }

    demo_bson();
    demo_wire();
    demo_connection(host, port);
    demo_client(uri);

    printf("All demos completed.\n");
    return 0;
}
