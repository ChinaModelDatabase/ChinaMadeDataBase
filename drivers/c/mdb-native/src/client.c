#define _POSIX_C_SOURCE 200809L
#include "mdb_native/client.h"
#include "mdb_native/connection.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifndef _MSC_VER
#include <strings.h>
#endif

static char *portable_strndup(const char *s, size_t n) {
    char *p = malloc(n + 1);
    if (!p) return NULL;
    memcpy(p, s, n);
    p[n] = '\0';
    return p;
}

struct mdb_client {
    char *host;
    int port;
    mdb_connection_t *conn;
    bool connected;
};

struct mdb_database {
    mdb_client_t *client;
    char *name;
};

static void parse_uri(const char *uri, char **host, int *port) {
    *host = strdup("127.0.0.1");
    *port = 27017;

    if (!uri) return;

    const char *p = uri;
    if (strncmp(p, "mongodb://", 10) == 0) {
        p += 10;
    } else if (strncmp(p, "mongodb+srv://", 14) == 0) {
        p += 14;
    }

    const char *at = strchr(p, '@');
    if (at) p = at + 1;

    const char *slash = strchr(p, '/');
    const char *colon = strchr(p, ':');

    size_t host_len;
    if (colon && (!slash || colon < slash)) {
        host_len = (size_t)(colon - p);
        *port = atoi(colon + 1);
    } else if (slash) {
        host_len = (size_t)(slash - p);
    } else {
        host_len = strlen(p);
    }

    free(*host);
    *host = portable_strndup(p, host_len);
}

mdb_client_t *mdb_client_new(const char *uri) {
    mdb_client_t *client = calloc(1, sizeof(mdb_client_t));
    if (!client) return NULL;
    parse_uri(uri, &client->host, &client->port);
    client->conn = mdb_connection_new(client->host, client->port);
    if (!client->conn) { free(client->host); free(client); return NULL; }
    return client;
}

void mdb_client_destroy(mdb_client_t *client) {
    if (!client) return;
    mdb_client_close(client);
    mdb_connection_destroy(client->conn);
    free(client->host);
    free(client);
}

bool mdb_client_connect(mdb_client_t *client) {
    if (!client) return false;
    bool ok = mdb_connection_connect(client->conn);
    client->connected = ok;
    return ok;
}

void mdb_client_close(mdb_client_t *client) {
    if (!client) return;
    mdb_connection_close(client->conn);
    client->connected = false;
}

mdb_database_t *mdb_client_database(mdb_client_t *client, const char *name) {
    if (!client || !name) return NULL;
    mdb_database_t *db = calloc(1, sizeof(mdb_database_t));
    if (!db) return NULL;
    db->client = client;
    db->name = strdup(name);
    return db;
}

mdb_bson_t *mdb_client_list_databases(mdb_client_t *client) {
    if (!client || !client->connected) return NULL;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return NULL;
    mdb_bson_append_int32(cmd, "listDatabases", 1);
    mdb_bson_t *result = mdb_connection_send_command(client->conn, cmd);
    mdb_bson_destroy(cmd);
    return result;
}

bool mdb_client_ping(mdb_client_t *client) {
    if (!client || !client->connected) return false;
    mdb_bson_t *cmd = mdb_bson_new();
    if (!cmd) return false;
    mdb_bson_append_int32(cmd, "ping", 1);
    mdb_bson_t *result = mdb_connection_send_command(client->conn, cmd);
    mdb_bson_destroy(cmd);
    if (!result) return false;
    mdb_bson_destroy(result);
    return true;
}
