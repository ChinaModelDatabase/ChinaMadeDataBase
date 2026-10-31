#include "mdb_native/connection.h"
#include "mdb_native/wire.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifdef MDB_PLATFORM_WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
typedef int socklen_t;
#define CLOSE_SOCKET closesocket
#ifndef ssize_t
#define ssize_t int
#endif
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <errno.h>
#define CLOSE_SOCKET close
#endif

struct mdb_connection {
    char *host;
    int port;
    int sockfd;
    bool connected;
    int32_t next_req_id;
};

mdb_connection_t *mdb_connection_new(const char *host, int port) {
    mdb_connection_t *conn = calloc(1, sizeof(mdb_connection_t));
    if (!conn) return NULL;
    conn->host = strdup(host ? host : "127.0.0.1");
    conn->port = port > 0 ? port : 27017;
    conn->sockfd = -1;
    conn->connected = false;
    conn->next_req_id = 1;
    return conn;
}

void mdb_connection_destroy(mdb_connection_t *conn) {
    if (!conn) return;
    mdb_connection_close(conn);
    free(conn->host);
    free(conn);
}

bool mdb_connection_connect(mdb_connection_t *conn) {
    if (!conn || conn->connected) return true;

#ifdef MDB_PLATFORM_WIN32
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) return false;
#endif

    struct addrinfo hints, *result, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", conn->port);

    if (getaddrinfo(conn->host, port_str, &hints, &result) != 0) return false;

    for (rp = result; rp; rp = rp->ai_next) {
        conn->sockfd = (int)socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (conn->sockfd < 0) continue;
        if (connect(conn->sockfd, rp->ai_addr, (socklen_t)rp->ai_addrlen) == 0) {
            break;
        }
        CLOSE_SOCKET(conn->sockfd);
        conn->sockfd = -1;
    }
    freeaddrinfo(result);

    if (conn->sockfd < 0) return false;

    int flag = 1;
    setsockopt(conn->sockfd, IPPROTO_TCP, TCP_NODELAY, (const char *)&flag, sizeof(flag));

    conn->connected = true;
    return true;
}

void mdb_connection_close(mdb_connection_t *conn) {
    if (!conn) return;
    if (conn->sockfd >= 0) {
        CLOSE_SOCKET(conn->sockfd);
        conn->sockfd = -1;
    }
    conn->connected = false;
#ifdef MDB_PLATFORM_WIN32
    WSACleanup();
#endif
}

bool mdb_connection_is_connected(const mdb_connection_t *conn) {
    return conn && conn->connected;
}

static bool send_all(mdb_connection_t *conn, const uint8_t *data, uint32_t len) {
    uint32_t sent = 0;
    while (sent < len) {
        ssize_t n = send(conn->sockfd, (const char *)(data + sent), len - sent, 0);
        if (n <= 0) return false;
        sent += (uint32_t)n;
    }
    return true;
}

static bool recv_all(mdb_connection_t *conn, uint8_t *data, uint32_t len) {
    uint32_t received = 0;
    while (received < len) {
        ssize_t n = recv(conn->sockfd, (char *)(data + received), len - received, 0);
        if (n <= 0) return false;
        received += (uint32_t)n;
    }
    return true;
}

static int32_t read_i32(const uint8_t *p) {
    return (int32_t)((uint32_t)p[0] | ((uint32_t)p[1] << 8) |
                     ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24));
}

mdb_bson_t *mdb_connection_send_command(mdb_connection_t *conn, const mdb_bson_t *cmd_doc) {
    return mdb_connection_send_command_with_docs(conn, cmd_doc, NULL, 0);
}

mdb_bson_t *mdb_connection_send_command_with_docs(mdb_connection_t *conn, const mdb_bson_t *cmd_doc,
                                                   const mdb_bson_t **extra_docs, size_t extra_count) {
    if (!conn || !conn->connected || !cmd_doc) return NULL;

    mdb_opmsg_t *req = mdb_opmsg_new(conn->next_req_id++);
    if (!req) return NULL;

    mdb_opmsg_set_body(req, cmd_doc);
    if (extra_docs) {
        for (size_t i = 0; i < extra_count; i++) {
            if (extra_docs[i]) {
                mdb_opmsg_add_document(req, extra_docs[i]);
            }
        }
    }

    uint32_t req_len = 0;
    uint8_t *req_bytes = mdb_opmsg_to_bytes(req, &req_len);
    mdb_opmsg_destroy(req);
    if (!req_bytes) return NULL;

    if (!send_all(conn, req_bytes, req_len)) {
        free(req_bytes);
        return NULL;
    }
    free(req_bytes);

    uint8_t header[16];
    if (!recv_all(conn, header, 16)) return NULL;

    int32_t resp_len = read_i32(header);
    if (resp_len < 16 || resp_len > 48 * 1024 * 1024) return NULL;

    uint8_t *resp_buf = malloc((uint32_t)resp_len);
    if (!resp_buf) return NULL;

    memcpy(resp_buf, header, 16);
    if (!recv_all(conn, resp_buf + 16, (uint32_t)resp_len - 16)) {
        free(resp_buf);
        return NULL;
    }

    mdb_opmsg_t *resp = mdb_opmsg_parse(resp_buf, (uint32_t)resp_len);
    free(resp_buf);
    if (!resp) return NULL;

    mdb_bson_t *body = mdb_opmsg_get_body(resp);

    uint32_t body_len = 0;
    uint8_t *body_bytes = mdb_bson_to_bytes(body, &body_len);
    mdb_bson_t *result = body_bytes ? mdb_bson_from_bytes(body_bytes, body_len) : NULL;
    free(body_bytes);

    mdb_opmsg_destroy(resp);
    return result;
}
