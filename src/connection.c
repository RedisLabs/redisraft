
/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include <time.h>

#include "redisraft.h"
#include "hiredis_redismodule.h"

#include <assert.h>
#include <netdb.h>
#include <arpa/inet.h>

#define CONN_LOG(level, conn, fmt, ...) \
    LOG(level, "{conn:%lu} " fmt, conn ? conn->id : 0, ##__VA_ARGS__)

#define CONN_LOG_ERROR(conn, fmt, ...) CONN_LOG(LOGLEVEL_ERROR, conn, fmt, ##__VA_ARGS__)
#define CONN_LOG_INFO(conn, fmt, ...) CONN_LOG(LOGLEVEL_INFO, conn, fmt, ##__VA_ARGS__)
#define CONN_LOG_VERBOSE(conn, fmt, ...) CONN_LOG(LOGLEVEL_VERBOSE, conn, fmt, ##__VA_ARGS__)
#define CONN_LOG_DEBUG(conn, fmt, ...) CONN_LOG(LOGLEVEL_DEBUG, conn, fmt, ##__VA_ARGS__)

#ifdef ENABLE_TRACE
#define CONN_TRACE(conn, fmt, ...) \
    LOG(LOGLEVEL_DEBUG, "%s:%d {conn:%lu} " fmt, \
            __FILE__, __LINE__, \
            (conn) ? (conn)->id : 0, \
            ##__VA_ARGS__)
#else
#define CONN_TRACE(conn, fmt, ...) do {} while (0)
#endif

static const char *ConnStateStr[] = {
    "disconnected",
    "resolving",
    "connecting",
    "connected",
    "connect_error"
};

/* A list of all connections */
static LIST_HEAD(conn_list, Connection) conn_list = LIST_HEAD_INITIALIZER(conn_list);

/* Create a new connection.
 *
 * The new connection is created in an idle state, so if it has an idle
 * callback it should be called shortly after.
 */

Connection *ConnCreate(RedisRaftCtx *rr, void *privdata, ConnectionCallbackFunc idle_cb, ConnectionFreeFunc free_cb)
{
    static unsigned long id = 0;

    Connection *conn = RedisModule_Calloc(1, sizeof(Connection));

    LIST_INSERT_HEAD(&conn_list, conn, entries);
    conn->rr = rr;
    conn->privdata = privdata;
    conn->idle_callback = idle_cb;
    conn->free_callback = free_cb;
    conn->id = ++id;

    conn->timeout.tv_usec = rr->config->connection_timeout;

    CONN_TRACE(conn, "Connection created.");

    return conn;
}

/* Free a connection object.
 */
static void ConnFree(Connection *conn)
{
    if (!conn) {
        return;
    }

    if (conn->free_callback) {
        conn->free_callback(conn->privdata);
    }

    CONN_TRACE(conn, "Connection freed.");

    LIST_REMOVE(conn, entries);

    RedisModule_Free(conn);
}

/* A callback we register on hiredis to make sure ConnFree gets called when
 * the async context is freed. FIXME: Do we need it?
 */
static void connDataCleanupCallback(void *privdata)
{
    Connection *conn = (Connection *) privdata;

    CONN_TRACE(conn, "connDataCleanupCallback: flags=%d, rc=%p",
         conn ? conn->flags : 0,
         conn ? conn->rc : NULL);

    /* If we got called hiredis is tearing down the context, make sure
     * we drop the reference to it.
     */
    conn->rc = NULL;
#ifdef HAVE_TLS
    if (conn->ssl) {
        redisFreeSSLContext(conn->ssl);
        conn->ssl = NULL;
    }
#endif

    /* If connection was not flagged for async termination, don't clean it up. It
     * may get reused or cleaned up at a later stage.
     */
    if (!(conn->flags & CONN_TERMINATING)) {
        return;
    }

    ConnFree(conn);
}

/* Request async termination of a connection. The connection will be closed and
 * in the next iteration.
 */

void ConnAsyncTerminate(Connection *conn)
{
    CONN_TRACE(conn, "ConnAsyncTerminate called.");

    conn->flags |= CONN_TERMINATING;
}

/* Connect callback (hiredis).
 *
 * This callback will always be called as a result of a prior redisAsyncConnect()
 * from handleResolved(), with state CONN_CONNECTING.
 *
 * Depending on status, state transitions to CONN_CONNECTED or CONN_CONNECT_ERROR.
 * User callback is called in both cases.
 */

static void handleConnected(const redisAsyncContext *c, int status)
{
    Connection *conn = (Connection *) c->data;

    CONN_TRACE(conn, "handleConnected: status=%d", status);

    if (status == REDIS_OK) {
        conn->state = CONN_CONNECTED;
        conn->connect_oks++;
        conn->last_connected_time = RedisModule_Milliseconds();
    } else {
        conn->state = CONN_CONNECT_ERROR;
        conn->rc = NULL;
#ifdef HAVE_TLS
        if (conn->ssl) {
            redisFreeSSLContext(conn->ssl);
            conn->ssl = NULL;
        };
#endif
        conn->connect_errors++;
    }

    /* If connection was flagged for termination between connection attempt
     * and now, we don't call the connect callback.
     */
    /* If we're terminating, abort now */
    if (conn->flags & CONN_TERMINATING) {
        return;
    }

    /* Call explicit connect callback (even if failed) */
    if (conn->connect_callback) {
        conn->connect_callback(conn);
    }
}

/* Disconnect callback (hiredis).
 *
 */
static void handleDisconnected(const redisAsyncContext *c, int status)
{
    UNUSED(status);
    Connection *conn = (Connection *) c->data;

    CONN_TRACE(conn, "handleDisconnected: rc=%p",
        conn ? conn->rc : NULL);

    if (conn) {
        conn->state = CONN_DISCONNECTED;
        conn->rc = NULL;    /* FIXME: Need this? */
#ifdef HAVE_TLS
        if (conn->ssl) {
            redisFreeSSLContext(conn->ssl);
            conn->ssl = NULL;   /* same as above comment */
        }
#endif
    }
}

/* Callback for ConnGetAddrinfo(). This will be called from Redis thread. */
static void handleResolved(void *arg)
{
    Connection *conn = arg;

    CONN_TRACE(conn, "handleResolved: flags=%d, state=%s, rc=%p",
        conn->flags,
        ConnStateStr[conn->state],
        conn->rc);

    struct AddrinfoResult *res = &conn->addrinfo_result;

    /* If flagged for terminated in the meanwhile, drop now.
     */
    if (conn->flags & CONN_TERMINATING) {
        conn->state = CONN_DISCONNECTED;
        if (res->addr) {
            freeaddrinfo(res->addr);
        }
        return;
    }

    if (res->rc != 0) {
        CONN_LOG_ERROR(conn, "Failed to resolve '%s': %s", conn->addr.host,
                       gai_strerror(res->rc));
        goto fail;
    }

    void* addr = &((struct sockaddr_in *)res->addr->ai_addr)->sin_addr;
    inet_ntop(AF_INET, addr, conn->ipaddr, INET_ADDRSTRLEN);
    freeaddrinfo(res->addr);

    /* Initiate connection */
    if (conn->rc != NULL) {
        redisAsyncFree(conn->rc);
    }

    /* copy default options setup from redisAsyncConnect with support for timeout */
    redisOptions options = {0};
    options.connect_timeout = &conn->timeout;

    REDIS_OPTIONS_SET_TCP(&options, conn->ipaddr, conn->addr.port);

    conn->rc = redisAsyncConnectWithOptions(&options);
    if (conn->rc->err) {
        goto fail;
    }

#ifdef HAVE_TLS
    if (conn->rr->config->tls_enabled) {
        redisSSLContextError ssl_error;
        conn->ssl = redisCreateSSLContext(conn->rr->config->tls_ca_cert,
                                          NULL,
                                          conn->rr->config->tls_cert,
                                          conn->rr->config->tls_key,
                                          NULL,
                                          &ssl_error);
        if (!conn->ssl) {
            CONN_LOG_ERROR(conn, "Error: %s", redisSSLContextGetError(ssl_error));
            goto fail;
        }

        if (redisInitiateSSLWithContext(&conn->rc->c, conn->ssl) != REDIS_OK) {
            CONN_LOG_ERROR(conn, "SSL Error!");
            goto fail;
        }
    }
#endif

    conn->rc->data = conn;
    conn->rc->dataCleanup = connDataCleanupCallback;
    conn->state = CONN_CONNECTING;
    conn->flags &= ~CONN_TERMINATING;

    redisModuleAttach(conn->rc);
    redisAsyncSetConnectCallback(conn->rc, handleConnected);
    redisAsyncSetDisconnectCallback(conn->rc, handleDisconnected);

    return;

fail:
    conn->state = CONN_CONNECT_ERROR;
    conn->connect_errors++;
    if (conn->rc) {
        redisAsyncFree(conn->rc);
        conn->rc = NULL;
    }
}

/* getaddrinfo() is quite slow, especially when it fails to resolve the address.
 * This function will be called in the threadpool not to stop Redis main thread.
 */
void ConnGetAddrinfo(void *arg)
{
    Connection *conn = arg;

    struct addrinfo hints = {
        .ai_family = PF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP,
        .ai_flags = 0
    };

    struct addrinfo *addr = NULL;
    int rc = getaddrinfo(conn->addr.host, NULL, &hints, &addr);

    conn->addrinfo_result.rc = rc;
    conn->addrinfo_result.addr = addr;

    RedisModule_EventLoopAddOneShot(handleResolved, conn);
}

RRStatus ConnConnect(Connection *conn, const NodeAddr *addr, ConnectionCallbackFunc connect_callback)
{
    CONN_TRACE(conn, "ConnConnect: connecting %s:%u.", addr->host, addr->port);

    assert(ConnIsIdle(conn));

    conn->addr = *addr;
    conn->state = CONN_RESOLVING;
    conn->connect_callback = connect_callback;

    /* Call slow getaddrinfo() in another thread asynchronously */
    threadPoolAdd(&redis_raft.thread_pool, conn, ConnGetAddrinfo);

    return RR_OK;
}

void ConnMarkDisconnected(Connection *conn)
{
    CONN_TRACE(conn, "ConnMarkDisconnected: rc=%p", conn->rc);

    conn->state = CONN_DISCONNECTED;
    if (conn->rc) {
        redisAsyncFree(conn->rc);
        conn->rc = NULL;
    }
#ifdef HAVE_TLS
    if (conn->ssl) {
        redisFreeSSLContext(conn->ssl);
        conn->ssl = NULL;
    }
#endif
}

/* An idle state is one that will not transition automatically to another
 * state, unless actively mutated.
 */

bool ConnIsIdle(Connection *conn)
{
    return (conn->state == CONN_DISCONNECTED || conn->state == CONN_CONNECT_ERROR);
}

bool ConnIsConnected(Connection *conn)
{
    return (conn->state == CONN_CONNECTED && !(conn->flags & CONN_TERMINATING));
}

const char *ConnGetStateStr(Connection *conn)
{
    return ConnStateStr[conn->state];
}

void *ConnGetPrivateData(Connection *conn)
{
    return conn->privdata;
}

RedisRaftCtx *ConnGetRedisRaftCtx(Connection *conn)
{
    return conn->rr;
}

redisAsyncContext *ConnGetRedisCtx(Connection *conn)
{
    return conn->rc;
}

void HandleIdleConnections(RedisRaftCtx *rr)
{
    if (rr->state == REDIS_RAFT_LOADING)
        return;

    Connection *conn, *tmp;
    LIST_FOREACH_SAFE(conn, &conn_list, entries, tmp) {
        /* Idle connections are either terminating and should be reaped,
         * or waiting for an idle callback which can re-connect them.
         */
        if (ConnIsIdle(conn)) {
            if (conn->flags & CONN_TERMINATING) {
                LIST_REMOVE(conn, entries);
                if (conn->rc) {
                    /* Note: redisAsyncFree will call the connDataCleanupCallback
                     * callback which will free the Conn structure!
                     */
                    redisAsyncContext *ac = conn->rc;
                    conn->rc = NULL;
                    redisAsyncFree(ac);
                } else {
                    ConnFree(conn);
                }
            } else {
                if (conn->idle_callback) {
                    conn->idle_callback(conn);
                }
            }
        }
    }
}

