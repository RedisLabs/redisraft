/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "hiredis_redismodule.h"
#include "redisraft.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>

#define CONN_LOG(level, conn, fmt, ...) \
    LOG(level, "{conn:%lu} " fmt, conn ? conn->id : 0, ##__VA_ARGS__)

#define CONN_LOG_DEBUG(conn, fmt, ...)   CONN_LOG(LOG_LEVEL_DEBUG, conn, fmt, ##__VA_ARGS__)
#define CONN_LOG_VERBOSE(conn, fmt, ...) CONN_LOG(LOG_LEVEL_VERBOSE, conn, fmt, ##__VA_ARGS__)
#define CONN_LOG_NOTICE(conn, fmt, ...)  CONN_LOG(LOG_LEVEL_NOTICE, conn, fmt, ##__VA_ARGS__)
#define CONN_LOG_WARNING(conn, fmt, ...) CONN_LOG(LOG_LEVEL_WARNING, conn, fmt, ##__VA_ARGS__)

#define CONN_TRACE(node, fmt, ...) \
    TRACE_MODULE(CONN, "<conn {%lu}> " fmt, (conn) ? (conn)->id : 0, ##__VA_ARGS__)

static const char *ConnStateStr[] = {
    "disconnected",
    "resolving",
    "connecting",
    "connected",
    "connect_error",
};

/* Create a new connection.
 *
 * The new connection is created in an idle state, so if it has an idle
 * callback it should be called shortly after.
 */

Connection *ConnCreate(RedisRaftCtx *rr, void *privdata,
                       ConnectionCallbackFunc idle_cb, ConnectionFreeFunc free_cb,
                       char *username, char *password)
{
    static unsigned long id = 0;

    Connection *conn = RedisModule_Calloc(1, sizeof(Connection));

    conn->rr = rr;
    conn->privdata = privdata;
    conn->idle_callback = idle_cb;
    conn->free_callback = free_cb;
    conn->id = ++id;
    sc_list_init(&conn->entries);

    if (username) {
        conn->username = RedisModule_Strdup(username);
    }
    if (password) {
        conn->password = RedisModule_Strdup(password);
    }

    int timeout_millis = rr->config.connection_timeout;

    conn->timeout = (struct timeval){
        .tv_sec = timeout_millis / 1000,
        .tv_usec = (timeout_millis % 1000) * 1000,
    };

    sc_list_add_head(&rr->connections, &conn->entries);
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

    RedisModule_Free(conn->username);
    conn->username = NULL;

    RedisModule_Free(conn->password);
    conn->password = NULL;

    CONN_TRACE(conn, "Connection freed.");

    sc_list_del(&conn->rr->connections, &conn->entries);

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

static void connectionSuccess(Connection *conn)
{
    conn->state = CONN_CONNECTED;
    conn->connect_oks++;
    conn->last_connected_time = RedisModule_Milliseconds();

    /* If connection was flagged for termination between connection attempt
     * and now, we don't call the connect callback.
     */
    /* If we're terminating, abort now */
    if (conn->flags & CONN_TERMINATING) {
        return;
    }

    /* Call callback even when failed */
    if (conn->connect_callback) {
        conn->connect_callback(conn);
    }
}

static void connectionFailure(Connection *conn)
{
    conn->state = CONN_CONNECT_ERROR;
    conn->rc = NULL;
    conn->connect_errors++;

    /* If connection was flagged for termination between connection attempt
     * and now, we don't call the connect callback.
     */
    /* If we're terminating, abort now */
    if (conn->flags & CONN_TERMINATING) {
        return;
    }

    /* Call callback even when failed */
    if (conn->connect_callback) {
        conn->connect_callback(conn);
    }
}

static void handleAuth(redisAsyncContext *c, void *r, void *privdata)
{
    Connection *conn = privdata;

    redisReply *reply = r;
    if (!reply) {
        LOG_WARNING("Redis connection authentication failed: connection died");
        ConnMarkDisconnected(conn);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        LOG_WARNING("Redis connection authentication failed: %s", reply->str);
        redisAsyncDisconnect(ConnGetRedisCtx(conn));
        ConnMarkDisconnected(conn);
    } else if (reply->type != REDIS_REPLY_STATUS || strcmp("OK", reply->str) != 0) {
        LOG_WARNING("Redis connection authentication failed: unexpected response");
        redisAsyncDisconnect(ConnGetRedisCtx(conn));
        ConnMarkDisconnected(conn);
    } else {
        connectionSuccess(conn);
        return;
    }

    connectionFailure(conn);
}

/* Connection with authentication
 *
 * If the connection status is ok, will attempt to authenticate and only then call the connection callback
 * If connection status is not ok, will immediately transition connection to CONN_CONNECT_ERROR
 * User callback is called in all cases except when the connection has already been terminated externally
 */
static void handleConnectedWithAuth(Connection *conn, int status)
{
    if (status == REDIS_OK) {
        /* don't try to authenticate if terminating */
        if (conn->flags & CONN_TERMINATING) {
            return;
        }

        if (redisAsyncCommand(ConnGetRedisCtx(conn), handleAuth, conn,
                              "AUTH %s %s",
                              conn->username,
                              conn->password) != REDIS_OK) {
            redisAsyncDisconnect(ConnGetRedisCtx(conn));
            ConnMarkDisconnected(conn);
            goto fail;
        }

        return;
    }
    /* if status != REDIS_OK, that means the connection failed, go direct to fail */
fail:
    connectionFailure(conn);
}

/* Connection without authentication
 *
 * Depending on status, state transitions to CONN_CONNECTED or CONN_CONNECT_ERROR.
 * User callback is called in all cases except when the connection has already been terminated externally
 */
static void handleConnectedWithoutAuth(Connection *conn, int status)
{
    if (status == REDIS_OK) {
        connectionSuccess(conn);
    } else {
        connectionFailure(conn);
    }
}

/* Connect callback (hiredis).
 *
 * This callback will always be called as a result of a prior redisAsyncConnect()
 * from handleResolved(), with state CONN_CONNECTING.
 *
 */
static void handleConnected(const redisAsyncContext *c, int status)
{
    Connection *conn = c->data;
    CONN_TRACE(conn, "handleConnected: status=%d", status);

    if (conn->username && conn->password) {
        handleConnectedWithAuth(conn, status);
    } else {
        handleConnectedWithoutAuth(conn, status);
    }
}

/* Disconnect callback (hiredis).
 *
 */
static void handleDisconnected(const redisAsyncContext *c, int status)
{
    UNUSED(status);
    Connection *conn = (Connection *) c->data;

    CONN_TRACE(conn, "handleDisconnected: rc=%p", conn ? conn->rc : NULL);

    if (conn) {
        conn->state = CONN_DISCONNECTED;
        conn->rc = NULL; /* FIXME: Need this? */
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

    /* If flagged for terminated in the meanwhile, drop now. */
    if (conn->flags & CONN_TERMINATING) {
        conn->state = CONN_DISCONNECTED;
        if (res->addr) {
            freeaddrinfo(res->addr);
        }
        return;
    }

    if (res->rc != 0) {
        CONN_LOG_WARNING(conn, "Failed to resolve '%s': %s", conn->addr.host,
                         gai_strerror(res->rc));
        goto fail;
    }

    void *addr = &((struct sockaddr_in *) res->addr->ai_addr)->sin_addr;
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
    if (conn->rr->config.tls_enabled) {
        SSL *ssl = SSL_new(conn->rr->config.ssl);
        if (!ssl) {
            unsigned long e = ERR_peek_last_error();
            CONN_LOG_WARNING(conn, "Couldn't create SSL object: %s", ERR_reason_error_string(e));
            goto fail;
        }
        int result = redisInitiateSSL(&conn->rc->c, ssl);
        if (result != REDIS_OK) {
            CONN_LOG_WARNING(conn, "redisInitiateSSL error(%d): %s", conn->rc->c.err, conn->rc->c.errstr);
            SSL_free(ssl);
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
        .ai_flags = 0,
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

    RedisModule_Assert(ConnIsIdle(conn));

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
}

/* An idle state is one that will not transition automatically to another
 * state, unless actively mutated.
 */
bool ConnIsIdle(Connection *conn)
{
    /* If the connection is in the "resolving" state, it means the hostname
     * resolution is in progress in another thread. Even though it was marked as
     * "terminating", the connection is not idle yet. */
    return conn->state == CONN_DISCONNECTED ||
           conn->state == CONN_CONNECT_ERROR ||
           (conn->state != CONN_RESOLVING && (conn->flags & CONN_TERMINATING));
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

    struct sc_list *tmp, *it;

    sc_list_foreach_safe (&rr->connections, tmp, it) {
        /* Idle connections are either terminating and should be reaped,
         * or waiting for an idle callback which can re-connect them.
         */
        Connection *conn = sc_list_entry(it, Connection, entries);
        if (ConnIsIdle(conn)) {
            if (conn->flags & CONN_TERMINATING) {
                sc_list_del(&rr->connections, &conn->entries);
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
