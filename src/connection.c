/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include "connection.h"

#include "hiredis_redismodule.h"
#include "threadpool.h"

#include <arpa/inet.h>
#include <assert.h>
#include <netdb.h>
#include <string.h>

static const char *ConnStateStr[] = {
    "disconnected",
    "resolving",
    "connecting",
    "connected",
    "connect_error",
};

/* A list of all connections */
static LIST_HEAD(conn_list, Connection) conn_list = LIST_HEAD_INITIALIZER(conn_list);

/* Create a new connection.
 *
 * The new connection is created in an idle state, so if it has an idle
 * callback it should be called shortly after.
 */

Connection *ConnCreate(RedisRaftCtx *rr, void *privdata,
                       ConnectionCallbackFunc idle_cb,
                       ConnectionFreeFunc free_cb)
{
    Connection *conn = RedisModule_Calloc(1, sizeof(*conn));

    LIST_INSERT_HEAD(&conn_list, conn, entries);
    conn->rr = rr;
    conn->privdata = privdata;
    conn->idle_callback = idle_cb;
    conn->free_callback = free_cb;

    int timeout_millis = rr->config.connection_timeout;

    conn->timeout = (struct timeval){
        .tv_sec = timeout_millis / 1000,
        .tv_usec = (timeout_millis % 1000) * 1000,
    };

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

    LIST_REMOVE(conn, entries);

    RedisModule_Free(conn);
}

/* A callback we register on hiredis to make sure ConnFree gets called when
 * the async context is freed. FIXME: Do we need it?
 */
static void connDataCleanupCallback(void *privdata)
{
    Connection *conn = privdata;

    /* If we got called hiredis is tearing down the context, make sure
     * we drop the reference to it.
     */
    conn->rc = NULL;

    /* If connection was not flagged for async termination, don't clean it up.
     * It may get reused or cleaned up at a later stage.
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
    conn->flags |= CONN_TERMINATING;
}

static void connectionSuccess(Connection *conn)
{
    conn->state = CONN_CONNECTED;
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

/* Connect callback (hiredis).
 *
 * This callback will always be called as a result of a prior redisAsyncConnect()
 * from handleResolved(), with state CONN_CONNECTING.
 *
 */
static void handleConnected(const redisAsyncContext *c, int status)
{
    Connection *conn = c->data;

    if (status == REDIS_OK) {
        connectionSuccess(conn);
    } else {
        connectionFailure(conn);
    }
}

/* Disconnect callback (hiredis).
 *
 */
static void handleDisconnected(const redisAsyncContext *c, int status)
{
    Connection *conn = c->data;

    if (conn) {
        conn->state = CONN_DISCONNECTED;
        conn->rc = NULL; /* FIXME: Need this? */
    }
}

/* Callback for ConnGetAddrinfo(). This will be called from Redis thread. */
static void handleResolved(void *arg)
{
    Connection *conn = arg;

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
        LOG_WARNING("Failed to resolve '%s': %s", conn->addr.host,
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

/* An idle state is one that will not transition automatically to another
 * state, unless actively mutated.
 */
bool ConnIsIdle(Connection *conn)
{
    return (conn->state == CONN_DISCONNECTED || conn->state == CONN_CONNECT_ERROR
            || (conn->flags & CONN_TERMINATING));
}

int ConnConnect(Connection *conn,
                const NodeAddr *addr,
                ConnectionCallbackFunc connect_callback)
{
    assert(ConnIsIdle(conn));

    conn->addr = *addr;
    conn->state = CONN_RESOLVING;
    conn->connect_callback = connect_callback;

    /* Call slow getaddrinfo() in another thread asynchronously */
    ThreadPoolAdd(&redis_raft.thread_pool, conn, ConnGetAddrinfo);

    return RR_OK;
}

void ConnMarkDisconnected(Connection *conn)
{
    conn->state = CONN_DISCONNECTED;

    if (conn->rc) {
        redisAsyncFree(conn->rc);
        conn->rc = NULL;
    }
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

void ConnectionHandleIdles(RedisRaftCtx *rr)
{
    Connection *conn, *tmp;

    LIST_FOREACH_SAFE (conn, &conn_list, entries, tmp) {
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
