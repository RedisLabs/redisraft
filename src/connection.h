#ifndef REDISRAFT_CONNECTION_H
#define REDISRAFT_CONNECTION_H

#include "async.h"
#include "node_addr.h"
#include "queue.h"
#include "redisraft.h"

#include <netinet/in.h>

struct Connection;

typedef enum ConnState {
    CONN_DISCONNECTED,
    CONN_RESOLVING,
    CONN_CONNECTING,
    CONN_CONNECTED,
    CONN_CONNECT_ERROR
} ConnState;

/* Connection flags for Connection.flags */
#define CONN_TERMINATING (1 << 0)

/* A connection represents a single outgoing Redis connection, such as the
 * one used to communicate with another node.
 *
 * Essentially it is a wrapper around a hiredis asyncRedisContext, providing
 * additional capabilities such as handling asynchronous DNS resolution,
 * dropped connections and re-connects, etc.
 */

typedef void (*ConnectionCallbackFunc)(struct Connection *conn);
typedef void (*ConnectionFreeFunc)(void *privdata);

typedef struct Connection {
    ConnState state;                   /* Connected, disconnected etc. */
    unsigned int flags;                /* Additional flags about connection state */
    NodeAddr addr;                     /* Address of last ConnConnect() */
    char ipaddr[INET6_ADDRSTRLEN + 1]; /* Resolved IP address */
    redisAsyncContext *rc;             /* hiredis async context */
    struct RedisRaftCtx *rr;           /* Pointer back to redis_raft */
    long long last_connected_time;     /* Last connection time */
    struct timeval timeout;            /* Timeout to use if not null */
    void *privdata;                    /* User provided pointer */

    struct AddrinfoResult {
        int rc;
        struct addrinfo *addr;
    } addrinfo_result;

    /* Connect callback is guaranteed after ConnConnect(); Callback should check
     * connection state as it will also be called on error.
     */
    ConnectionCallbackFunc connect_callback;

    /* Idle callback is called periodically for every connection that is in idle
     * state, i.e. CONN_DISCONNECTED or CONN_CONNECT_ERROR.
     *
     * Typically it is used to either (re-)establish the connection using ConnConnect()
     * or destroy the connection.
     */
    ConnectionCallbackFunc idle_callback;

    /* Free callback is called when the connection gets freed, and as a last chance
     * to free privdata.
     */
    ConnectionFreeFunc free_callback;

    /* Linkage to global connections list */
    LIST_ENTRY(Connection) entries;
} Connection;

Connection *ConnCreate(struct RedisRaftCtx *rr, void *privdata, ConnectionCallbackFunc idle_cb, ConnectionFreeFunc free_cb);
int ConnConnect(Connection *conn, const NodeAddr *addr, ConnectionCallbackFunc connect_callback);
void ConnAsyncTerminate(Connection *conn);
void ConnMarkDisconnected(Connection *conn);
void ConnectionHandleIdles(struct RedisRaftCtx *rr);
void *ConnGetPrivateData(Connection *conn);
struct RedisRaftCtx *ConnGetRedisRaftCtx(Connection *conn);
redisAsyncContext *ConnGetRedisCtx(Connection *conn);
bool ConnIsConnected(Connection *conn);
const char *ConnGetStateStr(Connection *conn);

#endif
