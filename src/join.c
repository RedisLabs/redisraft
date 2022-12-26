/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

/* This is the implementation of RAFT.CLUSTER JOIN.
 *
 * It involves creating a Connection object linked to a JoinState, which is
 * populated with one or more NodeAddr.
 *
 * We then iterate the address list, establish a connection and attempt to
 * perform a RAFT.NODE ADD operation. If -MOVED replies are received they
 * are processed by adding the new node address to our list.
 */

#include "redisraft.h"

#include <string.h>

/* Callback for the RAFT.NODE ADD command.
 */
static void handleNodeAddResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Connection *conn = privdata;
    JoinLinkState *state = ConnGetPrivateData(conn);
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);

    redisReply *reply = r;

    if (!reply) {
        LOG_WARNING("RAFT.NODE ADD failed: connection dropped.");
        ConnMarkDisconnected(conn);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        /* -MOVED? */
        if (strlen(reply->str) > 6 && !strncmp(reply->str, "MOVED ", 6)) {
            NodeAddr addr;
            if (!parseMovedReply(reply->str, &addr)) {
                LOG_WARNING("RAFT.NODE ADD failed: invalid MOVED response: %s", reply->str);
            } else {
                LOG_VERBOSE("Join redirected to leader: %s:%d", addr.host, addr.port);
                NodeAddrListAddElement(&state->addr, &addr);
            }
        } else if (strlen(reply->str) > 12 && !strncmp(reply->str, "CLUSTERDOWN ", 12)) {
            LOG_WARNING("RAFT.NODE ADD error: %s, retrying.", reply->str);
        } else {
            LOG_WARNING("RAFT.NODE ADD failed: %s", reply->str);
            state->failed = true;
        }
    } else if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
        LOG_WARNING("RAFT.NODE ADD invalid reply.");
    } else {
        LOG_NOTICE("Joined Raft cluster, node id: %lu, dbid: %.*s",
                   (unsigned long) reply->element[0]->integer,
                   (int) reply->element[1]->len, reply->element[1]->str);

        strncpy(rr->snapshot_info.dbid, reply->element[1]->str, reply->element[1]->len);
        rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

        rr->config.id = reply->element[0]->integer;
        state->complete_callback(state->req);
        RedisModule_Assert(rr->state == REDIS_RAFT_UP);

        ConnAsyncTerminate(conn);
    }

    redisAsyncDisconnect(c);
}

/* failed join -- reset cluster state */
static void failed_join_callback(Connection *conn)
{
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);
    rr->state = REDIS_RAFT_UNINITIALIZED;
}

/* Connect callback -- if connection was established successfully we
 * send the RAFT.NODE ADD command.
 */
static void sendNodeAddRequest(Connection *conn)
{
    int ret;
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);

    /* Connection is not good?  Terminate and continue */
    if (!ConnIsConnected(conn)) {
        return;
    }

    ret = redisAsyncCommand(ConnGetRedisCtx(conn), handleNodeAddResponse, conn,
                            "RAFT.NODE ADD %d %s:%u", rr->config.id,
                            rr->config.addr.host, rr->config.addr.port);
    if (ret != REDIS_OK) {
        redisAsyncDisconnect(ConnGetRedisCtx(conn));
        ConnMarkDisconnected(conn);
    }
}

void JoinCluster(RedisRaftCtx *rr, NodeAddrListElement *el, RaftReq *req,
                 void (*complete_callback)(RaftReq *req))
{
    JoinLinkState *st = RedisModule_Calloc(1, sizeof(*st));

    NodeAddrListConcat(&st->addr, el);
    st->type = "join";
    st->connect_callback = sendNodeAddRequest;
    st->complete_callback = complete_callback;
    st->fail_callback = failed_join_callback;
    st->start = time(NULL);
    st->req = req;

    /* We just create the connection with an idle callback, which will
     * shortly fire and handle connection setup.
     */
    st->conn = ConnCreate(rr, st, joinLinkIdleCallback, joinLinkFreeCallback,
                          rr->config.cluster_user, rr->config.cluster_password);
}
