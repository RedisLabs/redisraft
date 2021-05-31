/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

/* This is the implementation of RAFT.CLUSTER JOIN.
 *
 * It involves creating a Connection object linked to a JoinState, which is
 * populated with one or more NodeAddr.
 *
 * We then iterate the address list, establish a connection and attempt to
 * perform a RAFT.NODE ADD operation. If -MOVED replies are received they
 * are processed by adding the new node address to our list.
 *
 * We currently continue iterating the address list forever.
 * FIXME: Change the RAFT.CLUSTER JOIN implementation so it will (optionally?)
 * block, hold a reference from the JoinState to the RaftReq and produce a
 * reply only when successful - making this operation (optionally?) synchronous.
 */

#include <string.h>
#include <assert.h>
#include "redisraft.h"

void HandleClusterJoinFailed(RedisRaftCtx *rr, RaftReq *req) {
    RedisModule_ReplyWithError(req->ctx, "ERR Failed to join cluster, check logs");
    RaftReqFree(req);
}

/* Callback for the RAFT.NODE ADD command.
 */
static void handleNodeAddResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Connection *conn = privdata;
    JoinLinkState *state = ConnGetPrivateData(conn);
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);

    redisReply *reply = r;

    if (!reply) {
        LOG_ERROR("RAFT.NODE ADD failed: connection dropped.");
        ConnMarkDisconnected(conn);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        /* -MOVED? */
        if (strlen(reply->str) > 6 && !strncmp(reply->str, "MOVED ", 6)) {
            NodeAddr addr;
            if (!parseMovedReply(reply->str, &addr)) {
                LOG_ERROR("RAFT.NODE ADD failed: invalid MOVED response: %s", reply->str);
            } else {
                LOG_VERBOSE("Join redirected to leader: %s:%d", addr.host, addr.port);
                NodeAddrListAddElement(&state->addr, &addr);
            }
        } else if (strlen(reply->str) > 12 && !strncmp(reply->str, "CLUSTERDOWN ", 12)) {
            LOG_ERROR("RAFT.NODE ADD error: %s, retrying.", reply->str);
        } else {
            LOG_ERROR("RAFT.NODE ADD failed: %s", reply->str);
            state->failed = true;
        }
    } else if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
        LOG_ERROR("RAFT.NODE ADD invalid reply.");
    } else {
        LOG_INFO("Joined Raft cluster, node id: %lu, dbid: %.*s",
                 (unsigned long) reply->element[0]->integer,
                 (int) reply->element[1]->len, reply->element[1]->str);

        strncpy(rr->snapshot_info.dbid, reply->element[1]->str, reply->element[1]->len);
        rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

        rr->config->id = reply->element[0]->integer;

        HandleClusterJoinCompleted(rr, state->req);
        assert(rr->state == REDIS_RAFT_UP);

        ConnAsyncTerminate(conn);
    }

    redisAsyncDisconnect(c);
}

/* Connect callback -- if connection was established successfully we
 * send the RAFT.NODE ADD command.
 */
static void sendNodeAddRequest(Connection *conn)
{
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);

    /* Connection is not good?  Terminate and continue */
    if (!ConnIsConnected(conn)) {
        return;
    }

    if (redisAsyncCommand(ConnGetRedisCtx(conn), handleNodeAddResponse, conn,
        "RAFT.NODE %s %d %s:%u",
        "ADD",
        rr->config->id,
        rr->config->addr.host, rr->config->addr.port) != REDIS_OK) {

        redisAsyncDisconnect(ConnGetRedisCtx(conn));
        ConnMarkDisconnected(conn);
    }
}

void handleClusterJoin(RedisRaftCtx *rr, RaftReq *req)
{
    const char * type = "join";

    if (checkRaftNotLoading(rr, req) == RR_ERROR) {
        goto exit_fail;
    }

    if (rr->state != REDIS_RAFT_UNINITIALIZED) {
        RedisModule_ReplyWithError(req->ctx, "ERR Already cluster member");
        goto exit_fail;
    }

    /* Create a Snapshot Info meta-key */
    initializeSnapshotInfo(rr);

    JoinLinkState *state = RedisModule_Calloc(1, sizeof(*state));
    state->type = RedisModule_Calloc(1, strlen(type)+1);
    strcpy(state->type, type);
    state->connect_callback = sendNodeAddRequest;
    time(&(state->start));
    NodeAddrListConcat(&state->addr, req->r.cluster_join.addr);
    state->req = req;

    /* We just create the connection with an idle callback, which will
     * shortly fire and handle connection setup.
     */
    state->conn = ConnCreate(rr, state, joinLinkIdleCallback, joinLinkFreeCallback);

    rr->state = REDIS_RAFT_JOINING;

    return;

exit_fail:
    RaftReqFree(req);
}
