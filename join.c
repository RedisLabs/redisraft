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

typedef struct JoinState {
    NodeAddrListElement *addr;
    NodeAddrListElement *addr_iter;
    Connection *conn;
} JoinState;

/* Parse a -MOVED reply and update the returned address in addr.
 * Both standard Redis Cluster reply (with the hash slot) or the simplified
 * RedisRaft reply are supported.
 */
static bool parseMovedReply(const char *str, NodeAddr *addr)
{
    /* -MOVED 0 1.1.1.1:1 or -MOVED 1.1.1.1:1 */
    if (strlen(str) < 15 || strncmp(str, "MOVED ", 6))
        return false;

    const char *tok = str + 6;
    const char *tok2;

    /* Handle current or cluster-style -MOVED replies. */
    if ((tok2 = strchr(tok, ' ')) == NULL) {
        return NodeAddrParse(tok, strlen(tok), addr);
    } else {
        return NodeAddrParse(tok2, strlen(tok2), addr);
    }
}

/* Callback for the RAFT.NODE ADD command.
 */
static void handleNodeAddResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Connection *conn = privdata;
    JoinState *state = ConnGetPrivateData(conn);
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
        } else {
            LOG_ERROR("RAFT.NODE ADD failed: %s", reply->str);
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

        HandleClusterJoinCompleted(rr);
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

void joinFreeCallback(void *privdata)
{
    JoinState *state = (JoinState *) privdata;

    NodeAddrListFree(state->addr);
    RedisModule_Free(state);
}

/* Invoked when the connection is not connected or actively attempting
 * a connection.
 */
void joinIdleCallback(Connection *conn)
{
    JoinState *state = ConnGetPrivateData(conn);

    /* Advance iterator, wrap around to start */
    if (state->addr_iter) {
        state->addr_iter = state->addr_iter->next;
    }
    if (!state->addr_iter) {
        state->addr_iter = state->addr;

        /* FIXME: If we iterated through the entire list, we currently continue
         * forever. This should be changed along with the change of configuration
         * interface, so once we've exahusted all addresses we fail the
         * join operation.
         */
    }

    LOG_VERBOSE("Joining cluster, connecting to %s:%u",
            state->addr_iter->addr.host, state->addr_iter->addr.port);

    /* Establish connection. We silently ignore errors here as we'll
     * just get iterated again in the future.
     */
    ConnConnect(state->conn, &state->addr_iter->addr, sendNodeAddRequest);
}

/* Initiate the process of joining a cluster, using the specified list
 * of addresses.
 */

void InitiateJoinCluster(RedisRaftCtx *rr, const NodeAddrListElement *addr)
{
    JoinState *state = RedisModule_Calloc(1, sizeof(*state));
    NodeAddrListConcat(&state->addr, addr);

    /* We just create the connection with an idle callback, which will
     * shortly fire and handle connection setup.
     */ 
    state->conn = ConnCreate(rr, state, joinIdleCallback, joinFreeCallback);
}

