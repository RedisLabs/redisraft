/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include <string.h>
#include <strings.h>

#include "redisraft.h"

int redis_raft_in_rm_call = 0;

const char *getStateStr(RedisRaftCtx *rr)
{
    static const char *state_str[] = { "uninitialized", "up", "loading", "joining" };
    static const char *invalid = "<invalid>";

    if (rr->state < REDIS_RAFT_UNINITIALIZED ||
        rr->state > REDIS_RAFT_JOINING) {
            return invalid;
    }

    return state_str[rr->state];
}

const char *raft_logtype_str(int type)
{
    static const char *logtype_str[] = {
        "NORMAL",
        "ADD_NONVOTING_NODE",
        "ADD_NODE",
        "DEMOTE_NODE",
        "REMOVE_NODE",
        "(unknown)"
    };
    static const char *logtype_unknown = "(unknown)";

    if (type < RAFT_LOGTYPE_NORMAL || type > RAFT_LOGTYPE_REMOVE_NODE) {
        return logtype_unknown;
    } else {
        return logtype_str[type];
    }
}

/* Convert a Raft library error code to an error reply.
 */
void replyRaftError(RedisModuleCtx *ctx, int error)
{
    char buf[128];

    switch (error) {
        case RAFT_ERR_NOT_LEADER:
            RedisModule_ReplyWithError(ctx, "ERR not leader");
            break;
        case RAFT_ERR_SHUTDOWN:
            LOG_ERROR("Raft requires immediate shutdown!");
            RedisModule_Call(ctx, "SHUTDOWN", "");
            break;
        case RAFT_ERR_ONE_VOTING_CHANGE_ONLY:
            RedisModule_ReplyWithError(ctx, "ERR a voting change is already in progress");
            break;
        case RAFT_ERR_NOMEM:
            RedisModule_ReplyWithError(ctx, "OOM Raft out of memory");
            break;
        default:
            snprintf(buf, sizeof(buf) - 1, "ERR Raft error %d", error);
            RedisModule_ReplyWithError(ctx, buf);
            break;
    }
}

/* Create a -MOVED reply.
 *
 * Depending on cluster_mode, we produce a Redis Cluster compatible or old-style
 * reply. TODO: Consider always using Redis Cluster compatible replies.
 *
 * One anomaly here is that may redirect a client to the leader even for commands
 * that have no keys (hash_slot is -1), which is something Redis Cluster never does.
 *
 * In this case we return an arbitrary (0) hash slot, but we still need to consider
 * how this impacts clients which may not expect it.
 */
void replyRedirect(RedisRaftCtx *rr, RaftReq *req, NodeAddr *addr)
{
    size_t reply_maxlen = strlen(addr->host) + 40;
    char *reply = RedisModule_Alloc(reply_maxlen);
    int slot = 0;
    if (rr->config->sharding &&
        req->type == RR_REDISCOMMAND &&
        req->r.redis.hash_slot != -1) {
            slot = req->r.redis.hash_slot;
    }
    snprintf(reply, reply_maxlen, "MOVED %d %s:%u", slot, addr->host, addr->port);
    RedisModule_ReplyWithError(req->ctx, reply);
    RedisModule_Free(reply);
}

static const char *err_clusterdown = "CLUSTERDOWN No raft leader";

/* Returns the leader node (raft_node_t), or reply a -CLUSTERDOWN error
 * and return NULL.
 *
 * Note: it's possible to have an elected but unknown leader node, in which
 * case NULL will also be returned.
 */
raft_node_t getLeaderNodeOrReply(RedisRaftCtx *rr, RaftReq *req)
{
    raft_node_t *node = raft_get_leader_node(rr->raft);
    if (!node) {
        RedisModule_ReplyWithError(req->ctx, err_clusterdown);
    }

    return node;
}

/* Checks that we *have* a Raft leader and its identity.
 *
 * If no leader is currently elected, replies a -CLUSTERDOWN error and
 * return RR_ERROR.
 *
 * If ret_leader is provided, it is used to return the current leader
 * by reference along with an RR_OK return value.
 *
 * If ret_leader is not provided and the current node is NOT the leader,
 * a -MOVED reply is generated with the current leader and RR_ERROR is
 * returned.
 */
RRStatus checkLeader(RedisRaftCtx *rr, RaftReq *req, Node **ret_leader)
{
    raft_node_t *leader = raft_get_leader_node(rr->raft);
    if (!leader) {
        RedisModule_ReplyWithError(req->ctx, err_clusterdown);
        return RR_ERROR;
    }
    if (raft_node_get_id(leader) != raft_get_nodeid(rr->raft)) {
        Node *leader_node = raft_node_get_udata(leader);

        if (leader_node) {
            if (ret_leader) {
                *ret_leader = leader_node;
                return RR_OK;
            }

            replyRedirect(rr, req, &leader_node->addr);
        } else {
            RedisModule_ReplyWithError(req->ctx, err_clusterdown);
        }
        return RR_ERROR;
    }

    return RR_OK;
}

/* Check that we're not in REDIS_RAFT_LOADING state.  If not, reply with -LOADING
 * and return an error.
 */
RRStatus checkRaftNotLoading(RedisRaftCtx *rr, RaftReq *req)
{
    if (rr->state == REDIS_RAFT_LOADING) {
        RedisModule_ReplyWithError(req->ctx, "LOADING Raft module is loading data");
        return RR_ERROR;
    }
    return RR_OK;
}

/* Check that we're in a REDIS_RAFT_UP state.  If not, reply with an appropriate
 * error code and return an error.
 */
RRStatus checkRaftState(RedisRaftCtx *rr, RaftReq *req)
{
    switch (rr->state) {
        case REDIS_RAFT_UNINITIALIZED:
            RedisModule_ReplyWithError(req->ctx, "NOCLUSTER No Raft Cluster");
            return RR_ERROR;
        case REDIS_RAFT_JOINING:
            RedisModule_ReplyWithError(req->ctx, "NOCLUSTER No Raft Cluster (joining now)");
            return RR_ERROR;
        case REDIS_RAFT_LOADING:
            RedisModule_ReplyWithError(req->ctx, "LOADING Raft module is loading data");
            return RR_ERROR;
        case REDIS_RAFT_UP:
            break;
    }
    return RR_OK;

}

/* Parse a -MOVED reply and update the returned address in addr.
 * Both standard Redis Cluster reply (with the hash slot) or the simplified
 * RedisRaft reply are supported.
 */
bool parseMovedReply(const char *str, NodeAddr *addr)
{
    /* -MOVED 0 1.1.1.1:1 or -MOVED 1.1.1.1:1 */
    if (strlen(str) < 15 || strncmp(str, "MOVED ", 6) != 0)
        return false;

    /* Handle current or cluster-style -MOVED replies. */
    const char *p = strrchr(str, ' ');
    /* Move the pointer past the space. */
    p++;
    return NodeAddrParse(p, strlen(p), addr);
}

/* Invoked when the connection is not connected or actively attempting
 * a connection.
 */
void joinLinkIdleCallback(Connection *conn)
{
    char err_msg[50];

    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);
    JoinLinkState *state = ConnGetPrivateData(conn);

    time_t now;
    time(&now);

    if (state->failed) {
        LOG_ERROR("Cluster %s: unrecoverable error, check logs", state->type);
        goto exit_fail;
    }

    if (difftime(now, state->start) > rr->config->join_timeout) {
        LOG_ERROR("Cluster %s: timed out, took longer than %d seconds", state->type, rr->config->join_timeout);
        goto exit_fail;
    }

    /* Advance iterator, wrap around to start */
    if (state->addr_iter) {
        state->addr_iter = state->addr_iter->next;
    }
    if (!state->addr_iter) {
        state->addr_iter = state->addr;
    }

    LOG_VERBOSE("%s cluster, connecting to %s:%u", state->type, state->addr_iter->addr.host, state->addr_iter->addr.port);

    /* Establish connection. We silently ignore errors here as we'll
     * just get iterated again in the future.
     */
    ConnConnect(state->conn, &state->addr_iter->addr, state->connect_callback);
    return;

exit_fail:
    ConnAsyncTerminate(conn);
    rr->state = REDIS_RAFT_UNINITIALIZED;

    snprintf(err_msg, sizeof(err_msg), "ERR failed to %s cluster, please check logs", state->type);
    RedisModule_ReplyWithError(state->req->ctx, err_msg);
    RaftReqFree(state->req);
}

/* Invoked when the connection is terminated.
 */
void joinLinkFreeCallback(void *privdata)
{
    JoinLinkState *state = (JoinLinkState *) privdata;

    if (state->type != NULL) {
        RedisModule_Free(state->type);
    }

    NodeAddrListFree(state->addr);
    RedisModule_Free(state);
}
