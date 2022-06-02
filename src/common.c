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
            LOG_WARNING("Raft requires immediate shutdown!");
            enterRedisModuleCall();
            RedisModule_Call(ctx, "SHUTDOWN", "");
            exitRedisModuleCall();
            break;
        case RAFT_ERR_ONE_VOTING_CHANGE_ONLY:
            RedisModule_ReplyWithError(ctx, "ERR a voting change is already in progress");
            break;
        case RAFT_ERR_NOMEM:
            RedisModule_ReplyWithError(ctx, "OOM Raft out of memory");
            break;
        case RAFT_ERR_LEADER_TRANSFER_IN_PROGRESS:
            RedisModule_ReplyWithError(ctx, "ERR transfer already in progress");
            break;
        case RAFT_ERR_INVALID_NODEID:
            RedisModule_ReplyWithError(ctx, "ERR invalid node id");
            break;
        default:
            snprintf(buf, sizeof(buf), "ERR Raft error %d", error);
            RedisModule_ReplyWithError(ctx, buf);
            break;
    }
}

/* Create a -MOVED reply. */
void replyRedirect(RedisModuleCtx *ctx, int slot, NodeAddr *addr)
{
    char buf[sizeof(addr->host) + 256];

    snprintf(buf, sizeof(buf), "MOVED %d %s:%u", slot, addr->host, addr->port);
    RedisModule_ReplyWithError(ctx, buf);
}

/* Create a -ASK reply. */
void replyAsk(RedisRaftCtx *rr, RedisModuleCtx *ctx, int slot)
{
    ShardGroup *sg = rr->sharding_info->importing_slots_map[slot];
    if (!sg) {
        RedisModule_ReplyWithError(ctx, "ERR no importing shard group to ask");
        return;
    }

    char buf[sizeof(sg->nodes[0].addr.host) + 256];
    snprintf(buf, sizeof(buf), "ASK %d %s:%u", slot, sg->nodes[0].addr.host, sg->nodes[0].addr.port);
    RedisModule_ReplyWithError(ctx, buf);
}

static const char *err_clusterdown = "CLUSTERDOWN No raft leader";

/* Returns the leader node (raft_node_t), or reply a -CLUSTERDOWN error
 * and return NULL.
 *
 * Note: it's possible to have an elected but unknown leader node, in which
 * case NULL will also be returned.
 */
raft_node_t *getLeaderNodeOrReply(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    raft_node_t *node = raft_get_leader_node(rr->raft);
    if (!node) {
        RedisModule_ReplyWithError(ctx, err_clusterdown);
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
RRStatus checkLeader(RedisRaftCtx *rr, RedisModuleCtx *ctx, Node **ret_leader)
{
    Node *node = NULL;

    if (raft_is_leader(rr->raft)) {
        return RR_OK;
    }

    raft_node_t *leader = raft_get_leader_node(rr->raft);
    if (leader) {
        node = raft_node_get_udata(leader);
    }

    if (!node) {
        RedisModule_ReplyWithError(ctx, err_clusterdown);
        return RR_ERROR;
    }

    if (ret_leader) {
        *ret_leader = node;
        return RR_OK;
    }

    /* One anomaly here is that may redirect a client to the leader even for
     * commands have no keys, which is something Redis Cluster never does. We
     * still need to consider how this impacts clients which may not expect it.
     */
    replyRedirect(ctx, 0, &node->addr);
    return RR_ERROR;
}

/* Check that we're not in REDIS_RAFT_LOADING state.  If not, reply with -LOADING
 * and return an error.
 */
RRStatus checkRaftNotLoading(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    if (rr->state == REDIS_RAFT_LOADING) {
        RedisModule_ReplyWithError(ctx, "LOADING Raft module is loading data");
        return RR_ERROR;
    }
    return RR_OK;
}

RRStatus checkRaftUninitialized(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    if (rr->state != REDIS_RAFT_UNINITIALIZED) {
        RedisModule_ReplyWithError(ctx, "ERR Already cluster member");
        return RR_ERROR;
    }
    return RR_OK;
}

/* Check that we're in a REDIS_RAFT_UP state.  If not, reply with an appropriate
 * error code and return an error.
 */
RRStatus checkRaftState(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    switch (rr->state) {
        case REDIS_RAFT_UNINITIALIZED:
            RedisModule_ReplyWithError(ctx, "NOCLUSTER No Raft Cluster");
            return RR_ERROR;
        case REDIS_RAFT_JOINING:
            RedisModule_ReplyWithError(ctx, "NOCLUSTER No Raft Cluster (joining now)");
            return RR_ERROR;
        case REDIS_RAFT_LOADING:
            RedisModule_ReplyWithError(ctx, "LOADING Raft module is loading data");
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
    char err_msg[60];

    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);
    JoinLinkState *state = ConnGetPrivateData(conn);

    time_t now;
    time(&now);

    if (state->failed) {
        LOG_WARNING("Cluster %s: unrecoverable error, check logs", state->type);
        goto exit_fail;
    }

    if (difftime(now, state->start) > rr->config->join_timeout) {
        LOG_WARNING("Cluster %s: timed out, took longer than %d seconds", state->type, rr->config->join_timeout);
        goto exit_fail;
    }

    /* Advance iterator, wrap around to start */
    if (state->addr_iter) {
        state->addr_iter = state->addr_iter->next;
    }
    if (!state->addr_iter) {
        if (!state->started) {
            state->started = true;
            state->addr_iter = state->addr;
        } else {
            LOG_WARNING("exhausted all possible hosts to connect to");
            goto exit_fail;
        }
    }

    LOG_VERBOSE("%s cluster, connecting to %s:%u", state->type, state->addr_iter->addr.host, state->addr_iter->addr.port);

    /* Establish connection. We silently ignore errors here as we'll
     * just get iterated again in the future.
     */
    ConnConnect(state->conn, &state->addr_iter->addr, state->connect_callback);
    return;

exit_fail:
    ConnAsyncTerminate(conn);
    /* only reset state to UNINITIALIZED on a join failure */
    if (state->fail_callback) {
        state->fail_callback(conn);
    }

    snprintf(err_msg, sizeof(err_msg), "ERR failed to connect to cluster for %s, please check logs", state->type);
    RedisModule_ReplyWithError(state->req->ctx, err_msg);
    RaftReqFree(state->req);
}

/* Invoked when the connection is terminated.
 */
void joinLinkFreeCallback(void *privdata)
{
    JoinLinkState *state = privdata;

    NodeAddrListFree(state->addr);
    RedisModule_Free(state);
}
