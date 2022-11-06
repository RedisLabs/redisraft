/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include "redisraft.h"

#include <string.h>
#include <strings.h>

int redis_raft_in_rm_call = 0;

const char *getStateStr(RedisRaftCtx *rr)
{
    switch (rr->state) {
        case REDIS_RAFT_UNINITIALIZED:
            return "uninitialized";
        case REDIS_RAFT_UP:
            return "up";
        case REDIS_RAFT_LOADING:
            return "loading";
        case REDIS_RAFT_JOINING:
            return "joining";
        default:
            return "<invalid>";
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
void replyRedirect(RedisModuleCtx *ctx, unsigned int slot, NodeAddr *addr)
{
    char buf[sizeof(addr->host) + 256];

    snprintf(buf, sizeof(buf), "MOVED %u %s:%u", slot, addr->host, addr->port);
    RedisModule_ReplyWithError(ctx, buf);
}

/* Create a -ASK reply. */
void replyAsk(RedisModuleCtx *ctx, unsigned int slot, NodeAddr *addr)
{
    char buf[sizeof(addr->host) + 256];

    snprintf(buf, sizeof(buf), "ASK %u %s:%u", slot, addr->host, addr->port);
    RedisModule_ReplyWithError(ctx, buf);
}

/* Create a CROSSSLOT response */
void replyCrossSlot(RedisModuleCtx *ctx)
{
    RedisModule_ReplyWithError(ctx, "CROSSSLOT Keys in request don't hash to the same slot");
}

void replyClusterDown(RedisModuleCtx *ctx)
{
    RedisModule_ReplyWithError(ctx, "CLUSTERDOWN No raft leader");
}

void replyWithFormatErrorString(RedisModuleCtx *ctx, const char *fmt, ...)
{
    char buf[512];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    RedisModule_ReplyWithError(ctx, buf);
}

/* Returns the leader node (raft_node_t), or reply a -CLUSTERDOWN error
 * and return NULL.
 *
 * Note: it's possible to have an elected but unknown leader node, in which
 * case NULL will also be returned.
 */
raft_node_t *getLeaderRaftNodeOrReply(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    raft_node_t *node = raft_get_leader_node(rr->raft);
    if (!node) {
        replyClusterDown(ctx);
    }

    return node;
}

/* Returns the leader node (Node), or reply a -CLUSTERDOWN error
 * and return NULL.
 *
 * Note: it's possible to have an elected but unknown leader node, in which
 * case NULL will also be returned.
 */
Node *getLeaderNodeOrReply(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    raft_node_t *raft_node = getLeaderRaftNodeOrReply(rr, ctx);
    if (!raft_node) {
        return NULL;
    }

    Node *node = raft_node_get_udata(raft_node);
    if (!node) {
        replyClusterDown(ctx);
    }

    return node;
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

    if (difftime(now, state->start) > rr->config.join_timeout) {
        LOG_WARNING("Cluster %s: timed out, took longer than %d seconds", state->type, rr->config.join_timeout);
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

/*
* Serializes `raft_node_t` into a string, output has to point to a buffer sized `RAFT_SHARDGROUP_NODEID_LEN+1`
*/
void raftNodeToString(char *output, const char *dbid, raft_node_t *raft_node)
{
    raftNodeIdToString(output, dbid, raft_node_get_id(raft_node));
}

/*
 * Serializes `raft_node_id_t` into a string, output has to point to a buffer sized `RAFT_SHARDGROUP_NODEID_LEN+1`
 */
void raftNodeIdToString(char *output, const char *dbid, raft_node_id_t raft_id)
{
    snprintf(output, RAFT_SHARDGROUP_NODEID_LEN + 1, "%.32s%08x", dbid, raft_id);
}
