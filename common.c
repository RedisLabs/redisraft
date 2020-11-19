/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
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
            RedisModule_ReplyWithError(ctx, "-ERR not leader");
            break;
        case RAFT_ERR_SHUTDOWN:
            LOG_ERROR("Raft requires immediate shutdown!");
            RedisModule_Call(ctx, "SHUTDOWN", "");
            break;
        case RAFT_ERR_ONE_VOTING_CHANGE_ONLY:
            RedisModule_ReplyWithError(ctx, "-ERR a voting change is already in progress");
            break;
        case RAFT_ERR_NOMEM:
            RedisModule_ReplyWithError(ctx, "-OOM Raft out of memory");
            break;
        default:
            snprintf(buf, sizeof(buf) - 1, "-ERR Raft error %d", error);
            RedisModule_ReplyWithError(ctx, buf);
            break;
    }
}

/* Create a -MOVED reply.
 *
 * Depending on cluster_mode, we produce a Redis Cluster compatible or old-style
 * reply. TODO: Consider always using Redis Cluser compatible replies.
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
    if (req->type == RR_REDISCOMMAND && req->r.redis.hash_slot != -1)
        slot = req->r.redis.hash_slot;
    if (rr->config->cluster_mode) {
        snprintf(reply, reply_maxlen, "MOVED %d %s:%u", slot, addr->host, addr->port);
    } else {
        snprintf(reply, reply_maxlen, "MOVED %s:%u", addr->host, addr->port);
    }
    RedisModule_ReplyWithError(req->ctx, reply);
    RedisModule_Free(reply);
}

/* Check that this node is a Raft leader.  If not, reply with -MOVED and
 * return an error.
 */
RRStatus checkLeader(RedisRaftCtx *rr, RaftReq *req, Node **ret_leader)
{
    static const char *err_noleader = "NOLEADER No raft leader";
    static const char *err_clusterdown = "CLUSTERDOWN No raft leader";

    raft_node_t *leader = raft_get_current_leader_node(rr->raft);
    if (!leader) {
        RedisModule_ReplyWithError(req->ctx,
                rr->config->cluster_mode ? err_clusterdown : err_noleader);
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
            RedisModule_ReplyWithError(req->ctx,
                    rr->config->cluster_mode ? err_clusterdown : err_noleader);
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

static void raftize_commands(RedisModuleCommandFilterCtx *filter)
{
    static char *excluded_commands[] = {
        "auth",
        "ping",
        "save",
        "module",
        "raft",
        "info",
        "client",
        "config",
        "monitor",
        "command",
        "shutdown",
        "quit",
        NULL
    };

    /* If we're intercepting an RM_Call() processing a Raft entry,
     * skip.
     */
    if (checkInRedisModuleCall()) {
        return;
    }

    size_t cmdname_len;
    const char *cmdname = RedisModule_StringPtrLen(
            RedisModule_CommandFilterArgGet(filter, 0), &cmdname_len);

    /* Don't process any RAFT.* command */
    if (cmdname_len > 4 && strncasecmp(cmdname, "raft.", 5) == 0) {
       return;
    }

    /* Don't process any excluded command */
    char **c = excluded_commands;
    while (*c != NULL) {
        size_t len = strlen(*c);
        if (cmdname_len == len && strncasecmp(cmdname, *c, len) == 0) {
            return;
        }
        c++;
    }

    /* Prepend RAFT to the original command */
    RedisModuleString *raft_str = RedisModule_CreateString(NULL, "RAFT", 4);
    RedisModule_CommandFilterArgInsert(filter, 0, raft_str);
}


RRStatus setRaftizeMode(RedisRaftCtx *rr, RedisModuleCtx *ctx, bool flag)
{
    /* Is it supported by Redis? */
    if (!RedisModule_RegisterCommandFilter) {
        /* No; but if flag is false no harm is done. */
        return flag ? RR_ERROR : RR_OK;
    }

    /* Disable */
    if (rr->registered_filter && !flag) {
        RedisModule_UnregisterCommandFilter(ctx, rr->registered_filter);
        rr->registered_filter = NULL;
    } else if (!rr->registered_filter && flag) {
        rr->registered_filter = RedisModule_RegisterCommandFilter(ctx, 
                    raftize_commands, REDISMODULE_CMDFILTER_NOSELF);
        if (!rr->registered_filter) {
            return RR_ERROR;
        }
    }

    return RR_OK;
}

