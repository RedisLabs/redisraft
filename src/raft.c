/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <ctype.h>
#include <strings.h>
#include <inttypes.h>

#include "redisraft.h"

const char *RaftReqTypeStr[] = {
    [0]                       = "<undef>",
    [RR_CLUSTER_INIT]         = "RR_CLUSTER_INIT",
    [RR_CLUSTER_JOIN]         = "RR_CLUSTER_JOIN",
    [RR_CFGCHANGE_ADDNODE]    = "RR_CFGCHANGE_ADDNODE",
    [RR_CFGCHANGE_REMOVENODE] = "RR_CFGCHANGE_REMOVENODE",
    [RR_APPENDENTRIES]        = "RR_APPENDENTRIES",
    [RR_REDISCOMMAND]         = "RR_REDISCOMMAND",
    [RR_INFO]                 = "RR_INFO",
    [RR_DEBUG]                = "RR_DEBUG",
    [RR_CLIENT_DISCONNECT]    = "RR_CLIENT_DISCONNECT",
    [RR_SHARDGROUP_ADD]       = "RR_SHARDGROUP_ADD",
    [RR_SHARDGROUPS_REPLACE]  = "RR_SHARDGROUPS_REPLACE",
    [RR_SHARDGROUP_GET]       = "RR_SHARDGROUP_GET",
    [RR_SHARDGROUP_LINK]      = "RR_SHARDGROUP_LINK",
    [RR_TRANSFER_LEADER]      = "RR_TRANSFER_LEADER"
};

/* Forward declarations */
static void initRaftLibrary(RedisRaftCtx *rr);
static void configureFromSnapshot(RedisRaftCtx *rr);
static void applyShardGroupChange(RedisRaftCtx *rr, raft_entry_t *entry);
static void replaceShardGroups(RedisRaftCtx *rr, raft_entry_t *entry);

/* A dict that maps client ID to MultiClientState structs */
static RedisModuleDict *multiClientState = NULL;

/* ------------------------------------ Common helpers ------------------------------------ */

void shutdownAfterRemoval(RedisRaftCtx *rr)
{
    LOG_INFO("*** NODE REMOVED, SHUTTING DOWN.");

    if (rr->config->raft_log_filename) {
        RaftLogArchiveFiles(rr);
    }
    if (rr->config->rdb_filename) {
        archiveSnapshot(rr);
    }

    exit(0);
}

RaftReq *entryDetachRaftReq(RedisRaftCtx *rr, raft_entry_t *entry)
{
    RaftReq* req = entry->user_data;

    if (!req) {
        return NULL;
    }

    entry->user_data = NULL;
    entry->free_func = NULL;
    rr->client_attached_entries--;

    return req;
}

/* Set up a Raft log entry with an attached RaftReq. We use this when a user command provided
 * in a RaftReq should keep the client blocked until the log entry is committed and applied.
 */
void entryFreeAttachedRaftReq(raft_entry_t *ety)
{
    RaftReq *req = entryDetachRaftReq(&redis_raft, ety);

    if (req) {
        RedisModule_ReplyWithError(req->ctx, "TIMEOUT not committed yet");
        RaftReqFree(req);
    }

    RedisModule_Free(ety);
}

/* Attach a RaftReq to a Raft log entry. The common case for this is when a user request
 * needs to block until it gets committed, and only then a reply should be produced.
 *
 * To do that, we link the RaftReq to the Raft log entry and keep the client blocked.
 * When the entry will later reach the apply flow, the linkage to the RaftReq will
 * make it possible to generate the reply to the user.
 */
void entryAttachRaftReq(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req)
{
    entry->user_data = req;
    entry->free_func = entryFreeAttachedRaftReq;
    rr->client_attached_entries++;
}

/* ------------------------------------ RaftRedisCommand ------------------------------------ */

/* ---------------------- RAFT MULTI/EXEC Handlig ---------------------------- */

/* There are several concerns about MULTI/EXEC Handling:
 *
 * 1. We want to make sure that the commands are executed atomically across all
 *    cluster nodes. To do this, we need to pack them as a single Raft log entry.
 * 2. When executing the MULTI/EXEC we don't really need to wrap it because Redis
 *    wraps all module commands in MULTI/EXEC (although no harm is done).
 * 3. The MULTI/EXEC wrapping also ensures that any WATCHed keys will fail the
 *    transaction.  We do have to be careful though and never proxy such operations
 *    to a leader, as we don't synchronize WATCH.  (Note: we should also avoid
 *    proxying WATCH commands of course).
 */

/* ------------------------------------ Log Execution ------------------------------------ */

/* Execute all commands in a specified RaftRedisCommandArray.
 *
 * The commands are executed on ctx, which can be a real or thread-safe
 * context.  Caller is responsible to hold the lock.
 *
 * If reply_ctx is non-NULL, replies are delivered to it.
 * Otherwise no replies are delivered.
 */
static void executeRaftRedisCommandArray(RaftRedisCommandArray *array,
    RedisModuleCtx *ctx, RedisModuleCtx *reply_ctx)
{
    int i;

    for (i = 0; i < array->len; i++) {
        RaftRedisCommand *c = array->commands[i];

        size_t cmdlen;
        const char *cmd = RedisModule_StringPtrLen(c->argv[0], &cmdlen);

        /* We need to handle MULTI as a special case:
        * 1. Skip the command (no need to execute MULTI in a Module context).
        * 2. If we're returning a response, group it as an array (multibulk).
        */

        if (i == 0 && cmdlen == 5 && !strncasecmp(cmd, "MULTI", 5)) {
            if (reply_ctx) {
                RedisModule_ReplyWithArray(reply_ctx, array->len - 1);
            }

            continue;
        }

        enterRedisModuleCall();
        int eval = 0;
        int old_entered_eval = 0;
        if ((cmdlen == 4 && !strncasecmp(cmd, "eval", 4)) || (cmdlen == 7 && !strncasecmp(cmd, "evalsha", 7))) {
            old_entered_eval = redis_raft.entered_eval;
            eval = 1;
            redis_raft.entered_eval = 1;
        }
        RedisModuleCallReply *reply = RedisModule_Call(
                ctx, cmd, redis_raft.resp_call_fmt, &c->argv[1], c->argc - 1);
        int ret_errno = errno;
        exitRedisModuleCall();
        if (eval) {
            redis_raft.entered_eval = old_entered_eval;
        }

        if (reply_ctx) {
            if (reply) {
                RedisModule_ReplyWithCallReply(reply_ctx, reply);
            } else {
                handleRMCallError(reply_ctx, ret_errno, cmd, cmdlen);
            }
        }

        if (reply) {
            RedisModule_FreeCallReply(reply);
        }
    }
}

typedef enum KeysStatus{
    NoKeys,
    AllExist,
    SomeExist,
    NoneExist,
} KeysStatus;

KeysStatus validateKeyExistence(RedisRaftCtx *rr, RaftRedisCommandArray *cmds) {
    int total_keys = 0;
    int found = 0;

    for (int i = 0; i < cmds->len; i++) {
        RaftRedisCommand *cmd = cmds->commands[i];

        /* Iterate command keys */
        int num_keys = 0;
        int *keyindex = RedisModule_GetCommandKeys(rr->ctx, cmd->argv, cmd->argc, &num_keys);
        total_keys += num_keys;

        for (int j = 0; j < num_keys; j++) {
            found += RedisModule_KeyExists(rr->ctx, cmd->argv[keyindex[j]]);
        }
        RedisModule_Free(keyindex);

        /* shortcut as we know the result is now SomeExist */
        if (found != 0 && found != total_keys) {
            return SomeExist;
        }
    }

    if (found != total_keys) {
        return (found == 0) ? NoneExist : SomeExist;
    }

    return (total_keys == 0) ? NoKeys : AllExist;
}

RRStatus validateRaftRedisCommandArray(RedisRaftCtx *rr, RaftRedisCommandArray *cmds, RedisModuleCtx *reply_ctx) {
    /* Make sure hash slot is mapped and handled locally. */
    /* figure out the "owner shardgroup (osg)" and "importing shard group (isg)"
     * 'osg' the stable/migrating cluster, while 'isg' will always be the importer cluster if it exists
     *
     * we care about finding the correct local one.  if it's an asking, it will be isg, otherwise it will be osg
     *
     * first we get the right shardgroup for the request (stable/migrating/importing if asking)
     *
     * if the selected shardgroup doesn't exist, that's an error.  if it does, we verify that its the local cluster,
     * otherwise we redirect to the owning shardgroup.
     *
     * After this, we determine the "key status" (do they all exist/none exist/some exist). as can only process
     * if all keys exist locally.
     */
    SlotRangeType slot_type = SLOTRANGE_TYPE_STABLE;
    ShardGroup *osg = rr->sharding_info->stable_slots_map[cmds->slot];
    if (!osg) {
        slot_type = SLOTRANGE_TYPE_MIGRATING;
        osg = rr->sharding_info->migrating_slots_map[cmds->slot];
    }
    ShardGroup *isg = rr->sharding_info->importing_slots_map[cmds->slot];

    ShardGroup *sg = osg;
    if (cmds->asking) {
        slot_type = SLOTRANGE_TYPE_IMPORTING;
        sg = isg;
    }

    if (!sg) {
        if (reply_ctx) {
            RedisModule_ReplyWithError(reply_ctx, "CLUSTERDOWN Hash slot is not served");
        }
        return RR_ERROR;
    }

    if (!sg->local) {
        if (reply_ctx) {
            if (osg->next_redir >= osg->nodes_num) {
                osg->next_redir = 0;
            }

            replyRedirect(reply_ctx, cmds->slot, &osg->nodes[0].addr);
        }
        return RR_ERROR;
    }

    /* if our keys belong to a local migrating/importing slot, all keys must exist */
    if (slot_type == SLOTRANGE_TYPE_MIGRATING) {
        switch (validateKeyExistence(rr, cmds)) {
            case SomeExist:
                if (reply_ctx) {
                    RedisModule_ReplyWithError(reply_ctx, "TRYAGAIN");
                }
                return RR_ERROR;
            case NoneExist:
                if (reply_ctx) {
                    replyAsk(reply_ctx, cmds->slot, &isg->nodes[0].addr);
                }
                return RR_ERROR;
            case AllExist:
            case NoKeys:
                return RR_OK;
        }
    } else if (slot_type == SLOTRANGE_TYPE_IMPORTING) {
        switch (validateKeyExistence(rr, cmds)) {
            case SomeExist:
            case NoneExist:
                if (reply_ctx) {
                    RedisModule_ReplyWithError(reply_ctx, "TRYAGAIN");
                }
            case AllExist:
            case NoKeys:
                return RR_OK;
        }
    }

    return RR_OK;
}

/*
 * Execution of Raft log on the local instance.
 *
 * There are two variants:
 * 1) Execution of a raft entry received from another node.
 * 2) Execution of a locally initiated command.
 */
static void executeLogEntry(RedisRaftCtx *rr, raft_entry_t *entry, raft_index_t entry_idx)
{
    assert(entry->type == RAFT_LOGTYPE_NORMAL);

    RaftReq *req = entry->user_data;

    RedisModuleCtx *reply_ctx;
    RedisModuleCtx *ctx;
    RaftRedisCommandArray *cmds;
    RaftRedisCommandArray tmp = {0};

    if (req) {
        cmds = &req->r.redis.cmds;
        ctx = req->ctx;
        reply_ctx = req->ctx;
    } else {
        if (RaftRedisCommandArrayDeserialize(&tmp,
                                             entry->data,
                                             entry->data_len) != RR_OK) {
            PANIC("Invalid Raft entry");
        }

        cmds = &tmp;
        ctx = redis_raft.ctx;
        reply_ctx = NULL;
    }

    if (cmds->slot == -1 || validateRaftRedisCommandArray(rr, cmds, reply_ctx) == RR_OK) {
        executeRaftRedisCommandArray(cmds, ctx, reply_ctx);
    }

    if (req) {
        entryDetachRaftReq(rr, entry);
        RaftReqFree(req);
    } else {
        RaftRedisCommandArrayFree(cmds);
    }

    /* Update snapshot info in Redis dataset. This must be done now so it's
     * always consistent with what we applied and we never end up applying
     * an entry onto a snapshot where it was applied already.
     */
    rr->snapshot_info.last_applied_term = entry->term;
    rr->snapshot_info.last_applied_idx = entry_idx;
}

static void raftSendNodeShutdown(raft_node_t *raft_node)
{
    if (!raft_node) {
        return;
    }

    Node *node = raft_node_get_udata(raft_node);
    if (!node) {
        return;
    }

    if (!ConnIsConnected(node->conn)) {
        NODE_TRACE(node, "not connected, state=%s", ConnGetStateStr(node->conn));
        return;
    }

    if (redisAsyncCommand(ConnGetRedisCtx(node->conn), NULL, NULL,
                "RAFT.NODESHUTDOWN %d",
                (int) raft_node_get_id(raft_node)) != REDIS_OK) {
        NODE_TRACE(node, "failed to send raft.nodeshutdown");
    }
}

/* ------------------------------------ RequestVote ------------------------------------ */

static void handleRequestVoteResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;

    NodeDismissPendingResponse(node);
    if (!reply) {
        NODE_LOG_DEBUG(node, "RAFT.REQUESTVOTE failed: connection dropped.");
        ConnMarkDisconnected(node->conn);
        return;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        NODE_LOG_DEBUG(node, "RAFT.REQUESTVOTE error: %s", reply->str);
        return;
    }

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 4 ||
            reply->element[0]->type != REDIS_REPLY_INTEGER ||
            reply->element[1]->type != REDIS_REPLY_INTEGER ||
            reply->element[2]->type != REDIS_REPLY_INTEGER ||
            reply->element[3]->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_ERROR(node, "invalid RAFT.REQUESTVOTE reply");
        return;
    }

    msg_requestvote_response_t response = {
        .prevote = reply->element[0]->integer,
        .request_term = reply->element[1]->integer,
        .term = reply->element[2]->integer,
        .vote_granted = reply->element[3]->integer
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);
    if (!raft_node) {
        NODE_LOG_DEBUG(node, "RAFT.REQUESTVOTE stale reply.");
        return;
    }

    int ret;
    if ((ret = raft_recv_requestvote_response(
            rr->raft,
            raft_node,
            &response)) != 0) {
        TRACE("raft_recv_requestvote_response failed, error %d", ret);
    }
}


static int raftSendRequestVote(raft_server_t *raft, void *user_data,
        raft_node_t *raft_node, msg_requestvote_t *msg)
{
    Node *node = (Node *) raft_node_get_udata(raft_node);

    if (!ConnIsConnected(node->conn)) {
        NODE_TRACE(node, "not connected, state=%s", ConnGetStateStr(node->conn));
        return 0;
    }

    /* RAFT.REQUESTVOTE <src_node_id> <term> <candidate_id> <last_log_idx> <last_log_term> */
    if (redisAsyncCommand(ConnGetRedisCtx(node->conn), handleRequestVoteResponse,
                node, "RAFT.REQUESTVOTE %d %d %d:%ld:%d:%ld:%ld:%d",
                raft_node_get_id(raft_node),
                raft_get_nodeid(raft),
                msg->prevote,
                msg->term,
                msg->candidate_id,
                msg->last_log_idx,
                msg->last_log_term,
                msg->transfer_leader) != REDIS_OK) {
        NODE_TRACE(node, "failed requestvote");
    } else {
        NodeAddPendingResponse(node, false);
    }

    return 0;
}

/* ------------------------------------ AppendEntries ------------------------------------ */

static void handleAppendEntriesResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    NodeDismissPendingResponse(node);

    redisReply *reply = r;
    if (!reply) {
        NODE_TRACE(node, "RAFT.AE failed: connection dropped.");
        ConnMarkDisconnected(node->conn);
        return;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        NODE_TRACE(node, "RAFT.AE error: %s", reply->str);
        return;
    }
    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 4 ||
            reply->element[0]->type != REDIS_REPLY_INTEGER ||
            reply->element[1]->type != REDIS_REPLY_INTEGER ||
            reply->element[2]->type != REDIS_REPLY_INTEGER ||
            reply->element[3]->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_ERROR(node, "invalid RAFT.AE reply");
        return;
    }

    msg_appendentries_response_t response = {
        .term = reply->element[0]->integer,
        .success = reply->element[1]->integer,
        .current_idx = reply->element[2]->integer,
        .msg_id = reply->element[3]->integer
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);

    int ret;
    if ((ret = raft_recv_appendentries_response(
            rr->raft,
            raft_node,
            &response)) != 0) {
        NODE_TRACE(node, "raft_recv_appendentries_response failed, error %d", ret);
    }
}

static int raftSendAppendEntries(raft_server_t *raft, void *user_data,
        raft_node_t *raft_node, msg_appendentries_t *msg)
{
    Node *node = (Node *) raft_node_get_udata(raft_node);

    int argc = 5 + msg->n_entries * 2;
    char **argv = NULL;
    size_t *argvlen = NULL;

    if (!ConnIsConnected(node->conn)) {
        NODE_TRACE(node, "not connected, state=%s", ConnGetStateStr(node->conn));
        return 0;
    }

    argv = RedisModule_Alloc(sizeof(argv[0]) * argc);
    argvlen = RedisModule_Alloc(sizeof(argvlen[0]) * argc);

    char target_node_str[12];
    char source_node_str[12];
    char msg_str[100];
    char nentries_str[12];

    argv[0] = "RAFT.AE";
    argvlen[0] = strlen(argv[0]);

    argv[1] = target_node_str;
    argvlen[1] = snprintf(target_node_str, sizeof(target_node_str)-1, "%d", raft_node_get_id(raft_node));

    argv[2] = source_node_str;
    argvlen[2] = snprintf(source_node_str, sizeof(source_node_str)-1, "%d", raft_get_nodeid(raft));

    argv[3] = msg_str;
    argvlen[3] = snprintf(msg_str, sizeof(msg_str)-1, "%d:%ld:%ld:%ld:%ld:%lu",
            msg->leader_id,
            msg->term,
            msg->prev_log_idx,
            msg->prev_log_term,
            msg->leader_commit,
            msg->msg_id);

    argv[4] = nentries_str;
    argvlen[4] = snprintf(nentries_str, sizeof(nentries_str)-1, "%d", msg->n_entries);

    int i;
    for (i = 0; i < msg->n_entries; i++) {
        raft_entry_t *e = msg->entries[i];
        argv[5 + i*2] = RedisModule_Alloc(64);
        argvlen[5 + i*2] = snprintf(argv[5 + i*2], 63, "%ld:%d:%d", e->term, e->id, e->type);
        argvlen[6 + i*2] = e->data_len;
        argv[6 + i*2] = e->data;
    }

    if (redisAsyncCommandArgv(ConnGetRedisCtx(node->conn), handleAppendEntriesResponse,
                node, argc, (const char **)argv, argvlen) != REDIS_OK) {
        NODE_TRACE(node, "failed appendentries");
    } else{
        NodeAddPendingResponse(node, false);
    }

    for (i = 0; i < msg->n_entries; i++) {
        RedisModule_Free(argv[5 + i*2]);
    }

    RedisModule_Free(argv);
    RedisModule_Free(argvlen);

    return 0;
}

/* ------------------------------------ Timeout Follower --------------------------------- */

static void handleTimeoutNowResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    //RedisRaftCtx *rr = node->rr;

    NodeDismissPendingResponse(node);

    redisReply *reply = r;
    if (!reply) {
        NODE_TRACE(node, "RAFT.TIMEOUT_NOW failed: connection dropped.");
        ConnMarkDisconnected(node->conn);
        return;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        NODE_TRACE(node, "RAFT.TIMEOUT_NOW error: %s", reply->str);
        return;
    }

    if (reply->type != REDIS_REPLY_STATUS || strcmp("OK", reply->str)) {
        NODE_LOG_ERROR(node, "invalid RAFT.TIMEOUT_NOW reply");
        return;
    }
}

static int raftSendTimeoutNow(raft_server_t *raft, raft_node_t *raft_node)
{
    Node *node = raft_node_get_udata(raft_node);

    if (!ConnIsConnected(node->conn)) {
        NODE_TRACE(node, "not connected, state=%s", ConnGetStateStr(node->conn));
        return 0;
    }

    if (redisAsyncCommand(ConnGetRedisCtx(node->conn), handleTimeoutNowResponse,
                          node, "RAFT.TIMEOUT_NOW") != REDIS_OK) {
        NODE_TRACE(node, "failed timeout now");
    } else {
        NodeAddPendingResponse(node, false);
    }

    return 0;
}

/* ------------------------------------ Log Callbacks ------------------------------------ */

static int raftPersistVote(raft_server_t *raft, void *user_data, raft_node_id_t vote)
{
    RedisRaftCtx *rr = user_data;
    if (rr->state == REDIS_RAFT_LOADING) {
        return 0;
    }

    if (RaftMetaWrite(&rr->meta, rr->config->raft_log_filename,
                      raft_get_current_term(raft), vote) != RR_OK) {
        LOG_ERROR("ERROR: RaftMetaWrite()");
        return RAFT_ERR_SHUTDOWN;
    }

    return 0;
}

static int raftPersistTerm(raft_server_t *raft, void *user_data, raft_term_t term, raft_node_id_t vote)
{
    RedisRaftCtx *rr = user_data;
    if (rr->state == REDIS_RAFT_LOADING) {
        return 0;
    }

    if (RaftMetaWrite(&rr->meta, rr->config->raft_log_filename,
                      term, vote) != RR_OK) {
        LOG_ERROR("ERROR: RaftMetaWrite()");
        return RAFT_ERR_SHUTDOWN;
    }

    return 0;
}

static int raftApplyLog(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
    RedisRaftCtx *rr = user_data;
    RaftCfgChange *cfg;
    RaftReq *req;

    switch (entry->type) {
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            req = entryDetachRaftReq(rr, entry);
            cfg = (RaftCfgChange *) entry->data;
            if (!req) {
                break;
            }

            RedisModule_ReplyWithArray(req->ctx, 2);
            RedisModule_ReplyWithLongLong(req->ctx, cfg->id);
            RedisModule_ReplyWithSimpleString(req->ctx, rr->snapshot_info.dbid);
            RaftReqFree(req);
            break;
        case RAFT_LOGTYPE_REMOVE_NODE:
            req = entryDetachRaftReq(rr, entry);
            cfg = (RaftCfgChange *) entry->data;

            if (req) {
                RedisModule_ReplyWithSimpleString(req->ctx, "OK");
                RaftReqFree(req);
            }

            if (cfg->id == raft_get_nodeid(raft)) {
                shutdownAfterRemoval(rr);
            }

            if (raft_is_leader(raft)) {
                raftSendNodeShutdown(raft_get_node(raft, cfg->id));
            }
            break;
        case RAFT_LOGTYPE_NORMAL:
            executeLogEntry(rr, entry, entry_idx);
            break;
        case RAFT_LOGTYPE_ADD_SHARDGROUP:
        case RAFT_LOGTYPE_UPDATE_SHARDGROUP:
            applyShardGroupChange(rr, entry);
            break;
        case RAFT_LOGTYPE_REPLACE_SHARDGROUPS:
            replaceShardGroups(rr, entry);
            break;
        default:
            break;
    }

    rr->snapshot_info.last_applied_term = entry->term;
    rr->snapshot_info.last_applied_idx = entry_idx;

    return 0;
}

/* ------------------------------------ Utility Callbacks ------------------------------------ */

static void raftLog(raft_server_t *raft, raft_node_id_t node, void *user_data, const char *buf)
{
    raft_node_t* raft_node = raft_get_node(raft, node);
    if (raft_node) {
        Node *n = raft_node_get_udata(raft_node);
        if (n) {
            NODE_TRACE(n, "<raftlib> %s", buf);
            return;
        }
    }
    TRACE("<raftlib> %s", buf);
}

static raft_node_id_t raftLogGetNodeId(raft_server_t *raft, void *user_data, raft_entry_t *entry,
        raft_index_t entry_idx)
{
    RaftCfgChange *req = (RaftCfgChange *) entry->data;
    return req->id;
}

static int raftNodeHasSufficientLogs(raft_server_t *raft, void *user_data, raft_node_t *raft_node)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) user_data;
    if (rr->state == REDIS_RAFT_LOADING)
        return 0;

    /* Node may have sufficient logs to be promoted but be scheduled for
     * removal at the same time (i.e. RAFT_LOGTYPE_REMOVE_NODE already created
     * for its removal).
     *
     * In this case we don't want to create a promotion entry as it will
     * result with an unexpected state transition.
     *
     * We return -1 so we *WILL* get a chance to be notified again. For
     * example, if the removal entry is rolled back and the node becomes
     * active again.
     */
    if (!raft_node_is_active(raft_node)) {
        return -1;
    }

    Node *node = raft_node_get_udata(raft_node);
    assert (node != NULL);

    TRACE("node:%d has sufficient logs now, adding as voting node.", node->id);

    raft_entry_t *entry = raft_entry_new(sizeof(RaftCfgChange));
    entry->id = rand();
    entry->type = RAFT_LOGTYPE_ADD_NODE;

    msg_entry_response_t response;
    RaftCfgChange *cfgchange = (RaftCfgChange *) entry->data;
    cfgchange->id = node->id;
    cfgchange->addr = node->addr;

    int e = raft_recv_entry(raft, entry, &response);
    raft_entry_release(entry);

    return e;
}

void raftNotifyMembershipEvent(raft_server_t *raft, void *user_data, raft_node_t *raft_node,
        raft_entry_t *entry, raft_membership_e type)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) user_data;
    RaftCfgChange *cfgchange;
    raft_node_id_t my_id = raft_get_nodeid(raft);
    Node *node;

    switch (type) {
        case RAFT_MEMBERSHIP_ADD:
            /* When raft_add_node() is called explicitly, we get no entry so we
             * have nothing to do.
             */
            if (!entry) {
                addUsedNodeId(rr, my_id);
                break;
            }

            /* Ignore our own node, as we don't maintain a Node structure for it */
            assert(entry->type == RAFT_LOGTYPE_ADD_NODE || entry->type == RAFT_LOGTYPE_ADD_NONVOTING_NODE);
            cfgchange = (RaftCfgChange *) entry->data;
            if (cfgchange->id == my_id) {
                break;
            }

            /* Allocate a new node */
            node = NodeCreate(rr, cfgchange->id, &cfgchange->addr);
            assert(node != NULL);

            addUsedNodeId(rr, cfgchange->id);

            raft_node_set_udata(raft_node, node);
            break;

        case RAFT_MEMBERSHIP_REMOVE:
            node = raft_node_get_udata(raft_node);
            if (node != NULL) {
                ConnAsyncTerminate(node->conn);
                raft_node_set_udata(raft_node, NULL);
            }
            break;

        default:
            assert(0);
    }

}

static char *raftMembershipInfoString(raft_server_t *raft)
{
    size_t buflen = 1024;
    char *buf = RedisModule_Calloc(1, buflen);
    int i;

    buf = catsnprintf(buf, &buflen, "term:%ld index:%ld nodes:",
        raft_get_current_term(raft),
        raft_get_current_idx(raft));
    for (i = 0; i < raft_get_num_nodes(raft); i++) {
        raft_node_t *rn = raft_get_node_from_idx(raft, i);
        Node *n = raft_node_get_udata(rn);
        char addr[512];

        if (n) {
            snprintf(addr, sizeof(addr) - 1, "%s:%u",
                n->addr.host, n->addr.port);
        } else {
            addr[0] = '-';
            addr[1] = '\0';
        }

        buf = catsnprintf(buf, &buflen, " id=%d,voting=%d,active=%d,addr=%s",
            raft_node_get_id(rn),
            raft_node_is_voting(rn),
            raft_node_is_active(rn),
            addr);
    }

    return buf;
}

static void handleTransferLeaderComplete(raft_server_t *raft, raft_transfer_state_e state);

/* just keep libraft callbacks together
 * so this just calls the redisraft RaftReq compleition function, which is kept together with its functions
 */
static void raftNotifyTransferEvent(raft_server_t *raft, void *user_data, raft_transfer_state_e state)
{
    handleTransferLeaderComplete(raft, state);
}

static void raftNotifyStateEvent(raft_server_t *raft, void *user_data, raft_state_e state)
{
    switch (state) {
        case RAFT_STATE_FOLLOWER:
            LOG_INFO("State change: Node is now a follower, term %ld",
                    raft_get_current_term(raft));
            break;
        case RAFT_STATE_PRECANDIDATE:
            LOG_INFO("State change: Election starting, node is now a pre-candidate, term %ld",
                     raft_get_current_term(raft));
            break;
        case RAFT_STATE_CANDIDATE:
            LOG_INFO("State change: Node is now a candidate, term %ld",
                    raft_get_current_term(raft));
            break;
        case RAFT_STATE_LEADER:
            LOG_INFO("State change: Node is now a leader, term %ld",
                    raft_get_current_term(raft));
            break;
        default:
            break;
    }

    char *s = raftMembershipInfoString(raft);
    LOG_INFO("Cluster Membership: %s", s);
    RedisModule_Free(s);
}

/* Apply some backpressure for the node. Helps in two cases, first we don't
 * want to create many appaendentries messages for the node if we don't get
 * replies. Otherwise, it might cause out of memory. Second, we want to send
 * entries in batches for performance reasons. We are effectively batching
 * entries until we get replies from the previous ones. */
static int raftBackpressure(raft_server_t *raft, void *user_data, raft_node_t *raft_node)
{
    RedisRaftCtx *rr = &redis_raft;
    Node *node = raft_node_get_udata(raft_node);
    if (node->pending_raft_response_num >= rr->config->max_appendentries_inflight) {
        /* Don't send append req to this node */
        return 1;
    }

    return 0;
}

raft_cbs_t redis_raft_callbacks = {
    .send_requestvote = raftSendRequestVote,
    .send_appendentries = raftSendAppendEntries,
    .persist_vote = raftPersistVote,
    .persist_term = raftPersistTerm,
    .log = raftLog,
    .get_node_id = raftLogGetNodeId,
    .applylog = raftApplyLog,
    .node_has_sufficient_logs = raftNodeHasSufficientLogs,
    .send_snapshot = raftSendSnapshot,
    .load_snapshot = raftLoadSnapshot,
    .clear_snapshot = raftClearSnapshot,
    .get_snapshot_chunk = raftGetSnapshotChunk,
    .store_snapshot_chunk = raftStoreSnapshotChunk,
    .notify_membership_event = raftNotifyMembershipEvent,
    .notify_state_event = raftNotifyStateEvent,
    .send_timeoutnow = raftSendTimeoutNow,
    .notify_transfer_event = raftNotifyTransferEvent,
    .backpressure = raftBackpressure
};

/* ------------------------------------ Raft Thread ------------------------------------ */

/*
 * Handling of the Redis Raft context, including its own thread and
 * async I/O loop.
 */

RRStatus applyLoadedRaftLog(RedisRaftCtx *rr)
{
    /* Make sure the log we're going to apply matches the RDB we've loaded */
    if (rr->snapshot_info.loaded) {
        if (strcmp(rr->snapshot_info.dbid, rr->log->dbid)) {
            PANIC("Log and snapshot have different dbids: [log=%s/snapshot=%s]",
                    rr->log->dbid, rr->snapshot_info.dbid);
        }
        if (rr->snapshot_info.last_applied_term < rr->log->snapshot_last_term) {
            PANIC("Log term (%lu) does not match snapshot term (%lu), aborting.",
                    rr->log->snapshot_last_term, rr->snapshot_info.last_applied_term);
        }
        if (rr->snapshot_info.last_applied_idx + 1 < rr->log->snapshot_last_idx) {
            PANIC("Log initial index (%lu) does not match snapshot last index (%lu), aborting.",
                    rr->log->snapshot_last_idx, rr->snapshot_info.last_applied_idx);
        }
    } else {
        /* If there is no snapshot, the log should also not refer to it */
        if (rr->log->snapshot_last_idx) {
            PANIC("Log refers to snapshot (term=%lu/index=%lu which was not loaded, aborting.",
                    rr->log->snapshot_last_term, rr->log->snapshot_last_idx);
        }
    }

    /* Reset the log if snapshot is more advanced */
    if (RaftLogCurrentIdx(rr->log) < rr->snapshot_info.last_applied_idx) {
        RaftLogImpl.reset(rr, rr->snapshot_info.last_applied_idx + 1,
            rr->snapshot_info.last_applied_term);
    }

    raft_set_commit_idx(rr->raft, rr->snapshot_info.last_applied_idx);

    memcpy(rr->snapshot_info.dbid, rr->log->dbid, RAFT_DBID_LEN);
    rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

    raft_set_snapshot_metadata(rr->raft, rr->snapshot_info.last_applied_term,
            rr->snapshot_info.last_applied_idx);

    raft_apply_all(rr->raft);

    raft_set_current_term(rr->raft, rr->meta.term);
    raft_vote_for_nodeid(rr->raft, rr->meta.vote);

    LOG_INFO("Raft Meta: loaded current term=%lu, vote=%d", rr->meta.term, rr->meta.vote);
    LOG_INFO("Raft state after applying log: log_count=%lu, current_idx=%lu, last_applied_idx=%lu",
            raft_get_log_count(rr->raft),
            raft_get_current_idx(rr->raft),
            raft_get_last_applied_idx(rr->raft));

    return RR_OK;
}

/* Check if Redis is loading an RDB file */
static bool checkRedisLoading(RedisRaftCtx *rr)
{
    char *val = RedisInfoGetParam(rr, "persistence", "loading");
    assert(val != NULL);
    bool loading = (!strcmp(val, "1"));

    RedisModule_Free(val);
    return loading;
}

RRStatus loadRaftLog(RedisRaftCtx *rr);

static void handleLoadingState(RedisRaftCtx *rr)
{
    if (!checkRedisLoading(rr)) {
        /* If Redis loaded a snapshot (RDB), log some information and configure the
         * raft library as necessary.
         */
        LOG_INFO("Loading: Redis loading complete, snapshot %s",
                rr->snapshot_info.loaded ? "LOADED" : "NOT LOADED");

        /* If id is configured, confirm the log matches.  If not, we set it from
         * the log.
         */
        if (!rr->config->id) {
            rr->config->id = rr->log->node_id;
        } else {
            if (rr->config->id != rr->log->node_id) {
                PANIC("Raft log node id [%d] does not match configured id [%d]",
                        rr->log->node_id, rr->config->id);
            }
        }

        if (!rr->sharding_info->shard_groups_num) {
            AddBasicLocalShardGroup(rr);
        }

        initRaftLibrary(rr);

        raft_node_t *self = raft_add_non_voting_node(rr->raft, NULL, rr->config->id, 1);
        if (!self) {
            PANIC("Failed to create local Raft node [id %d]", rr->config->id);
        }

        initSnapshotTransferData(rr);

        if (rr->snapshot_info.loaded) {
            createOutgoingSnapshotMmap(rr);
            configureFromSnapshot(rr);
        }

        if (loadRaftLog(rr) == RR_OK) {
            if (rr->log->snapshot_last_term) {
                LOG_INFO("Loading: Log starts from snapshot term=%lu, index=%lu",
                        rr->log->snapshot_last_term, rr->log->snapshot_last_idx);
            } else {
                LOG_INFO("Loading: Log is complete.");
            }

            applyLoadedRaftLog(rr);
            rr->state = REDIS_RAFT_UP;
        } else {
            rr->state = REDIS_RAFT_UNINITIALIZED;
        }
    }
}

void callRaftPeriodic(RedisModuleCtx *ctx, void *arg)
{
    RedisRaftCtx *rr = arg;
    int ret;

    RedisModule_CreateTimer(rr->ctx, redis_raft.config->raft_interval,
                            callRaftPeriodic, rr);

    /* If we're in LOADING state, we need to wait for Redis to finish loading before
     * we can apply the log.
     */
    if (rr->state == REDIS_RAFT_LOADING) {
        handleLoadingState(rr);
    }

    /* Proceed only if we're initialized */
    if (rr->state != REDIS_RAFT_UP) {
        return;
    }

    /* If we're creating a persistent snapshot, check if we're done */
    if (rr->snapshot_in_progress) {
        SnapshotResult sr;

        ret = pollSnapshotStatus(rr, &sr);
        if (ret == -1) {
            LOG_ERROR("Snapshot operation failed, cancelling.");
            cancelSnapshot(rr, &sr);
        }  else if (ret) {
            LOG_DEBUG("Snapshot operation completed successfully.");
            finalizeSnapshot(rr, &sr);
        } /* else we're still in progress */
    }

    ret = raft_periodic(rr->raft, rr->config->raft_interval);
    if (ret == RAFT_ERR_SHUTDOWN) {
        shutdownAfterRemoval(rr);
    }

    assert(ret == 0);

    /* Compact cache */
    if (rr->config->raft_log_max_cache_size) {
        EntryCacheCompact(rr->logcache, rr->config->raft_log_max_cache_size);
    }

    /* Initiate snapshot if log size exceeds raft-log-file-max */
    if (!rr->snapshot_in_progress && rr->config->raft_log_max_file_size &&
            raft_get_num_snapshottable_logs(rr->raft) > 0 &&
            rr->log->file_size > rr->config->raft_log_max_file_size) {
        LOG_DEBUG("Raft log file size is %lu, initiating snapshot.",
                rr->log->file_size);
        initiateSnapshot(rr);
    }

    /* Call cluster */
    if (rr->config->sharding) {
        ShardingPeriodicCall(rr);
    }
}

/* A callback that invokes HandleNodeStates(), to handle node connection
 * management (reconnects, etc.).
 */
void callHandleNodeStates(RedisModuleCtx *ctx, void *arg)
{
    RedisRaftCtx *rr = arg;
    RedisModule_CreateTimer(rr->ctx, redis_raft.config->reconnect_interval,
                            callHandleNodeStates, rr);

    HandleIdleConnections(rr);
    HandleNodeStates(rr);
}

static int appendRaftCfgChangeEntry(RedisRaftCtx *rr, int type, int id, NodeAddr *addr)
{

    raft_entry_t *ety = raft_entry_new(sizeof(RaftCfgChange));
    RaftCfgChange *cfgchange = (RaftCfgChange *) ety->data;

    cfgchange->id = id;
    if (addr != NULL) {
        cfgchange->addr = *addr;
    }

    ety->id = rand();
    ety->type = type;

    RaftLogImpl.append(rr, ety);
    raft_entry_release(ety);

    return 0;
}

raft_node_id_t makeRandomNodeId(RedisRaftCtx *rr)
{
    unsigned int tmp;
    raft_node_id_t id;

    /* Generate a random id and validate:
     * 1. It's not zero (reserved value)
     * 2. Avoid negative numbers for better convenience
     * 3. Skip existing IDs, if library is already initialized
     */

    do {
        RedisModule_GetRandomBytes((unsigned char *) &tmp, sizeof(tmp));
        id = (raft_node_id_t) (tmp & ~(1u << 31));
    } while (!id || (rr->raft && raft_get_node(rr->raft, id) != NULL) || hasNodeIdBeenUsed(rr, id));

    return id;
}

RRStatus initRaftLog(RedisModuleCtx *ctx, RedisRaftCtx *rr)
{
    rr->log = RaftLogCreate(rr->config->raft_log_filename,
                            rr->snapshot_info.dbid, 1, 0, rr->config);
    if (!rr->log) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to initialize Raft log");
        return RR_ERROR;
    }

    return RR_OK;
}

RRStatus initCluster(RedisModuleCtx *ctx, RedisRaftCtx *rr, RedisRaftConfig *config, char *id)
{
    /* Initialize dbid */
    memcpy(rr->snapshot_info.dbid, id, RAFT_DBID_LEN);
    rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

    /* This is the first node, so there are no used node ids yet */
    rr->snapshot_info.used_node_ids = NULL;

    /* If node id was not specified, make up one */
    if (!config->id) {
        config->id = makeRandomNodeId(rr);
    }

    addUsedNodeId(rr, config->id);

    /* Initialize log */
    if (initRaftLog(ctx, rr) == RR_ERROR) {
        return RR_ERROR;
    }

    AddBasicLocalShardGroup(rr);

    initRaftLibrary(rr);

    /* Create our own node */
    raft_node_t *self = raft_add_node(rr->raft, NULL, config->id, 1);
    if (!self) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to initialize raft_node");
        return RR_ERROR;
    }

    initSnapshotTransferData(rr);

    /* Become leader and create initial entry */
    rr->state = REDIS_RAFT_UP;
    raft_set_current_term(rr->raft, 1);
    raft_become_leader(rr->raft);

    /* We need to create the first add node entry.  Because we don't have
     * callbacks set yet, we also need to manually push this in our log
     * as well.
     *
     * In the future it could be nicer to have callbacks already set and this
     * be done automatically (but some other raft lib fixes would be required).
     */

    if (appendRaftCfgChangeEntry(rr, RAFT_LOGTYPE_ADD_NODE, config->id, &config->addr) != 0) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to append initial configuration entry");
        return RR_ERROR;
    }

    return RR_OK;
}

bool hasNodeIdBeenUsed(RedisRaftCtx *rr, raft_node_id_t node_id) {
    for (NodeIdEntry *e = rr->snapshot_info.used_node_ids; e != NULL; e = e->next) {
        if (e->id == node_id) {
            return true;
        }
    }
    return false;
}

void addUsedNodeId(RedisRaftCtx *rr, raft_node_id_t node_id) {
    if (hasNodeIdBeenUsed(rr, node_id)) return;

    NodeIdEntry *entry = RedisModule_Alloc(sizeof(NodeIdEntry));
    entry->id = node_id;
    entry->next = rr->snapshot_info.used_node_ids;
    rr->snapshot_info.used_node_ids = entry;
}

static int loadEntriesCallback(void *arg, raft_entry_t *entry, raft_index_t idx)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) arg;

    if (rr->snapshot_info.last_applied_term <= entry->term &&
            rr->snapshot_info.last_applied_idx < rr->log->index &&
            raft_entry_is_cfg_change(entry)) {
        raft_handle_append_cfg_change(rr->raft, entry, idx);
    }

    return 0;
}

RRStatus loadRaftLog(RedisRaftCtx *rr)
{
    int entries = RaftLogLoadEntries(rr->log, loadEntriesCallback, rr);
    if (entries < 0) {
        LOG_ERROR("Failed to read Raft log");
        return RR_ERROR;
    } else {
        LOG_INFO("Loading: Log loaded, %d entries, snapshot last term=%lu, index=%lu",
               entries, rr->log->snapshot_last_term, rr->log->snapshot_last_idx);
    }

    return RR_OK;
}

static void initRaftLibrary(RedisRaftCtx *rr)
{
    rr->raft = raft_new_with_log(&RaftLogImpl, rr);
    if (!rr->raft) {
        PANIC("Failed to initialize Raft library");
    }
    raft_set_election_timeout(rr->raft, rr->config->election_timeout);
    raft_set_request_timeout(rr->raft, rr->config->request_timeout);

    // To avoid performance hit, get library logs only if log level is debug
    if (redis_raft_loglevel != LOGLEVEL_DEBUG) {
        redis_raft_callbacks.log = NULL;
    }

    raft_set_callbacks(rr->raft, &redis_raft_callbacks, rr);
    raft_set_auto_flush(rr->raft, 0);
}

static void configureFromSnapshot(RedisRaftCtx *rr)
{
    SnapshotCfgEntry *c;

    LOG_INFO("Loading: Snapshot: applied term=%lu index=%lu",
            rr->snapshot_info.last_applied_term,
            rr->snapshot_info.last_applied_idx);

    for (c = rr->snapshot_info.cfg; c != NULL; c = c->next) {
        LOG_INFO("Loading: Snapshot config: node id=%u [%s:%u], voting=%u",
                c->id, c->addr.host, c->addr.port, c->voting);
    }

    /* Load configuration loaded from the snapshot into Raft library.
     */
    configRaftFromSnapshotInfo(rr);
    raft_end_load_snapshot(rr->raft);
    raft_set_snapshot_metadata(rr->raft, rr->snapshot_info.last_applied_term,
            rr->snapshot_info.last_applied_idx);
}

RRStatus RedisRaftInit(RedisModuleCtx *ctx, RedisRaftCtx *rr, RedisRaftConfig *config)
{
    memset(rr, 0, sizeof(RedisRaftCtx));

    rr->ctx = RedisModule_GetDetachedThreadSafeContext(ctx);
    rr->config = config;

    /* for backwards compatibility with older redis version that don't support "0v"m */
    if (RedisModule_GetContextFlagsAll() & REDISMODULE_CTX_FLAGS_RESP3) {
        rr->resp_call_fmt = "0v";
    } else {
        rr->resp_call_fmt = "v";
    }

    /* Client state for MULTI support */
    multiClientState = RedisModule_CreateDict(ctx);

    /* Read configuration from Redis */
    if (ConfigReadFromRedis(rr) == RR_ERROR) {
        PANIC("Raft initialization failed: invalid Redis configuration!");
    }

    /* Cluster configuration */
    ShardingInfoInit(rr);

    /* Raft log exists -> go into RAFT_LOADING state:
     *
     * Redis will load RDB as a snapshot, if it exists. When done,
     * handleLoadingState() will be called, initialize Raft library and load
     * log file.
     *
     * Raft log does not exist -> go into RAFT_UNINITIALIZED state:
     *
     * Nothing will happen until users will initiate a RAFT.CLUSTER INIT
     * or RAFT.CLUSTER JOIN command.
     */
    if (RaftMetaRead(&rr->meta, rr->config->raft_log_filename) == RR_OK &&
        (rr->log = RaftLogOpen(rr->config->raft_log_filename, rr->config, 0)) != NULL) {
        rr->state = REDIS_RAFT_LOADING;
    } else {
        rr->state = REDIS_RAFT_UNINITIALIZED;
    }

    threadPoolInit(&rr->thread_pool, 5);

    return RR_OK;
}


/* ------------------------------------ RaftReq ------------------------------------ */

/* Free a RaftReq structure.
 *
 * If it is associated with a blocked client, it will be unblocked and
 * the thread safe context released as well.
 */

void RaftReqFree(RaftReq *req)
{
    TRACE("RaftReqFree: req=%p, req->ctx=%p, req->client=%p", req, req->ctx, req->client);

    switch (req->type) {
        case RR_APPENDENTRIES:
            /* Note: we only free the array of entries but not actual entries, as they
             * are owned by the log and should be freed when the log entry is freed.
             */
            if (req->r.appendentries.msg.entries) {
                int i;
                for (i = 0; i < req->r.appendentries.msg.n_entries; i++) {
                    raft_entry_t *e = req->r.appendentries.msg.entries[i];
                    if (e) {
                        raft_entry_release(e);
                    }
                }
                RedisModule_Free(req->r.appendentries.msg.entries);
                req->r.appendentries.msg.entries = NULL;
            }
            break;
        case RR_REDISCOMMAND:
            if (req->ctx && req->r.redis.cmds.size) {
                RaftRedisCommandArrayFree(&req->r.redis.cmds);
            }
            // TODO: hold a reference from entry so we can disconnect our req
            break;
        case RR_CLUSTER_JOIN:
            NodeAddrListFree(req->r.cluster_join.addr);
            break;
        case RR_SHARDGROUP_ADD:
            if (req->r.shardgroup_add) {
                ShardGroupFree(req->r.shardgroup_add);
                req->r.shardgroup_add = NULL;
            }
            break;
        case RR_SHARDGROUPS_REPLACE:
            if (req->r.shardgroups_replace.shardgroups != NULL) {
                for (unsigned int i = 0; i < req->r.shardgroups_replace.len; i++) {
                    ShardGroupFree(req->r.shardgroups_replace.shardgroups[i]);
                }
                RedisModule_Free(req->r.shardgroups_replace.shardgroups);
                req->r.shardgroups_replace.shardgroups = NULL;
            }
            break;
    }
    if (req->ctx) {
        RedisModule_FreeThreadSafeContext(req->ctx);
        req->ctx = NULL;

        RedisModule_UnblockClient(req->client, NULL);
    }
    RedisModule_Free(req);
}

RaftReq *RaftReqInit(RedisModuleCtx *ctx, enum RaftReqType type)
{
    RaftReq *req = RedisModule_Calloc(1, sizeof(RaftReq));
    if (ctx != NULL) {
        req->client = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
        req->ctx = RedisModule_GetThreadSafeContext(req->client);
    }
    req->type = type;

    TRACE("RaftReqInit: req=%p, type=%s, client=%p, ctx=%p",
            req, RaftReqTypeStr[req->type], req->client, req->ctx);

    return req;
}

/* ------------------------------------ RaftReq Implementation ------------------------------------ */

/*
 * Implementation of specific request types.
 */

void handleTransferLeaderComplete(raft_server_t *raft, raft_transfer_state_e state)
{
    if (!redis_raft.transfer_req) {
        LOG_ERROR("leader transfer update: but no req to correlate it to!");
        return;
    }

    char e[40];
    switch (state) {
        case RAFT_STATE_LEADERSHIP_TRANSFER_EXPECTED_LEADER:
            RedisModule_ReplyWithSimpleString(redis_raft.transfer_req->ctx, "OK");
            break;
        case RAFT_STATE_LEADERSHIP_TRANSFER_UNEXPECTED_LEADER:
            RedisModule_ReplyWithError(redis_raft.transfer_req->ctx, "ERR different node elected leader");
            break;
        case RAFT_STATE_LEADERSHIP_TRANSFER_TIMEOUT:
            RedisModule_ReplyWithError(redis_raft.transfer_req->ctx, "ERR transfer timed out");
            break;
        default:
            snprintf(e, 40,"ERR unknown case: %d", state);
            RedisModule_ReplyWithError(redis_raft.transfer_req->ctx, e);
            break;
    }

    RaftReqFree(redis_raft.transfer_req);
    redis_raft.transfer_req = NULL;
}

void handleAppendEntries(RedisRaftCtx *rr, RaftReq *req)
{
    msg_appendentries_response_t response;
    int err;

    if (checkRaftState(rr, req->ctx) == RR_ERROR) {
        goto exit;
    }

    if ((err = raft_recv_appendentries(rr->raft,
                raft_get_node(rr->raft, req->r.appendentries.src_node_id),
                &req->r.appendentries.msg,
                &response)) != 0) {
        char msg[128];
        snprintf(msg, sizeof(msg)-1, "operation failed, error %d", err);
        RedisModule_ReplyWithError(req->ctx, msg);
        goto exit;
    }

    RedisModule_ReplyWithArray(req->ctx, 4);
    RedisModule_ReplyWithLongLong(req->ctx, response.term);
    RedisModule_ReplyWithLongLong(req->ctx, response.success);
    RedisModule_ReplyWithLongLong(req->ctx, response.current_idx);
    RedisModule_ReplyWithLongLong(req->ctx, response.msg_id);

exit:
    RaftReqFree(req);
}

static void handleReadOnlyCommand(void *arg, int can_read)
{
    RaftReq *req = arg;

    if (!can_read) {
        RedisModule_ReplyWithError(req->ctx, "TIMEOUT no quorum for read");
        goto exit;
    }

    RaftRedisCommandArray * cmds = &req->r.redis.cmds;

    if (cmds->slot == -1 || validateRaftRedisCommandArray(&redis_raft, cmds, req->ctx) == RR_OK) {
        executeRaftRedisCommandArray(cmds, req->ctx, req->ctx);
    }

exit:
    RaftReqFree(req);
}

/* Handle MULTI/EXEC transactions here.
 *
 * If this logic was applied, the request is freeed (if necessary) and the
 * return value is true, indicating no further processing is required.
 * Otherwise, the main handleRedisCommand() flow is applied.
 *
 * 1) On MULTI, we create a RaftRedisCommandArray which will store all
 *    user commands as they are queued.
 * 2) On EXEC, we remove the RaftRedisCommandArray with all queued commands
 *    from multiClientState, place it in the RaftReq and let the rest of the
 *    code handle it.
 * 3) On DISCARD we simply remove the queued commands array.
 *
 * Important notes:
 * 1) Although as a module we don't need to pass MULTI to Redis, we still keep
 *    it in the array, because when processing the array we want to distinguish
 *    between a MULTI with a single command and a non-MULTI scenario.
 * 2) If our RaftReq contains multiple commands, we assume it was received as
 *    a RAFT.ENTRY in which case we need to process it as an EXEC.  That means
 *    we don't need to reply with +OK and multiple +QUEUED, but just process
 *    the commands atomically.  This is common when a follower proxies a batch
 *    of commands to a leader: the follower handles the user interaction and
 *    the leader only handles the execution (when the user issued the final
 *    EXEC).
 *
 * Error handling rules (derived from Redis):
 * 1) MULTI and DISCARD should always succeed.
 * 2) If we encounter errors inside a MULTI context, we need to flag that
 *    transaction as failed but keep going until EXEC/DISCARD.
 * 3) RAFT related state checks can be postponed and evaluated only at the
 *    time of EXEC.
 */

typedef struct MultiState {
    RaftRedisCommandArray cmds;
    bool asking;
    bool error;
} MultiState;

static void freeMultiState(MultiState *multiState)
{
    RaftRedisCommandArrayFree(&multiState->cmds);
    RedisModule_Free(multiState);
}

static void freeMultiExecState(unsigned long long client_id)
{

    MultiState *multiState = NULL;

    if (RedisModule_DictDelC(multiClientState, &client_id, sizeof(client_id),
               &multiState) == REDISMODULE_OK) {
       if (multiState) {
           freeMultiState(multiState);
       }
    }
}

static bool handleMultiExec(RedisRaftCtx *rr, RaftReq *req)
{
    unsigned long long client_id = RedisModule_GetClientId(req->ctx);

    /* Get Multi state */
    MultiState *multiState = RedisModule_DictGetC(multiClientState, &client_id, sizeof(client_id), NULL);

    /* Is this a MULTI command? */
    RaftRedisCommand *cmd = req->r.redis.cmds.commands[0];
    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[0], &cmd_len);
    if (req->r.redis.cmds.len == 1 && cmd_len == 5 && !strncasecmp(cmd_str, "MULTI", 5)) {
        if (multiState) {
            RedisModule_ReplyWithError(req->ctx, "ERR MULTI calls can not be nested");
        } else {
            multiState = RedisModule_Calloc(sizeof(MultiState), 1);
            multiState->asking = req->r.redis.cmds.asking;

            RedisModule_DictSetC(multiClientState, &client_id, sizeof(client_id), multiState);

            /* We put the MULTI as the first command in the array, as we still need to
             * distinguish single-MULTI array from a single command.
             */
            RaftRedisCommandArrayMove(&multiState->cmds, &req->r.redis.cmds);

            RedisModule_ReplyWithSimpleString(req->ctx, "OK");
        }

        RaftReqFree(req);
        return true;
    }

    if (cmd_len == 4 && !strncasecmp(cmd_str, "EXEC", 4)) {
        if (!multiState) {
            RedisModule_ReplyWithError(req->ctx, "ERR EXEC without MULTI");
            RaftReqFree(req);
            return true;
        }

        int ctx_flags = RedisModule_GetContextFlags(req->ctx);

        /* Check if we can execute:
         * 1) No errors in the transaction so far;
         * 2) No dirty watch.
         */
        if (multiState->error) {
            RedisModule_ReplyWithError(req->ctx, "EXECABORT Transaction discarded because of previous errors.");
            RaftReqFree(req);
            req = NULL;
        } else if (ctx_flags & REDISMODULE_CTX_FLAGS_MULTI_DIRTY) {
            RedisModule_ReplyWithNull(req->ctx);
            RaftReqFree(req);
            req = NULL;
        } else {
            /* Just swap our commands with the EXEC command and proceed. */
            RaftRedisCommandArrayFree(&req->r.redis.cmds);
            RaftRedisCommandArrayMove(&req->r.redis.cmds, &multiState->cmds);
            req->r.redis.cmds.asking = multiState->asking;
        }

        RedisModule_DictDelC(multiClientState, &client_id, sizeof(client_id), NULL);
        freeMultiState(multiState);
        return req == NULL;
    }

    if (cmd_len == 7 && !strncasecmp(cmd_str, "DISCARD", 7)) {
        if (!multiState) {
            RedisModule_ReplyWithError(req->ctx, "ERR DISCARD without MULTI");
        } else {
            RedisModule_DictDelC(multiClientState, &client_id, sizeof(client_id), NULL);
            freeMultiState(multiState);

            RedisModule_ReplyWithSimpleString(req->ctx, "OK");
        }

        RaftReqFree(req);
        return true;
    }

    /* Are we in MULTI? */
    if (multiState) {
        /* We have to detct commands that are unsupported or must not be intercepted,
         * and reject the transaction.
         */
        unsigned int cmd_flags = CommandSpecGetAggregateFlags(&req->r.redis.cmds, 0);

        if (cmd_flags & CMD_SPEC_UNSUPPORTED) {
            RedisModule_ReplyWithError(req->ctx, "ERR not supported by RedisRaft");
            multiState->error = 1;
        } else if (cmd_flags & CMD_SPEC_DONT_INTERCEPT) {
            RedisModule_ReplyWithError(req->ctx, "ERR not supported by RedisRaft inside MULTI/EXEC");
            multiState->error = 1;
        } else {
            RaftRedisCommandArrayMove(&multiState->cmds, &req->r.redis.cmds);
            RedisModule_ReplyWithSimpleString(req->ctx, "QUEUED");
        }

        RaftReqFree(req);
        return true;
    }

    return false;
}

void handleInfoCommand(RedisRaftCtx *rr, RaftReq *req)
{
    RedisModuleCallReply *reply;
    RaftRedisCommand *cmd = req->r.redis.cmds.commands[0];

    /* Skip "INFO" string */
    int argc = cmd->argc - 1;
    RedisModuleString **argv = cmd->argc == 1 ? NULL : &cmd->argv[1];

    enterRedisModuleCall();
    reply = RedisModule_Call(req->ctx, "INFO", "v", argv, argc);
    exitRedisModuleCall();

    size_t info_len;
    const char *info = RedisModule_CallReplyStringPtr(reply, &info_len);

    char *pos = strstr(info, "cluster_enabled:0");
    if (pos) {
        /* Always return cluster_enabled:1 */
        *(pos + strlen("cluster_enabled:")) = '1';
    }

    RedisModule_ReplyWithStringBuffer(req->ctx, info, info_len);
    RedisModule_FreeCallReply(reply);
    RaftReqFree(req);
}

/* Handle interception of Redis commands that have a different
 * implementation in RedisRaft.
 *
 * This is logically similar to handleMultiExec but implemented
 * separately for readability purposes.
 *
 * Currently intercepted commands:
 * - CLUSTER
 * - INFO
 *
 * Returns true if the command was intercepted, in which case the RaftReq has
 * been replied to and freed.
 */
static bool handleInterceptedCommands(RedisRaftCtx *rr, RaftReq *req)
{
    RaftRedisCommand *cmd = req->r.redis.cmds.commands[0];
    size_t len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[0], &len);

    if (len == strlen("CLUSTER") && strncasecmp(cmd_str, "CLUSTER", len) == 0) {
        handleClusterCommand(rr, req);
        return true;
    }

    if (len == strlen("INFO") && strncasecmp(cmd_str, "INFO", len) == 0) {
        handleInfoCommand(rr, req);
        return true;
    }

    return false;
}

ShardGroup * getSlotOwnerShardGroup(RedisRaftCtx *rr, int slot) {
    ShardGroup *ret = NULL;

    ret = rr->sharding_info->stable_slots_map[slot];
    if (!ret) {
        ret = rr->sharding_info->migrating_slots_map[slot];
    }

    return ret;
}

/* When sharding is enabled, handle sharding aspects before processing
 * the request:
 *
 * 1. Compute hash slot of all associated keys and validate there's no cross-slot
 *    violation.
 * 2. Update the request's hash_slot for future reference.
 * 3. If the hash slot is associated with a foreign ShardGroup, perform a redirect.
 * 4. If the hash slot is not mapped, produce a CLUSTERDOWN error.
 */

static RRStatus handleSharding(RedisRaftCtx *rr, RaftReq *req)
{
    int slot;
    RaftRedisCommandArray *cmds = &req->r.redis.cmds;

    if (computeHashSlotOrReplyError(rr, req->ctx, cmds, &slot) != RR_OK) {
        return RR_ERROR;
    }

    /* If command has no keys, continue */
    if (slot == -1) {
        return RR_OK;
    }

    cmds->slot = slot;

    /* Make sure hash slot is mapped and handled locally. */
    ShardGroup * osg = getSlotOwnerShardGroup(rr, slot);
    ShardGroup *sg = osg;
    if (req->r.redis.cmds.asking) {
        sg = rr->sharding_info->importing_slots_map[slot];
    }
    if (!sg) {
        RedisModule_ReplyWithError(req->ctx, "CLUSTERDOWN Hash slot is not served");
        return RR_ERROR;
    }

    /* If accessing a foreign shardgroup, issue a redirect. We use round-robin
     * to all nodes to compensate for the fact we do not have an up-to-date knowledge
     * of who the leader is and whether or not some configuration has changed since
     * last refresh (when refresh is implemented, in the future).
     */
    if (!sg->local) {
        if (!osg) {
            RedisModule_ReplyWithError(req->ctx, "CLUSTERDOWN Hash slot is not served");
            return RR_ERROR;
        }

        if (osg->next_redir >= osg->nodes_num) {
            osg->next_redir = 0;
        }
        replyRedirect(req->ctx, slot, &osg->nodes[osg->next_redir++].addr);
        return RR_ERROR;
    }

    return RR_OK;
}

void handleAsking(RaftReq *req)
{
    RaftRedisCommand *cmd = req->r.redis.cmds.commands[0];
    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[0], &cmd_len);

    if (cmd_len == 6 && !strncasecmp(cmd_str, "ASKING", 6)) {
        req->r.redis.cmds.asking = true;
        RedisModule_FreeString(NULL, req->r.redis.cmds.commands[0]->argv[0]);
        for (int i = 1; i < req->r.redis.cmds.commands[0]->argc; i++) {
            req->r.redis.cmds.commands[0]->argv[i-1] = req->r.redis.cmds.commands[0]->argv[i];
        }
        req->r.redis.cmds.commands[0]->argc--;
    }
}

void handleRedisCommand(RedisRaftCtx *rr,RaftReq *req)
{
    Node *leader_proxy = NULL;

    /* MULTI/EXEC bundling takes place only if we have a single command. If we have multiple
     * commands we've received this as a RAFT.ENTRY input and bundling, probably through a
     * proxy, and bundling was done before.
     */

    handleAsking(req);

    if (req->r.redis.cmds.len == 1) {
        if (handleMultiExec(rr, req)) {
            return;
        }
    }

    /* Handle intercepted commands. We do this also on non-leader nodes or if we don't
     * have a leader, so it's up to the commands to check these conditions if they have to.
     */
    if (handleInterceptedCommands(rr, req)) {
        return;
    }

    /* Check that we're part of a boostrapped cluster and not in the middle of
     * joining or loading data.
     */
    if (checkRaftState(rr, req->ctx) == RR_ERROR) {
        goto exit;
    }

    /* When we're in cluster mode, go through handleSharding. This will perform
     * hash slot validation and return an error / redirection if necessary. We do this
     * before checkLeader() to avoid multiple redirect hops.
     */
    if (rr->config->sharding && handleSharding(rr, req) != RR_OK) {
        goto exit;
    }

    /* Confirm that we're the leader and handle redirect or proxying if not. */
    if (checkLeader(rr, req->ctx, rr->config->follower_proxy ? &leader_proxy : NULL) == RR_ERROR) {
        goto exit;
    }

    /* Proxy */
    if (leader_proxy) {
        if (ProxyCommand(rr, req, leader_proxy) != RR_OK) {
            RedisModule_ReplyWithError(req->ctx, "NOTLEADER Failed to proxy command");
            goto exit;
        }
        return;
    }

    /* Handle the special case of read-only commands here: if quroum reads
     * are enabled schedule the request to be processed when we have a guarantee
     * we're still a leader. Otherwise, just process the reads.
     *
     * Normally we can expect a single command in the request, unless it is a
     * MULTI/EXEC transaction in which case all queued commands are handled at once.
     */
    unsigned int cmd_flags = CommandSpecGetAggregateFlags(&req->r.redis.cmds, CMD_SPEC_WRITE);
    if (cmd_flags & CMD_SPEC_UNSUPPORTED) {
        RedisModule_ReplyWithError(req->ctx, "ERR not supported by RedisRaft");
        goto exit;
    } else if (cmd_flags & CMD_SPEC_READONLY && !(cmd_flags & CMD_SPEC_WRITE)) {
        if (rr->config->quorum_reads) {
            raft_queue_read_request(rr->raft, handleReadOnlyCommand, req);
        } else {
            handleReadOnlyCommand(req, 1);
        }
        return;
    }

    raft_entry_t *entry = RaftRedisCommandArraySerialize(&req->r.redis.cmds);
    entry->id = rand();
    entry->type = RAFT_LOGTYPE_NORMAL;
    entryAttachRaftReq(rr, entry, req);
    int e = raft_recv_entry(rr->raft, entry, &req->r.redis.response);
    if (e != 0) {
        replyRaftError(req->ctx, e);
        entryDetachRaftReq(rr, entry);
        raft_entry_release(entry);
        goto exit;
    }

    raft_entry_release(entry);

    /* Unless applied by raft_apply_all() (and freed by it), the request
     * is pending so we don't free it or unblock the client.
     */
    return;

exit:
    RaftReqFree(req);
}

void handleInfo(RedisRaftCtx *rr, RaftReq *req)
{
    size_t slen = 1024;
    char *s = RedisModule_Calloc(1, slen);
    const char* role;

    if (!rr->raft) {
        role = "-";
    } else {
        switch (raft_get_state(rr->raft)) {
            case RAFT_STATE_FOLLOWER:
                role = "follower";
                break;
            case RAFT_STATE_LEADER:
                role = "leader";
                break;
            case RAFT_STATE_PRECANDIDATE:
                role = "pre-candidate";
                break;
            case RAFT_STATE_CANDIDATE:
                role = "candidate";
                break;
            default:
                role = "(none)";
                break;
        }
    }

    raft_node_t *me = rr->raft ? raft_get_my_node(rr->raft) : NULL;
    s = catsnprintf(s, &slen,
            "# RedisRaft\r\n"
            "redisraft_version:%s\r\n"
            "redisraft_git_sha1:%s\r\n"
            "\r\n"
            "# Raft\r\n"
            "dbid:%s\r\n"
            "node_id:%d\r\n"
            "state:%s\r\n"
            "role:%s\r\n"
            "is_voting:%s\r\n"
            "leader_id:%d\r\n"
            "current_term:%ld\r\n"
            "num_nodes:%d\r\n"
            "num_voting_nodes:%d\r\n",
            REDISRAFT_VERSION,
            REDISRAFT_GIT_SHA1,
            rr->snapshot_info.dbid,
            rr->config->id,
            getStateStr(rr),
            role,
            me ? (raft_node_is_voting(raft_get_my_node(rr->raft)) ? "yes" : "no") : "-",
            rr->raft ? raft_get_leader_id(rr->raft) : -1,
            rr->raft ? raft_get_current_term(rr->raft) : 0,
            rr->raft ? raft_get_num_nodes(rr->raft) : 0,
            rr->raft ? raft_get_num_voting_nodes(rr->raft) : 0);

    int i;
    long long now = RedisModule_Milliseconds();
    int num_nodes = rr->raft ? raft_get_num_nodes(rr->raft) : 0;
    for (i = 0; i < num_nodes; i++) {
        raft_node_t *rnode = raft_get_node_from_idx(rr->raft, i);
        Node *node = raft_node_get_udata(rnode);
        if (!node) {
            continue;
        }

        s = catsnprintf(s, &slen,
                "node%d:id=%d,state=%s,voting=%s,addr=%s,port=%d,last_conn_secs=%lld,conn_errors=%lu,conn_oks=%lu\r\n",
                i, node->id, ConnGetStateStr(node->conn),
                raft_node_is_voting(rnode) ? "yes" : "no",
                node->addr.host, node->addr.port,
                node->conn->last_connected_time ? (now - node->conn->last_connected_time)/1000 : -1,
                node->conn->connect_errors, node->conn->connect_oks);
    }

    s = catsnprintf(s, &slen,
            "\r\n# Log\r\n"
            "log_entries:%ld\r\n"
            "current_index:%ld\r\n"
            "commit_index:%ld\r\n"
            "last_applied_index:%ld\r\n"
            "file_size:%lu\r\n"
            "cache_memory_size:%lu\r\n"
            "cache_entries:%lu\r\n"
            "client_attached_entries:%lu\r\n"
            "fsync_count:%"PRIu64"\r\n"
            "fsync_max_microseconds:%"PRIu64"\r\n"
            "fsync_avg_microseconds:%"PRIu64"\r\n",
            rr->raft ? raft_get_log_count(rr->raft) : 0,
            rr->raft ? raft_get_current_idx(rr->raft) : 0,
            rr->raft ? raft_get_commit_idx(rr->raft) : 0,
            rr->raft ? raft_get_last_applied_idx(rr->raft) : 0,
            rr->log ? rr->log->file_size : 0,
            rr->logcache ? rr->logcache->entries_memsize : 0,
            rr->logcache ? rr->logcache->len : 0,
            rr->client_attached_entries,
            rr->log ? rr->log->fsync_count : 0,
            rr->log ? rr->log->fsync_max : 0,
            rr->log ? rr->log->fsync_count ?
                        (rr->log->fsync_total / rr->log->fsync_count) : 0 : 0);

    s = catsnprintf(s, &slen,
            "\r\n# Snapshot\r\n"
            "snapshot_in_progress:%s\r\n"
            "snapshots_loaded:%lu\r\n"
            "snapshots_created:%lu\r\n",
            rr->snapshot_in_progress ? "yes" : "no",
            rr->snapshots_loaded,
            rr->snapshots_created);

    s = catsnprintf(s, &slen,
            "\r\n# Clients\r\n"
            "clients_in_multi_state:%"PRIu64"\r\n"
            "proxy_reqs:%llu\r\n"
            "proxy_failed_reqs:%llu\r\n"
            "proxy_failed_responses:%llu\r\n"
            "proxy_outstanding_reqs:%ld\r\n",
            RedisModule_DictSize(multiClientState),
            rr->proxy_reqs,
            rr->proxy_failed_reqs,
            rr->proxy_failed_responses,
            rr->proxy_outstanding_reqs);

    RedisModule_ReplyWithStringBuffer(req->ctx, s, strlen(s));
    RedisModule_Free(s);

    RaftReqFree(req);
}

void handleClusterInit(RedisRaftCtx *rr, RaftReq *req)
{
    if (checkRaftNotLoading(rr, req->ctx) == RR_ERROR) {
        goto exit;
    }

    if (rr->state != REDIS_RAFT_UNINITIALIZED) {
        RedisModule_ReplyWithError(req->ctx, "ERR Already cluster member");
        goto exit;
    }

    if (initCluster(req->ctx, rr, rr->config, req->r.cluster_init.id) == RR_ERROR) {
        RedisModule_ReplyWithError(req->ctx, "ERR Failed to initialize, check logs");
        goto exit;
    }

    char reply[RAFT_DBID_LEN+5];
    snprintf(reply, sizeof(reply) - 1, "OK %s", rr->snapshot_info.dbid);

    rr->state = REDIS_RAFT_UP;
    RedisModule_ReplyWithSimpleString(req->ctx, reply);

    LOG_INFO("Raft Cluster initialized, node id: %d, dbid: %s", rr->config->id, rr->snapshot_info.dbid);
exit:
    RaftReqFree(req);
}

void HandleClusterJoinCompleted(RedisRaftCtx *rr, RaftReq *req)
{
    /* Initialize Raft log.  We delay this operation as we want to create the log
     * with the proper dbid which is only received now.
     */

    rr->log = RaftLogCreate(rr->config->raft_log_filename, rr->snapshot_info.dbid,
            rr->snapshot_info.last_applied_term, rr->snapshot_info.last_applied_idx,
            rr->config);
    if (!rr->log) {
        PANIC("Failed to initialize Raft log");
    }

    AddBasicLocalShardGroup(rr);

    initRaftLibrary(rr);

    /* Create our own node */
    raft_node_t *self = raft_add_non_voting_node(rr->raft, NULL, rr->config->id, 1);
    if (!self) {
        PANIC("Failed to initialize raft_node");
    }

    initSnapshotTransferData(rr);

    rr->state = REDIS_RAFT_UP;

    RedisModule_ReplyWithSimpleString(req->ctx, "OK");
    RaftReqFree(req);
}

void handleClientDisconnect(RedisRaftCtx *rr, RaftReq *req)
{
    freeMultiExecState(req->r.client_disconnect.client_id);
    RaftReqFree(req);
}

/* Apply a SHARDGROUP Add and Update log entries by deserializing the payload and
 * updating the in-memory shardgroup configuration.
 *
 * If the entry holds a user_data pointing to a RaftReq, this implies we're
 * applying an operation performed by a local client (vs. one received from
 * persisted log, or through AppendEntries). In that case, we also need to
 * generate the reply as the client is blocked waiting for it.
 */
void applyShardGroupChange(RedisRaftCtx *rr, raft_entry_t *entry)
{
    RRStatus ret;

    ShardGroup *sg;

    if ((sg = ShardGroupDeserialize(entry->data, entry->data_len)) == NULL) {
        LOG_ERROR("Failed to deserialize ADD_SHARDGROUP payload: [%.*s]",
                entry->data_len, entry->data);
        return;
    }

    switch (entry->type) {
        case RAFT_LOGTYPE_ADD_SHARDGROUP:
            if ((ret = ShardingInfoAddShardGroup(rr, sg)) != RR_OK)
                LOG_ERROR("Failed to add a shardgroup");
            break;
        case RAFT_LOGTYPE_UPDATE_SHARDGROUP:
            if ((ret = ShardingInfoUpdateShardGroup(rr, sg)) != RR_OK)
                LOG_ERROR("Failed to update shardgroup");
            break;
        default:
            PANIC("Unknown entry type %d", entry->type);
            break;
    }

    /* If we have an attached client, handle the reply */
    if (entry->user_data) {
        RaftReq *req = entry->user_data;
        if (ret == RR_OK) {
            RedisModule_ReplyWithSimpleString(req->ctx, "OK");
        } else {
            RedisModule_ReplyWithError(req->ctx, "ERR Invalid ShardGroup Update");
        }
        RaftReqFree(req);

        entry->user_data = NULL;
    }
}

void replaceShardGroups(RedisRaftCtx *rr, raft_entry_t *entry)
{
    // 1. reset sharding info
    ShardingInfo *si = rr->sharding_info;

    if (si->shard_group_map != NULL) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);

        char *key;
        size_t key_len;
        ShardGroup *data;

        while ((key = RedisModule_DictNextC(iter, &key_len, (void **) &data)) != NULL) {
            ShardGroupFree(data);
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(rr->ctx, si->shard_group_map);
        si->shard_group_map = NULL;
    }

    si->shard_groups_num = 0;

    si->shard_group_map = RedisModule_CreateDict(rr->ctx);
    for (int i = 0; i <= REDIS_RAFT_HASH_MAX_SLOT; i++) {
        si->stable_slots_map[i] = NULL;
        si->importing_slots_map[i] = NULL;
        si->migrating_slots_map[i] = NULL;
    }

    /* 2. iterate over payloads
     * payload structure
     * "# shard groups:payload1 len:payload1:....:payload n len:payload n:"
     */
    char *payload = entry->data;

    char *nl = strchr(payload, '\n');
    char *endptr;
    size_t num_payloads = strtoul(payload, &endptr, 10);
    RedisModule_Assert(endptr == nl);
    payload = nl + 1;

    for (int i = 0; i < num_payloads; i++) {
        nl = strchr(payload, '\n');
        size_t payload_len = strtoul(payload, &endptr, 10);
        RedisModule_Assert(endptr == nl);
        payload = nl + 1;

        ShardGroup *sg;
        if ((sg = ShardGroupDeserialize(payload, payload_len)) == NULL) {
            LOG_ERROR("Failed to deserialize shardgroup payload: [%.*s]", (int) payload_len, payload);
            return;
        }

        /* local cluster has an empty string sg.id */
        if (!strncmp(sg->id, rr->log->dbid, RAFT_DBID_LEN)) {
            sg->local = true;
        }

        RedisModule_Assert(ShardingInfoAddShardGroup(rr, sg) == RR_OK);
        payload += payload_len + 1;
    }

    /* If we have an attached client, handle the reply */
    if (entry->user_data) {
        RaftReq *req = entry->user_data;

        RedisModule_ReplyWithSimpleString(req->ctx, "OK");
        RaftReqFree(req);

        entry->user_data = NULL;
    }
}

/* Handle adding of ShardGroup.
 * FIXME: Currently this is done locally, should instead create a
 * custom Raft log entry which calls addShardGroup when applied only.
 */
void handleShardGroupAdd(RedisRaftCtx *rr, RaftReq *req)
{
    /* Must be done on a leader */
    if (checkRaftState(rr, req->ctx) == RR_ERROR ||
        checkLeader(rr, req->ctx, NULL) == RR_ERROR) {
        goto exit;
    }

    /* Validate now before pushing this as a log entry. */
    if (ShardingInfoValidateShardGroup(rr, req->r.shardgroup_add) != RR_OK) {
        RedisModule_ReplyWithError(req->ctx, "ERR invalid shardgroup configuration. Consult the logs for more info.");
        goto exit;
    }

    if (ShardGroupAppendLogEntry(rr, req->r.shardgroup_add,
                                 RAFT_LOGTYPE_ADD_SHARDGROUP, req) == RR_ERROR) goto exit;

    return;

exit:
    RedisModule_ReplyWithError(req->ctx, "failed, please check logs.");
    RaftReqFree(req);
}

void handleShardGroupsReplace(RedisRaftCtx *rr, RaftReq *req)
{
    /* Must be done on a leader */
    if (checkRaftState(rr, req->ctx) == RR_ERROR ||
        checkLeader(rr, req->ctx, NULL) == RR_ERROR) {
        goto exit;
    }

    if (ShardGroupsAppendLogEntry(rr, req->r.shardgroups_replace.len, req->r.shardgroups_replace.shardgroups,
                                 RAFT_LOGTYPE_REPLACE_SHARDGROUPS, req) == RR_ERROR) {
        RedisModule_ReplyWithError(req->ctx, "failed, please check logs.");
        goto exit;
    }

    /* wait till return till after update is applied */
    return;

exit:
    RaftReqFree(req);
}

/* Handles RAFT.SHARDGROUP GET which includes:
 * - Description of the local cluster as a shardgroup, including all nodes
 * - Description of remote shardgroups as last tracked.
 */

void handleShardGroupGet(RedisRaftCtx *rr, RaftReq *req)
{
    /* Must be done on a leader */
    if (checkRaftState(rr, req->ctx) == RR_ERROR ||
        checkLeader(rr, req->ctx, NULL) == RR_ERROR) {
        goto exit;
    }

    ShardGroup *sg = getShardGroupById(rr, rr->log->dbid);
    /* 2 arrays
     * 1. slot ranges -> each element is a 3 element array start/end/type
     * 2. nodes -> each element is a 2 element array id/address
     */
    RedisModule_ReplyWithArray(req->ctx, 3);
    RedisModule_ReplyWithCString(req->ctx, redis_raft.snapshot_info.dbid);
    RedisModule_ReplyWithArray(req->ctx, sg->slot_ranges_num);
    for(int i = 0; i < sg->slot_ranges_num; i++) {
        ShardGroupSlotRange *sr = &sg->slot_ranges[i];
        RedisModule_ReplyWithArray(req->ctx, 3);
        RedisModule_ReplyWithLongLong(req->ctx, sr->start_slot);
        RedisModule_ReplyWithLongLong(req->ctx, sr->end_slot);
        RedisModule_ReplyWithLongLong(req->ctx, sr->type);
    }

    RedisModule_ReplyWithArray(req->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    int node_count = 0;
    for (int i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *raft_node = raft_get_node_from_idx(rr->raft, i);
        if (!raft_node_is_active(raft_node))
            continue;

        NodeAddr *addr = NULL;
        if (raft_node == raft_get_my_node(rr->raft)) {
            addr = &rr->config->addr;
        } else {
            Node *node = raft_node_get_udata(raft_node);
            if (!node) continue;

            addr = &node->addr;
        }

        node_count++;
        RedisModule_ReplyWithArray(req->ctx, 2);
        char node_id[RAFT_SHARDGROUP_NODEID_LEN+1];
        snprintf(node_id, sizeof(node_id), "%s%08x", rr->log->dbid, raft_node_get_id(raft_node));
        RedisModule_ReplyWithStringBuffer(req->ctx, node_id, strlen(node_id));

        char addrstr[512];
        snprintf(addrstr, sizeof(addrstr), "%s:%u", addr->host, addr->port);
        RedisModule_ReplyWithStringBuffer(req->ctx, addrstr, strlen(addrstr));

    }
    RedisModule_ReplySetArrayLength(req->ctx, node_count);
exit:
    RaftReqFree(req);
}

/* Callback for fsync thread. This will be triggerred by fsync thread but will
 * be called by Redis thread. */
void handleFsyncCompleted(void *result)
{
    FsyncThreadResult *rs = result;
    RedisRaftCtx *rr = &redis_raft;

    rr->log->fsync_count++;
    rr->log->fsync_total += rs->time;
    rr->log->fsync_max = MAX(rs->time, rr->log->fsync_max);

    int e = raft_flush(rr->raft, rs->fsync_index);
    if (e == RAFT_ERR_SHUTDOWN) {
        shutdownAfterRemoval(rr);
    }
    RedisModule_Assert(e == 0);

    RedisModule_Free(rs);
}

/* Redis thread callback, called just before Redis main thread goes to sleep,
 * e.g epoll_wait(). At this point, we've read all new network messages for this
 * event loop iteration. We can trigger new appendentries messages for the new
 * entries and check for committing/applying new entries. */
void handleBeforeSleep(RedisRaftCtx *rr)
{
    raft_index_t flushed = 0;

    if (rr->state != REDIS_RAFT_UP) {
        return;
    }

    if (!raft_is_leader(rr->raft)) {
        return;
    }

    raft_index_t next = raft_get_index_to_sync(rr->raft);
    if (next > 0) {
        fflush(rr->log->file);

        if (rr->config->raft_log_fsync) {
            /* Trigger async fsync() for the current index */
            fsyncThreadAddTask(&rr->fsyncThread, fileno(rr->log->file), next);
        } else {
            /* Skipping fsync(), we can just update the sync'd index. */
            flushed = next;
        }
    }

    int e = raft_flush(rr->raft, flushed);
    if (e == RAFT_ERR_SHUTDOWN) {
        shutdownAfterRemoval(rr);
    }
    RedisModule_Assert(e == 0);
}