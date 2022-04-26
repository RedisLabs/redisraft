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

#define RAFTLIB_TRACE(fmt, ...) TRACE_MODULE(RAFTLIB, fmt, ##__VA_ARGS__)

const char *RaftReqTypeStr[] = {
    [0]                       = "<undef>",
    [RR_GENERIC]              = "RR_GENERIC",
    [RR_CLUSTER_JOIN]         = "RR_CLUSTER_JOIN",
    [RR_CFGCHANGE_ADDNODE]    = "RR_CFGCHANGE_ADDNODE",
    [RR_CFGCHANGE_REMOVENODE] = "RR_CFGCHANGE_REMOVENODE",
    [RR_REDISCOMMAND]         = "RR_REDISCOMMAND",
    [RR_DEBUG]                = "RR_DEBUG",
    [RR_SHARDGROUP_ADD]       = "RR_SHARDGROUP_ADD",
    [RR_SHARDGROUPS_REPLACE]  = "RR_SHARDGROUPS_REPLACE",
    [RR_SHARDGROUP_LINK]      = "RR_SHARDGROUP_LINK",
    [RR_TRANSFER_LEADER]      = "RR_TRANSFER_LEADER"
};

/* Forward declarations */
static void configureFromSnapshot(RedisRaftCtx *rr);
static void applyShardGroupChange(RedisRaftCtx *rr, raft_entry_t *entry);
static void replaceShardGroups(RedisRaftCtx *rr, raft_entry_t *entry);

/* ------------------------------------ Common helpers ------------------------------------ */

void shutdownAfterRemoval(RedisRaftCtx *rr)
{
    LOG_NOTICE("*** NODE REMOVED, SHUTTING DOWN.");

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
 * If reply_ctx is non-NULL, replies are delivered to it.
 * Otherwise, no replies are delivered.
 */
void RaftExecuteCommandArray(RedisModuleCtx *ctx,
                             RedisModuleCtx *reply_ctx,
                             RaftRedisCommandArray *cmds)
{
    int i;

    HandleAsking(cmds);

    for (i = 0; i < cmds->len; i++) {
        RaftRedisCommand *c = cmds->commands[i];

        size_t cmdlen;
        const char *cmd = RedisModule_StringPtrLen(c->argv[0], &cmdlen);

        /* We need to handle MULTI as a special case:
        * 1. Skip the command (no need to execute MULTI in a Module context).
        * 2. If we're returning a response, group it as an array (multibulk).
        */

        if (i == 0 && cmdlen == 5 && !strncasecmp(cmd, "MULTI", 5)) {
            if (reply_ctx) {
                RedisModule_ReplyWithArray(reply_ctx, cmds->len - 1);
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

/*
 * Execution of Raft log on the local instance.
 *
 * There are two variants:
 * 1) Execution of a raft entry received from another node.
 * 2) Execution of a locally initiated command.
 */
static void executeLogEntry(RedisRaftCtx *rr, raft_entry_t *entry, raft_index_t entry_idx)
{
    RedisModule_Assert(entry->type == RAFT_LOGTYPE_NORMAL);

    RaftReq *req = entry->user_data;

    if (req) {
        RaftExecuteCommandArray(req->ctx, req->ctx, &req->r.redis.cmds);
        entryDetachRaftReq(rr, entry);
        RaftReqFree(req);
    } else {
        RaftRedisCommandArray tmp = {0};

        if (RaftRedisCommandArrayDeserialize(&tmp,
                                             entry->data,
                                             entry->data_len) != RR_OK) {
            PANIC("Invalid Raft entry");
        }

        RaftExecuteCommandArray(rr->ctx, NULL, &tmp);
        RaftRedisCommandArrayFree(&tmp);
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
        NODE_LOG_WARNING(node, "invalid RAFT.REQUESTVOTE reply");
        return;
    }

    raft_requestvote_resp_t response = {
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
        LOG_DEBUG("raft_recv_requestvote_response failed, error %d", ret);
    }
}


static int raftSendRequestVote(raft_server_t *raft, void *user_data,
        raft_node_t *raft_node, raft_requestvote_req_t *msg)
{
    Node *node = (Node *) raft_node_get_udata(raft_node);

    if (!ConnIsConnected(node->conn)) {
        NODE_TRACE(node, "not connected, state=%s", ConnGetStateStr(node->conn));
        return 0;
    }

    /* RAFT.REQUESTVOTE <src_node_id> <term> <candidate_id> <last_log_idx> <last_log_term> */
    if (redisAsyncCommand(ConnGetRedisCtx(node->conn), handleRequestVoteResponse,
                node, "RAFT.REQUESTVOTE %d %d %d:%ld:%d:%ld:%ld",
                raft_node_get_id(raft_node),
                raft_get_nodeid(raft),
                msg->prevote,
                msg->term,
                msg->candidate_id,
                msg->last_log_idx,
                msg->last_log_term) != REDIS_OK) {
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
        NODE_LOG_WARNING(node, "invalid RAFT.AE reply");
        return;
    }

    raft_appendentries_resp_t response = {
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
        raft_node_t *raft_node, raft_appendentries_req_t *msg)
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
        NODE_LOG_WARNING(node, "invalid RAFT.TIMEOUT_NOW reply");
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
        LOG_WARNING("ERROR: RaftMetaWrite()");
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
        LOG_WARNING("ERROR: RaftMetaWrite()");
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
            RAFTLIB_TRACE("<raftlib> node{%p,%d}: %s", n, n->id, buf);
            return;
        }
    }
    RAFTLIB_TRACE("<raftlib> %s", buf);
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

    LOG_DEBUG("node:%d has sufficient logs, adding as voting node.", node->id);

    raft_entry_req_t *entry = raft_entry_new(sizeof(RaftCfgChange));
    entry->id = rand();
    entry->type = RAFT_LOGTYPE_ADD_NODE;

    RaftCfgChange *cfgchange = (RaftCfgChange *) entry->data;
    cfgchange->id = node->id;
    cfgchange->addr = node->addr;

    raft_entry_resp_t response;

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

static void handleTransferLeaderComplete(raft_server_t *raft, raft_leader_transfer_e result);

/* just keep libraft callbacks together
 * so this just calls the redisraft RaftReq compleition function, which is kept together with its functions
 */
static void raftNotifyTransferEvent(raft_server_t *raft, void *user_data, raft_leader_transfer_e result)
{
    handleTransferLeaderComplete(raft, result);
}

static void raftNotifyStateEvent(raft_server_t *raft, void *user_data, raft_state_e state)
{
    switch (state) {
        case RAFT_STATE_FOLLOWER:
            LOG_NOTICE("State change: Node is now a follower, term %ld",
                       raft_get_current_term(raft));
            break;
        case RAFT_STATE_PRECANDIDATE:
            LOG_NOTICE("State change: Election starting, node is now a pre-candidate, term %ld",
                       raft_get_current_term(raft));
            break;
        case RAFT_STATE_CANDIDATE:
            LOG_NOTICE("State change: Node is now a candidate, term %ld",
                       raft_get_current_term(raft));
            break;
        case RAFT_STATE_LEADER:
            LOG_NOTICE("State change: Node is now a leader, term %ld",
                       raft_get_current_term(raft));
            break;
        default:
            break;
    }

    char *s = raftMembershipInfoString(raft);
    LOG_NOTICE("Cluster Membership: %s", s);
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

    LOG_NOTICE("Raft Meta: loaded current term=%lu, vote=%d", rr->meta.term, rr->meta.vote);
    LOG_NOTICE("Raft state after applying log: log_count=%lu, current_idx=%lu, last_applied_idx=%lu",
               raft_get_log_count(rr->raft),
               raft_get_current_idx(rr->raft),
               raft_get_last_applied_idx(rr->raft));

    return RR_OK;
}

/* Check if Redis is loading an RDB file */
static bool checkRedisLoading(RedisRaftCtx *rr)
{
    int err;
    RedisModuleServerInfoData *in;

    in = RedisModule_GetServerInfo(rr->ctx, "persistence");
    int val = (int) RedisModule_ServerInfoGetFieldSigned(in, "loading", &err);
    RedisModule_Assert(err == REDISMODULE_OK);
    RedisModule_FreeServerInfo(rr->ctx, in);

    return val;
}

RRStatus loadRaftLog(RedisRaftCtx *rr);

static void handleLoadingState(RedisRaftCtx *rr)
{
    if (!checkRedisLoading(rr)) {
        /* If Redis loaded a snapshot (RDB), log some information and configure the
         * raft library as necessary.
         */
        LOG_NOTICE("Loading: Redis loading complete, snapshot %s",
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

        RaftLibraryInit(rr, false);
        initSnapshotTransferData(rr);

        if (rr->snapshot_info.loaded) {
            createOutgoingSnapshotMmap(rr);
            configureFromSnapshot(rr);
        }

        if (loadRaftLog(rr) == RR_OK) {
            if (rr->log->snapshot_last_term) {
                LOG_NOTICE("Loading: Log starts from snapshot term=%lu, index=%lu",
                           rr->log->snapshot_last_term, rr->log->snapshot_last_idx);
            } else {
                LOG_NOTICE("Loading: Log is complete.");
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
            LOG_WARNING("Snapshot operation failed, cancelling.");
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
        LOG_WARNING("Failed to read Raft log");
        return RR_ERROR;
    } else {
        LOG_NOTICE("Loading: Log loaded, %d entries, snapshot last term=%lu, index=%lu",
                   entries, rr->log->snapshot_last_term, rr->log->snapshot_last_idx);
    }

    return RR_OK;
}

void RaftLibraryInit(RedisRaftCtx *rr, bool cluster_init)
{
    raft_node_t *node;

    rr->raft = raft_new_with_log(&RaftLogImpl, rr);
    if (!rr->raft) {
        PANIC("Failed to initialize Raft library");
    }

    int eltimeo = rr->config->election_timeout;
    int reqtimeo = rr->config->request_timeout;

    if (raft_config(rr->raft, 1, RAFT_CONFIG_ELECTION_TIMEOUT, eltimeo) != 0 ||
        raft_config(rr->raft, 1, RAFT_CONFIG_REQUEST_TIMEOUT, reqtimeo) != 0 ||
        raft_config(rr->raft, 1, RAFT_CONFIG_AUTO_FLUSH, 0) != 0 ||
        raft_config(rr->raft, 1, RAFT_CONFIG_NONBLOCKING_APPLY, 1) != 0) {
        PANIC("Failed to configure libraft");
    }

    raft_set_callbacks(rr->raft, &redis_raft_callbacks, rr);

    node = raft_add_non_voting_node(rr->raft, NULL, rr->config->id, 1);
    if (!node) {
        PANIC("Failed to create local Raft node [id %d]", rr->config->id);
    }

    if (cluster_init) {
        if (raft_set_current_term(rr->raft, 1) != 0 ||
            raft_become_leader(rr->raft) != 0) {
            PANIC("Failed to init raft library");
        }

        RaftCfgChange cfg = {
            .id = rr->config->id,
            .addr = rr->config->addr
        };

        raft_entry_resp_t resp = {0};
        raft_entry_t *ety = raft_entry_new(sizeof(cfg));

        ety->id = rand();
        ety->type = RAFT_LOGTYPE_ADD_NODE;
        memcpy(ety->data, &cfg, sizeof(cfg));

        if (raft_recv_entry(rr->raft, ety, &resp) != 0) {
            PANIC("Failed to init raft library");
        }
        raft_entry_release(ety);
    }

    rr->state = REDIS_RAFT_UP;
}

static void configureFromSnapshot(RedisRaftCtx *rr)
{
    SnapshotCfgEntry *c;

    LOG_NOTICE("Loading: Snapshot: applied term=%lu index=%lu",
               rr->snapshot_info.last_applied_term,
               rr->snapshot_info.last_applied_idx);

    for (c = rr->snapshot_info.cfg; c != NULL; c = c->next) {
        LOG_NOTICE("Loading: Snapshot config: node id=%u [%s:%u], voting=%u",
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
    rr->multi_client_state = RedisModule_CreateDict(ctx);

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
    TRACE("RaftReqFree: req=%p, req->ctx=%p, req->client=%p",
          req, req->ctx, req->client);

    if (req->type == RR_REDISCOMMAND) {
        if (req->r.redis.cmds.size) {
            RaftRedisCommandArrayFree(&req->r.redis.cmds);
        }
    }

    if (req->ctx) {
        RedisModule_FreeThreadSafeContext(req->ctx);
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

void handleTransferLeaderComplete(raft_server_t *raft, raft_leader_transfer_e result)
{
    char buf[64];
    RedisRaftCtx *rr = &redis_raft;

    if (!rr->transfer_req) {
        LOG_WARNING("leader transfer update: but no req to correlate it to!");
        return;
    }

    RedisModuleCtx *ctx = rr->transfer_req->ctx;

    switch (result) {
        case RAFT_LEADER_TRANSFER_EXPECTED_LEADER:
            RedisModule_ReplyWithSimpleString(ctx, "OK");
            break;
        case RAFT_LEADER_TRANSFER_UNEXPECTED_LEADER:
            RedisModule_ReplyWithError(ctx, "ERR different node elected leader");
            break;
        case RAFT_LEADER_TRANSFER_TIMEOUT:
            RedisModule_ReplyWithError(ctx, "ERR transfer timed out");
            break;
        default:
            snprintf(buf, sizeof(buf),"ERR unknown case: %d", result);
            RedisModule_ReplyWithError(ctx, buf);
            break;
    }

    RaftReqFree(rr->transfer_req);
    rr->transfer_req = NULL;
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
        LOG_WARNING("Failed to deserialize ADD_SHARDGROUP payload: [%.*s]",
                    entry->data_len, entry->data);
        return;
    }

    switch (entry->type) {
        case RAFT_LOGTYPE_ADD_SHARDGROUP:
            if ((ret = ShardingInfoAddShardGroup(rr, sg)) != RR_OK)
                LOG_WARNING("Failed to add a shardgroup");
            break;
        case RAFT_LOGTYPE_UPDATE_SHARDGROUP:
            if ((ret = ShardingInfoUpdateShardGroup(rr, sg)) != RR_OK)
                LOG_WARNING("Failed to update shardgroup");
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
    int num_payloads = (int) strtoul(payload, &endptr, 10);
    RedisModule_Assert(endptr == nl);
    payload = nl + 1;

    for (int i = 0; i < num_payloads; i++) {
        nl = strchr(payload, '\n');
        size_t payload_len = strtoul(payload, &endptr, 10);
        RedisModule_Assert(endptr == nl);
        payload = nl + 1;

        ShardGroup *sg;
        if ((sg = ShardGroupDeserialize(payload, payload_len)) == NULL) {
            LOG_WARNING("Failed to deserialize shardgroup payload: [%.*s]", (int) payload_len, payload);
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