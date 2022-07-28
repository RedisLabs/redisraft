/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include "redisraft.h"

#include "entrycache.h"
#include "fsync.h"
#include "log.h"
#include "meta.h"
#include "node_addr.h"
#include "serialization.h"
#include "snapshot.h"
#include "threadpool.h"
#include "util.h"

#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

RedisModuleCtx *redisraft_log_ctx = NULL;

int redisraft_loglevel = LOG_LEVEL_NOTICE;

const char *redisraft_loglevels[] = {
    REDISMODULE_LOGLEVEL_DEBUG,
    REDISMODULE_LOGLEVEL_VERBOSE,
    REDISMODULE_LOGLEVEL_NOTICE,
    REDISMODULE_LOGLEVEL_WARNING,
};

int redisraft_loglevel_enums[] = {
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_VERBOSE,
    LOG_LEVEL_NOTICE,
    LOG_LEVEL_WARNING,
    LOG_LEVEL_COUNT,
};

RedisRaftCtx redis_raft = {0};

/* This is needed for newer pthread versions to properly link and work */
#ifdef LINUX
void *__dso_handle;
#endif

#define VALID_NODE_ID(x) ((x) > 0)

RaftReq *entryDetachRaftReq(RedisRaftCtx *rr, raft_entry_t *entry)
{
    RaftReq *req = entry->user_data;

    if (!req) {
        return NULL;
    }

    entry->user_data = NULL;
    entry->free_func = NULL;

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
}

static void executeCommand(RedisRaftCtx *rr, RedisModuleCtx *ctx, Command *c)
{
    size_t cmd_len;
    const char *cmd = RedisModule_StringPtrLen(c->argv[0], &cmd_len);

    RedisModule_Free(rr->value);

    rr->value = RedisModule_Alloc(cmd_len + 1);
    memcpy(rr->value, cmd, cmd_len);
    rr->value[cmd_len] = '\0';

    if (ctx) {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
}

static void executeLogEntry(RedisRaftCtx *rr, raft_entry_t *entry, raft_index_t entry_idx)
{
    RedisModule_Assert(entry->type == RAFT_LOGTYPE_NORMAL);

    RaftReq *req = entry->user_data;

    if (req) {
        executeCommand(rr, req->ctx, &req->cmd);
        entryDetachRaftReq(rr, entry);
        RaftReqFree(req);
    } else {
        Command tmp = {0};

        if (CommandDecode(&tmp, entry->data, entry->data_len) == 0) {
            PANIC("Invalid Raft entry");
        }

        executeCommand(rr, NULL, &tmp);
        CommandFree(&tmp);
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
        return;
    }

    redisAsyncCommand(ConnGetRedisCtx(node->conn), NULL, NULL,
                      "RAFT.NODESHUTDOWN %d",
                      (int) raft_node_get_id(raft_node));
}

static void handleRequestVoteResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;
    redisReply *reply = r;

    NodeDismissPendingResponse(node);

    if (!reply) {
        LOG_DEBUG("RAFT.REQUESTVOTE failed: connection dropped.");
        ConnMarkDisconnected(node->conn);
        return;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        LOG_DEBUG("RAFT.REQUESTVOTE error: %s", reply->str);
        return;
    }

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 4 ||
        reply->element[0]->type != REDIS_REPLY_INTEGER ||
        reply->element[1]->type != REDIS_REPLY_INTEGER ||
        reply->element[2]->type != REDIS_REPLY_INTEGER ||
        reply->element[3]->type != REDIS_REPLY_INTEGER) {
        LOG_WARNING("invalid RAFT.REQUESTVOTE reply");
        return;
    }

    raft_requestvote_resp_t response = {
        .prevote = reply->element[0]->integer,
        .request_term = reply->element[1]->integer,
        .term = reply->element[2]->integer,
        .vote_granted = reply->element[3]->integer,
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);
    if (!raft_node) {
        LOG_DEBUG("RAFT.REQUESTVOTE stale reply.");
        return;
    }

    int ret = raft_recv_requestvote_response(rr->raft, raft_node, &response);
    if (ret != 0) {
        LOG_DEBUG("raft_recv_requestvote_response failed, error %d", ret);
    }
}

static int raftSendRequestVote(raft_server_t *raft,
                               void *user_data,
                               raft_node_t *raft_node,
                               raft_requestvote_req_t *msg)
{
    Node *node = raft_node_get_udata(raft_node);

    if (!ConnIsConnected(node->conn)) {
        return 0;
    }

    int ret = redisAsyncCommand(ConnGetRedisCtx(node->conn),
                                handleRequestVoteResponse,
                                node, "RAFT.REQUESTVOTE %d %d %d:%ld:%d:%ld:%ld",
                                raft_node_get_id(raft_node),
                                raft_get_nodeid(raft),
                                msg->prevote,
                                msg->term,
                                msg->candidate_id,
                                msg->last_log_idx,
                                msg->last_log_term);
    if (ret == REDIS_OK) {
        NodeAddPendingResponse(node);
    }

    return 0;
}

static void handleAppendEntriesResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;
    redisReply *reply = r;

    NodeDismissPendingResponse(node);

    if (!reply) {
        ConnMarkDisconnected(node->conn);
        return;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        return;
    }

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 4 ||
        reply->element[0]->type != REDIS_REPLY_INTEGER ||
        reply->element[1]->type != REDIS_REPLY_INTEGER ||
        reply->element[2]->type != REDIS_REPLY_INTEGER ||
        reply->element[3]->type != REDIS_REPLY_INTEGER) {
        LOG_WARNING("invalid RAFT.AE reply");
        return;
    }

    raft_appendentries_resp_t response = {
        .term = reply->element[0]->integer,
        .success = reply->element[1]->integer,
        .current_idx = reply->element[2]->integer,
        .msg_id = reply->element[3]->integer,
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);

    int ret = raft_recv_appendentries_response(rr->raft, raft_node, &response);
    if (ret != 0) {
        LOG_DEBUG("raft_recv_appendentries_response() : %d", ret);
    }
}

static int raftSendAppendEntries(raft_server_t *raft,
                                 void *user_data,
                                 raft_node_t *raft_node,
                                 raft_appendentries_req_t *msg)
{
    Node *node = raft_node_get_udata(raft_node);

    int argc = 5 + (msg->n_entries * 2);
    char **argv = NULL;
    size_t *argvlen = NULL;

    if (!ConnIsConnected(node->conn)) {
        return 0;
    }

    argv = RedisModule_Alloc(sizeof(argv[0]) * argc);
    argvlen = RedisModule_Alloc(sizeof(argvlen[0]) * argc);

    char dst_node[12];
    char src_node[12];
    char msg_str[100];
    char nentries_str[12];

    argv[0] = "RAFT.AE";
    argvlen[0] = strlen(argv[0]);

    argv[1] = dst_node;
    argvlen[1] = snprintf(dst_node, sizeof(dst_node), "%d", raft_node_get_id(raft_node));

    argv[2] = src_node;
    argvlen[2] = snprintf(src_node, sizeof(src_node), "%d", raft_get_nodeid(raft));

    argv[3] = msg_str;
    argvlen[3] = snprintf(msg_str, sizeof(msg_str) - 1, "%d:%ld:%ld:%ld:%ld:%lu",
                          msg->leader_id,
                          msg->term,
                          msg->prev_log_idx,
                          msg->prev_log_term,
                          msg->leader_commit,
                          msg->msg_id);

    argv[4] = nentries_str;
    argvlen[4] = snprintf(nentries_str, sizeof(nentries_str), "%ld", msg->n_entries);

    int i;
    for (i = 0; i < msg->n_entries; i++) {
        raft_entry_t *e = msg->entries[i];
        argv[5 + i * 2] = RedisModule_Alloc(64);
        argvlen[5 + i * 2] = snprintf(argv[5 + i * 2], 63, "%ld:%d:%d", e->term, e->id, e->type);
        argvlen[6 + i * 2] = e->data_len;
        argv[6 + i * 2] = e->data;
    }

    int ret = redisAsyncCommandArgv(ConnGetRedisCtx(node->conn),
                                    handleAppendEntriesResponse,
                                    node, argc, (const char **) argv, argvlen);
    if (ret == REDIS_OK) {
        NodeAddPendingResponse(node);
    }

    for (i = 0; i < msg->n_entries; i++) {
        RedisModule_Free(argv[5 + (i * 2)]);
    }

    RedisModule_Free(argv);
    RedisModule_Free(argvlen);

    return 0;
}

static void handleTimeoutNowResponse(redisAsyncContext *c, void *r, void *privdata)
{
    redisReply *reply = r;
    Node *node = privdata;

    NodeDismissPendingResponse(node);

    if (!reply) {
        ConnMarkDisconnected(node->conn);
        return;
    }

    if (reply->type == REDIS_REPLY_ERROR ||
        reply->type != REDIS_REPLY_STATUS) {
        LOG_WARNING("Error RAFT.TIMEOUT_NOW reply");
        return;
    }
}

static int raftSendTimeoutNow(raft_server_t *raft, raft_node_t *raft_node)
{
    Node *node = raft_node_get_udata(raft_node);

    if (!ConnIsConnected(node->conn)) {
        return 0;
    }

    int ret = redisAsyncCommand(ConnGetRedisCtx(node->conn),
                                handleTimeoutNowResponse,
                                node, "RAFT.TIMEOUT_NOW");
    if (ret == REDIS_OK) {
        NodeAddPendingResponse(node);
    }

    return 0;
}

static int raftPersistVote(raft_server_t *raft, void *user_data, raft_node_id_t vote)
{
    RedisRaftCtx *rr = user_data;
    raft_term_t term = raft_get_current_term(raft);

    if (RaftMetaWrite(&rr->meta, rr->config.log_filename, term, vote) != RR_OK) {
        LOG_WARNING("ERROR: RaftMetaWrite()");
        return RAFT_ERR_SHUTDOWN;
    }

    return 0;
}

static int raftPersistTerm(raft_server_t *raft, void *user_data, raft_term_t term, raft_node_id_t vote)
{
    RedisRaftCtx *rr = user_data;

    if (RaftMetaWrite(&rr->meta, rr->config.log_filename, term, vote) != RR_OK) {
        LOG_WARNING("ERROR: RaftMetaWrite()");
        return RAFT_ERR_SHUTDOWN;
    }

    return 0;
}

static int raftApplyLog(raft_server_t *raft, void *user_data,
                        raft_entry_t *entry, raft_index_t entry_idx)
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

            RedisModule_ReplyWithArray(req->ctx, 1);
            RedisModule_ReplyWithLongLong(req->ctx, cfg->id);
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
                exit(0);
            }

            if (raft_is_leader(raft)) {
                raftSendNodeShutdown(raft_get_node(raft, cfg->id));
            }
            break;
        case RAFT_LOGTYPE_NORMAL:
            executeLogEntry(rr, entry, entry_idx);
            break;
        default:
            break;
    }

    rr->snapshot_info.last_applied_term = entry->term;
    rr->snapshot_info.last_applied_idx = entry_idx;

    return 0;
}

static void raftLog(raft_server_t *raft, raft_node_id_t node,
                    void *user_data, const char *buf)
{
    raft_node_t *raft_node = raft_get_node(raft, node);
    if (raft_node) {
        Node *n = raft_node_get_udata(raft_node);
        if (n) {
            LOG_DEBUG("<raftlib> node{%p,%d}: %s", n, n->id, buf);
            return;
        }
    }
    LOG_DEBUG("<raftlib> %s", buf);
}

static raft_node_id_t raftLogGetNodeId(raft_server_t *raft, void *user_data,
                                       raft_entry_t *entry,
                                       raft_index_t entry_idx)
{
    RaftCfgChange *req = (RaftCfgChange *) entry->data;
    return req->id;
}

static int raftNodeHasSufficientLogs(raft_server_t *raft, void *user_data, raft_node_t *raft_node)
{
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
    assert(node != NULL);

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

bool hasNodeIdBeenUsed(RedisRaftCtx *rr, raft_node_id_t node_id)
{
    for (int i = 0; i < rr->snapshot_info.used_id_count; i++) {
        if (node_id == rr->snapshot_info.used_node_ids[i]) {
            return true;
        }
    }

    return false;
}

void addUsedNodeId(RedisRaftCtx *rr, raft_node_id_t node_id)
{
    RaftSnapshotInfo *info = &rr->snapshot_info;

    if (hasNodeIdBeenUsed(rr, node_id)) {
        return;
    }

    size_t alloc_size = sizeof(size_t) * (info->used_id_count + 1);

    info->used_node_ids = RedisModule_Realloc(info->used_node_ids, alloc_size);
    info->used_node_ids[info->used_id_count++] = node_id;
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

void raftNotifyMembershipEvent(raft_server_t *raft, void *user_data,
                               raft_node_t *raft_node, raft_entry_t *entry,
                               raft_membership_e type)
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

    buf = catsnprintf(buf, &buflen, "term:%ld index:%ld nodes:",
                      raft_get_current_term(raft),
                      raft_get_current_idx(raft));

    for (int i = 0; i < raft_get_num_nodes(raft); i++) {
        raft_node_t *rn = raft_get_node_from_idx(raft, i);
        Node *n = raft_node_get_udata(rn);
        char addr[512];

        if (n) {
            snprintf(addr, sizeof(addr) - 1, "%s:%u", n->addr.host, n->addr.port);
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

static void raftNotifyTransferEvent(raft_server_t *raft, void *user_data, raft_leader_transfer_e result)
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
            snprintf(buf, sizeof(buf), "ERR unknown case: %d", result);
            RedisModule_ReplyWithError(ctx, buf);
            break;
    }

    RaftReqFree(rr->transfer_req);
    rr->transfer_req = NULL;
}

static void raftNotifyStateEvent(raft_server_t *raft, void *user_data, raft_state_e state)
{
    raft_term_t term = raft_get_current_term(raft);

    switch (state) {
        case RAFT_STATE_FOLLOWER:
            LOG_NOTICE("State change: Node is now a follower, term: %ld", term);
            break;
        case RAFT_STATE_PRECANDIDATE:
            LOG_NOTICE("State change: Node is now a pre-candidate, term: %ld", term);
            break;
        case RAFT_STATE_CANDIDATE:
            LOG_NOTICE("State change: Node is now a candidate, term: %ld", term);
            break;
        case RAFT_STATE_LEADER:
            LOG_NOTICE("State change: Node is now a leader, term %ld", term);
            break;
        default:
            break;
    }

    char *s = raftMembershipInfoString(raft);
    LOG_NOTICE("Cluster Membership: %s", s);
    RedisModule_Free(s);
}

static int raftBackpressure(raft_server_t *raft, void *user_data, raft_node_t *raft_node)
{
    RedisRaftCtx *rr = &redis_raft;
    Node *node = raft_node_get_udata(raft_node);

    if (node->pending_raft_response_num >= rr->config.append_req_max_count) {
        /* Don't send append req to this node */
        return 1;
    }

    return 0;
}

static raft_time_t raftTimestamp(raft_server_t *raft, void *user_data)
{
    (void) raft;
    (void) user_data;

    return (raft_time_t) RedisModule_MonotonicMicroseconds();
}

static raft_index_t raftGetEntriesToSend(raft_server_t *raft, void *user_data,
                                         raft_node_t *node, raft_index_t idx,
                                         raft_index_t entries_n,
                                         raft_entry_t **entries)
{
    (void) node;
    (void) user_data;

    raft_index_t i;
    long long serialized_size = 0;
    RedisRaftCtx *rr = &redis_raft;

    for (i = 0; i < entries_n; i++) {
        raft_entry_t *e = raft_get_entry_from_idx(raft, idx + i);
        if (!e) {
            break;
        }
        serialized_size += e->data_len;
        /* A single entry can be larger than the limit, so, we always allow the
         * first entry. */
        if (i != 0 && serialized_size > rr->config.append_req_max_size) {
            break;
        }
        entries[i] = e;
    }

    return i;
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
    .backpressure = raftBackpressure,
    .timestamp = raftTimestamp,
    .get_entries_to_send = raftGetEntriesToSend,
};

void periodic(RedisModuleCtx *ctx, void *arg)
{
    RedisRaftCtx *rr = arg;

    RedisModule_CreateTimer(rr->ctx, rr->config.periodic_interval, periodic, rr);

    /* Proceed only if we're initialized */
    if (rr->state != REDIS_RAFT_UP) {
        return;
    }

    int ret = raft_periodic(rr->raft);
    if (ret == RAFT_ERR_SHUTDOWN) {
        exit(0);
    }

    assert(ret == 0);

    /* Compact cache */
    if (rr->config.log_max_cache_size) {
        EntryCacheCompact(rr->logcache, rr->config.log_max_cache_size);
    }

    /* Initiate snapshot if log size exceeds raft-log-file-max */
    if (rr->config.log_max_file_size &&
        raft_get_num_snapshottable_logs(rr->raft) > 0 &&
        rr->log->file_size > rr->config.log_max_file_size) {
        LOG_DEBUG("Raft log file size is %lu, initiating snapshot.",
                  rr->log->file_size);
        SnapshotSave(rr);
    }
}

/* A callback that invokes HandleNodeStates(), to handle node connection
 * management (reconnects, etc.).
 */
void reconnectNodes(RedisModuleCtx *ctx, void *arg)
{
    (void) ctx;
    RedisRaftCtx *rr = arg;

    RedisModule_CreateTimer(rr->ctx, rr->config.reconnect_interval, reconnectNodes, rr);
    ConnectionHandleIdles(rr);
    NodeReconnect(rr);
}

static int loadEntriesCallback(void *arg, raft_entry_t *entry, raft_index_t idx)
{
    RedisRaftCtx *rr = arg;

    if (rr->snapshot_info.last_applied_term <= entry->term &&
        rr->snapshot_info.last_applied_idx < rr->log->index &&
        raft_entry_is_cfg_change(entry)) {
        raft_handle_append_cfg_change(rr->raft, entry, idx);
    }

    return 0;
}

void loadRaftLog(RedisRaftCtx *rr)
{
    int entries = RaftLogLoadEntries(rr->log, loadEntriesCallback, rr);
    if (entries < 0) {
        LOG_WARNING("Failed to read Raft log");
        abort();
    }

    LOG_NOTICE("Log loaded, %d entries, snapshot last term=%lu, index=%lu",
               entries, rr->log->snapshot_last_term, rr->log->snapshot_last_idx);

    /* Make sure the log we're going to apply matches the RDB we've loaded */
    if (rr->snapshot_info.last_applied_idx != 0) {
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
    raft_set_snapshot_metadata(rr->raft, rr->snapshot_info.last_applied_term,
                               rr->snapshot_info.last_applied_idx);
    raft_apply_all(rr->raft);

    raft_set_current_term(rr->raft, rr->meta.term);
    raft_vote_for_nodeid(rr->raft, rr->meta.vote);

    LOG_NOTICE("Raft Meta: loaded current term=%lu, vote=%d",
               rr->meta.term, rr->meta.vote);

    LOG_NOTICE("Raft state after applying log: log_count=%lu, current_idx=%lu, last_applied_idx=%lu",
               raft_get_log_count(rr->raft),
               raft_get_current_idx(rr->raft),
               raft_get_last_applied_idx(rr->raft));
}

void RaftLibraryInit(RedisRaftCtx *rr, bool cluster_init)
{
    raft_node_t *node;

    raft_set_heap_functions(RedisModule_Alloc,
                            RedisModule_Calloc,
                            RedisModule_Realloc,
                            RedisModule_Free);

    rr->raft = raft_new_with_log(&RaftLogImpl, rr);
    if (!rr->raft) {
        PANIC("Failed to initialize Raft library");
    }

    int eltimeo = rr->config.election_timeout;
    int reqtimeo = rr->config.request_timeout;

    if (raft_config(rr->raft, 1, RAFT_CONFIG_ELECTION_TIMEOUT, eltimeo) != 0 ||
        raft_config(rr->raft, 1, RAFT_CONFIG_REQUEST_TIMEOUT, reqtimeo) != 0 ||
        raft_config(rr->raft, 1, RAFT_CONFIG_AUTO_FLUSH, 0) != 0) {
        PANIC("Failed to configure libraft");
    }

    raft_set_callbacks(rr->raft, &redis_raft_callbacks, rr);

    node = raft_add_non_voting_node(rr->raft, NULL, rr->config.id, 1);
    if (!node) {
        PANIC("Failed to create local Raft node [id %d]", rr->config.id);
    }

    if (cluster_init) {
        if (raft_set_current_term(rr->raft, 1) != 0 ||
            raft_become_leader(rr->raft) != 0) {
            PANIC("Failed to init raft library");
        }

        RaftCfgChange cfg = {
            .id = rr->config.id,
            .addr = rr->config.addr,
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

/* Callback for fsync thread. This will be triggerred by fsync thread but will
 * be called by Redis thread. */
void handleFsyncCompleted(void *result)
{
    FsyncThreadResult *rs = result;
    RedisRaftCtx *rr = &redis_raft;

    rr->log->fsync_index = rs->fsync_index;
    RedisModule_Free(rs);
}

RRStatus RedisRaftInit(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    *rr = (RedisRaftCtx){
        .state = REDIS_RAFT_UNINITIALIZED,
        .ctx = RedisModule_GetDetachedThreadSafeContext(ctx),
    };

    if (ConfigInit(&rr->config, ctx) != RR_OK) {
        RedisModule_FreeThreadSafeContext(rr->ctx);
        return RR_ERROR;
    }

    /* Check if metadata file exists. If it exists, it means cluster is
     * initialized. */
    if (RaftMetaRead(&rr->meta, rr->config.log_filename) == RR_OK) {
        rr->log = RaftLogOpen(rr->config.log_filename);
        if (!rr->log) {
            abort();
        }

        /* If id is configured, confirm the log matches.  If not, we set it from
         * the log.
         */
        if (!rr->config.id) {
            rr->config.id = rr->log->node_id;
        } else {
            if (rr->config.id != rr->log->node_id) {
                PANIC("Raft log node id [%d] does not match configured id [%d]",
                      rr->log->node_id, rr->config.id);
            }
        }

        RaftLibraryInit(rr, false);
        SnapshotLoad(rr);
        loadRaftLog(rr);

        rr->state = REDIS_RAFT_UP;
    }

    RedisModule_CreateTimer(ctx, rr->config.periodic_interval, periodic, rr);
    RedisModule_CreateTimer(ctx, rr->config.reconnect_interval, reconnectNodes, rr);
    ThreadPoolInit(&rr->thread_pool, 5);
    FsyncThreadStart(&rr->fsync_thread, handleFsyncCompleted);

    return RR_OK;
}

RaftReq *RaftReqInit(RedisModuleCtx *ctx, enum RaftReqType type)
{
    RaftReq *req = RedisModule_Calloc(1, sizeof(*req));

    req->client = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    req->ctx = RedisModule_GetThreadSafeContext(req->client);
    req->type = type;

    return req;
}

void RaftReqFree(RaftReq *req)
{
    if (req->type == RR_REDISCOMMAND) {
        CommandFree(&req->cmd);
    }

    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    RedisModule_Free(req);
}

static void noopCallback(void *result)
{
    (void) result;
}

/* Parse a node address from a RedisModuleString */
static RRStatus getNodeAddrFromArg(RedisModuleCtx *ctx, RedisModuleString *arg, NodeAddr *addr)
{
    size_t node_addr_len;
    const char *node_addr_str = RedisModule_StringPtrLen(arg, &node_addr_len);

    if (!NodeAddrParse(node_addr_str, node_addr_len, addr)) {
        RedisModule_ReplyWithError(ctx, "invalid node address");
        return RR_ERROR;
    }

    return RR_OK;
}

/* RAFT.NODE ADD [id] [address:port]
 *   Add a new node to the cluster.  The [id] can be an explicit non-zero value,
 *   or zero to let the cluster choose one.
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   -CLUSTERDOWN ||
 *   -MOVED <slot> <addr>:<port> ||
 *   *2
 *   :<new node id>
 *   :<dbid>
 *
 * RAFT.NODE REMOVE [id]
 *   Remove an existing node from the cluster.
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   -CLUSTERDOWN ||
 *   -MOVED <slot> <addr>:<port> ||
 *   +OK
 */
static int cmdRaftNode(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR ||
        checkLeader(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    size_t cmd_len;
    const char *cmd = RedisModule_StringPtrLen(argv[1], &cmd_len);

    if (!strncasecmp(cmd, "ADD", cmd_len)) {
        if (argc != 4) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        /* Validate node id */
        long long node_id;
        if (RedisModule_StringToLongLong(argv[2], &node_id) != REDISMODULE_OK ||
            (node_id && !VALID_NODE_ID(node_id))) {
            RedisModule_ReplyWithError(ctx, "invalid node id");
            return REDISMODULE_OK;
        }

        if (hasNodeIdBeenUsed(rr, (raft_node_id_t) node_id)) {
            RedisModule_ReplyWithError(ctx, "node id has already been used in this cluster");
            return REDISMODULE_OK;
        }

        if (node_id == 0) {
            node_id = makeRandomNodeId(rr);
        }

        RaftCfgChange cfg = {
            .id = (raft_node_id_t) node_id,
        };

        /* Parse address */
        if (getNodeAddrFromArg(ctx, argv[3], &cfg.addr) == RR_ERROR) {
            /* Error already produced */
            return REDISMODULE_OK;
        }

        raft_entry_resp_t resp;
        RaftReq *req = RaftReqInit(ctx, RR_CFGCHANGE_ADDNODE);

        raft_entry_req_t *entry = raft_entry_new(sizeof(cfg));
        entry->id = rand();
        entry->type = RAFT_LOGTYPE_ADD_NONVOTING_NODE;
        memcpy(entry->data, &cfg, sizeof(cfg));
        entryAttachRaftReq(rr, entry, req);

        int e = raft_recv_entry(rr->raft, entry, &resp);
        if (e != 0) {
            entryDetachRaftReq(rr, entry);
            replyRaftError(req->ctx, e);
            raft_entry_release(entry);
            RaftReqFree(req);
            return REDISMODULE_OK;
        }

        raft_entry_release(entry);

    } else if (!strncasecmp(cmd, "REMOVE", cmd_len)) {
        if (argc != 3) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        long long node_id;
        if (RedisModule_StringToLongLong(argv[2], &node_id) != REDISMODULE_OK ||
            !VALID_NODE_ID(node_id)) {
            RedisModule_ReplyWithError(ctx, "invalid node id");
            return REDISMODULE_OK;
        }

        /* Validate it exists */
        if (!raft_get_node(rr->raft, (raft_node_id_t) node_id)) {
            RedisModule_ReplyWithError(ctx, "node id does not exist");
            return REDISMODULE_OK;
        }

        RaftCfgChange cfg = {
            .id = (raft_node_id_t) node_id,
        };

        raft_entry_resp_t resp;
        RaftReq *req = RaftReqInit(ctx, RR_CFGCHANGE_REMOVENODE);

        raft_entry_req_t *entry = raft_entry_new(sizeof(cfg));
        entry->id = rand();
        entry->type = RAFT_LOGTYPE_REMOVE_NODE;
        memcpy(entry->data, &cfg, sizeof(cfg));
        entryAttachRaftReq(rr, entry, req);

        int e = raft_recv_entry(rr->raft, entry, &resp);
        if (e != 0) {
            entryDetachRaftReq(rr, entry);
            replyRaftError(req->ctx, e);
            raft_entry_release(entry);
            RaftReqFree(req);
            return REDISMODULE_OK;
        }

        raft_entry_release(entry);

        /* Special case, we are the only node and our node has been removed */
        if (cfg.id == raft_get_nodeid(rr->raft) &&
            raft_get_num_voting_nodes(rr->raft) == 0) {
            exit(0);
        }
    } else {
        RedisModule_ReplyWithError(ctx, "RAFT.NODE supports ADD / REMOVE only");
    }

    return REDISMODULE_OK;
}

/* RAFT.TRANSFER_LEADER [target_node_id]
  *   Attempt to transfer raft cluster leadership to targeted node
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   -ERR
 *   +OK
 */
static int cmdRaftTransferLeader(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;
    raft_node_id_t target = RAFT_NODE_ID_NONE;

    if (argc > 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (argc == 2) {
        if (RedisModuleStringToInt(argv[1], &target) == REDISMODULE_ERR) {
            RedisModule_ReplyWithError(ctx, "invalid target node id");
            return REDISMODULE_OK;
        }
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    int err = raft_transfer_leader(rr->raft, target, 0);
    if (err != 0) {
        replyRaftError(ctx, err);
        return REDISMODULE_OK;
    }

    rr->transfer_req = RaftReqInit(ctx, RR_TRANSFER_LEADER);

    return REDISMODULE_OK;
}

/* RAFT.TIMEOUT_NOW
 *   instruct this node to force an election
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   +OK
 */
static int cmdRaftTimeoutNow(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 1) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    raft_timeout_now(rr->raft);
    RedisModule_ReplyWithSimpleString(ctx, "OK");

    return REDISMODULE_OK;
}

/* RAFT.REQUESTVOTE [target_node_id] [src_node_id] [prevote]:[term]:[candidate_id]:[last_log_idx]:[last_log_term]
 *   Request a node's vote (per Raft paper).
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   *2
 *   :<term>
 *   :<granted> (0 or 1)
 */
static int cmdRaftRequestVote(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 4) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    int target_node_id;
    if (RedisModuleStringToInt(argv[1], &target_node_id) == REDISMODULE_ERR ||
        target_node_id != rr->config.id) {

        RedisModule_ReplyWithError(ctx, "invalid or incorrect target node id");
        return REDISMODULE_OK;
    }

    int src_node_id;
    if (RedisModuleStringToInt(argv[2], &src_node_id) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "invalid source node id");
        return REDISMODULE_OK;
    }

    const char *tmpstr = RedisModule_StringPtrLen(argv[3], NULL);
    raft_requestvote_req_t req = {0};

    if (sscanf(tmpstr, "%d:%ld:%d:%ld:%ld",
               &req.prevote,
               &req.term,
               &req.candidate_id,
               &req.last_log_idx,
               &req.last_log_term) != 5) {
        RedisModule_ReplyWithError(ctx, "invalid message");
        return REDISMODULE_OK;
    }

    raft_requestvote_resp_t resp = {0};
    raft_node_t *node = raft_get_node(rr->raft, src_node_id);

    if (raft_recv_requestvote(rr->raft, node, &req, &resp) != 0) {
        RedisModule_ReplyWithError(ctx, "ERR operation failed");
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithArray(ctx, 4);
    RedisModule_ReplyWithLongLong(ctx, resp.prevote);
    RedisModule_ReplyWithLongLong(ctx, resp.request_term);
    RedisModule_ReplyWithLongLong(ctx, resp.term);
    RedisModule_ReplyWithLongLong(ctx, resp.vote_granted);

    return REDISMODULE_OK;
}

static void handleReadOnlyCommand(void *arg, int can_read)
{
    RedisRaftCtx *rr = &redis_raft;
    RaftReq *req = arg;

    if (!can_read) {
        RedisModule_ReplyWithError(req->ctx, "TIMEOUT no quorum for read");
        goto exit;
    }

    RedisModule_ReplyWithSimpleString(req->ctx, rr->value ? rr->value : "");
exit:
    RaftReqFree(req);
}

/* RAFT.WRITE [value] */
static int cmdRaftWrite(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR ||
        checkLeader(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    RaftReq *req = RaftReqInit(ctx, RR_REDISCOMMAND);
    req->cmd = (Command){
        .argc = argc - 1,
        .argv = RedisModule_Alloc((argc - 1) * sizeof(RedisModuleString *)),
    };

    for (int i = 0; i < argc - 1; i++) {
        RedisModule_RetainString(ctx, argv[i + 1]);
        req->cmd.argv[i] = argv[i + 1];
    }

    raft_entry_t *entry = CommandEncode(&req->cmd);
    entry->id = rand();
    entry->type = RAFT_LOGTYPE_NORMAL;
    entryAttachRaftReq(rr, entry, req);

    int e = raft_recv_entry(rr->raft, entry, NULL);
    if (e != 0) {
        replyRaftError(ctx, e);
        entryDetachRaftReq(rr, entry);
        raft_entry_release(entry);
        RaftReqFree(req);
        return REDISMODULE_OK;
    }

    raft_entry_release(entry);

    return REDISMODULE_OK;
}

/* RAFT.READ */
static int cmdRaftRead(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 1) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR ||
        checkLeader(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    RaftReq *req = RaftReqInit(ctx, RR_REDISCOMMAND);

    int rc = raft_recv_read_request(rr->raft, handleReadOnlyCommand, req);
    if (rc != 0) {
        replyRaftError(ctx, rc);
        RaftReqFree(req);
    }

    return REDISMODULE_OK;
}

/* RAFT.AE [target_node_id] [src_node_id]
 *         [leader_id]:[term]:[prev_log_idx]:[prev_log_term]:[leader_commit]:[msg_id]
 *         [n_entries] [<term>:<id>:<type> <entry>]...
 *
 *   A leader request to append entries to the Raft log (per Raft paper).
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   *4
 *   :<term>
 *   :<success> (0 or 1)
 *   :<current_idx>
 *   :<msg_id>
 */
static int cmdRaftAppendEntries(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 5) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    int target_node_id;
    if (RedisModuleStringToInt(argv[1], &target_node_id) == REDISMODULE_ERR ||
        target_node_id != rr->config.id) {
        RedisModule_ReplyWithError(ctx, "invalid or incorrect target node id");
        return REDISMODULE_OK;
    }

    long long n_entries;
    if (RedisModule_StringToLongLong(argv[4], &n_entries) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "invalid n_entries value");
        return REDISMODULE_OK;
    }
    if (argc != 5 + 2 * n_entries) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    raft_node_id_t src_node_id;
    if (RedisModuleStringToInt(argv[2], &src_node_id) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "invalid source node id");
        return REDISMODULE_OK;
    }

    raft_appendentries_req_t msg = {0};

    size_t tmplen;
    const char *tmpstr = RedisModule_StringPtrLen(argv[3], &tmplen);
    if (sscanf(tmpstr, "%d:%ld:%ld:%ld:%ld:%lu",
               &msg.leader_id,
               &msg.term,
               &msg.prev_log_idx,
               &msg.prev_log_term,
               &msg.leader_commit,
               &msg.msg_id) != 6) {
        RedisModule_ReplyWithError(ctx, "invalid message");
        return REDISMODULE_OK;
    }

    msg.n_entries = (int) n_entries;
    if (n_entries > 0) {
        msg.entries = RedisModule_Calloc(n_entries, sizeof(raft_entry_t));
    }

    for (int i = 0; i < n_entries; i++) {
        /* Create entry with payload */
        tmpstr = RedisModule_StringPtrLen(argv[6 + 2 * i], &tmplen);
        raft_entry_t *e = raft_entry_new(tmplen);
        memcpy(e->data, tmpstr, tmplen);

        /* Parse additional entry fields */
        tmpstr = RedisModule_StringPtrLen(argv[5 + 2 * i], &tmplen);
        if (sscanf(tmpstr, "%ld:%d:%hd", &e->term, &e->id, &e->type) != 3) {
            RedisModule_ReplyWithError(ctx, "invalid entry");
            goto out;
        }

        msg.entries[i] = e;
    }

    raft_appendentries_resp_t resp = {0};
    raft_node_t *node = raft_get_node(rr->raft, src_node_id);

    if (raft_recv_appendentries(rr->raft, node, &msg, &resp) != 0) {
        RedisModule_ReplyWithError(ctx, "ERR operation failed");
        goto out;
    }

    RedisModule_ReplyWithArray(ctx, 4);
    RedisModule_ReplyWithLongLong(ctx, resp.term);
    RedisModule_ReplyWithLongLong(ctx, resp.success);
    RedisModule_ReplyWithLongLong(ctx, resp.current_idx);
    RedisModule_ReplyWithLongLong(ctx, resp.msg_id);

out:
    if (msg.n_entries > 0) {
        for (int i = 0; i < msg.n_entries; i++) {
            raft_entry_t *e = msg.entries[i];
            if (e) {
                raft_entry_release(e);
            }
        }
        RedisModule_Free(msg.entries);
    }

    return REDISMODULE_OK;
}

/* RAFT.SNAPSHOT [target-node-id] [src_node_id]
 *               [term]:[leader_id]:[msg_id]:[snapshot_index]:[snapshot_term]:[chunk_offset]:[last_chunk]
 *               [chunk_data]
 *   Store the specified snapshot chunk (e.g. Raft paper's InstallSnapshot RPC).
 *
 *  Reply:
 *    -NOCLUSTER ||
 *    -LOADING ||
 *    *5
 *    :<term>
 *    :<msg_id>
 *    :<offset>
 *    :<success>
 *    :<last_chunk>
 */
static int cmdRaftSnapshot(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 5) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    int target_node_id;
    if (RedisModuleStringToInt(argv[1], &target_node_id) != REDISMODULE_OK ||
        target_node_id != rr->config.id) {

        RedisModule_ReplyWithError(ctx, "ERR invalid or incorrect target node id");
        return REDISMODULE_OK;
    }

    int src_node_id;
    if (RedisModuleStringToInt(argv[2], &src_node_id) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "invalid source node id");
        return REDISMODULE_OK;
    }

    raft_snapshot_req_t req = {0};
    const char *tmpstr = RedisModule_StringPtrLen(argv[3], NULL);

    if (sscanf(tmpstr, "%lu:%d:%lu:%lu:%lu:%llu:%d",
               &req.term,
               &req.leader_id,
               &req.msg_id,
               &req.snapshot_index,
               &req.snapshot_term,
               &req.chunk.offset,
               &req.chunk.last_chunk) != 7) {
        RedisModule_ReplyWithError(ctx, "invalid message");
        return REDISMODULE_OK;
    }

    size_t len;
    void *data = (void *) RedisModule_StringPtrLen(argv[4], &len);

    req.chunk.data = data;
    req.chunk.len = len;

    raft_snapshot_resp_t resp = {0};
    raft_node_t *node = raft_get_node(rr->raft, (raft_node_id_t) src_node_id);

    if (raft_recv_snapshot(rr->raft, node, &req, &resp) != 0) {
        RedisModule_ReplyWithError(ctx, "ERR operation failed");
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithArray(ctx, 5);
    RedisModule_ReplyWithLongLong(ctx, resp.term);
    RedisModule_ReplyWithLongLong(ctx, resp.msg_id);
    RedisModule_ReplyWithLongLong(ctx, resp.offset);
    RedisModule_ReplyWithLongLong(ctx, resp.success);
    RedisModule_ReplyWithLongLong(ctx, resp.last_chunk);

    return REDISMODULE_OK;
}

static void clusterInit()
{
    RedisRaftCtx *rr = &redis_raft;

    /* If node id was not specified, make up one */
    if (!rr->config.id) {
        rr->config.id = makeRandomNodeId(rr);
    }

    rr->log = RaftLogCreate(rr->config.log_filename, 1, 0, rr->config.id);
    if (!rr->log) {
        PANIC("Failed to initialize Raft log");
    }

    addUsedNodeId(rr, rr->config.id);
    RaftLibraryInit(rr, true);
    SnapshotInit(rr);

    LOG_NOTICE("Raft Cluster initialized, node id: %d", rr->config.id);
}

typedef struct JoinState {
    NodeAddrListElement *addr;
    NodeAddrListElement *addr_iter;
    Connection *conn;
    time_t start; /* Timestamp in seconds when we initiated the join. */
    RaftReq *req; /* Original RaftReq, so we can return a reply */
    bool failed;  /* unrecoverable failure */
    bool started; /* we have started connecting */
} JoinState;

static void clusterJoinCompleted(RaftReq *req)
{
    RedisRaftCtx *rr = &redis_raft;

    /* Initialize Raft log.  We delay this operation as we want to create the
     * log with the proper dbid which is only received now.
     */
    rr->log = RaftLogCreate(rr->config.log_filename,
                            rr->snapshot_info.last_applied_term,
                            rr->snapshot_info.last_applied_idx,
                            rr->config.id);
    if (!rr->log) {
        PANIC("Failed to initialize Raft log");
    }

    RaftLibraryInit(rr, false);
    SnapshotInit(rr);

    RedisModule_ReplyWithSimpleString(req->ctx, "OK");
    RaftReqFree(req);
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
        LOG_WARNING("RAFT.NODE ADD failed: connection dropped.");
        ConnMarkDisconnected(conn);
        redisAsyncDisconnect(c);
        return;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
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

        redisAsyncDisconnect(c);
        return;
    }

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 1) {
        LOG_WARNING("RAFT.NODE ADD invalid reply.");
        redisAsyncDisconnect(c);
        return;
    }

    LOG_NOTICE("Joined Raft cluster, node id: %lu",
               (unsigned long) reply->element[0]->integer);

    rr->config.id = reply->element[0]->integer;
    clusterJoinCompleted(state->req);
    assert(rr->state == REDIS_RAFT_UP);

    ConnAsyncTerminate(conn);
    redisAsyncDisconnect(c);
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

/* Invoked when the connection is not connected or actively attempting
 * a connection.
 */
static void joinIdleCallback(Connection *conn)
{
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);
    JoinState *state = ConnGetPrivateData(conn);

    time_t now = time(NULL);

    if (state->failed) {
        LOG_WARNING("Cluster join: unrecoverable error, check logs");
        goto exit_fail;
    }

    if (difftime(now, state->start) > rr->config.join_timeout) {
        LOG_WARNING("Cluster join timed out, took longer than %d seconds",
                    rr->config.join_timeout);
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

    LOG_VERBOSE("Joining cluster, connecting to %s:%u",
                state->addr_iter->addr.host, state->addr_iter->addr.port);

    /* Establish connection. We silently ignore errors here as we'll
     * just get iterated again in the future.
     */
    ConnConnect(state->conn, &state->addr_iter->addr, sendNodeAddRequest);
    return;

exit_fail:
    ConnAsyncTerminate(conn);
    rr->state = REDIS_RAFT_UNINITIALIZED;
    RedisModule_ReplyWithError(state->req->ctx, "ERR failed to connect to cluster");
    RaftReqFree(state->req);
}

/* Invoked when the connection is terminated.
 */
static void joinFreeCallback(void *privdata)
{
    JoinState *state = privdata;

    NodeAddrListFree(state->addr);
    RedisModule_Free(state);
}

static void joinCluster(RedisRaftCtx *rr, NodeAddrListElement *el, RaftReq *req)
{
    JoinState *st = RedisModule_Calloc(1, sizeof(*st));

    NodeAddrListConcat(&st->addr, el);
    st->start = time(NULL);
    st->req = req;
    st->conn = ConnCreate(rr, st, joinIdleCallback, joinFreeCallback);
}

/* RAFT.CLUSTER INIT <id>
 *   Initializes a new Raft cluster.
 *   <id> is an optional 32 character string, if set, cluster will use it for the id
 * Reply:
 *   +OK [dbid]
 *
 * RAFT.CLUSTER JOIN [addr:port]
 *   Join an existing cluster.
 *   The operation is asynchronous and may take place/retry in the background.
 * Reply:
 *   +OK
 */
static int cmdRaftCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftUninitialized(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    size_t cmd_len;
    const char *cmd = RedisModule_StringPtrLen(argv[1], &cmd_len);

    if (!strncasecmp(cmd, "INIT", cmd_len)) {
        if (argc != 2) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        clusterInit();
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else if (!strncasecmp(cmd, "JOIN", cmd_len)) {
        if (argc < 3) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        NodeAddrListElement *addrs = NULL;

        for (int i = 2; i < argc; i++) {
            NodeAddr addr;
            if (getNodeAddrFromArg(ctx, argv[i], &addr) == RR_ERROR) {
                /* Error already produced */
                return REDISMODULE_OK;
            }
            NodeAddrListAddElement(&addrs, &addr);
        }

        RaftReq *req = RaftReqInit(ctx, RR_CLUSTER_JOIN);
        joinCluster(rr, addrs, req);

        NodeAddrListFree(addrs);
        rr->state = REDIS_RAFT_JOINING;
    } else {
        RedisModule_ReplyWithError(ctx, "RAFT.CLUSTER supports INIT / JOIN only");
    }

    return REDISMODULE_OK;
}

static int cmdRaftNodeShutdown(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    long long node_id;
    if (RedisModule_StringToLongLong(argv[1], &node_id) != REDISMODULE_OK ||
        node_id != rr->config.id) {
        RedisModule_ReplyWithError(ctx, "invalid node id");
        return REDISMODULE_OK;
    }

    exit(0);
}

/* Redis thread callback, called just before Redis main thread goes to sleep,
 * e.g epoll_wait(). At this point, we've read all new network messages for this
 * event loop iteration. We can trigger new appendentries messages for the new
 * entries and check for committing/applying new entries. */
static void beforeSleep(RedisModuleCtx *ctx, RedisModuleEvent eid,
                        uint64_t subevent, void *data)
{
    RedisRaftCtx *rr = &redis_raft;

    if (subevent != REDISMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP ||
        rr->state != REDIS_RAFT_UP) {
        return;
    }

    raft_index_t flushed = rr->log->fsync_index;
    raft_index_t next = raft_get_index_to_sync(rr->raft);
    if (next > 0) {
        fflush(rr->log->file);
        FsyncThreadAddTask(&rr->fsync_thread, fileno(rr->log->file), next);
    }

    int e = raft_flush(rr->raft, flushed);
    if (e == RAFT_ERR_SHUTDOWN) {
        exit(0);
    }
    RedisModule_Assert(e == 0);

    if (raft_pending_operations(rr->raft)) {
        /* If there are pending operations, we need to call raft_flush() again.
         * We'll do it in the next iteration as we want to process messages
         * from the network first. Here, we just wake up the event loop. In the
         * next iteration, beforeSleep() callback will be called again. */
        RedisModule_EventLoopAddOneShot(noopCallback, NULL);
    }
}

static void handleInfo(RedisModuleInfoCtx *ctx, int for_crash_report)
{
    (void) for_crash_report;

    RedisRaftCtx *rr = &redis_raft;

    RedisModule_InfoAddSection(ctx, "version");
    RedisModule_InfoAddFieldCString(ctx, "version", REDISRAFT_VERSION);
    RedisModule_InfoAddFieldCString(ctx, "git_sha1", REDISRAFT_GIT_SHA1);

    RedisModule_InfoAddSection(ctx, "general");
    RedisModule_InfoAddFieldLongLong(ctx, "node_id", rr->config.id);
    RedisModule_InfoAddFieldCString(ctx, "state", getStateStr(rr));
    RedisModule_InfoAddFieldCString(ctx, "role", rr->raft ? raft_get_state_str(rr->raft) : "(none)");

    char *voting = "-";
    if (rr->raft) {
        voting = raft_node_is_voting(raft_get_my_node(rr->raft)) ? "yes" : "no";
    }
    RedisModule_InfoAddFieldCString(ctx, "is_voting", voting);
    RedisModule_InfoAddFieldLongLong(ctx, "leader_id", rr->raft ? raft_get_leader_id(rr->raft) : -1);
    RedisModule_InfoAddFieldLongLong(ctx, "current_term", rr->raft ? raft_get_current_term(rr->raft) : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "num_nodes", rr->raft ? raft_get_num_nodes(rr->raft) : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "num_voting_nodes", rr->raft ? raft_get_num_voting_nodes(rr->raft) : 0);

    long long now = RedisModule_Milliseconds();
    int num_nodes = rr->raft ? raft_get_num_nodes(rr->raft) : 0;
    for (int i = 0; i < num_nodes; i++) {
        raft_node_t *rn = raft_get_node_from_idx(rr->raft, i);
        Node *n = raft_node_get_udata(rn);
        if (!n) {
            continue;
        }

        char name[32];
        snprintf(name, sizeof(name), "node%d", i);

        RedisModule_InfoBeginDictField(ctx, name);
        RedisModule_InfoAddFieldLongLong(ctx, "id", n->id);
        RedisModule_InfoAddFieldCString(ctx, "state", ConnGetStateStr(n->conn));
        RedisModule_InfoAddFieldCString(ctx, "voting", raft_node_is_voting(rn) ? "yes" : "no");
        RedisModule_InfoAddFieldCString(ctx, "addr", n->addr.host);
        RedisModule_InfoAddFieldULongLong(ctx, "port", n->addr.port);

        long long int last = -1;
        if (n->conn->last_connected_time) {
            last = (now - n->conn->last_connected_time) / 1000;
        }
        RedisModule_InfoAddFieldLongLong(ctx, "last_conn_secs", last);
        RedisModule_InfoEndDictField(ctx);
    }

    RedisModule_InfoAddSection(ctx, "log");
    RedisModule_InfoAddFieldLongLong(ctx, "log_entries", rr->raft ? raft_get_log_count(rr->raft) : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "current_index", rr->raft ? raft_get_current_idx(rr->raft) : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "commit_index", rr->raft ? raft_get_commit_idx(rr->raft) : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "last_applied_index", rr->raft ? raft_get_last_applied_idx(rr->raft) : 0);
    RedisModule_InfoAddFieldULongLong(ctx, "file_size", rr->log ? rr->log->file_size : 0);
    RedisModule_InfoAddFieldULongLong(ctx, "cache_memory_size", rr->logcache ? rr->logcache->entries_memsize : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "cache_entries", rr->logcache ? rr->logcache->len : 0);
}

static int registerRaftCommands(RedisModuleCtx *ctx)
{
    if (RedisModule_CreateCommand(ctx, "raft.write", cmdRaftWrite,
                                  "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.read", cmdRaftRead,
                                  "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.cluster", cmdRaftCluster,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.node", cmdRaftNode,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.ae", cmdRaftAppendEntries,
                                  "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.transfer_leader",
                                  cmdRaftTransferLeader,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.timeout_now", cmdRaftTimeoutNow,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.requestvote", cmdRaftRequestVote,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.snapshot", cmdRaftSnapshot,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.nodeshutdown", cmdRaftNodeShutdown,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    int ret;

    ret = RedisModule_Init(ctx, "raft", 1, REDISMODULE_APIVER_1);
    if (ret != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    /* Create a logging context */
    redisraft_log_ctx = RedisModule_GetDetachedThreadSafeContext(ctx);
    RedisModule_RegisterInfoFunc(ctx, handleInfo);

    if (registerRaftCommands(ctx) == RR_ERROR) {
        LOG_WARNING("Failed to register commands");
        return REDISMODULE_ERR;
    }

    ret = RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_EventLoop,
                                             beforeSleep);
    if (ret != REDISMODULE_OK) {
        LOG_WARNING("Failed to subscribe to server events.");
        return REDISMODULE_ERR;
    }

    RedisRaftCtx *rr = &redis_raft;

    if (RedisRaftInit(rr, ctx) == RR_ERROR) {
        return REDISMODULE_ERR;
    }

    LOG_NOTICE("Raft module loaded, state is '%s'", getStateStr(rr));

    return REDISMODULE_OK;
}
