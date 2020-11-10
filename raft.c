/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <ctype.h>
#include <strings.h>

#include "redisraft.h"

const char *RaftReqTypeStr[] = {
    "<undef>",
    "RR_CLUSTER_INIT",
    "RR_CLUSTER_JOIN",
    "RR_CFGCHANGE_ADDNODE",
    "RR_CFGCHANGE_REMOVENODE",
    "RR_APPENDENTRIES",
    "RR_REQUESTVOTE",
    "RR_REDISCOMMAND",
    "RR_INFO",
    "RR_LOADSNAPSHOT",
    "RR_COMPACT",
    "RR_CLIENT_DISCONNECT",
    "RR_SHARDGROUP_ADD",
    "RR_SHARDGROUP_GET"
};

/* Forward declarations */
static void initRaftLibrary(RedisRaftCtx *rr);
static void configureFromSnapshot(RedisRaftCtx *rr);
static void applyAddShardGroup(RedisRaftCtx *rr, raft_entry_t *entry);
static RaftReqHandler RaftReqHandlers[];

static bool processExiting = false;
static void __setProcessExiting(void) {
    processExiting = true;
}

/* A dict that maps client ID to MultiClientState structs */
static RedisModuleDict *multiClientState = NULL;

/* ------------------------------------ Command Classification ------------------------------------ */

static RedisModuleDict *readonlyCommandDict = NULL;

static void populateReadonlyCommandDict(RedisModuleCtx *ctx)
{
    static char *commands[] = {
        "get",
        "strlen",
        "exists",
        "getbit",
        "getrange",
        "substr",
        "mget",
        "llen",
        "lindex",
        "lrange",
        "scard",
        "sismember",
        "srandmember",
        "sinter",
        "sunion",
        "sdiff",
        "smembers",
        "sscan",
        "zrange",
        "zrangebyscore",
        "zrevrangebyscore",
        "zrangebylex",
        "zrevrangebylex",
        "zcount",
        "zlexcount",
        "zrevrange",
        "zcard",
        "zscore",
        "zrank",
        "zrevrank",
        "zscan",
        "hmget",
        "hlen",
        "hstrlen",
        "hkeys",
        "hvals",
        "hgetall",
        "hexists",
        "hscan",
        "randomkey",
        "keys",
        "scan",
        "dbsize",
        "ttl",
        "bitcount",
        "georadius_ro",
        "georadiusbymember_ro",
        "geohash",
        "geopos",
        "geodist",
        "pfcount",
        NULL
    };

    readonlyCommandDict = RedisModule_CreateDict(ctx);
    int i;
    for (i = 0; commands[i] != NULL; i++) {
        RedisModule_DictSetC(readonlyCommandDict, commands[i], strlen(commands[i]), (void *) 0x01);
    }
}

/* ------------------------------------ Common helpers ------------------------------------ */

/* Set up a Raft log entry with an attached RaftReq. We use this when a user command provided
 * in a RaftReq should keep the client blocked until the log entry is committed and applied.
 */

static void entryFreeAttachedRaftReq(raft_entry_t *ety)
{
    RaftReq *req = (RaftReq *) ety->user_data;
    ety->user_data = NULL;

    if (req) {
        redis_raft.client_attached_entries--;
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
static void entryAttachRaftReq(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req)
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
        RedisModuleCallReply *reply = RedisModule_Call(
                ctx, cmd, "v", &c->argv[1], c->argc - 1);
        exitRedisModuleCall();

        if (reply_ctx) {
            if (reply) {
                RedisModule_ReplyWithCallReply(reply_ctx, reply);
            } else {
                RedisModule_ReplyWithError(reply_ctx, "ERR Unknown command/arguments");
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
    assert(entry->type == RAFT_LOGTYPE_NORMAL);

    /* TODO: optimize and avoid deserialization here, we can use the
     * original argv in RaftReq
     */

    RaftRedisCommandArray entry_cmds = { 0 };
    if (RaftRedisCommandArrayDeserialize(&entry_cmds, entry->data, entry->data_len) != RR_OK) {
        PANIC("Invalid Raft entry");
    }

    RaftReq *req = entry->user_data;
    RedisModuleCtx *ctx = req ? req->ctx : rr->ctx;

    /* Redis Module API requires commands executing on a locked thread
     * safe context.
     */

    RedisModule_ThreadSafeContextLock(ctx);
    executeRaftRedisCommandArray(&entry_cmds, ctx, req? req->ctx : NULL);

    /* Update snapshot info in Redis dataset. This must be done now so it's
     * always consistent with what we applied and we never end up applying
     * an entry onto a snapshot where it was applied already.
     */
    rr->snapshot_info.last_applied_term = entry->term;
    rr->snapshot_info.last_applied_idx = entry_idx;

    RedisModule_ThreadSafeContextUnlock(ctx);
    RaftRedisCommandArrayFree(&entry_cmds);

    if (req) {
        /* Free request now, we don't need it anymore */
        entry->user_data = NULL;
        rr->client_attached_entries--;
        RaftReqFree(req);
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
        NODE_LOG_DEBUG(node, "RAFT.REQUESTVOTE failed: connection dropped.\n");
        NodeMarkDisconnected(node);
        return;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        NODE_LOG_DEBUG(node, "RAFT.REQUESTVOTE error: %s\n", reply->str);
        return;
    }

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2 ||
            reply->element[0]->type != REDIS_REPLY_INTEGER ||
            reply->element[1]->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_ERROR(node, "invalid RAFT.REQUESTVOTE reply\n");
        return;
    }

    msg_requestvote_response_t response = {
        .term = reply->element[0]->integer,
        .vote_granted = reply->element[1]->integer
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);
    if (!raft_node) {
        NODE_LOG_DEBUG(node, "RAFT.REQUESTVOTE stale reply.\n");
        return;
    }

    int ret;
    if ((ret = raft_recv_requestvote_response(
            rr->raft,
            raft_node,
            &response)) != 0) {
        TRACE("raft_recv_requestvote_response failed, error %d\n", ret);
    }
}


static int raftSendRequestVote(raft_server_t *raft, void *user_data,
        raft_node_t *raft_node, msg_requestvote_t *msg)
{
    Node *node = (Node *) raft_node_get_udata(raft_node);

    if (!NODE_IS_CONNECTED(node)) {
        NODE_TRACE(node, "not connected, state=%s\n", NodeStateStr[node->state]);
        return 0;
    }

    /* RAFT.REQUESTVOTE <src_node_id> <term> <candidate_id> <last_log_idx> <last_log_term> */
    if (redisAsyncCommand(node->rc, handleRequestVoteResponse,
                node, "RAFT.REQUESTVOTE %d %d %d:%d:%d:%d",
                raft_node_get_id(raft_node),
                raft_get_nodeid(raft),
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
        NODE_TRACE(node, "RAFT.AE failed: connection dropped.\n");
        NodeMarkDisconnected(node);
        return;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        NODE_TRACE(node, "RAFT.AE error: %s\n", reply->str);
        return;
    }
    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 4 ||
            reply->element[0]->type != REDIS_REPLY_INTEGER ||
            reply->element[1]->type != REDIS_REPLY_INTEGER ||
            reply->element[2]->type != REDIS_REPLY_INTEGER ||
            reply->element[3]->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_ERROR(node, "invalid RAFT.AE reply\n");
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
        NODE_TRACE(node, "raft_recv_appendentries_response failed, error %d\n", ret);
    }

    /* Maybe we have pending stuff to apply now */
    raft_apply_all(rr->raft);
    raft_process_read_queue(rr->raft);
}

static int raftSendAppendEntries(raft_server_t *raft, void *user_data,
        raft_node_t *raft_node, msg_appendentries_t *msg)
{
    Node *node = (Node *) raft_node_get_udata(raft_node);

    int argc = 5 + msg->n_entries * 2;
    char *argv[argc];
    size_t argvlen[argc];

    if (!NODE_IS_CONNECTED(node)) {
        NODE_TRACE(node, "not connected, state=%s\n", NodeStateStr[node->state]);
        return 0;
    }

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
    argvlen[3] = snprintf(msg_str, sizeof(msg_str)-1, "%ld:%ld:%ld:%ld:%lu",
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

    if (redisAsyncCommandArgv(node->rc, handleAppendEntriesResponse,
                node, argc, (const char **)argv, argvlen) != REDIS_OK) {
        NODE_TRACE(node, "failed appendentries");
    } else{
        NodeAddPendingResponse(node, false);
    }

    for (i = 0; i < msg->n_entries; i++) {
        RedisModule_Free(argv[5 + i*2]);
    }
    return 0;
}

/* ------------------------------------ Log Callbacks ------------------------------------ */

static int raftPersistVote(raft_server_t *raft, void *user_data, raft_node_id_t vote)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) user_data;
    if (!rr->log || rr->state == REDIS_RAFT_LOADING) {
        return 0;
    }

    if (RaftLogSetVote(rr->log, vote) != RR_OK) {
        LOG_ERROR("ERROR: RaftLogSetVote\n");
        return RAFT_ERR_SHUTDOWN;
    }

    return 0;
}

static int raftPersistTerm(raft_server_t *raft, void *user_data, raft_term_t term, raft_node_id_t vote)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) user_data;
    if (!rr->log || rr->state == REDIS_RAFT_LOADING) {
        return 0;
    }

    if (RaftLogSetTerm(rr->log, term, vote) != RR_OK) {
        LOG_ERROR("ERROR: RaftLogSetTerm\n");
        return RAFT_ERR_SHUTDOWN;
    }

    return 0;
}

static int raftApplyLog(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
    RedisRaftCtx *rr = user_data;
    RaftCfgChange *req;

    switch (entry->type) {
        case RAFT_LOGTYPE_DEMOTE_NODE:
            if (rr->state == REDIS_RAFT_UP && raft_is_leader(rr->raft)) {
                raft_entry_t *rem_entry = raft_entry_new(sizeof(RaftCfgChange));
                msg_entry_response_t resp;

                rem_entry->type = RAFT_LOGTYPE_REMOVE_NODE;
                rem_entry->id = rand();
                ((RaftCfgChange *) rem_entry->data)->id = ((RaftCfgChange *) entry->data)->id;

                int e = raft_recv_entry(rr->raft, rem_entry, &resp);
                assert (e == 0);

                raft_entry_release(rem_entry);
                break;
            }
        case RAFT_LOGTYPE_REMOVE_NODE:
            req = (RaftCfgChange *) entry->data;
            if (req->id == raft_get_nodeid(raft)) {
                LOG_DEBUG("Removing this node from the cluster\n");
                return RAFT_ERR_SHUTDOWN;
            }
            break;
        case RAFT_LOGTYPE_NORMAL:
            executeLogEntry(rr, entry, entry_idx);
            break;
        case RAFT_LOGTYPE_ADD_SHARDGROUP:
            applyAddShardGroup(rr, entry);
            break;
        default:
            break;
    }

    rr->snapshot_info.last_applied_term = entry->term;
    rr->snapshot_info.last_applied_idx = entry_idx;

    return 0;
}

/* ------------------------------------ Utility Callbacks ------------------------------------ */

static void raftLog(raft_server_t *raft, raft_node_t *node, void *user_data, const char *buf)
{
    if (node) {
        Node *n = raft_node_get_udata(node);
        if (n) {
            NODE_TRACE(n, "<raftlib> %s\n", buf);
            return;
        }
    }
    TRACE("<raftlib> %s\n", buf);
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

    TRACE("node:%d has sufficient logs now, adding as voting node.\n", node->id);

    raft_entry_t *entry = raft_entry_new(sizeof(RaftCfgChange));
    entry->id = rand();
    entry->type = RAFT_LOGTYPE_ADD_NODE;

    msg_entry_response_t response;
    RaftCfgChange *cfgchange = (RaftCfgChange *) entry->data;
    cfgchange->id = node->id;
    cfgchange->addr = node->addr;

    int e = raft_recv_entry(raft, entry, &response);
    assert(e == 0);

    raft_entry_release(entry);

    return 0;
}

void raftNotifyMembershipEvent(raft_server_t *raft, void *user_data, raft_node_t *raft_node,
        raft_entry_t *entry, raft_membership_e type)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) user_data;
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
            RaftCfgChange *cfgchange = (RaftCfgChange *) entry->data;
            if (cfgchange->id == my_id) {
                break;
            }

            /* Allocate a new node */
            node = NodeInit(cfgchange->id, &cfgchange->addr);
            assert(node != NULL);

            addUsedNodeId(rr, cfgchange->id);

            raft_node_set_udata(raft_node, node);
            break;

        case RAFT_MEMBERSHIP_REMOVE:
            node = raft_node_get_udata(raft_node);
            if (node != NULL) {
                NodeMarkRemoved(node);
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

        buf = catsnprintf(buf, &buflen, " id=%ld,voting=%d,active=%d,addr=%s",
            raft_node_get_id(rn),
            raft_node_is_voting(rn),
            raft_node_is_active(rn),
            addr);
    }

    return buf;
}

static void raftNotifyStateEvent(raft_server_t *raft, void *user_data, raft_state_e state)
{
    switch (state) {
        case RAFT_STATE_FOLLOWER:
            LOG_INFO("State change: Node is now a follower, term %d\n",
                    raft_get_current_term(raft));
            break;
        case RAFT_STATE_CANDIDATE:
            LOG_INFO("State change: Election starting, node is now a candidate, term %d\n",
                    raft_get_current_term(raft));
            break;
        case RAFT_STATE_LEADER:
            LOG_INFO("State change: Node is now a leader, term %d\n",
                    raft_get_current_term(raft));
            break;
        default:
            break;
    }

    char *s = raftMembershipInfoString(raft);
    LOG_INFO("Cluster Membership: %s\n", s);
    RedisModule_Free(s);
}

raft_cbs_t redis_raft_callbacks = {
    .send_requestvote = raftSendRequestVote,
    .send_appendentries = raftSendAppendEntries,
    .persist_vote = raftPersistVote,
    .persist_term = raftPersistTerm,
    .log = raftLog,
    .log_get_node_id = raftLogGetNodeId,
    .applylog = raftApplyLog,
    .node_has_sufficient_logs = raftNodeHasSufficientLogs,
    .send_snapshot = raftSendSnapshot,
    .notify_membership_event = raftNotifyMembershipEvent,
    .notify_state_event = raftNotifyStateEvent
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
            PANIC("Log and snapshot have different dbids: [log=%s/snapshot=%s]\n",
                    rr->log->dbid, rr->snapshot_info.dbid);
        }
        if (rr->snapshot_info.last_applied_term < rr->log->snapshot_last_term) {
            PANIC("Log term (%lu) does not match snapshot term (%lu), aborting.\n",
                    rr->log->snapshot_last_term, rr->snapshot_info.last_applied_term);
        }
        if (rr->snapshot_info.last_applied_idx + 1 < rr->log->snapshot_last_idx) {
            PANIC("Log initial index (%lu) does not match snapshot last index (%lu), aborting.\n",
                    rr->log->snapshot_last_idx, rr->snapshot_info.last_applied_idx);
        }
    } else {
        /* If there is no snapshot, the log should also not refer to it */
        if (rr->log->snapshot_last_idx) {
            PANIC("Log refers to snapshot (term=%lu/index=%lu which was not loaded, aborting.\n",
                    rr->log->snapshot_last_term, rr->log->snapshot_last_idx);
        }
    }

    /* Reset the log if snapshot is more advanced */
    if (RaftLogCurrentIdx(rr->log) < rr->snapshot_info.last_applied_idx) {
        RaftLogImpl.reset(rr, rr->snapshot_info.last_applied_idx + 1,
            rr->snapshot_info.last_applied_term);
    }

    /* Special case: if no other nodes, set commit index to the latest
     * entry in the log.
     */
    if (raft_get_num_voting_nodes(rr->raft) == 1 || raft_get_num_nodes(rr->raft) == 1) {
        LOG_DEBUG("No other voting nodes, setting commit index to %ld\n",
            raft_get_current_idx(rr->raft));
        raft_set_commit_idx(rr->raft, raft_get_current_idx(rr->raft));
    } else {
        raft_set_commit_idx(rr->raft, rr->snapshot_info.last_applied_idx);
    }

    memcpy(rr->snapshot_info.dbid, rr->log->dbid, RAFT_DBID_LEN);
    rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

    raft_set_snapshot_metadata(rr->raft, rr->snapshot_info.last_applied_term,
            rr->snapshot_info.last_applied_idx);

    raft_apply_all(rr->raft);

    raft_set_current_term(rr->raft, rr->log->term);
    raft_vote_for_nodeid(rr->raft, rr->log->vote);

    LOG_INFO("Raft Log: loaded current term=%lu, vote=%d\n", rr->log->term, rr->log->vote);
    LOG_INFO("Raft state after applying log: log_count=%lu, current_idx=%lu, last_applied_idx=%lu\n",
            raft_get_log_count(rr->raft),
            raft_get_current_idx(rr->raft),
            raft_get_last_applied_idx(rr->raft));

    initializeSnapshotInfo(rr);
    return RR_OK;
}

int raft_get_num_snapshottable_logs(raft_server_t *);

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
        LOG_INFO("Loading: Redis loading complete, snapshot %s\n",
                rr->snapshot_info.loaded ? "LOADED" : "NOT LOADED");

        /* If id is configured, confirm the log matches.  If not, we set it from
         * the log.
         */
        if (!rr->config->id) {
            rr->config->id = rr->log->node_id;
        } else {
            if (rr->config->id != rr->log->node_id) {
                PANIC("Raft log node id [%d] does not match configured id [%d]\n",
                        rr->log->node_id, rr->config->id);
            }
        }

        initRaftLibrary(rr);

        raft_node_t *self = raft_add_non_voting_node(rr->raft, NULL, rr->config->id, 1);
        if (!self) {
            PANIC("Failed to create local Raft node [id %d]\n", rr->config->id);
        }

        if (rr->snapshot_info.loaded) {
            configureFromSnapshot(rr);
        }

        if (loadRaftLog(rr) == RR_OK) {
            if (rr->log->snapshot_last_term) {
                LOG_INFO("Loading: Log starts from snapshot term=%lu, index=%lu\n",
                        rr->log->snapshot_last_term, rr->log->snapshot_last_idx);
            } else {
                LOG_INFO("Loading: Log is complete.\n");
            }

            applyLoadedRaftLog(rr);
            rr->state = REDIS_RAFT_UP;
        } else {
            rr->state = REDIS_RAFT_UNINITIALIZED;
        }
    }
}

static void callRaftPeriodic(uv_timer_t *handle)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) uv_handle_get_data((uv_handle_t *) handle);
    int ret;

    if (processExiting) {
        return;
    }

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
            LOG_ERROR("Snapshot operation failed, cancelling.\n");
            cancelSnapshot(rr, &sr);
        }  else if (ret) {
            LOG_DEBUG("Snapshot operation completed successfully.\n");
            finalizeSnapshot(rr, &sr);
        } /* else we're still in progress */
    }

    ret = raft_periodic(rr->raft, rr->config->raft_interval);
    if (ret == 0) {
        ret = raft_apply_all(rr->raft);
    }

    if (ret == RAFT_ERR_SHUTDOWN) {
        LOG_INFO("*** NODE REMOVED, SHUTTING DOWN.\n");

        if (rr->config->raft_log_filename)
            RaftLogArchiveFiles(rr);
        if (rr->config->rdb_filename)
            archiveSnapshot(rr);
        exit(0);
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
        LOG_DEBUG("Raft log file size is %lu, initiating snapshot.\n",
                rr->log->file_size);
        initiateSnapshot(rr);
    }
}

/* A libuv callback that invokes HandleNodeStates(), to handle node connection
 * management (reconnects, etc.).
 */
static void callHandleNodeStates(uv_timer_t *handle)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) uv_handle_get_data((uv_handle_t *) handle);
    if (processExiting) {
        return;
    }

    HandleNodeStates(rr);
}

/* Main Raft thread, which handles:
 * 1. The libuv loop for managing all connections with other Raft nodes.
 * 2. All Raft periodic tasks.
 * 3. Processing of Raft request queue, for serving RAFT* commands issued
 *    locally on the main Redis thread.
 */
static void RedisRaftThread(void *arg)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) arg;

    /* TODO: Properly handle the race condition here.  We need to be sure Redis
     * initialization has got to the point where RDB loading started.
     */
    usleep(500000);

    uv_timer_start(&rr->raft_periodic_timer, callRaftPeriodic,
            rr->config->raft_interval, rr->config->raft_interval);
    uv_timer_start(&rr->node_reconnect_timer, callHandleNodeStates, 0,
            rr->config->reconnect_interval);
    uv_run(rr->loop, UV_RUN_DEFAULT);
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

static raft_node_id_t makeRandomNodeId(RedisRaftCtx *rr)
{
    unsigned int id;

    /* Generate a random id and validate:
     * 1. It's not zero (reserved value)
     * 2. Avoid negative numbers for better convenience
     * 3. Skip existing IDs, if library is already initialized
     */

    do {
        RedisModule_GetRandomBytes((unsigned char *) &id, sizeof(id));
        id &= ~(1<<31);
    } while (!id || (rr->raft && raft_get_node(rr->raft, id) != NULL) || hasNodeIdBeenUsed(rr, id));

    return id;
}

RRStatus initRaftLog(RedisModuleCtx *ctx, RedisRaftCtx *rr)
{
    rr->log = RaftLogCreate(rr->config->raft_log_filename, rr->snapshot_info.dbid,
            1, 0, 1, -1, rr->config);
    if (!rr->log) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to initialize Raft log");
        return RR_ERROR;
    }

    return RR_OK;
}

RRStatus initCluster(RedisModuleCtx *ctx, RedisRaftCtx *rr, RedisRaftConfig *config)
{
    /* Initialize dbid */
    RedisModule_GetRandomHexChars(rr->snapshot_info.dbid, RAFT_DBID_LEN);
    rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

    /* If node id was not specified, make up one */
    if (!config->id) {
        config->id = makeRandomNodeId(rr);
    }

    /* This is the first node, so there are no used node ids yet */
    rr->snapshot_info.used_node_ids = NULL;

    addUsedNodeId(rr, config->id);

    /* Initialize log */
    if (initRaftLog(ctx, rr) == RR_ERROR) {
        return RR_ERROR;
    }

    initRaftLibrary(rr);

    /* Create our own node */
    raft_node_t *self = raft_add_node(rr->raft, NULL, config->id, 1);
    if (!self) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to initialize raft_node");
        return RR_ERROR;
    }

    /* Become leader and create initial entry */
    rr->state = REDIS_RAFT_UP;
    raft_become_leader(rr->raft);
    raft_set_current_term(rr->raft, 1);

    /* Create a Snapshot Info meta-key */
    initializeSnapshotInfo(rr);

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
        LOG_ERROR("Failed to read Raft log\n");
        return RR_ERROR;
    } else {
        LOG_INFO("Loading: Log loaded, %d entries, snapshot last term=%lu, index=%lu\n",
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
    raft_set_callbacks(rr->raft, &redis_raft_callbacks, rr);
}

static void configureFromSnapshot(RedisRaftCtx *rr)
{
    SnapshotCfgEntry *c;

    LOG_INFO("Loading: Snapshot: applied term=%lu index=%lu\n",
            rr->snapshot_info.last_applied_term,
            rr->snapshot_info.last_applied_idx);

    for (c = rr->snapshot_info.cfg; c != NULL; c = c->next) {
        LOG_INFO("Loading: Snapshot config: node id=%u [%s:%u], voting=%u\n",
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
    STAILQ_INIT(&rr->rqueue);

    /* Register an atexit handler to tell us we're exiting.  Redis offers no
     * other way and we need to be aware of this to avoid getting into execution
     * flows that involve libuv workers that self destructor using .dtors.
     */
    atexit(__setProcessExiting);

    /* Populate Read-only command dict */
    populateReadonlyCommandDict(ctx);

    /* Initialize uv loop */
    rr->loop = RedisModule_Alloc(sizeof(uv_loop_t));
    uv_loop_init(rr->loop);

    /* Requests queue */
    uv_mutex_init(&rr->rqueue_mutex);
    uv_async_init(rr->loop, &rr->rqueue_sig, RaftReqHandleQueue);
    uv_handle_set_data((uv_handle_t *) &rr->rqueue_sig, rr);

    /* Periodic timer */
    uv_timer_init(rr->loop, &rr->raft_periodic_timer);
    uv_handle_set_data((uv_handle_t *) &rr->raft_periodic_timer, rr);

    /* Connection timer */
    uv_timer_init(rr->loop, &rr->node_reconnect_timer);
    uv_handle_set_data((uv_handle_t *) &rr->node_reconnect_timer, rr);

    rr->ctx = RedisModule_GetThreadSafeContext(NULL);
    rr->config = config;

    /* Client state for MULTI support */
    multiClientState = RedisModule_CreateDict(ctx);

    /* Read configuration from Redis */
    if (ConfigReadFromRedis(rr) == RR_ERROR) {
        PANIC("Raft initialization failed: invalid Redis configuration!");
    }

    /* Cluster configuration */
    if (rr->config->cluster_mode) {
        ShardingInfoInit(rr);
    }

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

    if ((rr->log = RaftLogOpen(rr->config->raft_log_filename, rr->config, 0)) != NULL) {
        rr->state = REDIS_RAFT_LOADING;
    } else {
        rr->state = REDIS_RAFT_UNINITIALIZED;
    }

    return RR_OK;
}

RRStatus RedisRaftStart(RedisModuleCtx *ctx, RedisRaftCtx *rr)
{
    /* Start Raft thread */
    if (uv_thread_create(&rr->thread, RedisRaftThread, rr) < 0) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to initialize redis_raft thread");
        return RR_ERROR;
    }

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
    TRACE("RaftReqFree: req=%p, req->ctx=%p, req->client=%p\n", req, req->ctx, req->client);

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
        case RR_LOADSNAPSHOT:
            RedisModule_FreeString(req->ctx, req->r.loadsnapshot.snapshot);
            break;
        case RR_CLUSTER_JOIN:
            NodeAddrListFree(req->r.cluster_join.addr);
            break;
        case RR_DEBUG:
            switch (req->r.debug.type) {
                case RR_DEBUG_COMPACT:
                    break;
                case RR_DEBUG_NODECFG:
                    if (req->r.debug.d.nodecfg.str) {
                        RedisModule_Free(req->r.debug.d.nodecfg.str);
                    }
                    break;
                case RR_DEBUG_SENDSNAPSHOT:
                    break;
            }
            break;
        case RR_SHARDGROUP_ADD:
            if (req->r.shardgroup_add.nodes) {
                RedisModule_Free(req->r.shardgroup_add.nodes);
                req->r.shardgroup_add.nodes = NULL;
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

    TRACE("RaftReqInit: req=%p, type=%s, client=%p, ctx=%p\n",
            req, RaftReqTypeStr[req->type], req->client, req->ctx);

    return req;
}

RaftReq *RaftDebugReqInit(RedisModuleCtx *ctx, enum RaftDebugReqType type)
{
    RaftReq *req = RaftReqInit(ctx, RR_DEBUG);
    req->r.debug.type = type;

    return req;
}

void RaftReqSubmit(RedisRaftCtx *rr, RaftReq *req)
{
    uv_mutex_lock(&rr->rqueue_mutex);
    STAILQ_INSERT_TAIL(&rr->rqueue, req, entries);
    uv_mutex_unlock(&rr->rqueue_mutex);
    uv_async_send(&rr->rqueue_sig);
}

static RaftReq *raft_req_fetch(RedisRaftCtx *rr)
{
    RaftReq *r = NULL;

    uv_mutex_lock(&rr->rqueue_mutex);
    if (!STAILQ_EMPTY(&rr->rqueue)) {
        r = STAILQ_FIRST(&rr->rqueue);
        STAILQ_REMOVE_HEAD(&rr->rqueue, entries);
    }
    uv_mutex_unlock(&rr->rqueue_mutex);

    return r;
}

void RaftReqHandleQueue(uv_async_t *handle)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) uv_handle_get_data((uv_handle_t *) handle);
    RaftReq *req;

    while ((req = raft_req_fetch(rr))) {
        TRACE("RaftReqHandleQueue: req=%p, type=%s\n",
                req, RaftReqTypeStr[req->type]);
        RaftReqHandlers[req->type](rr, req);
    }
}

/* ------------------------------------ RaftReq Implementation ------------------------------------ */

/*
 * Implementation of specific request types.
 */

static void handleRequestVote(RedisRaftCtx *rr, RaftReq *req)
{
    msg_requestvote_response_t response;

    if (checkRaftState(rr, req) == RR_ERROR) {
        goto exit;
    }

    if (raft_recv_requestvote(rr->raft,
                raft_get_node(rr->raft, req->r.requestvote.src_node_id),
                &req->r.requestvote.msg,
                &response) != 0) {
        RedisModule_ReplyWithError(req->ctx, "ERR operation failed"); // TODO: Identify cases
        goto exit;
    }

    RedisModule_ReplyWithArray(req->ctx, 2);
    RedisModule_ReplyWithLongLong(req->ctx, response.term);
    RedisModule_ReplyWithLongLong(req->ctx, response.vote_granted);

exit:
    RaftReqFree(req);
}


static void handleAppendEntries(RedisRaftCtx *rr, RaftReq *req)
{
    msg_appendentries_response_t response;
    int err;

    if (checkRaftState(rr, req) == RR_ERROR) {
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

static void handleCfgChange(RedisRaftCtx *rr, RaftReq *req)
{
    raft_entry_t *entry;
    msg_entry_response_t response;
    raft_node_t *rn;
    int e;

    if (checkRaftState(rr, req) == RR_ERROR ||
        checkLeader(rr, req, NULL) == RR_ERROR) {
        goto exit;
    }

    entry = raft_entry_new(sizeof(req->r.cfgchange));
    entry->id = rand();

    switch (req->type) {
        case RR_CFGCHANGE_ADDNODE:
            entry->type = RAFT_LOGTYPE_ADD_NONVOTING_NODE;
            if (!req->r.cfgchange.id) {
                req->r.cfgchange.id = makeRandomNodeId(rr);
            }
            break;
        case RR_CFGCHANGE_REMOVENODE:
            /* To remove a voting node, we demote it first. */
            rn = raft_get_node(rr->raft, req->r.cfgchange.id);
            assert (rn != NULL);    /* Should have been verified by now! */
            if (raft_node_is_voting(rn) || raft_node_is_voting_committed(rn)) {
                entry->type = RAFT_LOGTYPE_DEMOTE_NODE;
            } else {
                entry->type = RAFT_LOGTYPE_REMOVE_NODE;
            }
            break;
        default:
            assert(0);
    }

    memcpy(entry->data, &req->r.cfgchange, sizeof(req->r.cfgchange));
    if ((e = raft_recv_entry(rr->raft, entry, &response)) != 0) {
        replyRaftError(req->ctx, e);
    } else {
        if (req->type == RR_CFGCHANGE_REMOVENODE) {
            RedisModule_ReplyWithSimpleString(req->ctx, "OK");
        } else {
            RedisModule_ReplyWithArray(req->ctx, 2);
            RedisModule_ReplyWithLongLong(req->ctx, req->r.cfgchange.id);
            RedisModule_ReplyWithSimpleString(req->ctx, rr->snapshot_info.dbid);
        }
    }

    raft_entry_release(entry);

exit:
    RaftReqFree(req);
}

static void handleReadOnlyCommand(void *arg, int can_read)
{
    RaftReq *req = (RaftReq *) arg;

    if (!can_read) {
        RedisModule_ReplyWithError(req->ctx, "TIMEOUT no quorum for read");
        goto exit;
    }

    RedisModule_ThreadSafeContextLock(req->ctx);
    executeRaftRedisCommandArray(&req->r.redis.cmds, req->ctx, req->ctx);
    RedisModule_ThreadSafeContextUnlock(req->ctx);

exit:
    RaftReqFree(req);
}

static bool checkReadOnlyCommand(RedisModuleString *cmd)
{
    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd, &cmd_len);
    char lcmd[cmd_len];

    int i;
    for (i = 0; i < cmd_len; i++) {
        lcmd[i] = tolower(cmd_str[i]);
    }

    return RedisModule_DictGetC(readonlyCommandDict, lcmd, cmd_len, NULL) != NULL;
}

static bool checkReadOnlyCommandArray(RaftRedisCommandArray *array)
{
    int i;

    for (i = 0; i < array->len; i++) {
        if (!checkReadOnlyCommand(array->commands[i]->argv[0]))
            return false;
    }

    return true;
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
        /* TODO: Add command checks and set multiState->error if necessary; This probably requires
         * Module API extensions.
         */
        RaftRedisCommandArrayMove(&multiState->cmds, &req->r.redis.cmds);
        RedisModule_ReplyWithSimpleString(req->ctx, "QUEUED");

        RaftReqFree(req);
        return true;
    }

    return false;
}

/* Handle interception of Redis commands that have a different
 * implementation in RedisRaft.
 *
 * This is logically similar to handleMultiExec but implemented
 * separately for readability purposes.
 *
 * Currently intercepted commands:
 * - CLUSTER
 *
 * Returns true if the command was intercepted, in which case the RaftReq has
 * been replied to and freed.
 */

static bool handleInterceptedCommands(RedisRaftCtx *rr, RaftReq *req)
{
    const char _cmd_cluster[] = "CLUSTER";
    RaftRedisCommand *cmd = req->r.redis.cmds.commands[0];
    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[0], &cmd_len);

    if (cmd_len == sizeof(_cmd_cluster) - 1 &&
        !strncasecmp(cmd_str, _cmd_cluster, sizeof(_cmd_cluster) - 1)) {
            handleClusterCommand(rr, req);
            return true;
    }

    return false;
}

/* When cluster_mode is enabled, handle clustering aspects before processing
 * the request:
 *
 * 1. Compute hash slot of all associated keys and validate there's no cross-slot
 *    violation.
 * 2. Update the request's hash_slot for future refrence.
 * 3. If the hash slot is associated with a foreign ShardGroup, perform a redirect.
 * 4. If the hash slot is not mapped, produce a CLUSTERDOWN error.
 */

static RRStatus handleClustering(RedisRaftCtx *rr, RaftReq *req)
{
    if (computeHashSlot(rr, req) != RR_OK) {
        RedisModule_ReplyWithError(req->ctx, "CROSSSLOT Keys in request don't hash to the same slot");
        return RR_ERROR;
    }

    /* If command has no keys, continue */
    if (req->r.redis.hash_slot == -1) return RR_OK;

    /* Make sure hash slot is mapped and handled locally. */
    int sgid = rr->sharding_info->hash_slots_map[req->r.redis.hash_slot];
    if (!sgid) {
        RedisModule_ReplyWithError(req->ctx, "CLUSTERDOWN Hash slot is not served");
        return RR_ERROR;
    }

    /* If accessing a foreign shardgroup, issue a redirect. We use round-robin
     * to all nodes to compensate for the fact we do not have an up-to-date knowledge
     * of who the leader is and whether or not some configuration has changed since
     * last refresh (when refresh is implemented, in the future).
     */
    if (sgid != 1) {
        ShardGroup *sg = &rr->sharding_info->shard_groups[sgid-1];
        if (sg->next_redir >= sg->nodes_num)
            sg->next_redir = 0;
        replyRedirect(rr, req, &sg->nodes[sg->next_redir++].addr);
        return RR_ERROR;
    }

    return RR_OK;
}

static void handleRedisCommand(RedisRaftCtx *rr,RaftReq *req)
{
    Node *leader_proxy = NULL;

    /* If this is a request from a local client which is no longer connected,
     * dont process it.
     *
     * NOTE: This is required for consistency reasons. MULTI/EXEC needs to do CAS checks at
     * EXEC time, however the state of the client will be unavailable if it is connected.
     * In that case, we need not only to discard the EXEC but also any command that followed
     * in order to maintain consistency.
     */

    if (req->ctx && RedisModule_BlockedClientDisconnected(req->ctx)) {
        goto exit;
    }

    /* MULTI/EXEC bundling takes place only if we have a single command. If we have multiple
     * commands we've received this as a RAFT.ENTRY input and bundling, probably through a
     * proxy, and bundling was done before.
     */
    if (req->r.redis.cmds.len == 1) {
        if (handleMultiExec(rr, req)) {
            return;
        }
    }

    /* Check that we're part of a boostrapped cluster and not in the middle of joining
     * or loading data.
     */
    if (checkRaftState(rr, req) == RR_ERROR) {
        goto exit;
    }

    /* Handle intercepted commands. We do this also on non-leader nodes or if we don't
     * have a leader, so it's up to the commands to check these conditions if they have to.
     */
    if (handleInterceptedCommands(rr, req)) {
        return;
    }

    /* Confirm that we're the leader and handle redirect or proxying if not. */
    if (checkLeader(rr, req, rr->config->follower_proxy ? &leader_proxy : NULL) == RR_ERROR) {
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

    /* When we're in cluster mode, go through handleClustering. This will perform
     * hash slot validation and return an error / redirection if necessary.
     */
    if (rr->config->cluster_mode && handleClustering(rr, req) != RR_OK) {
        goto exit;
    }

    /* If command is read only we don't push it to the log, but queue it
     * until we can confirm it's safe to execute (i.e. still a leader).
     */
    if (checkReadOnlyCommandArray(&req->r.redis.cmds)) {
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
    raft_entry_release(entry);

    if (e != 0) {
        replyRaftError(req->ctx, e);
        goto exit;
    }

    /* If we're a single node we can try to apply now, as we have no need
     * or way to wait for AE responses to do that.
     */
    if (raft_get_current_idx(rr->raft) == raft_get_commit_idx(rr->raft)) {
        raft_apply_all(rr->raft);
    }

    /* Unless applied by raft_apply_all() (and freed by it), the request
     * is pending so we don't free it or unblock the client.
     */
    return;

exit:
    RaftReqFree(req);
}

static void handleInfo(RedisRaftCtx *rr, RaftReq *req)
{
    size_t slen = 1024;
    char *s = RedisModule_Calloc(1, slen);

    char role[10];
    if (!rr->raft) {
        strcpy(role, "-");
    } else {
        switch (raft_get_state(rr->raft)) {
            case RAFT_STATE_FOLLOWER:
                strcpy(role, "follower");
                break;
            case RAFT_STATE_LEADER:
                strcpy(role, "leader");
                break;
            case RAFT_STATE_CANDIDATE:
                strcpy(role, "candidate");
                break;
            default:
                strcpy(role, "(none)");
                break;
        }
    }

    raft_node_t *me = rr->raft ? raft_get_my_node(rr->raft) : NULL;
    s = catsnprintf(s, &slen,
            "# Raft\r\n"
            "node_id:%d\r\n"
            "state:%s\r\n"
            "role:%s\r\n"
            "is_voting:%s\r\n"
            "leader_id:%d\r\n"
            "current_term:%d\r\n"
            "num_nodes:%d\r\n"
            "num_voting_nodes:%d\r\n",
            rr->config->id,
            getStateStr(rr),
            role,
            me ? (raft_node_is_voting(raft_get_my_node(rr->raft)) ? "yes" : "no") : "-",
            rr->raft ? raft_get_current_leader(rr->raft) : -1,
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
                "node%d:id=%d,state=%s,voting=%s,addr=%s,port=%d,last_conn_secs=%ld,conn_errors=%lu,conn_oks=%lu\r\n",
                i, node->id, NodeStateStr[node->state],
                raft_node_is_voting(rnode) ? "yes" : "no",
                node->addr.host, node->addr.port,
                node->last_connected_time ? (now - node->last_connected_time)/1000 : -1,
                node->connect_errors, node->connect_oks);
    }

    s = catsnprintf(s, &slen,
            "\r\n# Log\r\n"
            "log_entries:%d\r\n"
            "current_index:%d\r\n"
            "commit_index:%d\r\n"
            "last_applied_index:%d\r\n"
            "file_size:%lu\r\n"
            "cache_memory_size:%lu\r\n"
            "cache_entries:%lu\r\n"
            "client_attached_entries:%lu\r\n",
            rr->raft ? raft_get_log_count(rr->raft) : 0,
            rr->raft ? raft_get_current_idx(rr->raft) : 0,
            rr->raft ? raft_get_commit_idx(rr->raft) : 0,
            rr->raft ? raft_get_last_applied_idx(rr->raft) : 0,
            rr->log ? rr->log->file_size : 0,
            rr->logcache ? rr->logcache->entries_memsize : 0,
            rr->logcache ? rr->logcache->len : 0,
            rr->client_attached_entries);

    s = catsnprintf(s, &slen,
            "\r\n# Snapshot\r\n"
            "snapshot_in_progress:%s\r\n"
            "snapshots_loaded:%lu\r\n",
            rr->snapshot_in_progress ? "yes" : "no",
            rr->snapshots_loaded);

    s = catsnprintf(s, &slen,
            "\r\n# Clients\r\n"
            "clients_in_multi_state:%d\r\n"
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

static void handleClusterInit(RedisRaftCtx *rr, RaftReq *req)
{
    if (checkRaftNotLoading(rr, req) == RR_ERROR) {
        goto exit;
    }

    if (rr->state != REDIS_RAFT_UNINITIALIZED) {
        RedisModule_ReplyWithError(req->ctx, "ERR Already cluster member");
        goto exit;
    }

    if (initCluster(req->ctx, rr, rr->config) == RR_ERROR) {
        RedisModule_ReplyWithError(req->ctx, "ERR Failed to initialize, check logs");
        goto exit;
    }

    char reply[RAFT_DBID_LEN+5];
    snprintf(reply, sizeof(reply) - 1, "OK %s", rr->snapshot_info.dbid);

    rr->state = REDIS_RAFT_UP;
    RedisModule_ReplyWithSimpleString(req->ctx, reply);

    LOG_INFO("Raft Cluster initialized, node id: %ld, dbid: %s\n", rr->config->id, rr->snapshot_info.dbid);
exit:
    RaftReqFree(req);
}

static void handleClusterJoin(RedisRaftCtx *rr, RaftReq *req)
{
    if (checkRaftNotLoading(rr, req) == RR_ERROR) {
        goto exit;
    }

    if (rr->state != REDIS_RAFT_UNINITIALIZED) {
        RedisModule_ReplyWithError(req->ctx, "ERR Already cluster member");
        goto exit;
    }

    assert(!rr->join_state);
    rr->join_state = RedisModule_Calloc(1, sizeof(RaftJoinState));

    rr->join_state->addr = req->r.cluster_join.addr;
    req->r.cluster_join.addr = NULL;    /* We now own it in join_state! */

    /* Create a Snapshot Info meta-key */
    initializeSnapshotInfo(rr);

    rr->state = REDIS_RAFT_JOINING;

    RedisModule_ReplyWithSimpleString(req->ctx, "OK");

exit:
    RaftReqFree(req);
}

void HandleClusterJoinCompleted(RedisRaftCtx *rr)
{
    /* Initialize Raft log.  We delay this operation as we want to create the log
     * with the proper dbid which is only received now.
     */
    rr->log = RaftLogCreate(rr->config->raft_log_filename, rr->snapshot_info.dbid,
            rr->snapshot_info.last_applied_term, rr->snapshot_info.last_applied_idx,
            1, -1,
            rr->config);
    if (!rr->log) {
        PANIC("Failed to initialize Raft log");
    }

    initRaftLibrary(rr);

    /* Create our own node */
    raft_node_t *self = raft_add_non_voting_node(rr->raft, NULL, rr->config->id, 1);
    if (!self) {
        PANIC("Failed to initialize raft_node\n");
    }

    rr->state = REDIS_RAFT_UP;
}

static void handleClientDisconnect(RedisRaftCtx *rr, RaftReq *req)
{
    freeMultiExecState(req->r.client_disconnect.client_id);
    RaftReqFree(req);
}

static void handleDebugNodeCfg(RedisRaftCtx *rr, RaftReq *req)
{
    char *saveptr = NULL;
    char *tok;

    raft_node_t *node = raft_get_node(rr->raft, req->r.debug.d.nodecfg.id);
    if (!node) {
        RedisModule_ReplyWithError(req->ctx, "ERR node does not exist");
        goto exit;
    }

    tok = strtok_r(req->r.debug.d.nodecfg.str, " ", &saveptr);
    while (tok != NULL) {
        if (!strcasecmp(tok, "+voting")) {
            raft_node_set_voting(node, 1);
            raft_node_set_voting_committed(node, 1);
        } else if (!strcasecmp(tok, "-voting")) {
            raft_node_set_voting(node, 0);
            raft_node_set_voting_committed(node, 0);
        } else if (!strcasecmp(tok, "+active")) {
            raft_node_set_active(node, 1);
        } else if (!strcasecmp(tok, "-active")) {
            raft_node_set_active(node, 0);
        } else {
            RedisModule_ReplyWithError(req->ctx, "ERR invalid nodecfg option specified");
            goto exit;
        }
        tok = strtok_r(NULL, " ", &saveptr);
    }

    RedisModule_ReplyWithSimpleString(req->ctx, "OK");

exit:
    RaftReqFree(req);
}

static void handleDebugSendSnapshot(RedisRaftCtx *rr, RaftReq *req)
{
    raft_node_t *node = raft_get_node(rr->raft, req->r.debug.d.nodecfg.id);
    if (!node) {
        RedisModule_ReplyWithError(req->ctx, "ERR node does not exist");
        goto exit;
    }

    if (raftSendSnapshot(rr->raft, rr, node) != 0) {
        RedisModule_ReplyWithError(req->ctx, "ERR failed to send snapshot");
        goto exit;
    }

    RedisModule_ReplyWithSimpleString(req->ctx, "OK");

exit:
    RaftReqFree(req);
}

void handleDebug(RedisRaftCtx *rr, RaftReq *req)
{
    switch (req->r.debug.type) {
        case RR_DEBUG_COMPACT:
            rr->debug_req = req;

            if (initiateSnapshot(rr) != RR_OK) {
                LOG_VERBOSE("RAFT.DEBUG COMPACT requested but failed.\n");
                RedisModule_ReplyWithError(req->ctx, "ERR operation failed, nothing to compact?");
                rr->debug_req = NULL;
                RaftReqFree(req);
            }
            break;
        case RR_DEBUG_NODECFG:
            handleDebugNodeCfg(rr, req);
            break;
        case RR_DEBUG_SENDSNAPSHOT:
            handleDebugSendSnapshot(rr, req);
            break;
        default:
            assert(0);
    }
}

/* Apply a RAFT_LOGTYPE_ADD_SHARDGROUP log entry by deserializing its payload and
 * updating the in-memory shardgroup configuration.
 *
 * If the entry holds a user_data pointing to a RaftReq, this implies we're
 * applying an operation performed by a local client (vs. one received from
 * persisted log, or through AppendEntries). In that case, we also need to
 * generate the reply as the client is blocked waiting for it.
 */
void applyAddShardGroup(RedisRaftCtx *rr, raft_entry_t *entry)
{
    ShardGroup sg;
    RRStatus ret;

    if (ShardGroupDeserialize(entry->data, entry->data_len, &sg) != RR_OK) {
        LOG_ERROR("Failed to deserialize ADD_SHARDGROUP payload: [%.*s]",
                entry->data_len, entry->data);
        return;
    }

    if ((ret = ShardingInfoAddShardGroup(rr, &sg)) != RR_OK)
        LOG_ERROR("Failed to add a shardgroup");

    ShardGroupFree(&sg);

    /* If we have an attached client, handle the reply */
    if (entry->user_data) {
        RaftReq *req = entry->user_data;

        if (ret == RR_OK) {
            RedisModule_ReplyWithSimpleString(req->ctx, "OK");
        } else {
            /* Should never happen! */
            RedisModule_ReplyWithError(req->ctx, "ERR failed to locally apply change, should never happen.");
        }
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
    if (checkRaftState(rr, req) == RR_ERROR ||
        checkLeader(rr, req, NULL) == RR_ERROR) {
        goto exit;
    }

    /* Validate now before pushing this as a log entry. */
    if (ShardingInfoValidateShardGroup(rr, &req->r.shardgroup_add) != RR_OK) {
        RedisModule_ReplyWithError(req->ctx, "ERR invalid shardgroup configuration. Consult the logs for more info.");
        goto exit;
    }

    /* Serialize the shardgroup */
    char *payload = ShardGroupSerialize(&req->r.shardgroup_add);
    if (!payload)
        goto exit;

    /* Set up a Raft log entry */
    raft_entry_t *entry = raft_entry_new(strlen(payload));
    entry->type = RAFT_LOGTYPE_ADD_SHARDGROUP;
    entry->id = rand();
    entryAttachRaftReq(rr, entry, req); /* So we can produce a reply */
    memcpy(entry->data, payload, strlen(payload));
    RedisModule_Free(payload);

    /* Submit */
    msg_entry_response_t response;
    int e = raft_recv_entry(rr->raft, entry, &response);
    if (e != 0) {
        replyRaftError(req->ctx, e);
    }

    raft_entry_release(entry);
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
    int alen;

    /* Must be done on a leader */
    if (checkRaftState(rr, req) == RR_ERROR ||
        checkLeader(rr, req, NULL) == RR_ERROR) {
        goto exit;
    }

    RedisModule_ReplyWithArray(req->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModule_ReplyWithLongLong(req->ctx, rr->config->cluster_start_hslot);
    RedisModule_ReplyWithLongLong(req->ctx, rr->config->cluster_end_hslot);
    alen = 2;

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

        char node_id[RAFT_SHARDGROUP_NODEID_LEN+1];
        snprintf(node_id, sizeof(node_id), "%s%08x", rr->log->dbid, raft_node_get_id(raft_node));
        RedisModule_ReplyWithStringBuffer(req->ctx, node_id, strlen(node_id));

        char addrstr[512];
        snprintf(addrstr, sizeof(addrstr), "%s:%u", addr->host, addr->port);
        RedisModule_ReplyWithStringBuffer(req->ctx, addrstr, strlen(addrstr));

        alen += 2;
    }

    RedisModule_ReplySetArrayLength(req->ctx, alen);

exit:
    RaftReqFree(req);
}


static RaftReqHandler RaftReqHandlers[] = {
    NULL,
    handleClusterInit,      /* RR_CLUSTER_INIT */
    handleClusterJoin,      /* RR_CLUSTER_JOIN */
    handleCfgChange,        /* RR_CFGCHANGE_ADDNODE */
    handleCfgChange,        /* RR_CFGCHANGE_REMOVENODE */
    handleAppendEntries,    /* RR_APPENDENTRIES */
    handleRequestVote,      /* RR_REQUESTVOTE */
    handleRedisCommand,     /* RR_REDISCOMMAND */
    handleInfo,             /* RR_INFO */
    handleLoadSnapshot,     /* RR_LOADSNAPSHOT */
    handleDebug,            /* RR_DEBUG */
    handleClientDisconnect, /* RR_CLIENT_DISCONNECT */
    handleShardGroupAdd,    /* RR_SHARDGROUP_ADD */
    handleShardGroupGet,    /* RR_SHARDGROUP_GET */
    NULL
};
