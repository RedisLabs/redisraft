#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <endian.h>

#if __BYTE_ORDER != __LITTLE_ENDIAN
#error Byte order swapping is currently not implemented.
#endif

#include "redisraft.h"

const char *raft_logtype_str(int type)
{
    static const char *logtype_str[] = {
        "NORMAL",
        "ADD_NONVOTING_NODE",
        "ADD_NODE",
        "DEMOTE_NODE",
        "REMOVE_NODE",
        "SNAPSHOT",
        "(unknown)"
    };
    static const char *logtype_unknown = "(unknown)";

    if (type < RAFT_LOGTYPE_NORMAL || type > RAFT_LOGTYPE_SNAPSHOT) {
        return logtype_unknown;
    } else {
        return logtype_str[type];
    }
}

static void replyRaftError(RedisModuleCtx *ctx, int error)
{
    char buf[128];

    switch (error) {
        case RAFT_ERR_NOT_LEADER:
            RedisModule_ReplyWithError(ctx, "-ERR not leader");
            break;
        case RAFT_ERR_SHUTDOWN:
            LOG_ERROR("Raft requires immediate shutdown!\n");
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

/* ------------------------------------ RaftRedisCommand ------------------------------------ */

/* Serialize a RaftRedisCommand into a Raft entry */
void RaftRedisCommandSerialize(raft_entry_data_t *target, RaftRedisCommand *source)
{
    size_t sz = sizeof(size_t) * (source->argc + 1);
    size_t len;
    int i;
    char *p;

    /* Compute sizes */
    for (i = 0; i < source->argc; i++) {
        RedisModule_StringPtrLen(source->argv[i], &len);
        sz += len;
    }

    /* Serialize argc */
    p = target->buf = RedisModule_Alloc(sz);
    target->len = sz;

    *(size_t *)p = source->argc;
    p += sizeof(size_t);

    /* Serialize argumnets */
    for (i = 0; i < source->argc; i++) {
        const char *d = RedisModule_StringPtrLen(source->argv[i], &len);
        *(size_t *)p = len;
        p += sizeof(size_t);
        memcpy(p, d, len);
        p += len;
    }
}

/* Deserialize a RaftRedisCommand from a Raft entry */
bool RaftRedisCommandDeserialize(RedisModuleCtx *ctx,
        RaftRedisCommand *target, raft_entry_data_t *source)
{
    char *p = source->buf;
    size_t argc = *(size_t *)p;
    p += sizeof(size_t);

    target->argv = RedisModule_Calloc(argc, sizeof(RedisModuleString *));
    target->argc = argc;

    int i;
    for (i = 0; i < argc; i++) {
        size_t len = *(size_t *)p;
        p += sizeof(size_t);

        target->argv[i] = RedisModule_CreateString(ctx, p, len);
        p += len;
    }

    return true;
}

/* Free a RaftRedisCommand */
void RaftRedisCommandFree(RedisModuleCtx *ctx, RaftRedisCommand *r)
{
    int i;

    for (i = 0; i < r->argc; i++) {
        RedisModule_FreeString(ctx, r->argv[i]);
    }
    RedisModule_Free(r->argv);
}


/* ------------------------------------ Log Execution ------------------------------------ */

/*
 * Execution of Raft log on the local instance.  There are two variants:
 * 1) Execution of a raft entry received from another node.
 * 2) Execution of a locally initiated command.
 */

static void executeLogEntry(RedisRaftCtx *rr, raft_entry_t *entry)
{
    RaftRedisCommand rcmd;
    RaftRedisCommandDeserialize(rr->ctx, &rcmd, &entry->data);
    RaftReq *req = entry->user_data;
    RedisModuleCtx *ctx = req ? req->ctx : rr->ctx;

    size_t cmdlen;
    const char *cmd = RedisModule_StringPtrLen(rcmd.argv[0], &cmdlen);

    RedisModule_ThreadSafeContextLock(ctx);
    RedisModuleCallReply *reply = RedisModule_Call(
            ctx, cmd, "v",
            &rcmd.argv[1],
            rcmd.argc - 1);
    RedisModule_ThreadSafeContextUnlock(ctx);
    if (req) {
        if (reply) {
            RedisModule_ReplyWithCallReply(ctx, reply);
        } else {
            RedisModule_ReplyWithError(ctx, "Unknown command/arguments");
        }
        RedisModule_UnblockClient(req->client, NULL);
    }

    if (reply) {
        RedisModule_FreeCallReply(reply);
    }

    RaftRedisCommandFree(rr->ctx, &rcmd);
}


/* ------------------------------------ RequestVote ------------------------------------ */

static void handleRequestVoteResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;
    assert(reply != NULL);

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        NODE_LOG_ERROR(node, "RAFT.REQUESTVOTE failed: %s\n", reply ? reply->str : "connection dropped.");
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
    assert(raft_node != NULL);

    int ret;
    if ((ret = raft_recv_requestvote_response(
            rr->raft,
            raft_node,
            &response)) != 0) {
        LOG_ERROR("raft_recv_requestvote_response failed, error %d\n", ret);
    }
    NODE_LOG_INFO(node, "received requestvote response\n");
}


static int raftSendRequestVote(raft_server_t *raft, void *user_data,
        raft_node_t *raft_node, msg_requestvote_t *msg)
{
    Node *node = (Node *) raft_node_get_udata(raft_node);
    RedisRaftCtx *rr = user_data;

    if (node->state != NODE_CONNECTED) {
        NODE_LOG_DEBUG(node, "not connected, state=%u\n", node->state);
        return 0;
    }

    /* RAFT.REQUESTVOTE <src_node_id> <term> <candidate_id> <last_log_idx> <last_log_term> */
    if (redisAsyncCommand(node->rc, handleRequestVoteResponse,
                node, "RAFT.REQUESTVOTE %d %d:%d:%d:%d",
                raft_get_nodeid(raft),
                msg->term,
                msg->candidate_id,
                msg->last_log_idx,
                msg->last_log_term) != REDIS_OK) {
        NODE_LOG_ERROR(node, "failed requestvote");
    }

    return 0;
}

/* ------------------------------------ AppendEntries ------------------------------------ */

static void handleAppendEntriesResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        NODE_LOG_ERROR(node, "RAFT.APPENDENTRIES failed: %s\n", reply ? reply->str : "connection dropped.");
        return;
    }

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 4 ||
            reply->element[0]->type != REDIS_REPLY_INTEGER ||
            reply->element[1]->type != REDIS_REPLY_INTEGER ||
            reply->element[2]->type != REDIS_REPLY_INTEGER ||
            reply->element[3]->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_ERROR(node, "invalid RAFT.APPENDENTRIES reply\n");
        return;
    }

    msg_appendentries_response_t response = {
        .term = reply->element[0]->integer,
        .success = reply->element[1]->integer,
        .current_idx = reply->element[2]->integer,
        .first_idx = reply->element[3]->integer
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);
    assert(raft_node != NULL);

    int ret;
    if ((ret = raft_recv_appendentries_response(
            rr->raft,
            raft_node,
            &response)) != 0) {
        NODE_LOG_ERROR(node, "raft_recv_appendentries_response failed, error %d\n", ret);
    }

    /* Maybe we have pending stuff to apply now */
    raft_apply_all(rr->raft);
}

static int raftSendAppendEntries(raft_server_t *raft, void *user_data,
        raft_node_t *raft_node, msg_appendentries_t *msg)
{
    Node *node = (Node *) raft_node_get_udata(raft_node);
    RedisRaftCtx *rr = user_data;

    int argc = 4 + msg->n_entries * 2;
    char *argv[argc];
    size_t argvlen[argc];

    if (node->state != NODE_CONNECTED) {
        NODE_LOG_ERROR(node, "not connected, state=%u\n", node->state);
        return 0;
    }

    char argv1_buf[12];
    char argv2_buf[50];
    char argv3_buf[12];
    argv[0] = "RAFT.APPENDENTRIES";
    argvlen[0] = strlen(argv[0]);
    argv[1] = argv1_buf;
    argvlen[1] = snprintf(argv1_buf, sizeof(argv1_buf)-1, "%d", raft_get_nodeid(raft));
    argv[2] = argv2_buf;
    argvlen[2] = snprintf(argv2_buf, sizeof(argv2_buf)-1, "%d:%d:%d:%d",
            msg->term,
            msg->prev_log_idx,
            msg->prev_log_term,
            msg->leader_commit);
    argv[3] = argv3_buf;
    argvlen[3] = snprintf(argv3_buf, sizeof(argv3_buf)-1, "%d", msg->n_entries);

    int i;
    for (i = 0; i < msg->n_entries; i++) {
        raft_entry_t *e = &msg->entries[i];
        argv[4 + i*2] = RedisModule_Alloc(64);
        argvlen[4 + i*2] = snprintf(argv[4 + i*2], 63, "%d:%d:%d", e->term, e->id, e->type);
        argvlen[5 + i*2] = e->data.len;
        argv[5 + i*2] = e->data.buf;
    }

    if (redisAsyncCommandArgv(node->rc, handleAppendEntriesResponse,
                node, argc, (const char **)argv, argvlen) != REDIS_OK) {
        NODE_LOG_ERROR(node, "failed appendentries");
    }

    for (i = 0; i < msg->n_entries; i++) {
        RedisModule_Free(argv[4 + i*2]);
    }
    return 0;
}

/* ------------------------------------ Log Callbacks ------------------------------------ */

static int raftPersistVote(raft_server_t *raft, void *user_data, int vote)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) user_data;
    if (!rr->log) {
        return 0;
    }

    rr->log->header->vote = vote;
    if (!RaftLogUpdate(rr->log, true)) {
        return RAFT_ERR_SHUTDOWN;
    }

    return 0;
}

static int raftPersistTerm(raft_server_t *raft, void *user_data, int term, int vote)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) user_data;
    if (!rr->log) {
        return 0;
    }

    rr->log->header->term = term;
    if (rr->log && !RaftLogUpdate(rr->log, true)) {
        return RAFT_ERR_SHUTDOWN;
    }

    return 0;
}

static int raftLogOffer(raft_server_t *raft, void *user_data, raft_entry_t *entry, int entry_idx)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) user_data;
    raft_node_t *raft_node;
    Node *node;

    TRACE("raftLogOffer: entry_idx=%d\n", entry_idx);

    /* If we're in the process of loading from disk, don't feedback back to
     * the disk.
     */
    if (rr->log && rr->state != REDIS_RAFT_LOADING) {
        if (!RaftLogAppend(rr->log, entry)) {
            return RAFT_ERR_SHUTDOWN;
        }
    }

    if (!raft_entry_is_cfg_change(entry)) {
        return 0;
    }

    RaftCfgChange *req = (RaftCfgChange *) entry->data.buf;

    TRACE("Processing RaftCfgChange for node:%d, entry id=%d, type=%s\n",
            req->id, entry->id, raft_logtype_str(entry->type));

    switch (entry->type) {
        case RAFT_LOGTYPE_REMOVE_NODE:
            raft_node = raft_get_node(raft, req->id);
            assert(raft_node != NULL);
            raft_remove_node(raft, raft_node);
            break;

        case RAFT_LOGTYPE_ADD_NODE:
            /* When adding a node that is not us, create a Node object to
             * manage communication with it.
             *
             * Node object may already exist, as add node may be used to just
             * promote to voting. */
            raft_node = raft_get_node(raft, req->id);
            node = NULL;
            if (req->id != raft_get_nodeid(raft)) {
                if (raft_node) {
                    node = raft_node_get_udata(raft_node);
                } else {
                    node = NodeInit(req->id, &req->addr);
                }
                assert(node != NULL);
            }
            raft_node = raft_add_node(raft, node, req->id,
                    req->id == raft_get_nodeid(raft));
            assert(raft_node != NULL);
            assert(raft_node_is_voting(raft_node));
            break;

        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            node = NodeInit(req->id, &req->addr);
            raft_node = raft_add_non_voting_node(raft, node, node->id,
                    node->id == raft_get_nodeid(raft));
            // Catch errors, but it may be okay to fail adding ourselves as
            // non-voting because we did that already.
            //assert(raft_node == NULL && node->id != raft_get_nodeid(raft));
            break;
    }

    return 0;
}

static int raftLogPop(raft_server_t *raft, void *user_data, raft_entry_t *entry, int entry_idx)
{
    RedisRaftCtx *rr = user_data;

    if (!rr->log) {
        return 0;
    }

    if (!RaftLogRemoveTail(rr->log)) {
        return -1;
    }

    return 0;
}

static int raftLogPoll(raft_server_t *raft, void *user_data, raft_entry_t *entry, int entry_idx)
{
    RedisRaftCtx *rr = user_data;

    TRACE("raftLogPoll: entry_idx=%d\n", entry_idx);

    if (!rr->log) {
        return 0;
    }

    if (!RaftLogRemoveHead(rr->log)) {
        LOG_DEBUG("raftLogPoll: RaftLogRemoveHead() failed!\n");
        return -1;
    }

    return 0;
}

static int raftApplyLog(raft_server_t *raft, void *user_data, raft_entry_t *entry, int entry_idx)
{
    RedisRaftCtx *rr = user_data;
    RaftCfgChange *req;

    /* Update commit index.
     * TODO: Do we want to write it now? Probably not sync though.
     */
    if (rr->log) {
        if (entry_idx > rr->log->header->commit_idx) {
            rr->log->header->commit_idx = entry_idx;
            RaftLogUpdate(rr->log, false);
        }
    }

    switch (entry->type) {
        case RAFT_LOGTYPE_REMOVE_NODE:

            req = (RaftCfgChange *) entry->data.buf;
            if (req->id == raft_get_nodeid(raft)) {
                return RAFT_ERR_SHUTDOWN;
            }
            break;

        case RAFT_LOGTYPE_NORMAL:
            executeLogEntry(rr, entry);
            break;
        default:
            break;
    }
    return 0;
}

/* ------------------------------------ Utility Callbacks ------------------------------------ */

static void raftLog(raft_server_t *raft, raft_node_t *node, void *user_data, const char *buf)
{
    if (node) {
        Node *n = raft_node_get_udata(node);
        if (n) {
            NODE_LOG_DEBUG(n, "[raft] %s\n", buf);
            return;
        }
    }
    LOG_DEBUG("[raft] %s\n", buf);
}

static int raftLogGetNodeId(raft_server_t *raft, void *user_data, raft_entry_t *entry, int entry_idx)
{
    RaftCfgChange *req = (RaftCfgChange *) entry->data.buf;
    return req->id;
}

static int raftNodeHasSufficientLogs(raft_server_t *raft, void *user_data, raft_node_t *raft_node)
{
    Node *node = raft_node_get_udata(raft_node);
    assert (node != NULL);

    TRACE("node:%u has sufficient logs now, adding as voting node.\n", node->id);

    raft_entry_t entry = {
        .id = rand(),
        .type = RAFT_LOGTYPE_ADD_NODE
    };
    msg_entry_response_t response;

    RaftCfgChange *req;
    entry.data.len = sizeof(RaftCfgChange);
    entry.data.buf = RedisModule_Alloc(entry.data.len);
    req = (RaftCfgChange *) entry.data.buf;
    req->id = node->id;
    req->addr = node->addr;

    int e = raft_recv_entry(raft, &entry, &response);
    assert(e == 0);

    return 0;
}

/* TODO -- move this to Raft library header file */
void raft_node_set_next_idx(raft_node_t* me_, int nextIdx);

static void handleLoadSnapshotResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;
    assert(reply != NULL);

    node->load_snapshot_in_progress = false;

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        NODE_LOG_ERROR(node, "RAFT.LOADSNAPSHOT failure: %s\n",
                reply ? reply->str : "connection dropped.");
    } else if (reply->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_ERROR(node, "RAFT.LOADSNAPSHOT invalid response type\n");
    } else {
        NODE_LOG_DEBUG(node, "RAFT.LOADSNAPSHOT response %lld\n",
                reply->integer);

        /* If snapshot is already available on node, update it.
         *
         * The Raft paper does not address this issue and whether this should be
         * deduced by AE heartbeats or not.  The Raft library doesn't so it's
         * required here.
         */
        if (!reply->integer) {
            raft_node_t *n = raft_get_node(rr->raft, node->id);
            raft_node_set_next_idx(n, node->load_snapshot_idx);
        }
    }
}

static int raftSendSnapshot(raft_server_t *raft, void *user_data, raft_node_t *raft_node)
{
    RedisRaftCtx *rr = user_data;
    Node *node = (Node *) raft_node_get_udata(raft_node);

    /* We don't attempt to load a snapshot before we receive a response.
     *
     * Note: a response in this case only lets us know the operation begun,
     * but it's not a blocking operation.  See RAFT.LOADSNAPSHOT for more info.
     */
    if (node->load_snapshot_in_progress) {
        return 0;
    }

    NODE_LOG_DEBUG(node, "raftSendSnapshot: idx %u, node idx %u\n",
            raft_get_snapshot_last_idx(raft),
            raft_node_get_next_idx(raft_node));

    if (node->state != NODE_CONNECTED) {
        NODE_LOG_ERROR(node, "not connected, state=%u\n", node->state);
        return -1;
    }

    if (redisAsyncCommand(node->rc, handleLoadSnapshotResponse, node,
        "RAFT.LOADSNAPSHOT %u %u %s:%u",
        raft_get_snapshot_last_term(raft),
        raft_get_snapshot_last_idx(raft),
        rr->config->addr.host,
        rr->config->addr.port) != REDIS_OK) {

        node->state = NODE_CONNECT_ERROR;
        return -1;
    }

    node->load_snapshot_idx = raft_get_snapshot_last_idx(raft);
    node->load_snapshot_in_progress = true;

    return 0;
}

raft_cbs_t redis_raft_callbacks = {
    .send_requestvote = raftSendRequestVote,
    .send_appendentries = raftSendAppendEntries,
    .persist_vote = raftPersistVote,
    .persist_term = raftPersistTerm,
    .log_offer = raftLogOffer,
    .log_pop = raftLogPop,
    .log_poll = raftLogPoll,
    .log = raftLog,
    .log_get_node_id = raftLogGetNodeId,
    .applylog = raftApplyLog,
    .node_has_sufficient_logs = raftNodeHasSufficientLogs,
    .send_snapshot = raftSendSnapshot
};

/* ------------------------------------ AddNode ------------------------------------ */

void handleAddNodeResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;
    assert(reply != NULL);

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        NODE_LOG_ERROR(node, "RAFT.ADDNODE failed: %s\n", reply ? reply->str : "connection dropped.");
    } else if (reply->type != REDIS_REPLY_STATUS || strcmp(reply->str, "OK")) {
        NODE_LOG_ERROR(node, "invalid RAFT.ADDNODE reply: %s\n", reply->str);
    } else {
        NODE_LOG_INFO(node, "Cluster join request placed as node:%d.\n",
                raft_get_nodeid(rr->raft));
        rr->state = REDIS_RAFT_UP;
    }

    redisAsyncDisconnect(c);
}

void sendAddNodeRequest(const redisAsyncContext *c, int status)
{
    Node *node = (Node *) c->data;
    RedisRaftCtx *rr = node->rr;

    /* Connection is not good?  Terminate and continue */
    if (status != REDIS_OK) {
        node->state = NODE_CONNECT_ERROR;
    } else if (redisAsyncCommand(node->rc, handleAddNodeResponse, node,
        "RAFT.ADDNODE %d %s:%u",
        raft_get_nodeid(rr->raft),
        rr->config->addr.host,
        rr->config->addr.port) != REDIS_OK) {

        node->state = NODE_CONNECT_ERROR;
    }
}

static void initiateAddNode(RedisRaftCtx *rr)
{
    if (!rr->join_addr) {
        rr->join_addr = rr->config->join;
    }

    /* Allocate a node and initiate connection */
    if (!rr->join_node) {
        rr->join_node = NodeInit(0, &rr->join_addr->addr);
    } else {
        rr->join_node->addr = rr->join_addr->addr;
    }

    NodeConnect(rr->join_node, rr, sendAddNodeRequest);
}

/* ------------------------------------ Raft Thread ------------------------------------ */

/*
 * Handling of the Redis Raft context, including its own thread and
 * async I/O loop.
 */

int applyLoadedRaftLog(RedisRaftCtx *rr)
{
    raft_set_commit_idx(rr->raft, rr->log->header->commit_idx);
    raft_apply_all(rr->raft);

    raft_vote_for_nodeid(rr->raft, rr->log->header->vote);
    raft_set_current_term(rr->raft, rr->log->header->term);

    /* TODO is this needed? */
#if 0
    if (raft_get_num_nodes(rr->raft) == 1) {
        raft_node_set_voting(raft_get_my_node(rr->raft), 1);
        raft_become_leader(rr->raft);
    }
#endif

    return REDISMODULE_OK;
}

int raft_get_num_snapshottable_logs(raft_server_t *);

static void callRaftPeriodic(uv_timer_t *handle)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) uv_handle_get_data((uv_handle_t *) handle);

    /* If we're loading a snapshot, check if we're done */
    if (rr->loading_snapshot) {
        checkLoadSnapshotProgress(rr);
    }

    int ret = raft_periodic(rr->raft, rr->config->raft_interval);
    assert(ret == 0);
    raft_apply_all(rr->raft);

    /* Do we need a snapshot? */
    /* TODO: Change logic here.
     * 1) If we're persistent we should probably sync with AOF/RDB saving.
     * 2) If we don't persist anything, snapshotting is cheap and should be
     *    done every time we apply log entries.
     */

    if (raft_get_num_snapshottable_logs(rr->raft) > rr->config->max_log_entries) {
        performSnapshot(rr);
    }
}

static void handleNodeReconnects(uv_timer_t *handle)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) uv_handle_get_data((uv_handle_t *) handle);

    /* When in joining state, we don't have nodes to worry about but only the
     * join_node synthetic node which establishes the initial connection.
     */
    if (rr->state == REDIS_RAFT_JOINING) {
        if (!rr->join_node) {
            initiateAddNode(rr);
        }
        return;
    }

    /* Iterate nodes and find nodes that require reconnection */
    int i;
    for (i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *rnode = raft_get_node_from_idx(rr->raft, i);
        Node *node = raft_node_get_udata(rnode);
        if (!node) {
            continue;
        }

        if (NODE_STATE_IDLE(node->state)) {
            NodeConnect(node, rr, NULL);
        }
    }
}

static void RedisRaftThread(void *arg)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) arg;

    if (rr->state == REDIS_RAFT_LOADING) {
        applyLoadedRaftLog(rr);
        rr->state = REDIS_RAFT_UP;
    }

    uv_timer_start(&rr->raft_periodic_timer, callRaftPeriodic,
            rr->config->raft_interval, rr->config->raft_interval);
    uv_timer_start(&rr->node_reconnect_timer, handleNodeReconnects, 0,
            rr->config->reconnect_interval);
    uv_run(rr->loop, UV_RUN_DEFAULT);
}

int appendRaftCfgChangeEntry(RedisRaftCtx *rr, int type, int id, NodeAddr *addr)
{
    msg_entry_t msg;
    msg_entry_response_t response;

    msg.id = rand();
    msg.type = type;
    msg.data.len = sizeof(RaftCfgChange);
    msg.data.buf = RedisModule_Calloc(1, msg.data.len);

    RaftCfgChange *cc = (RaftCfgChange *) msg.data.buf;
    cc->id = id;
    if (addr != NULL) {
        cc->addr = *addr;
    }

    return raft_recv_entry(rr->raft, &msg, &response);
}

int initRaftLog(RedisModuleCtx *ctx, RedisRaftCtx *rr)
{
    if (rr->config->persist) {
        rr->log = RaftLogCreate(rr->config->raftlog ? rr->config->raftlog : REDIS_RAFT_DEFAULT_RAFTLOG,
                rr->config->id);
        if (!rr->log) {
            RedisModule_Log(ctx, REDIS_WARNING, "Failed to initialize Raft log");
            return REDISMODULE_ERR;
        }
    }
    return REDISMODULE_OK;
}

int initCluster(RedisModuleCtx *ctx, RedisRaftCtx *rr, RedisRaftConfig *config)
{
    /* Create our own node */
    raft_node_t *self = raft_add_node(rr->raft, NULL, config->id, 1);
    if (!self) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to initialize raft_node");
        return REDISMODULE_ERR;
    }

    /* Initialize log */
    if (initRaftLog(ctx, rr) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    /* Become leader and create initial entry */
    rr->state = REDIS_RAFT_UP;
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
        return REDISMODULE_ERR;
    }

    if (rr->log) {
        raft_entry_t *entry = raft_get_entry_from_idx(rr->raft, 1);
        assert(entry != NULL);
        assert(RaftLogAppend(rr->log, entry) == true);
    }

    return REDISMODULE_OK;
}

int joinCluster(RedisModuleCtx *ctx, RedisRaftCtx *rr, RedisRaftConfig *config)
{
    /* Create our own node */
    raft_node_t *self = raft_add_non_voting_node(rr->raft, NULL, config->id, 1);
    if (!self) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to initialize raft_node");
        return REDISMODULE_ERR;
    }
    rr->state = REDIS_RAFT_JOINING;

    return initRaftLog(ctx, rr);
}

int loadRaftLog(RedisModuleCtx *ctx, RedisRaftCtx *rr)
{
    rr->state = REDIS_RAFT_LOADING;
    rr->log = RaftLogOpen(rr->config->raftlog ? rr->config->raftlog : REDIS_RAFT_DEFAULT_RAFTLOG);
    if (!rr->log)  {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to open Raft log");
        return REDISMODULE_ERR;
    }

    raft_node_t *self = raft_add_non_voting_node(rr->raft, NULL, rr->log->header->node_id, 1);
    if (!self) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to create local Raft node [id %d]\n", rr->log->header->node_id);
        return REDISMODULE_ERR;
    }

    RedisModule_Log(ctx, REDIS_NOTICE, "Reading Raft log\n");
    int entries = RaftLogLoadEntries(rr->log, raft_append_entry, rr->raft);
    if (entries < 0) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to read Raft log");
        return REDISMODULE_ERR;
    } else {
        RedisModule_Log(ctx, REDIS_NOTICE, "%d entries loaded from Raft log", entries);
    }

    return REDISMODULE_OK;
}

int RedisRaftInit(RedisModuleCtx *ctx, RedisRaftCtx *rr, RedisRaftConfig *config)
{
    memset(rr, 0, sizeof(RedisRaftCtx));
    STAILQ_INIT(&rr->rqueue);

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

    /* Initialize raft library */
    rr->raft = raft_new();
    raft_set_election_timeout(rr->raft, rr->config->election_timeout);
    raft_set_request_timeout(rr->raft, rr->config->request_timeout);

    /* Configure Raft library to join/init */
    if (config->init) {
        initCluster(ctx, rr, config);
    } else if (config->join) {
        joinCluster(ctx, rr, config);
    }

    /* NOTE: The Raft library apparently does not handle well initial
     * setup if callbacks are already set. Not sure if this is by design,
     * and it may be a good idea to fix at some point.
     */
    raft_set_callbacks(rr->raft, &redis_raft_callbacks, rr);

    if (!config->init && !config->join) {
        if (!config->persist) {
            RedisModule_Log(ctx, REDIS_WARNING, "No persist, init or join");
            return REDISMODULE_ERR;
        }
        return loadRaftLog(ctx, rr);
    }

    return REDISMODULE_OK;
}

int RedisRaftStart(RedisModuleCtx *ctx, RedisRaftCtx *rr)
{
    /* Start Raft thread */
    if (uv_thread_create(&rr->thread, RedisRaftThread, rr) < 0) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to initialize redis_raft thread");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

/* ------------------------------------ RaftReq ------------------------------------ */

/*
 * Raft Requests, which are exchanged between the Redis main thread
 * and the Raft thread over the requests queue.
 */

void RaftReqFree(RaftReq *req)
{
    int i;

    switch (req->type) {
        case RR_APPENDENTRIES:
            if (req->r.appendentries.msg.entries) {
                RedisModule_Free(req->r.appendentries.msg.entries);
                req->r.appendentries.msg.entries = NULL;
            }
            break;
        case RR_REDISCOMMAND:
            if (req->ctx) {
                for (i = 0; i < req->r.redis.cmd.argc; i++) {
                    RedisModule_FreeString(req->ctx, req->r.redis.cmd.argv[i]);
                }
                RedisModule_Free(req->r.redis.cmd.argv);
            }
            break;
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
        if (rr->loading_snapshot && req->type != RR_INFO) {
            if (req->ctx) {
                RedisModule_ReplyWithError(req->ctx, "-LOADING loading snapshot");
                RedisModule_FreeThreadSafeContext(req->ctx);
                RedisModule_UnblockClient(req->client, NULL);
                RaftReqFree(req);
            }
            continue;
        }

        g_RaftReqHandlers[req->type](rr, req);
        if (!(req->flags & RR_PENDING_COMMIT)) {
            RaftReqFree(req);
        }
    }
}

/* ------------------------------------ RaftReq Implementation ------------------------------------ */

/*
 * Implementation of specific request types.
 */

static int handleRequestVote(RedisRaftCtx *rr, RaftReq *req)
{
    msg_requestvote_response_t response;

    if (raft_recv_requestvote(rr->raft,
                raft_get_node(rr->raft, req->r.requestvote.src_node_id),
                &req->r.requestvote.msg,
                &response) != 0) {
        RedisModule_ReplyWithError(req->ctx, "operation failed"); // TODO: Identify cases
        goto exit;
    }

    RedisModule_ReplyWithArray(req->ctx, 2);
    RedisModule_ReplyWithLongLong(req->ctx, response.term);
    RedisModule_ReplyWithLongLong(req->ctx, response.vote_granted);

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;

    return REDISMODULE_OK;
}


static int handleAppendEntries(RedisRaftCtx *rr, RaftReq *req)
{
    msg_appendentries_response_t response;
    int err;

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
    RedisModule_ReplyWithLongLong(req->ctx, response.first_idx);

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;

    return REDISMODULE_OK;
}

static int handleCfgChange(RedisRaftCtx *rr, RaftReq *req)
{
    raft_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    entry.id = rand();

    switch (req->type) {
        case RR_CFGCHANGE_ADDNODE:
            entry.type = RAFT_LOGTYPE_ADD_NONVOTING_NODE;
            break;
        case RR_CFGCHANGE_REMOVENODE:
            entry.type = RAFT_LOGTYPE_REMOVE_NODE;
            break;
        default:
            assert(0);
    }

    entry.data.len = sizeof(req->r.cfgchange);
    entry.data.buf = RedisModule_Alloc(sizeof(req->r.cfgchange));
    memcpy(entry.data.buf, &req->r.cfgchange, sizeof(req->r.cfgchange));

    int e = raft_recv_entry(rr->raft, &entry, &req->r.redis.response);
    if (e == 0) {
        RedisModule_ReplyWithSimpleString(req->ctx, "OK");
    } else  {
        replyRaftError(req->ctx, e);
        RedisModule_Free(entry.data.buf);
    }

    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;

    return REDISMODULE_OK;
}

static int handleRedisCommand(RedisRaftCtx *rr,RaftReq *req)
{
    raft_node_t *leader = raft_get_current_leader_node(rr->raft);
    if (!leader) {
        RedisModule_ReplyWithError(req->ctx, "NOLEADER");
        goto exit;
    }
    if (raft_node_get_id(leader) != raft_get_nodeid(rr->raft)) {
        Node *l = raft_node_get_udata(leader);
        char *reply;

        asprintf(&reply, "MOVED %s:%u", l->addr.host, l->addr.port);

        RedisModule_ReplyWithError(req->ctx, reply);
        free(reply);
        goto exit;
    }

    raft_entry_t entry = {
        .id = rand(),
        .type = RAFT_LOGTYPE_NORMAL,
        .user_data = req,
    };

    RaftRedisCommandSerialize(&entry.data, &req->r.redis.cmd);
    int e = raft_recv_entry(rr->raft, &entry, &req->r.redis.response);
    if (e != 0) {
        replyRaftError(req->ctx, e);
        RedisModule_Free(entry.data.buf);
        goto exit;
    }

    /* If we're a single node we can try to apply now, as we have no need
     * or way to wait for AE responses to do that.
     */
    if (raft_get_current_idx(rr->raft) == raft_get_commit_idx(rr->raft)) {
        raft_apply_all(rr->raft);
    } else {
        // We're now waiting
        req->flags |= RR_PENDING_COMMIT;
    }

    return REDISMODULE_OK;

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;
    return REDISMODULE_OK;

}

static int handleInfo(RedisRaftCtx *rr, RaftReq *req)
{
    size_t slen = 1024;
    char *s = RedisModule_Calloc(1, slen);

    char role[10];
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

    char state[15];
    switch (rr->state) {
        case REDIS_RAFT_LOADING:
            strcpy(state, "loading");
            break;
        case REDIS_RAFT_UP:
            strcpy(state, "up");
            break;
        case REDIS_RAFT_JOINING:
            strcpy(state, "joining");
            break;
        default:
            strcpy(state, "(none)");
            break;
    }
    s = catsnprintf(s, &slen,
            "# Raft\r\n"
            "node_id:%d\r\n"
            "state:%s\r\n"
            "role:%s\r\n"
            "leader_id:%d\r\n"
            "current_term:%d\r\n"
            "num_nodes:%d\r\n"
            "num_voting_nodes:%d\r\n",
            raft_get_nodeid(rr->raft),
            state,
            role,
            raft_get_current_leader(rr->raft),
            raft_get_current_term(rr->raft),
            raft_get_num_nodes(rr->raft),
            raft_get_num_voting_nodes(rr->raft));

    int i;
    for (i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *rnode = raft_get_node_from_idx(rr->raft, i);
        Node *node = raft_node_get_udata(rnode);
        if (!node) {
            continue;
        }

        char state[40];
        switch (node->state) {
            case NODE_DISCONNECTED:
                strcpy(state, "disconnected");
                break;
            case NODE_CONNECTING:
                strcpy(state, "connecting");
                break;
            case NODE_CONNECTED:
                strcpy(state, "connected");
                break;
            case NODE_CONNECT_ERROR:
                strcpy(state, "connect_error");
                break;
            default:
                strcpy(state, "--");
                break;
        }

        s = catsnprintf(s, &slen,
                "node%d:id=%d,state=%s,voting=%s,addr=%s,port=%d\r\n",
                i, node->id, state,
                raft_node_is_voting(rnode) ? "yes" : "no",
                node->addr.host, node->addr.port);
    }

    s = catsnprintf(s, &slen,
            "\r\n# Log\r\n"
            "log_entries:%d\r\n"
            "current_index:%d\r\n"
            "commit_index:%d\r\n"
            "last_applied_index:%d\r\n",
            raft_get_log_count(rr->raft),
            raft_get_current_idx(rr->raft),
            raft_get_commit_idx(rr->raft),
            raft_get_last_applied_idx(rr->raft));

    s = catsnprintf(s, &slen,
            "\r\n# Snapshot\r\n"
            "loading_snapshot:%s\r\n",
            rr->loading_snapshot ? "yes" :"no");

    RedisModule_ReplyWithStringBuffer(req->ctx, s, strlen(s));
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;

    return REDISMODULE_OK;
}

RaftReqHandler g_RaftReqHandlers[] = {
    NULL,
    handleCfgChange,        /* RR_CFGCHANGE_ADDNODE */
    handleCfgChange,        /* RR_CFGCHANGE_REMOVENODE */
    handleAppendEntries,    /* RR_APPENDENTRIES */
    handleRequestVote,      /* RR_REQUESTVOTE */
    handleRedisCommand,     /* RR_REDISOCMMAND */
    handleInfo,             /* RR_INFO */
    handleLoadSnapshot,     /* RR_LOADSNAPSHOT */
    handleCompact,          /* RR_COMPACT */
    NULL
};


