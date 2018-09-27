#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

/* Verify we're little endian, as our encoding is such and we
 * don't do network/host reodering.
 */
#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
#error Byte order swapping is currently not implemented.
#endif

#include "redisraft.h"

/* Forward declaration */
static RaftReqHandler RaftReqHandlers[];

const char *getStateStr(RedisRaftCtx *rr)
{
    static const char *state_str[] = { "up", "loading", "joining" };
    static const char *invalid = "<invalid>";

    if (rr->state < REDIS_RAFT_UP ||
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
    /* TODO: optimize and avoid deserialization here, we can use the
     * original argv in RaftReq
     */
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
    }
    RaftRedisCommandFree(rr->ctx, &rcmd);

    if (reply) {
        RedisModule_FreeCallReply(reply);
    }

    if (req) {
        /* Free request now, we don't need it anymore */
        entry->user_data = NULL;
        RaftReqFree(req);
    }
}


/* ------------------------------------ RequestVote ------------------------------------ */

static void handleRequestVoteResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;

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
    argvlen[2] = snprintf(argv2_buf, sizeof(argv2_buf)-1, "%ld:%ld:%ld:%ld",
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
        argvlen[4 + i*2] = snprintf(argv[4 + i*2], 63, "%ld:%d:%d", e->term, e->id, e->type);
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

static int raftPersistVote(raft_server_t *raft, void *user_data, raft_node_id_t vote)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) user_data;
    if (!rr->log || rr->state == REDIS_RAFT_LOADING) {
        return 0;
    }

    if (!RaftLogSetVote(rr->log, vote)) {
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

    if (!RaftLogSetTerm(rr->log, term, vote)) {
        return RAFT_ERR_SHUTDOWN;
    }

    return 0;
}

static int raftLogOffer(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) user_data;
    raft_node_t *raft_node;
    Node *node;

    TRACE("raftLogOffer: entry_idx=%ld, id=%d\n", entry_idx, entry->id);

    /* Memory management policy: we always make a copy of the data here. In some cases
     * we could avoid it but the Raft library makes no distinction of how an entry was
     * received so we stick to it.  Free is on raftLogPoll()/raftLogPop().
     */
    if (entry->data.len) {
        void *old_buf = entry->data.buf;
        entry->data.buf = RedisModule_Alloc(entry->data.len);
        memcpy(entry->data.buf, old_buf, entry->data.len);
    } else {
        entry->data.buf = NULL;
    }

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

    return 0;
}

static void raftFreeEntry(raft_entry_t *entry)
{
    if (entry->user_data) {
        RaftReqFree((RaftReq *)entry->user_data);
        entry->user_data = NULL;
    }
    if (entry->data.buf) {
        RedisModule_Free(entry->data.buf);
        entry->data.buf = NULL;
    }
}

static int raftLogPop(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
    RedisRaftCtx *rr = user_data;

    TRACE("raftLogPop: entry_idx=%ld, id=%d\n", entry_idx, entry->id);

    if (!rr->log || rr->state == REDIS_RAFT_LOADING) {
        raftFreeEntry(entry);
        return 0;
    }

    if (!RaftLogRemoveTail(rr->log)) {
        return -1;
    }

    raftFreeEntry(entry);
    return 0;
}

static int raftLogPoll(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
    RedisRaftCtx *rr = user_data;

    TRACE("raftLogPoll: entry_idx=%ld, id=%d\n", entry_idx, entry->id);

    if (!rr->log || rr->state == REDIS_RAFT_LOADING || rr->snapshot_in_progress) {
        raftFreeEntry(entry);
        return 0;
    }

    if (!RaftLogRemoveHead(rr->log)) {
        LOG_DEBUG("raftLogPoll: RaftLogRemoveHead() failed!\n");
        return -1;
    }

    raftFreeEntry(entry);
    return 0;
}

static int raftApplyLog(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
    RedisRaftCtx *rr = user_data;
    RaftCfgChange *req;

    switch (entry->type) {
        case RAFT_LOGTYPE_REMOVE_NODE:
            req = (RaftCfgChange *) entry->data.buf;
            if (req->id == raft_get_nodeid(raft)) {
                return RAFT_ERR_SHUTDOWN;
            }
        case RAFT_LOGTYPE_NORMAL:
            executeLogEntry(rr, entry);
            break;
        default:
            break;
    }

    /* Update snapshot info in Redis dataset. This must be done now so it's
     * always consistent with what we applied and we never end up applying
     * an entry onto a snapshot where it was applied already.
     */
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
            NODE_LOG_DEBUG(n, "<raftlib> %s\n", buf);
            return;
        }
    }
    LOG_DEBUG("<raftlib> %s\n", buf);
}

static raft_node_id_t raftLogGetNodeId(raft_server_t *raft, void *user_data, raft_entry_t *entry,
        raft_index_t entry_idx)
{
    RaftCfgChange *req = (RaftCfgChange *) entry->data.buf;
    return req->id;
}

static int raftNodeHasSufficientLogs(raft_server_t *raft, void *user_data, raft_node_t *raft_node)
{
    Node *node = raft_node_get_udata(raft_node);
    assert (node != NULL);

    TRACE("node:%d has sufficient logs now, adding as voting node.\n", node->id);

    raft_entry_t entry = { 0 };
    entry.id = rand();
    entry.type = RAFT_LOGTYPE_ADD_NODE;

    msg_entry_response_t response;
    RaftCfgChange cfgchange = { 0 };
    cfgchange.id = node->id;
    cfgchange.addr = node->addr;

    entry.data.len = sizeof(cfgchange);
    entry.data.buf = &cfgchange;

    int e = raft_recv_entry(raft, &entry, &response);
    assert(e == 0);

    return 0;
}

/* TODO -- move this to Raft library header file */
void raft_node_set_next_idx(raft_node_t* me_, raft_index_t nextIdx);

static void handleLoadSnapshotResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;

    node->load_snapshot_in_progress = false;

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        NODE_LOG_ERROR(node, "RAFT.LOADSNAPSHOT failure: %s\n",
                reply ? reply->str : "connection dropped.");
    } else if (reply->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_ERROR(node, "RAFT.LOADSNAPSHOT invalid response type\n");
    } else {
        NODE_LOG_DEBUG(node, "RAFT.LOADSNAPSHOT response %lld\n",
                reply->integer);
        raft_node_t *n = raft_get_node(rr->raft, node->id);
        raft_node_set_next_idx(n, node->load_snapshot_idx + 1);
    }
}

static char *loadSnapshotFile(const char *filename, size_t *size)
{
    int fd = open(filename, O_RDONLY);
    struct stat st;
    char *buf = NULL;

    if (fd < 0 || fstat(fd, &st) < 0) {
        LOG_ERROR("Failed to load snapshot file: %s: %s\n",
                filename, strerror(errno));
        goto exit;
    }

    buf = RedisModule_Alloc(st.st_size);
    assert(buf != NULL);

    if (read(fd, buf, st.st_size) != st.st_size) {
        LOG_ERROR("Failed to read snapshot file: %s: %s\n",
                filename, strerror(errno));
        RedisModule_Free(buf);
        buf = NULL;
        goto exit;
    }

    *size = st.st_size;

exit:
    if (fd != -1) {
        close(fd);
    }
    return buf;
}

static int raftSendSnapshot(raft_server_t *raft, void *user_data, raft_node_t *raft_node)
{
    RedisRaftCtx *rr = user_data;
    Node *node = (Node *) raft_node_get_udata(raft_node);
    time_t now = time(NULL);

    /* Don't attempt to send a snapshot if we're in the process of creating one */
    if (rr->snapshot_in_progress) {
        NODE_LOG_DEBUG(node, "not sending snapshot, snapshot_in_progress\n");
        return 0;
    }

    /* We don't attempt to load a snapshot before we receive a response.
     *
     * Note: a response in this case only lets us know the operation begun,
     * but it's not a blocking operation.  See RAFT.LOADSNAPSHOT for more info.
     */
    if (node->load_snapshot_in_progress) {
        return 0;
    }

    NODE_LOG_DEBUG(node, "raftSendSnapshot: idx %ld, node idx %ld\n",
            raft_get_snapshot_last_idx(raft),
            raft_node_get_next_idx(raft_node));

    if (node->state != NODE_CONNECTED) {
        NODE_LOG_ERROR(node, "not connected, state=%u\n", node->state);
        return -1;
    }

    /* Load snapshot data */
    char last_term[30];
    snprintf(last_term, sizeof(last_term) - 1, "%lu", raft_get_snapshot_last_term(raft));
    char last_idx[30];
    snprintf(last_idx, sizeof(last_idx) - 1, "%lu", raft_get_snapshot_last_idx(raft));
    size_t snapshot_size;
    char *snapshot_data = loadSnapshotFile(rr->config->rdb_filename, &snapshot_size);
    if (!snapshot_data) {
        return -1;
    }

    LOG_DEBUG("Loaded snapshot: %s: %lu bytes\n",
            rr->config->rdb_filename, snapshot_size);

    const char *args[4] = {
        "RAFT.LOADSNAPSHOT",
        last_term,
        last_idx,
        snapshot_data
    };
    size_t args_len[4] = {
        strlen(args[0]),
        strlen(args[1]),
        strlen(args[2]),
        snapshot_size
    };

    node->load_snapshot_idx = raft_get_snapshot_last_idx(raft);
    node->load_snapshot_in_progress = true;
    node->load_snapshot_last_time = now;

    if (redisAsyncCommandArgv(node->rc, handleLoadSnapshotResponse, node, 4, args, args_len) != REDIS_OK) {
        node->state = NODE_CONNECT_ERROR;
        node->load_snapshot_in_progress = false;
        RedisModule_Free(snapshot_data);
        return -1;
    }

    RedisModule_Free(snapshot_data);

    return 0;
}

void raftNotifyMembershipEvent(raft_server_t *raft, void *user_data, raft_node_t *raft_node,
        raft_entry_t *entry, raft_membership_e type)
{
    RedisRaftCtx *rr = user_data;
    RaftCfgChange *cfgchange;
    Node *node;

    switch (type) {
        case RAFT_MEMBERSHIP_ADD:
            /* When raft_add_node() is called explicitly, we get no entry so we
             * have nothing to do.
             */
            if (!entry) {
                break;
            }

            /* Ignore our own node, as we don't maintain a Node structure for it */
            assert(entry->type == RAFT_LOGTYPE_ADD_NODE || entry->type == RAFT_LOGTYPE_ADD_NONVOTING_NODE);
            cfgchange = (RaftCfgChange *) entry->data.buf;
            if (cfgchange->id == raft_get_nodeid(raft)) {
                break;
            }

            /* Allocate a new node */
            node = NodeInit(cfgchange->id, &cfgchange->addr);
            assert(node != NULL);

            raft_node_set_udata(raft_node, node);
            break;

        case RAFT_MEMBERSHIP_REMOVE:
            node = raft_node_get_udata(raft_node);
            if (node != NULL) {
                node->flags |= NODE_TERMINATING;
                raft_node_set_udata(raft_node, NULL);
            }
            break;

        default:
            assert(0);
    }

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
    .send_snapshot = raftSendSnapshot,
    .notify_membership_event = raftNotifyMembershipEvent
};

/* ------------------------------------ Raft Thread ------------------------------------ */

/*
 * Handling of the Redis Raft context, including its own thread and
 * async I/O loop.
 */

int applyLoadedRaftLog(RedisRaftCtx *rr)
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
        if (rr->snapshot_info.last_applied_idx < rr->log->snapshot_last_idx) {
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

    /* Special case: if no other nodes, set commit index to the latest
     * entry in the log.
     */
    if (raft_get_num_nodes(rr->raft) == 1) {
        raft_set_commit_idx(rr->raft, raft_get_current_idx(rr->raft));
    }

    memcpy(rr->snapshot_info.dbid, rr->log->dbid, RAFT_DBID_LEN);
    rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

    raft_set_snapshot_metadata(rr->raft, rr->snapshot_info.last_applied_term,
            rr->snapshot_info.last_applied_idx);

    raft_apply_all(rr->raft);

    raft_set_current_term(rr->raft, rr->log->term);
    raft_vote_for_nodeid(rr->raft, rr->log->vote);

    LOG_DEBUG("Raft Log: loaded current term=%lu, vote=%d\n", rr->log->term, rr->log->vote);
    LOG_DEBUG("Raft state after applying log: log_count=%lu, current_idx=%lu, last_applied_idx=%lu\n",
            raft_get_log_count(rr->raft),
            raft_get_current_idx(rr->raft),
            raft_get_last_applied_idx(rr->raft));

    initializeSnapshotInfo(rr);

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

static bool checkRedisLoading(RedisRaftCtx *rr)
{
    char *val = RedisInfoGetParam(rr, "persistence", "loading");
    assert(val != NULL);
    bool loading = (!strcmp(val, "1"));

    RedisModule_Free(val);
    return loading;
}

int loadRaftLog(RedisRaftCtx *rr);

static void callRaftPeriodic(uv_timer_t *handle)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) uv_handle_get_data((uv_handle_t *) handle);
    int ret;

    /* If we're in LOADING state, we need to wait for Redis to finish loading before
     * we can apply the log.
     */
    if (rr->state == REDIS_RAFT_LOADING) {
        if (!checkRedisLoading(rr)) {
            /* If Redis loaded a snapshot (RDB), log some information and configure the
             * raft library as necessary.
             */
            LOG_INFO("Loading: Redis loading complete, snapshot %s\n",
                    rr->snapshot_info.loaded ? "LOADED" : "NOT LOADED");
            if (rr->snapshot_info.loaded) {
                SnapshotCfgEntry *c;
                LOG_INFO("Loading: Snapshot: applied term=%lu index=%lu\n",
                        rr->snapshot_info.last_applied_term,
                        rr->snapshot_info.last_applied_idx);

                for (c = rr->snapshot_info.cfg; c != NULL; c = c->next) {
                    LOG_INFO("Loading: Snapshot config: node id=%u [%s:%u], active=%u, voting=%u\n",
                            c->id, c->addr.host, c->addr.port, c->active, c->voting);
                }

                if ((ret = raft_begin_load_snapshot(rr->raft, rr->snapshot_info.last_applied_term,
                            rr->snapshot_info.last_applied_idx)) < 0) {
                    assert(0);
                    PANIC("Failed to begin snapshot loading [%d], aborting.\n", ret);
                }
                configRaftFromSnapshotInfo(rr);
                if ((ret = raft_end_load_snapshot(rr->raft)) < 0) {
                    PANIC("Failed to end snapshot loading [%d], aborting.\n", ret);
                }
            }

            if (loadRaftLog(rr) < 0) {
                LOG_ERROR("Failed to read Raft log, aborting.\n");
                exit(1);
            }

            if (rr->log) {
                if (rr->log->snapshot_last_term) {
                    LOG_INFO("Loading: Log starts from snapshot term=%lu, index=%lu\n",
                            rr->log->snapshot_last_term, rr->log->snapshot_last_idx);
                } else {
                    LOG_INFO("Loading: Log is complete.\n");
                }

                applyLoadedRaftLog(rr);
            }
            rr->state = REDIS_RAFT_UP;
        }

        /* There is nothing for us to do until we finish loading. */
        return;
    }

    /* If we're creating a persistent snapshot, check if we're done */
    if (rr->snapshot_in_progress) {
        ret = pollSnapshotStatus(rr);
        if (ret == -1) {
            LOG_ERROR("Snapshot operation failed, cancelling.\n");
            cancelSnapshot(rr);
        }  else if (ret) {
            LOG_DEBUG("Snapshot operation completed successfuly.\n");
            finalizeSnapshot(rr);
        } /* else we're still in progress */
    }

    ret = raft_periodic(rr->raft, rr->config->raft_interval);
    assert(ret == 0);
    raft_apply_all(rr->raft);

    /* Do we need a snapshot? */
    /* TODO: Change logic here.
     * 1) If we're persistent we should probably sync with AOF/RDB saving.
     * 2) If we don't persist anything, snapshotting is cheap and should be
     *    done every time we apply log entries.
     */

    if (!rr->snapshot_in_progress &&
            raft_get_num_snapshottable_logs(rr->raft) > rr->config->max_log_entries) {
        LOG_DEBUG("Log reached max_log_entries (%d/%d), initiating snapshot.\n",
                raft_get_num_snapshottable_logs(rr->raft), rr->config->max_log_entries);
        initiateSnapshot(rr);
    }
}

static void callHandleNodeStates(uv_timer_t *handle)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) uv_handle_get_data((uv_handle_t *) handle);
    HandleNodeStates(rr);
}

static void RedisRaftThread(void *arg)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) arg;

    /* TODO: Properly handle the race condition here */
    sleep(0.5);
    uv_timer_start(&rr->raft_periodic_timer, callRaftPeriodic,
            rr->config->raft_interval, rr->config->raft_interval);
    uv_timer_start(&rr->node_reconnect_timer, callHandleNodeStates, 0,
            rr->config->reconnect_interval);
    uv_run(rr->loop, UV_RUN_DEFAULT);
}

static int appendRaftCfgChangeEntry(RedisRaftCtx *rr, int type, int id, NodeAddr *addr)
{
    msg_entry_t msg = { 0 };
    msg_entry_response_t response;

    RaftCfgChange cfgchange = { 0 };
    cfgchange.id = id;
    if (addr != NULL) {
        cfgchange.addr = *addr;
    }

    msg.id = rand();
    msg.type = type;
    msg.data.len = sizeof(RaftCfgChange);

    /* We need to explicitly copy the buffer if callbacks were not set and
     * raftLogOffer() will not have a chance to do this for us.
     */
    if (rr->callbacks_set) {
        msg.data.buf = &cfgchange;
    } else {
        msg.data.buf = RedisModule_Alloc(sizeof(cfgchange));
        memcpy(msg.data.buf, &cfgchange, sizeof(cfgchange));
    }

    return raft_recv_entry(rr->raft, &msg, &response);
}

int initRaftLog(RedisModuleCtx *ctx, RedisRaftCtx *rr)
{
    if (rr->config->persist) {
        rr->log = RaftLogCreate(rr->config->raftlog ? rr->config->raftlog : REDIS_RAFT_DEFAULT_RAFTLOG,
                rr->snapshot_info.dbid,
                1, 0);
        if (!rr->log) {
            RedisModule_Log(ctx, REDIS_WARNING, "Failed to initialize Raft log");
            return REDISMODULE_ERR;
        }
    }
    return REDISMODULE_OK;
}

int initCluster(RedisModuleCtx *ctx, RedisRaftCtx *rr, RedisRaftConfig *config)
{
    /* Initialize dbid */
    RedisModule_GetRandomHexChars(rr->snapshot_info.dbid, RAFT_DBID_LEN);
    rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

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
    /* Create a Snapshot Info meta-key */
    initializeSnapshotInfo(rr);

    /* Create our own node */
    raft_node_t *self = raft_add_non_voting_node(rr->raft, NULL, config->id, 1);
    if (!self) {
        RedisModule_Log(ctx, REDIS_WARNING, "Failed to initialize raft_node");
        return REDISMODULE_ERR;
    }
    rr->state = REDIS_RAFT_JOINING;

    /* We don't yet initialize the log, as we're waiting for dbid */
    return REDISMODULE_OK;
}

static int loadEntriesCallback(void *arg, LogEntryAction action, raft_entry_t *entry)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) arg;

    switch (action) {
        case LA_APPEND:
            if (rr->snapshot_info.last_applied_term <= entry->term &&
                    rr->snapshot_info.last_applied_idx < rr->log->index) {
                return raft_append_entry(rr->raft, entry);
            } else {
                return 0;
            }
        case LA_REMOVE_HEAD:
            return raft_poll_entry(rr->raft, &entry);
        case LA_REMOVE_TAIL:
            return raft_pop_entry(rr->raft);
        default:
            return -1;
    }
}

int loadRaftLog(RedisRaftCtx *rr)
{
    const char *filename = rr->config->raftlog ? rr->config->raftlog : REDIS_RAFT_DEFAULT_RAFTLOG;

    rr->state = REDIS_RAFT_LOADING;
    rr->log = RaftLogOpen(filename);
    if (!rr->log)  {
        LOG_ERROR("Failed to open Raft log: %s\n", filename);
        return REDISMODULE_ERR;
    }

    int entries = RaftLogLoadEntries(rr->log, loadEntriesCallback, rr);
    if (entries < 0) {
        LOG_ERROR("Failed to read Raft log\n");
        return REDISMODULE_ERR;
    } else {
        LOG_INFO("Loading: Log loaded, %d entries, snapshot last term=%lu, index=%lu\n",
               entries, rr->log->snapshot_last_term, rr->log->snapshot_last_idx);
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

    /* Configure file names */
    ConfigSetupFilenames(rr);

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
    rr->callbacks_set = true;

    if (ValidateRedisConfig(rr, ctx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    if (!config->init && !config->join) {
        if (!config->persist) {
            RedisModule_Log(ctx, REDIS_WARNING, "No persist, init or join");
            return REDISMODULE_ERR;
        }

        /* We're in loading state until Redis loads the RDB and we load the log */
        rr->state = REDIS_RAFT_LOADING;

        /* Create local Raft node */
        raft_node_t *self = raft_add_non_voting_node(rr->raft, NULL, rr->config->id, 1);
        if (!self) {
            LOG_ERROR("Failed to create local Raft node [id %d]\n", rr->config->id);
            return REDISMODULE_ERR;
        }
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

/* Free a RaftReq structure.
 *
 * If it is associated with a blocked client, it will be unblocked and
 * the thread safe context released as well.
 */

void RaftReqFree(RaftReq *req)
{
    int i;

    switch (req->type) {
        case RR_APPENDENTRIES:
            /* Note: we only free the array of entries but not actual entries, as they
             * are owned by the log and should be freed when the log entry is freed.
             */
            if (req->r.appendentries.msg.entries) {
                int i;
                for (i = 0; i < req->r.appendentries.msg.n_entries; i++) {
                    RedisModule_Free(req->r.appendentries.msg.entries[i].data.buf);
                }
                RedisModule_Free(req->r.appendentries.msg.entries);
                req->r.appendentries.msg.entries = NULL;
            }
            break;
        case RR_REDISCOMMAND:
            if (req->ctx && req->r.redis.cmd.argv) {
                for (i = 0; i < req->r.redis.cmd.argc; i++) {
                    RedisModule_FreeString(req->ctx, req->r.redis.cmd.argv[i]);
                }
                RedisModule_Free(req->r.redis.cmd.argv);
            }
            req->r.redis.cmd.argv = NULL;
            break;
        case RR_LOADSNAPSHOT:
            RedisModule_Free(req->r.loadsnapshot.snapshot);
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
                RaftReqFree(req);
            }
            continue;
        }

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

    if (rr->state != REDIS_RAFT_UP) {
        RedisModule_ReplyWithError(req->ctx, "LOADING Raft module loading");
        RaftReqFree(req);
        return;
    }

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
    RaftReqFree(req);
}


static void handleAppendEntries(RedisRaftCtx *rr, RaftReq *req)
{
    msg_appendentries_response_t response;
    int err;

    if (rr->state != REDIS_RAFT_UP) {
        RedisModule_ReplyWithError(req->ctx, "LOADING Raft module loading");
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
    RedisModule_ReplyWithLongLong(req->ctx, response.first_idx);

exit:
    RaftReqFree(req);
}

static void handleCfgChange(RedisRaftCtx *rr, RaftReq *req)
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
    entry.data.buf = &req->r.cfgchange;

    int e = raft_recv_entry(rr->raft, &entry, &req->r.redis.response);
    if (e == 0) {
        char r[RAFT_DBID_LEN + 5];
        if (req->type == RR_CFGCHANGE_ADDNODE) {
            snprintf(r, sizeof(r) - 1, "OK %s", rr->snapshot_info.dbid);
        } else {
            strcpy(r, "OK");
        }

        RedisModule_ReplyWithSimpleString(req->ctx, r);
    } else  {
        replyRaftError(req->ctx, e);
    }

    RaftReqFree(req);
}

static void handleRedisCommand(RedisRaftCtx *rr,RaftReq *req)
{
    raft_node_t *leader = raft_get_current_leader_node(rr->raft);
    if (rr->state != REDIS_RAFT_UP) {
        RedisModule_ReplyWithError(req->ctx, "LOADING");
        goto exit;
    }
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
    void *buf = entry.data.buf;     /* Store it because raft_recv_entry will overwrite */
    int e = raft_recv_entry(rr->raft, &entry, &req->r.redis.response);
    RedisModule_Free(buf);
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

    raft_node_t *me = raft_get_my_node(rr->raft);
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
            raft_get_nodeid(rr->raft),
            getStateStr(rr),
            role,
            me ? (raft_node_is_voting(raft_get_my_node(rr->raft)) ? "yes" : "no") : "-",
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
            case NODE_RESOLVING:
                strcpy(state, "resolving");
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
            "loading_snapshot:%s\r\n"
            "snapshot_in_progress:%s\r\n",
            rr->loading_snapshot ? "yes" :"no",
            rr->snapshot_in_progress ? "yes" : "no"
            );

    RedisModule_ReplyWithStringBuffer(req->ctx, s, strlen(s));
    RedisModule_Free(s);

    RaftReqFree(req);
}

static RaftReqHandler RaftReqHandlers[] = {
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


