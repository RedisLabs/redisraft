#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "redisraft.h"

/* ------------------------------------ Snapshot metadata ------------------------------------ */

/* Generate a configuration field string from the current Raft configuration state.
 * This string can then be parsed into a series of SnapshotCfgEntry structs when
 * loading a snapshot.
 */
static SnapshotCfgEntry *generateSnapshotCfgEntryList(RedisRaftCtx *rr)
{
    SnapshotCfgEntry *head = NULL;
    SnapshotCfgEntry **entry = &head;
    int i;

    for (i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *rnode = raft_get_node_from_idx(rr->raft, i);
        Node *node = raft_node_get_udata(rnode);

        /* Skip uncommitted nodes from the snapshot */
        if (!raft_node_is_addition_committed(rnode)) {
            continue;
        }

        NodeAddr *na = NULL;
        if (raft_node_get_id(rnode) == raft_get_nodeid(rr->raft)) {
            na = &rr->config->addr;
        } else if (node != NULL) {
            na = &node->addr;
        } else {
            assert(0);
        }

        *entry = RedisModule_Calloc(1, sizeof(SnapshotCfgEntry));
        SnapshotCfgEntry *e = *entry;

        e->id = raft_node_get_id(rnode);
        e->active = raft_node_is_active(rnode);
        e->voting = raft_node_is_voting_committed(rnode);
        e->addr = *na;

        entry = &e->next;
    }

    return head;
}

/* Free a chain of SnapshotCfgEntry structs */
static void freeSnapshotCfgEntryList(SnapshotCfgEntry *head)
{
    while (head != NULL) {
        SnapshotCfgEntry *next = head->next;
        RedisModule_Free(head);
        head = next;
    }
}


/* ------------------------------------ Generate snapshots ------------------------------------ */

void cancelSnapshot(RedisRaftCtx *rr, SnapshotResult *sr)
{
    assert(rr->snapshot_in_progress);

    raft_cancel_snapshot(rr->raft);
    rr->snapshot_in_progress = false;

    if (sr != NULL) {
        if (sr->rdb_filename[0]) {
            unlink(sr->rdb_filename);
        }
        RaftLogRemoveFiles(sr->log_filename);
    }
}

RRStatus finalizeSnapshot(RedisRaftCtx *rr, SnapshotResult *sr)
{
    RaftLog *new_log = NULL;
    unsigned long new_log_entries = sr->num_entries;

    assert(rr->snapshot_in_progress);

    TRACE("Finalizing snapshot.\n");

    /* If a persistent log is in use, we now have to append any new
     * entries to the temporary log and switch.
     */
    if (rr->log) {
        long long int n = RaftLogRewriteAppend(rr, sr->log_filename, rr->snapshot_rewrite_last_idx + 1);
        if (n < 0) {
            LOG_ERROR("Failed to append entries to rewritten log, aborting snapshot.\n");
            cancelSnapshot(rr, sr);
            return -1;
        }

        new_log_entries += n;
        LOG_VERBOSE("Log rewrite complete, %lld entries appended (from idx %lu).\n", n,
                raft_get_snapshot_last_idx(rr->raft));

        new_log = RaftLogOpen(sr->log_filename);
        if (!new_log) {
            LOG_ERROR("Failed to open log after rewrite: %s\n", strerror(errno));
            cancelSnapshot(rr, sr);
            return -1;
        }
    }

    /* We now have to switch temp files. We need to rename two files in a non-atomic
     * operation, so order is critical and we must rename the snapshot file first.
     * This guarantees we lose no data if we fail now before renaming the log -- all
     * we'll have to do is skip redundant log entries.
     */

    if (rename(sr->rdb_filename, rr->config->rdb_filename) < 0) {
        LOG_ERROR("Failed to switch snapshot filename (%s to %s): %s\n",
                sr->rdb_filename, rr->config->rdb_filename, strerror(errno));
        if (new_log) {
            RaftLogClose(new_log);
        }
        cancelSnapshot(rr, sr);
        return -1;
    }

    if (RaftLogRewriteSwitch(rr, new_log, new_log_entries) != RR_OK) {
        RaftLogClose(new_log);
        cancelSnapshot(rr, sr);
        return -1;
    }

    /* Clear cached entries */
    EntryCacheDeleteHead(rr->logcache, raft_get_snapshot_last_idx(rr->raft) + 1);

    raft_end_snapshot(rr->raft);
    rr->snapshot_in_progress = false;

    return RR_OK;
}

int pollSnapshotStatus(RedisRaftCtx *rr, SnapshotResult *sr)
{
    memset(sr, 0, sizeof(*sr));

    int ret = read(rr->snapshot_child_fd, sr, sizeof(*sr));
    if (ret == -1) {
        /* Not ready yet? */
        if (errno == EAGAIN) {
            return 0;
        }

        LOG_ERROR("Failed to read snapshot result from child process: %s\n", strerror(errno));
        goto exit;
    }

    if (sr->magic != SNAPSHOT_RESULT_MAGIC) {
        LOG_ERROR("Corrupted snapshot result (magic=%08x)\n", sr->magic);
        goto exit;
    }

    if (!sr->success) {
        LOG_ERROR("Snapshot failed: %s", sr->err);
        goto exit;
    }

    LOG_VERBOSE("Snapshot created, %lld log entries rewritten to log.\n", sr->num_entries);
    ret = 1;

exit:
    /* If this is a result of a RAFT.COMPACT request, we need to reply. */
    if (rr->compact_req) {
        if (ret == 1) {
            LOG_DEBUG("RAFT.DEBUG COMPACT completed successfully.\n");
            RedisModule_ReplyWithSimpleString(rr->compact_req->ctx, "OK");
        } else {
            LOG_DEBUG("RAFT.DEBUG COMPACT failed: %s\n", sr->err);
            RedisModule_ReplyWithError(rr->compact_req->ctx, sr->err);
        }
        RaftReqFree(rr->compact_req);
        rr->compact_req = NULL;
    }

    close(rr->snapshot_child_fd);
    rr->snapshot_child_fd = -1;

    return ret;
}

RRStatus initiateSnapshot(RedisRaftCtx *rr)
{
    if (rr->snapshot_in_progress) {
       return RR_ERROR;
    }

    LOG_DEBUG("Initiating snapshot%s.\n", rr->compact_req ? ", trigerred by COMPACT" : "");

    if (raft_begin_snapshot(rr->raft, RAFT_SNAPSHOT_NONBLOCKING_APPLY) < 0) {
        LOG_DEBUG("Failed to iniaite snapshot, raft_begin_snapshot() failed.\n");
        return RR_ERROR;
    }
    LOG_DEBUG("Snapshot scope: first_entry_idx=%lu, current_idx=%lu\n",
            raft_get_first_entry_idx(rr->raft),
            raft_get_current_idx(rr->raft));

    rr->snapshot_rewrite_last_idx = raft_get_current_idx(rr->raft);
    rr->snapshot_in_progress = true;

    /* Create a snapshot of the nodes configuration */
    freeSnapshotCfgEntryList(rr->snapshot_info.cfg);
    rr->snapshot_info.cfg = generateSnapshotCfgEntryList(rr);

    /* Initiate a background child process that will:
     * 1. Create an RDB file that serves as a persistence snapshot.
     * 2. Create a new temporary log with old entries removed.
     * 3. Notify us back when it's done, so we can append any new log entries received
     *    since and rotate.
     */

    int snapshot_fds[2];    /* [0] our side, [1] child's side */
    if (pipe(snapshot_fds) < 0) {
        LOG_ERROR("Failed to create snapshot child pipe: %s\n", strerror(errno));
        cancelSnapshot(rr, NULL);
        return RR_ERROR;
    }

    rr->snapshot_child_fd = snapshot_fds[0];
    if (fcntl(rr->snapshot_child_fd, F_SETFL, O_NONBLOCK) < 0) {
        LOG_ERROR("Failed to prepare child pipe: %s\n", strerror(errno));
        cancelSnapshot(rr, NULL);
        return RR_ERROR;
    }

    /* Flush stdio files to avoid leaks from child */
    fflush(redis_raft_logfile);
    if (rr->log) {
        RaftLogSync(rr->log);
    }

    pid_t child = fork();
    if (child < 0) {
        LOG_ERROR("Failed to fork snapshot child: %s\n", strerror(errno));
        cancelSnapshot(rr, NULL);
        return RR_ERROR;
    } else if (!child) {
        /* Report result */
        SnapshotResult sr = { 0 };

        redis_raft_logfile = NULL;

        /* Handle compact delay, used for strictly as a debugging tool for testing */
        if (rr->compact_req && rr->config->compact_delay) {
            sleep(rr->config->compact_delay);
        }

        sr.magic = SNAPSHOT_RESULT_MAGIC;
        snprintf(sr.rdb_filename, sizeof(sr.rdb_filename) - 1, "%s.tmp", rr->config->rdb_filename);
        if (rr->config->raftlog) {
            snprintf(sr.log_filename, sizeof(sr.log_filename) - 1, "%s.tmp", rr->config->raftlog);
        }

        /* Configure Redis to dump to our temporary file */
        RedisModuleCallReply *reply = RedisModule_Call(rr->ctx, "CONFIG", "ccc", "SET", "dbfilename",
                sr.rdb_filename);
        if (!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
            snprintf(sr.err, sizeof(sr.err) - 1, "%s", "CONFIG SET dbfilename failed");
            goto exit;
        }
        RedisModule_FreeCallReply(reply);

        /* Save */
        reply = RedisModule_Call(rr->ctx, "SAVE", "");
        if (!reply || RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_STRING) {
            snprintf(sr.err, sizeof(sr.err) - 1, "%s", "SAVE failed");
            goto exit;
        }

        size_t len;
        const char *s = RedisModule_CallReplyStringPtr(reply, &len);
        if (len != 2 && memcmp(s, "OK", 2)) {
            snprintf(sr.err, sizeof(sr.err) - 1, "SAVE failed: %.*s", (int) len, s);
            goto exit;
        }
        RedisModule_FreeCallReply(reply);

        /* Now create a compact log file */
        sr.num_entries = RaftLogRewrite(rr, sr.log_filename);
        if (sr.num_entries < 0) {
            snprintf(sr.err, sizeof(sr.err) - 1, "%s", "Log rewrite failed");
            goto exit;
        }
        sr.success = 1;

exit:
        write(snapshot_fds[1], &sr, sizeof(sr));

        _exit(0);
    }

    /* Close pipe's other side */
    close(snapshot_fds[1]);

    return RR_OK;
}

/* ------------------------------------ Load snapshots ------------------------------------ */

static void removeAllNodes(RedisRaftCtx *rr)
{
    int i;

    for (i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *rn = raft_get_node_from_idx(rr->raft, i);
        assert(rn != NULL);

        /* Leave our node */
        if (raft_node_get_id(rn) == raft_get_nodeid(rr->raft)) {
            continue;
        }

        Node *n = raft_node_get_udata(rn);
        if (n != NULL) {
            NodeFree(n);
        }
        raft_remove_node(rr->raft, rn);
    }
}

/* Load node configuration from snapshot metadata.  We assume no duplicate nodes
 * here, so removeAllNodes() should be called beforehand.
 */
static void loadSnapshotNodes(RedisRaftCtx *rr, SnapshotCfgEntry *cfg)
{
    while (cfg != NULL) {
        Node *n = NULL;

        /* Skip myself */
        if (cfg->id == raft_get_nodeid(rr->raft)) {
            n = NULL;
        } else {
            n = NodeInit(cfg->id, &cfg->addr);
        }

        /* Set up new node */
        raft_node_t *rn;
        if (cfg->voting) {
            rn = raft_add_node(rr->raft, n, cfg->id, 0);
        } else {
            rn = raft_add_non_voting_node(rr->raft, n, cfg->id, 0);
        }

        if (rn) {
            raft_node_set_active(rn, cfg->active);
        }
        cfg = cfg->next;
    }

}

void configRaftFromSnapshotInfo(RedisRaftCtx *rr)
{
    /* Load node configuration */
    removeAllNodes(rr);
    loadSnapshotNodes(rr, rr->snapshot_info.cfg);

    LOG_DEBUG("Snapshot configuration loaded. Raft state:\n");
    int i;
    for (i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *rnode = raft_get_node_from_idx(rr->raft, i);
        Node *node = raft_node_get_udata(rnode);

        if (!node) {
            LOG_DEBUG("  node <unknown?>\n", i);
        } else {
            LOG_DEBUG("  node id=%d,addr=%s,port=%d\n",
                    node->id, node->addr.host, node->addr.port);
        }
    }
}

/* After a snapshot is received (becomes the Redis dataset), load it into the Raft
 * library:
 * 1. Configure index/term/etc.
 * 2. Reconfigure nodes based on the snapshot metadata configuration.
 */
static int loadSnapshot(RedisRaftCtx *rr)
{
    if (!rr->snapshot_info.loaded) {
        LOG_ERROR("No snapshot metadata received, aborting.\n");
        return -1;
    }

    LOG_DEBUG("Begining snapshot load, term=%lu, last_included_index=%lu\n",
            rr->snapshot_info.last_applied_term,
            rr->snapshot_info.last_applied_idx);

    int ret;
    if ((ret = raft_begin_load_snapshot(rr->raft, rr->snapshot_info.last_applied_term,
                rr->snapshot_info.last_applied_idx)) != 0) {
        LOG_ERROR("Cannot load snapshot: already loaded?\n");
        return -1;
    }

    configRaftFromSnapshotInfo(rr);

    raft_end_load_snapshot(rr->raft);
    raft_set_snapshot_metadata(rr->raft, rr->snapshot_info.last_applied_term,
            rr->snapshot_info.last_applied_idx);
    return 0;
}

static RRStatus storeSnapshotData(RedisRaftCtx *rr, RedisModuleString *data_str)
{
    size_t data_len;
    const char *data = RedisModule_StringPtrLen(data_str, &data_len);
    int fd = open(rr->config->rdb_filename, O_CREAT|O_TRUNC|O_RDWR, 0666);

    if (fd < 0) {
        LOG_ERROR("Failed to open snapshot file: %s: %s",
                rr->config->rdb_filename, strerror(errno));
        return RR_ERROR;
    }

    int r = write(fd, data, data_len);
    if (r < data_len) {
        if (r < 0) {
            LOG_ERROR("Failed to write snapshot file: %s: %s", rr->config->rdb_filename,
                    strerror(errno));
        } else {
            LOG_ERROR("Short write on snapshot file: %s", rr->config->rdb_filename);
        }
        close(fd);

        return RR_ERROR;
    }

    LOG_DEBUG("Saved received snapshot to file: %s, %lu bytes\n",
            rr->config->rdb_filename, data_len);

    return RR_OK;
}

int rdbLoad(const char *filename, void *info);

void handleLoadSnapshot(RedisRaftCtx *rr, RaftReq *req)
{
    RedisModuleCallReply *reply = NULL;

    /* Ignore load snapshot request if we are leader, or if we already have
     * what we are looking for.
     */
    raft_node_t *leader = raft_get_current_leader_node(rr->raft);
    if (leader && raft_node_get_id(leader) == raft_get_nodeid(rr->raft)) {
        LOG_VERBOSE("Skipping RAFT.LOADSNAPSHOT as I am the leader.");
        RedisModule_ReplyWithError(req->ctx, "ERR leader does not accept snapshots");
        goto exit;
    }

    if (rr->snapshot_in_progress) {
        LOG_VERBOSE("Skipping queued RAFT.LOADSNAPSHOT because of snapshot in progress");
        RedisModule_ReplyWithError(req->ctx, "ERR snapshot is in progress");
        goto exit;
    }

    if (storeSnapshotData(rr, req->r.loadsnapshot.snapshot) != RR_OK) {
        RedisModule_ReplyWithError(req->ctx, "ERR failed to store snapshot");
        goto exit;
    }

    RedisModule_ThreadSafeContextLock(rr->ctx);
    reply = RedisModule_Call(rr->ctx, "FLUSHALL", "");
    if (!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        RedisModule_ReplyWithError(req->ctx, "ERR failed to flush db before loading snapshot");
        RedisModule_ThreadSafeContextUnlock(rr->ctx);
        goto exit;
    }

    rr->snapshot_info.loaded = false;

    if (rdbLoad(rr->config->rdb_filename, NULL) != 0 ||
            !rr->snapshot_info.loaded ||
            loadSnapshot(rr) < 0) {
        LOG_ERROR("Failed to load snapshot");
        RedisModule_ReplyWithError(req->ctx, "ERR failed to load snapshot");
        RedisModule_ThreadSafeContextUnlock(rr->ctx);
        goto exit;
    }

    /* Restart the log where the snapshot ends */
    if (rr->log) {
        RaftLogClose(rr->log);
        rr->log = RaftLogCreate(rr->config->raftlog,
                rr->snapshot_info.dbid,
                rr->snapshot_info.last_applied_term,
                rr->snapshot_info.last_applied_idx);
    }

    RedisModule_ThreadSafeContextUnlock(rr->ctx);
    RedisModule_ReplyWithLongLong(req->ctx, 1);

exit:
    if (reply) {
        RedisModule_FreeCallReply(reply);
    }

    RaftReqFree(req);
}

void handleCompact(RedisRaftCtx *rr, RaftReq *req)
{
    rr->compact_req = req;

    if (initiateSnapshot(rr) != RR_OK) {
        LOG_VERBOSE("RAFT.DEBUG COMPACT requested but failed.\n");
        RedisModule_ReplyWithError(req->ctx, "ERR operation failed, nothing to compact?");
        RaftReqFree(req);
        return;
    }
}


/* ------------------------------------ Snapshot metadata type ------------------------------------ */

static const char snapshot_info_metakey[] = "__raft_snapshot_info__";

extern RedisRaftCtx redis_raft;

void initializeSnapshotInfo(RedisRaftCtx *rr)
{
    RedisModuleString *name = RedisModule_CreateString(rr->ctx, snapshot_info_metakey,
            sizeof(snapshot_info_metakey) - 1);
    RedisModuleKey *k = RedisModule_OpenKey(rr->ctx, name, REDISMODULE_WRITE);
    RedisModule_ModuleTypeSetValue(k, RedisRaftType, &rr->snapshot_info);
    RedisModule_CloseKey(k);
    RedisModule_FreeString(rr->ctx, name);
}


RedisModuleType *RedisRaftType = NULL;

void *rdbLoadSnapshotInfo(RedisModuleIO *rdb, int encver)
{
    size_t len;
    char *buf;

    RaftSnapshotInfo *info = &redis_raft.snapshot_info;

    /* dbid */
    buf = RedisModule_LoadStringBuffer(rdb, &len);
    assert(len <= RAFT_DBID_LEN);
    if (len) {
        memcpy(info->dbid, buf, len);
    }
    info->dbid[len] = '\0';
    RedisModule_Free(buf);

    /* Load term/index */
    info->last_applied_term = RedisModule_LoadUnsigned(rdb);
    info->last_applied_idx = RedisModule_LoadUnsigned(rdb);

    /* Load configuration */
    freeSnapshotCfgEntryList(info->cfg);
    info->cfg = NULL;
    SnapshotCfgEntry **ep = &info->cfg;

    do {
        unsigned long _id = RedisModule_LoadUnsigned(rdb);

        if (!_id) {
            break;
        }

        /* Allocate new entry, advance ep */
        *ep = RedisModule_Calloc(1, sizeof(SnapshotCfgEntry));
        SnapshotCfgEntry *entry = *ep;
        ep = &entry->next;

        /* Populate */
        entry->id = _id;
        entry->active = RedisModule_LoadUnsigned(rdb);
        entry->voting = RedisModule_LoadUnsigned(rdb);

        buf = RedisModule_LoadStringBuffer(rdb, &len);
        entry->addr.port = RedisModule_LoadUnsigned(rdb);

        assert(len < sizeof(entry->addr.host));
        memcpy(entry->addr.host, buf, len);
        RedisModule_Free(buf);
    } while (1);

    info->loaded = true;

    return info;
}

void rdbSaveSnapshotInfo(RedisModuleIO *rdb, void *value)
{
    RaftSnapshotInfo *info = (RaftSnapshotInfo *) value;

    /* dbid */
    RedisModule_SaveStringBuffer(rdb, info->dbid, strlen(info->dbid));

    /* Term/Index */
    RedisModule_SaveUnsigned(rdb, info->last_applied_term);
    RedisModule_SaveUnsigned(rdb, info->last_applied_idx);

    /* Nodes configuration */
    SnapshotCfgEntry *cfg = info->cfg;
    while (cfg != NULL) {
        RedisModule_SaveUnsigned(rdb, cfg->id);
        RedisModule_SaveUnsigned(rdb, cfg->active);
        RedisModule_SaveUnsigned(rdb, cfg->voting);
        RedisModule_SaveStringBuffer(rdb, cfg->addr.host, strlen(cfg->addr.host));
        RedisModule_SaveUnsigned(rdb, cfg->addr.port);

        cfg = cfg->next;
    }

    /* Last node marker */
    RedisModule_SaveUnsigned(rdb, 0);
}

static void clearSnapshotInfo(void *value)
{
}

RedisModuleTypeMethods RedisRaftTypeMethods = {
    .version = REDISMODULE_TYPE_METHOD_VERSION,
    .rdb_load = rdbLoadSnapshotInfo,
    .rdb_save = rdbSaveSnapshotInfo,
    .free = clearSnapshotInfo
};

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

static int snapshotSendData(Node *node)
{
    RedisRaftCtx *rr = node->rr;
    time_t now = time(NULL);

    /* Load snapshot data */
    char last_term[30];
    snprintf(last_term, sizeof(last_term) - 1, "%lu", raft_get_snapshot_last_term(rr->raft));
    char last_idx[30];
    snprintf(last_idx, sizeof(last_idx) - 1, "%lu", raft_get_snapshot_last_idx(rr->raft));

    const char *args[4] = {
        "RAFT.LOADSNAPSHOT",
        last_term,
        last_idx,
        node->snapshot_buf
    };
    size_t args_len[4] = {
        strlen(args[0]),
        strlen(args[1]),
        strlen(args[2]),
        node->snapshot_size
    };

    node->load_snapshot_idx = raft_get_snapshot_last_idx(rr->raft);
    node->load_snapshot_in_progress = true;
    node->load_snapshot_last_time = now;

    if (redisAsyncCommandArgv(node->rc, handleLoadSnapshotResponse, node, 4, args, args_len) != REDIS_OK) {
        node->state = NODE_CONNECT_ERROR;
        node->load_snapshot_in_progress = false;
        return -1;
    }

    NODE_LOG_DEBUG(node, "Sent snapshot: %lu bytes\n",
                node->snapshot_size);
    return 0;
}

static void cleanSnapshotDelivery(Node *node)
{
    if (node->snapshot_buf != NULL) {
        RedisModule_Free(node->snapshot_buf);
        node->snapshot_buf = NULL;
    }

    uv_fs_t close_req;
    int ret = uv_fs_close(node->rr->loop, &close_req, node->uv_snapshot_file, NULL);
    assert(ret == 0);
}

static void snapshotOnRead(uv_fs_t *req)
{
    Node *node = uv_req_get_data((uv_req_t *) req);
    RedisRaftCtx *rr = node->rr;

    uv_fs_req_cleanup(req);

    if (req->result == node->snapshot_size && snapshotSendData(node) == 0) {
        NODE_LOG_DEBUG(node, "Loaded snapshot: %s: %lu bytes\n",
                rr->config->rdb_filename, node->snapshot_size);
    }

    cleanSnapshotDelivery(node);
}

static void snapshotOnOpen(uv_fs_t *req)
{
    Node *node = uv_req_get_data((uv_req_t *) req);
    uv_fs_t stat_req;

    uv_fs_req_cleanup(req);

    if (req->result < 0) {
        NODE_LOG_DEBUG(node, "Failed to deliver snapshot: open: %s\n",
                uv_strerror(req->result));
        node->load_snapshot_in_progress = false;
        return;
    }

    node->uv_snapshot_file = req->result;
    if (uv_fs_fstat(req->loop, (uv_fs_t *) &stat_req, node->uv_snapshot_file, NULL) < 0) {
        NODE_LOG_DEBUG(node, "Failed to delivery snapshot: open: %s\n",
                uv_strerror(req->result));
        cleanSnapshotDelivery(node);
        return;
    }

    /* prepare buffer and read */
    node->snapshot_size = uv_fs_get_statbuf(&stat_req)->st_size;
    node->snapshot_buf = RedisModule_Alloc(node->snapshot_size);
    node->uv_snapshot_buf = uv_buf_init(node->snapshot_buf, node->snapshot_size);
    int ret = uv_fs_read(node->rr->loop, &node->uv_snapshot_req, node->uv_snapshot_file,
            &node->uv_snapshot_buf, 1, 0, snapshotOnRead);
    assert(ret == 0);
}

static int snapshotInitiateRead(RedisRaftCtx *rr, Node *node, const char *filename)
{
    uv_req_set_data((uv_req_t *) &node->uv_snapshot_req, node);
    int ret = uv_fs_open(rr->loop, &node->uv_snapshot_req, filename, 0, O_RDONLY, snapshotOnOpen);
    assert(ret == 0);

    return 0;
}

int raftSendSnapshot(raft_server_t *raft, void *user_data, raft_node_t *raft_node)
{
    RedisRaftCtx *rr = user_data;
    Node *node = (Node *) raft_node_get_udata(raft_node);

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

    /* Initiate loading of snapshot.  We use libuv to handle loading in the background
     * and avoid blocking the Raft thread.
     *
     * TODO: Refactor hiredis so we can actually stream this directly to the socket
     * instead of buffering the entire file in memory.
     */
    node->load_snapshot_in_progress = true;
    snapshotInitiateRead(rr, node, rr->config->rdb_filename);

    return 0;
}

