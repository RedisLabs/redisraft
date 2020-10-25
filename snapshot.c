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
#include "redisraft.h"

/* These are ugly hacks to work around missing Redis Module API calls!
 * 
 * For rdbSave() we could use SAVE, but we'd anyway stay with rdbLoad() so
 * we have both.
 */

int rdbLoad(const char *filename, void *info, int flags);
int rdbSave(const char *filename, void *info);

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

static void freeNodeIdEntryList(NodeIdEntry *head)
{
    while (head != NULL) {
        struct NodeIdEntry *next = head->next;
        RedisModule_Free(head);
        head = next;
    }
}


/* ------------------------------------ Generate snapshots ------------------------------------ */

/* Returns the number of nodes currently being pushed snapshots */
static int snapshotSendInProgress(RedisRaftCtx *rr)
{
    int i;
    int count = 0;

    for (i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *rn = raft_get_node_from_idx(rr->raft, i);
        Node *n = raft_node_get_udata(rn);
        if (n && n->load_snapshot_in_progress) {
            count++;
        }
    }

    return count;
}

void cancelSnapshot(RedisRaftCtx *rr, SnapshotResult *sr)
{
    assert(rr->snapshot_in_progress);

    raft_cancel_snapshot(rr->raft);
    rr->snapshot_in_progress = false;

    if (sr != NULL) {
        if (sr->rdb_filename[0]) {
            unlink(sr->rdb_filename);
        }
    }
}

RRStatus finalizeSnapshot(RedisRaftCtx *rr, SnapshotResult *sr)
{
    RaftLog *new_log = NULL;
    unsigned long num_log_entries;

    char temp_log_filename[256];
    snprintf(temp_log_filename, sizeof(temp_log_filename) - 1, "%s.tmp",
        rr->config->raft_log_filename);

    assert(rr->snapshot_in_progress);

    TRACE("Finalizing snapshot.\n");

    /* Rewrite any additional log entries beyond the snapshot to a new
     * log file.
     */
    num_log_entries = RaftLogRewrite(rr, temp_log_filename,
        rr->last_snapshot_idx, rr->last_snapshot_term);

    new_log = RaftLogOpen(temp_log_filename, rr->config, RAFTLOG_KEEP_INDEX);
    if (!new_log) {
        LOG_ERROR("Failed to open log after rewrite: %s\n", strerror(errno));
        cancelSnapshot(rr, sr);
        return -1;
    }

    LOG_VERBOSE("Log rewrite complete, %lld entries rewritten (from idx %lu).\n",
            num_log_entries, raft_get_snapshot_last_idx(rr->raft));

    /* We now have to switch temp files. We need to rename two files in a non-atomic
     * operation, so order is critical and we must rename the snapshot file first.
     * This guarantees we lose no data if we fail now before renaming the log -- all
     * we'll have to do is skip redundant log entries.
     */

    if (rename(sr->rdb_filename, rr->config->rdb_filename) < 0) {
        LOG_ERROR("Failed to switch snapshot filename (%s to %s): %s\n",
                sr->rdb_filename, rr->config->rdb_filename, strerror(errno));
        RaftLogClose(new_log);
        cancelSnapshot(rr, sr);
        return -1;
    }

    if (RaftLogRewriteSwitch(rr, new_log, num_log_entries) != RR_OK) {
        RaftLogClose(new_log);
        cancelSnapshot(rr, sr);
        return -1;
    }

    /* Finalize snapshot */
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
        LOG_ERROR("Snapshot failed: %s\n", sr->err);
        goto exit;
    }

    LOG_VERBOSE("Snapshot created successfuly.\n");
    ret = 1;

exit:
    /* If this is a result of a RAFT.DEBUG COMPACT request, we need to reply. */
    if (rr->debug_req) {
        assert(rr->debug_req->r.debug.type == RR_DEBUG_COMPACT);

        if (ret == 1) {
            LOG_DEBUG("RAFT.DEBUG COMPACT completed successfully.\n");
            RedisModule_ReplyWithSimpleString(rr->debug_req->ctx, "OK");
        } else {
            LOG_DEBUG("RAFT.DEBUG COMPACT failed: %s\n", sr->err);
            RedisModule_ReplyWithError(rr->debug_req->ctx, sr->err);
        }
        RaftReqFree(rr->debug_req);
        rr->debug_req = NULL;
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

    if (snapshotSendInProgress(rr) > 0) {
        LOG_DEBUG("Delaying snapshot as snapshot delivery is in progress.\n");
        return RR_ERROR;
    }

    if (rr->debug_req) {
        assert(rr->debug_req->r.debug.type == RR_DEBUG_COMPACT);
        LOG_DEBUG("Initiating RAFT.DEBUG COMPACT initiated snapshot.\n");
    } else {
        LOG_DEBUG("Initiating snapshot.\n");
    }

    if (raft_begin_snapshot(rr->raft, RAFT_SNAPSHOT_NONBLOCKING_APPLY) < 0) {
        LOG_DEBUG("Failed to iniaite snapshot, raft_begin_snapshot() failed.\n");
        return RR_ERROR;
    }
    LOG_DEBUG("Snapshot scope: first_entry_idx=%lu, current_idx=%lu\n",
            raft_get_first_entry_idx(rr->raft),
            raft_get_current_idx(rr->raft));

    rr->last_snapshot_idx = rr->snapshot_info.last_applied_idx;
    rr->last_snapshot_term = rr->snapshot_info.last_applied_term;
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

    pid_t child = RedisModule_Fork(NULL, NULL);
    if (child < 0) {
        LOG_ERROR("Failed to fork snapshot child: %s\n", strerror(errno));
        cancelSnapshot(rr, NULL);
        return RR_ERROR;
    } else if (!child) {
        /* Report result */
        SnapshotResult sr = { 0 };

        redis_raft_logfile = NULL;

        /* Handle compact delay, used for strictly as a debugging tool for testing */
        if (rr->debug_req) {
            int delay = rr->debug_req->r.debug.d.compact.delay;
            if (delay) {
                sleep(delay);
            }
        }

        sr.magic = SNAPSHOT_RESULT_MAGIC;
        snprintf(sr.rdb_filename, sizeof(sr.rdb_filename) - 1, "%s.tmp.%d",
            rr->config->rdb_filename, getpid());

        if (rdbSave(sr.rdb_filename, NULL) != 0) {
            snprintf(sr.err, sizeof(sr.err) - 1, "%s", "rdbSave() failed");
            goto exit;
        }

        sr.success = 1;

exit:
        write(snapshot_fds[1], &sr, sizeof(sr));

        RedisModule_ExitFromChild(0);
    }

    /* Close pipe's other side */
    close(snapshot_fds[1]);

    return RR_OK;
}

/* ------------------------------------ Load snapshots ------------------------------------ */

static int updateNodeFromSnapshot(RedisRaftCtx *rr, raft_node_t *node, SnapshotCfgEntry *cfg)
{
    int ret = 0;

    if (cfg->voting != raft_node_is_voting(node)) {
        raft_node_set_voting(node, cfg->voting);
        raft_node_set_voting_committed(node, cfg->voting);
        ret = 1;
    }

    /* NOTE: We currently assume address and port cannot be configured on the fly,
     * so they'll always involve a node id change.
     */
    return ret;
}

static void createNodeFromSnapshot(RedisRaftCtx *rr, SnapshotCfgEntry *cfg)
{
    Node *n = NodeInit(cfg->id, &cfg->addr);
    raft_node_t *rn;

    if (cfg->voting) {
        rn = raft_add_node(rr->raft, n, cfg->id, 0);
    } else {
        rn = raft_add_non_voting_node(rr->raft, n, cfg->id, 0);
    }

    assert(rn != NULL);

    LOG_DEBUG("Snapshot Load: adding node %d: %s: voting=%s\n",
        cfg->id,
        cfg->addr,
        cfg->voting ? "yes" : "no");
}


/* Load node configuration from snapshot metadata.
 *
 * We assume we're being called right after raft_begin_load_snapshot()
 * so all nodes except self have been removed.
 */
void configRaftFromSnapshotInfo(RedisRaftCtx *rr)
{
    SnapshotCfgEntry *cfg = rr->snapshot_info.cfg;
    int added = 0;

    while (cfg != NULL) {
        if (cfg->id == raft_get_nodeid(rr->raft)) {
            raft_node_t *rn = raft_get_node(rr->raft, cfg->id);
            assert(rn != NULL);
            assert(rn == raft_get_my_node(rr->raft));

            updateNodeFromSnapshot(rr, rn, cfg);
        } else {
            createNodeFromSnapshot(rr, cfg);
            added++;
        }
        cfg = cfg->next;
    }

    assert(raft_get_num_nodes(rr->raft) == added + 1);
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

    LOG_DEBUG("Beginning snapshot load, term=%lu, last_included_index=%lu\n",
            rr->snapshot_info.last_applied_term,
            rr->snapshot_info.last_applied_idx);

    if (raft_begin_load_snapshot(rr->raft, rr->snapshot_info.last_applied_term,
                                 rr->snapshot_info.last_applied_idx) != 0) {
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
        LOG_ERROR("Failed to open snapshot file: %s: %s\n",
                rr->config->rdb_filename, strerror(errno));
        return RR_ERROR;
    }

    int r = write(fd, data, data_len);
    if (r < data_len) {
        if (r < 0) {
            LOG_ERROR("Failed to write snapshot file: %s: %s\n", rr->config->rdb_filename,
                    strerror(errno));
        } else {
            LOG_ERROR("Short write on snapshot file: %s\n", rr->config->rdb_filename);
        }
        close(fd);

        return RR_ERROR;
    }

    LOG_DEBUG("Saved received snapshot to file: %s, %lu bytes\n",
            rr->config->rdb_filename, data_len);

    return RR_OK;
}

void handleLoadSnapshot(RedisRaftCtx *rr, RaftReq *req)
{
    if (checkRaftState(rr, req) == RR_ERROR) {
        goto exit;
    }

    /* Ignore load snapshot request if we are leader, or if we already have
     * what we are looking for.
     */
    raft_node_t *leader = raft_get_current_leader_node(rr->raft);
    if (leader && raft_node_get_id(leader) == raft_get_nodeid(rr->raft)) {
        LOG_VERBOSE("Skipping RAFT.LOADSNAPSHOT as I am the leader.\n");
        RedisModule_ReplyWithError(req->ctx, "ERR leader does not accept snapshots");
        goto exit;
    }

    if (rr->snapshot_in_progress) {
        LOG_VERBOSE("Skipping queued RAFT.LOADSNAPSHOT because of snapshot in progress.\n");
        RedisModule_ReplyWithError(req->ctx, "ERR snapshot is in progress");
        goto exit;
    }

    /* Verify snapshot term */
    if (req->r.loadsnapshot.term < raft_get_current_term(rr->raft)) {
        LOG_VERBOSE("Skipping queued RAFT.LOADSNAPSHOT with old term %d\n",
            req->r.loadsnapshot.term);
        RedisModule_ReplyWithLongLong(req->ctx, 0);
        goto exit;
    }

    /* Verify snapshot index and term before attempting to load it. */
    if (req->r.loadsnapshot.idx < raft_get_last_applied_idx(rr->raft)) {
        LOG_VERBOSE("Skipping queued RAFT.LOADSNAPSHOT with index %ld, already applied %d\n",
            req->r.loadsnapshot.idx, raft_get_last_applied_idx(rr->raft));
        RedisModule_ReplyWithLongLong(req->ctx, 0);
        goto exit;
    }

    if (req->r.loadsnapshot.idx < raft_get_current_idx(rr->raft)) {
        LOG_VERBOSE("Skipping queued RAFT.LOADSNAPSHOT with index %ld, current idx is %ld\n",
            req->r.loadsnapshot.idx, raft_get_current_idx(rr->raft));
        RedisModule_ReplyWithLongLong(req->ctx, 0);
        goto exit;
    }

    if (req->r.loadsnapshot.term == raft_get_snapshot_last_term(rr->raft) &&
        req->r.loadsnapshot.idx == raft_get_snapshot_last_idx(rr->raft)) {
            LOG_VERBOSE("Skipping queued RAFT.LOADSNAPSHOT with identical term %ld index %ld\n",
                raft_get_snapshot_last_term(rr->raft),
                raft_get_snapshot_last_idx(rr->raft));
            RedisModule_ReplyWithLongLong(req->ctx, 0);
            goto exit;
    }

    if (storeSnapshotData(rr, req->r.loadsnapshot.snapshot) != RR_OK) {
        RedisModule_ReplyWithError(req->ctx, "ERR failed to store snapshot");
        goto exit;
    }

    RedisModule_ThreadSafeContextLock(rr->ctx);
    RedisModule_ResetDataset(0, 0);
    rr->snapshot_info.loaded = false;

    if (rdbLoad(rr->config->rdb_filename, NULL, 0) != 0 ||
            !rr->snapshot_info.loaded ||
            loadSnapshot(rr) < 0) {
        LOG_ERROR("Failed to load snapshot\n");
        RedisModule_ReplyWithError(req->ctx, "ERR failed to load snapshot");
        RedisModule_ThreadSafeContextUnlock(rr->ctx);
        goto exit;
    }

    /* Restart the log where the snapshot ends */
    if (rr->log) {
        RaftLogClose(rr->log);
        rr->log = RaftLogCreate(rr->config->raft_log_filename,
                rr->snapshot_info.dbid,
                rr->snapshot_info.last_applied_term,
                rr->snapshot_info.last_applied_idx,
                raft_get_current_term(rr->raft),
                raft_get_voted_for(rr->raft),
                rr->config);
        EntryCacheDeleteHead(rr->logcache, raft_get_snapshot_last_idx(rr->raft) + 1);
    }

    /* Recreate the snapshot key in keyspace, to be sure we'll get a chance to
     * serialize it into the RDB file when it is saved.
     *
     * Note: this is just a precaution, because the snapshot we load should contain
     * the meta-key anyway so we should be safe either way.
     *
     * Future improvement: consider using hooks to automatically handle this. It
     * won't be just cleaner, but also be fool-proof in case someone decides to
     * manually dump an RDB file etc.
     */
    initializeSnapshotInfo(rr);

    RedisModule_ThreadSafeContextUnlock(rr->ctx);
    RedisModule_ReplyWithLongLong(req->ctx, 1);

    rr->snapshots_loaded++;

exit:
    RaftReqFree(req);
}

/* ------------------------------------ Snapshot metadata type ------------------------------------ */

static const char snapshot_info_metakey[] = "__raft_snapshot_info__";

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
        entry->voting = RedisModule_LoadUnsigned(rdb);

        buf = RedisModule_LoadStringBuffer(rdb, &len);
        entry->addr.port = RedisModule_LoadUnsigned(rdb);

        assert(len < sizeof(entry->addr.host));
        memcpy(entry->addr.host, buf, len);
        RedisModule_Free(buf);
    } while (1);

    /* Load used node ids */
    freeNodeIdEntryList(info->used_node_ids);
    info->used_node_ids = NULL;
    NodeIdEntry **np = &info->used_node_ids;

    do {
        unsigned long _id = RedisModule_LoadUnsigned(rdb);

        if (!_id) {
            break;
        }

        NodeIdEntry *entry = RedisModule_Calloc(1, sizeof(NodeIdEntry));
        entry->id = _id;

        *np = entry;
        np = &entry->next;
    } while (1);

    /* Load ShardingInfo */
    ShardingInfoRDBLoad(rdb);

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
        RedisModule_SaveUnsigned(rdb, cfg->voting);
        RedisModule_SaveStringBuffer(rdb, cfg->addr.host, strlen(cfg->addr.host));
        RedisModule_SaveUnsigned(rdb, cfg->addr.port);

        cfg = cfg->next;
    }

    /* Last node marker */
    RedisModule_SaveUnsigned(rdb, 0);

    /* Used node ids */
    NodeIdEntry *e = info->used_node_ids;
    while (e != NULL) {
        RedisModule_SaveUnsigned(rdb, e->id);
        e = e->next;
    }

    /* Last node id marker */
    RedisModule_SaveUnsigned(rdb, 0);

    /* Save ShardingInfo */
    ShardingInfoRDBSave(rdb);
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

    NodeDismissPendingResponse(node);
    if (!reply) {
        NODE_LOG_ERROR(node, "RAFT.LOADSNAPSHOT failure: connection dropped\n");
        NodeMarkDisconnected(node);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        NODE_LOG_ERROR(node, "RAFT.LOADSNAPSHOT error: %s\n", reply->str);
    } else if (reply->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_ERROR(node, "RAFT.LOADSNAPSHOT invalid response type\n");
    } else {
        NODE_LOG_DEBUG(node, "RAFT.LOADSNAPSHOT response %lld\n",
                reply->integer);
        raft_node_t *n = raft_get_node(rr->raft, node->id);
        if (n != NULL) {
            raft_node_set_next_idx(n, node->load_snapshot_idx + 1);
        } else {
            NODE_LOG_DEBUG(node, "Node %d no longer exists, not updating next_idx\n",
                    node->id);
        }
    }
}

static int snapshotSendData(Node *node)
{
    RedisRaftCtx *rr = node->rr;
    time_t now = time(NULL);

    /* Load snapshot data */
    char target_node_id[30];
    snprintf(target_node_id, sizeof(target_node_id) - 1, "%d", node->id);

    char term[30];
    snprintf(term, sizeof(term) - 1, "%lu", raft_get_current_term(rr->raft));

    char idx[30];
    snprintf(idx, sizeof(idx) - 1, "%lu", raft_get_snapshot_last_idx(rr->raft));

    const char *args[5] = {
        "RAFT.LOADSNAPSHOT",
        target_node_id,
        term,
        idx,
        node->snapshot_buf
    };
    size_t args_len[5] = {
        strlen(args[0]),
        strlen(args[1]),
        strlen(args[2]),
        strlen(args[3]),
        node->snapshot_size
    };

    node->load_snapshot_idx = raft_get_snapshot_last_idx(rr->raft);
    node->load_snapshot_in_progress = true;
    node->load_snapshot_last_time = now;

    if (!NODE_IS_CONNECTED(node)) {
        node->load_snapshot_in_progress = false;
        return -1;
    }

    if (redisAsyncCommandArgv(node->rc, handleLoadSnapshotResponse, node, 5, args, args_len) != REDIS_OK) {
        node->load_snapshot_in_progress = false;
        return -1;
    }

    NodeAddPendingResponse(node, false);

    NODE_LOG_DEBUG(node, "Sent snapshot: %lu bytes, term %ld, index %ld\n",
                node->snapshot_size, raft_get_snapshot_last_term(rr->raft),
                raft_get_snapshot_last_idx(rr->raft));
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
        return -1;
    }

    /* We don't attempt to load a snapshot before we receive a response.
     *
     * Note: a response in this case only lets us know the operation begun,
     * but it's not a blocking operation.  See RAFT.LOADSNAPSHOT for more info.
     */
    if (node->load_snapshot_in_progress) {
        return -1;
    }

    NODE_LOG_DEBUG(node, "raftSendSnapshot: snapshot_last_idx %ld term %ld, node next_idx %ld\n",
            raft_get_snapshot_last_idx(raft),
            raft_get_snapshot_last_term(raft),
            raft_node_get_next_idx(raft_node));

    if (!NODE_IS_CONNECTED(node)) {
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

void archiveSnapshot(RedisRaftCtx *rr)
{
    size_t bak_rdb_filename_maxlen = strlen(rr->config->rdb_filename);
    char bak_rdb_filename[bak_rdb_filename_maxlen];

    snprintf(bak_rdb_filename, bak_rdb_filename_maxlen - 1,
            "%s.bak.%d", rr->config->rdb_filename, raft_get_nodeid(rr->raft));
    rename(rr->config->rdb_filename, bak_rdb_filename);
}
