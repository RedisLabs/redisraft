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
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "redisraft.h"

/* These are ugly hacks to work around missing Redis Module API calls!
 *
 * For rdbSave() we could use SAVE, but we'd anyway stay with rdbLoad() so
 * we have both.
 */

int rdbLoad(const char *filename, void *info, int flags);
int rdbSave(int flags, const char *filename, void *info);

/* ------------------------------------ Snapshot metadata ------------------------------------ */

void initSnapshotTransferData(RedisRaftCtx *ctx)
{
    ctx->outgoing_snapshot_file.mmap = NULL;
    ctx->outgoing_snapshot_file.len = 0;

    /* Generate temp file name for incoming snapshots */
    snprintf(ctx->incoming_snapshot_file, sizeof(ctx->incoming_snapshot_file),
             "%s.tmp.recv", ctx->config->rdb_filename);

    /* Delete if there is a partial incoming snapshot file from previous run */
    int ret = unlink(ctx->incoming_snapshot_file);
    if (ret != 0 && errno != ENOENT) {
        LOG_WARNING("Unlink file:%s, error :%s \n", ctx->incoming_snapshot_file,
                    strerror(errno));
    }
}

static void releaseSnapshotMmap(RedisRaftCtx *ctx)
{
    if (ctx->outgoing_snapshot_file.mmap != NULL) {
        munmap(ctx->outgoing_snapshot_file.mmap, ctx->outgoing_snapshot_file.len);

        ctx->outgoing_snapshot_file.mmap = NULL;
        ctx->outgoing_snapshot_file.len = 0;
    }
}

void createOutgoingSnapshotMmap(RedisRaftCtx *ctx)
{
    const int mode = S_IRUSR | S_IRGRP | S_IROTH;
    struct stat st;

    releaseSnapshotMmap(ctx);

    int fd = open(ctx->config->rdb_filename, O_RDONLY, mode);
    if (fd == -1) {
        PANIC("Cannot open rdb file at : %s \n", ctx->config->rdb_filename);
    }

    int rc = stat(ctx->config->rdb_filename, &st);
    if (rc != 0) {
        PANIC("stat failed: %s \n", strerror(errno));
    }

    void* p = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
        PANIC("mmap failed: %s \n", strerror(errno));
    }

    close(fd);

    ctx->outgoing_snapshot_file.mmap = p;
    ctx->outgoing_snapshot_file.len = st.st_size;
}

int raftGetSnapshotChunk(raft_server_t* raft, void *user_data,
                         raft_node_t* raft_node, unsigned long long offset,
                         raft_snapshot_chunk_t* chunk)
{
    const unsigned int MAX_CHUNK_SIZE = 32 * 1024;
    RedisRaftCtx *rr = user_data;
    Node *node = raft_node_get_udata(raft_node);

    /* To apply some backpressure, we allow maximum 32 messages on the fly */
    if (node->pending_raft_response_num >= 32 || !ConnIsConnected(node->conn)) {
        return RAFT_ERR_DONE;
    }

    chunk->len = MIN(MAX_CHUNK_SIZE, rr->outgoing_snapshot_file.len - offset);
    if (chunk->len == 0) {
        /* All chunks are sent */
        return RAFT_ERR_DONE;
    }

    chunk->data = (char*) rr->outgoing_snapshot_file.mmap + offset;
    chunk->last_chunk = (offset + chunk->len == rr->outgoing_snapshot_file.len);

    return 0;
}

int raftStoreSnapshotChunk(raft_server_t* raft, void *user_data,
                           raft_index_t snapshot_index,
                           raft_size_t offset,
                           raft_snapshot_chunk_t* chunk)
{
    RedisRaftCtx *rr = user_data;


    if (offset == 0) {
        rr->incoming_snapshot_idx = snapshot_index;
    }

    if (rr->incoming_snapshot_idx != snapshot_index) {
        PANIC("Snapshot index was : %ld, received a chunk for %ld \n",
              rr->incoming_snapshot_idx, snapshot_index);
    }

    int flags = O_WRONLY | O_CREAT;
    if (offset == 0) {
        flags |= O_TRUNC;
    }

    int fd = open(rr->incoming_snapshot_file, flags, S_IWUSR | S_IRUSR);
    if (fd == -1) {
        LOG_WARNING("open file:%s, error:%s \n", rr->incoming_snapshot_file,
                    strerror(errno));
        return -1;
    }

    off_t ret_offset = lseek(fd, offset, SEEK_CUR);
    if (ret_offset != (off_t) offset) {
        LOG_WARNING("lseek file:%s, error:%s \n", rr->incoming_snapshot_file,
                    strerror(errno));
        close(fd);
        return -1;
    }

    size_t len = write(fd, chunk->data, chunk->len);
    if (len != chunk->len) {
        LOG_WARNING("write, written: %zu, chunk len : %llu, err :%s \n", len,
                    chunk->len, strerror(errno));
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

int raftClearSnapshot(raft_server_t* raft, void *user_data)
{
    RedisRaftCtx *rr = user_data;

    int ret = unlink(rr->incoming_snapshot_file);
    if (ret != 0 && errno != ENOENT) {
        LOG_WARNING("Unlink file:%s, error :%s \n", rr->incoming_snapshot_file,
                    strerror(errno));
    }

    return 0;
}

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

    LOG_DEBUG("Finalizing snapshot.");

    /* Rewrite any additional log entries beyond the snapshot to a new
     * log file.
     */
    num_log_entries = RaftLogRewrite(rr, temp_log_filename,
        rr->last_snapshot_idx, rr->last_snapshot_term);

    new_log = RaftLogOpen(temp_log_filename, rr->config, RAFTLOG_KEEP_INDEX);
    if (!new_log) {
        LOG_WARNING("Failed to open log after rewrite: %s", strerror(errno));
        cancelSnapshot(rr, sr);
        return -1;
    }

    LOG_VERBOSE("Log rewrite complete, %lu entries rewritten (from idx %lu).",
            num_log_entries, raft_get_snapshot_last_idx(rr->raft));

    /* We now have to switch temp files. We need to rename two files in a non-atomic
     * operation, so order is critical and we must rename the snapshot file first.
     * This guarantees we lose no data if we fail now before renaming the log -- all
     * we'll have to do is skip redundant log entries.
     */

    if (rename(sr->rdb_filename, rr->config->rdb_filename) < 0) {
        LOG_WARNING("Failed to switch snapshot filename (%s to %s): %s",
                    sr->rdb_filename, rr->config->rdb_filename, strerror(errno));
        RaftLogClose(new_log);
        cancelSnapshot(rr, sr);
        return -1;
    }

    fsyncThreadWaitUntilCompleted(&rr->fsyncThread);

    if (RaftLogRewriteSwitch(rr, new_log, num_log_entries) != RR_OK) {
        RaftLogClose(new_log);
        cancelSnapshot(rr, sr);
        return -1;
    }

    createOutgoingSnapshotMmap(rr);

    /* Finalize snapshot */
    raft_end_snapshot(rr->raft);
    rr->snapshot_in_progress = false;
    rr->snapshots_created++;

    return RR_OK;
}

int pollSnapshotStatus(RedisRaftCtx *rr, SnapshotResult *sr)
{
    memset(sr, 0, sizeof(*sr));

    int ret = (int) read(rr->snapshot_child_fd, sr, sizeof(*sr));
    if (ret < 0) {
        /* Not ready yet? */
        if (errno == EAGAIN) {
            return 0;
        }

        LOG_WARNING("Failed to read snapshot result from child process: %s", strerror(errno));
        goto exit;
    }

    if (sr->magic != SNAPSHOT_RESULT_MAGIC) {
        LOG_WARNING("Corrupted snapshot result (magic=%08x)", sr->magic);
        ret = -1;
        goto exit;
    }

    if (!sr->success) {
        LOG_WARNING("Snapshot failed: %s", sr->err);
        ret = -1;
        goto exit;
    }

    LOG_VERBOSE("Snapshot created successfuly.");
    ret = 1;

exit:
    /* If this is a result of a RAFT.DEBUG COMPACT request, we need to reply. */
    if (rr->debug_req) {
        if (ret == 1) {
            LOG_DEBUG("RAFT.DEBUG COMPACT completed successfully.");
            RedisModule_ReplyWithSimpleString(rr->debug_req->ctx, "OK");
        } else {
            LOG_DEBUG("RAFT.DEBUG COMPACT failed: %s", sr->err);
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

    if (rr->debug_req) {
        LOG_DEBUG("Initiating RAFT.DEBUG COMPACT initiated snapshot.");
    } else {
        LOG_DEBUG("Initiating snapshot.");
    }

    if (raft_begin_snapshot(rr->raft) < 0) {
        LOG_DEBUG("Failed to initiate snapshot, raft_begin_snapshot() failed.");
        return RR_ERROR;
    }
    LOG_DEBUG("Snapshot scope: first_entry_idx=%lu, current_idx=%lu",
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
        LOG_WARNING("Failed to create snapshot child pipe: %s", strerror(errno));
        cancelSnapshot(rr, NULL);
        return RR_ERROR;
    }

    rr->snapshot_child_fd = snapshot_fds[0];
    if (fcntl(rr->snapshot_child_fd, F_SETFL, O_NONBLOCK) < 0) {
        LOG_WARNING("Failed to prepare child pipe: %s", strerror(errno));
        cancelSnapshot(rr, NULL);
        return RR_ERROR;
    }

    /* Flush stdio files to avoid leaks from child */
    if (rr->log) {
        fsyncThreadWaitUntilCompleted(&rr->fsyncThread);

        if (RaftLogSync(rr->log, true) != RR_OK) {
            PANIC("RaftLogSync() failed.");
        }
    }

    pid_t child = RedisModule_Fork(NULL, NULL);
    if (child < 0) {
        LOG_WARNING("Failed to fork snapshot child: %s", strerror(errno));
        cancelSnapshot(rr, NULL);
        return RR_ERROR;
    } else if (!child) {
        /* Report result */
        SnapshotResult sr = { .magic = SNAPSHOT_RESULT_MAGIC};

        snprintf(sr.rdb_filename, sizeof(sr.rdb_filename) - 1, "%s.tmp.%d",
                 rr->config->rdb_filename, (int) getpid());

        /* Handle compact delay, used for strictly as a debugging tool for testing */
        if (rr->debug_req) {
            int delay = rr->debug_req->r.debug.delay;
            if (delay) {
                sleep(delay);
            }

            if (rr->debug_req->r.debug.fail) {
                strncpy(sr.err, "debug rdbSave() failed", sizeof(sr.err));
                sr.err[sizeof(sr.err) - 1] = '\0';
                goto exit;
            }
        }

        if (rdbSave(0, sr.rdb_filename, NULL) != 0) {
            snprintf(sr.err, sizeof(sr.err) - 1, "%s", "rdbSave() failed");
            goto exit;
        }

        sr.success = 1;

exit:
        if (write(snapshot_fds[1], &sr, sizeof(sr)) != sizeof(sr)) {
            PANIC("Failed to write snapshot result : %s", strerror(errno));
        }

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

    /* Clear inactive flag, a node in the snapshot cannot be inactive */
    raft_node_set_active(node, 1);

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
    Node *n = NodeCreate(rr, cfg->id, &cfg->addr);
    raft_node_t *rn;

    if (cfg->voting) {
        rn = raft_add_node(rr->raft, n, cfg->id, 0);
    } else {
        rn = raft_add_non_voting_node(rr->raft, n, cfg->id, 0);
    }

    RedisModule_Assert(rn != NULL);

    LOG_DEBUG("Snapshot Load: adding node %d: %s:%d: voting=%s",
        cfg->id,
        cfg->addr.host,
        cfg->addr.port,
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

/* After a snapshot is received, load it into the Raft library:
 * 1. Replace received snapshot file with the current one.
 * 2. Load rdb file.
 * 3. Configure index/term/etc.
 * 4. Reconfigure nodes based on the snapshot metadata configuration.
 * 5. Create a new snapshot memory map.
 */
int raftLoadSnapshot(raft_server_t* raft, void *user_data, raft_index_t index, raft_term_t term)
{
    int ret;
    RedisRaftCtx *rr = user_data;

    if (rr->snapshot_in_progress) {
        LOG_VERBOSE("Skipping queued loadsnapshot because of snapshot in progress.");
        return -1;
    }

    ret = rename(rr->incoming_snapshot_file, rr->config->rdb_filename);
    if (ret != 0) {
        LOG_WARNING("rename(): %s to %s failed with error : %s \n",
                    rr->incoming_snapshot_file, rr->config->rdb_filename, strerror(errno));
        return -1;
    }

    LOG_DEBUG("Beginning snapshot load, term=%lu, index=%lu", term, index);

    if (raft_begin_load_snapshot(rr->raft, term, index) != 0) {
        LOG_DEBUG("Cannot load snapshot: already loaded?");
        return -1;
    }

    RedisModule_ResetDataset(0, 0);
    rr->snapshot_info.loaded = false;

    if (rdbLoad(rr->config->rdb_filename, NULL, 0) != 0 ||
        !rr->snapshot_info.loaded) {
        PANIC("Failed to load snapshot");
    }

    configRaftFromSnapshotInfo(rr);
    raft_end_load_snapshot(rr->raft);

    /* Restart the log where the snapshot ends */
    if (rr->log) {
        fsyncThreadWaitUntilCompleted(&rr->fsyncThread);
        RaftLogClose(rr->log);
        rr->log = RaftLogCreate(rr->config->raft_log_filename,
                rr->snapshot_info.dbid,
                rr->snapshot_info.last_applied_term,
                rr->snapshot_info.last_applied_idx,
                rr->config);
        EntryCacheDeleteHead(rr->logcache, raft_get_snapshot_last_idx(rr->raft) + 1);
    }

    createOutgoingSnapshotMmap(rr);
    rr->snapshots_loaded++;

    return 0;
}

/* ------------------------------------ Snapshot metadata type ------------------------------------ */

RedisModuleType *RedisRaftType = NULL;

void LockedKeysRDBLoad(RedisModuleIO *rdb)
{
    size_t count = RedisModule_LoadUnsigned(rdb);

    for (size_t i = 0; i < count; i++) {
        RedisModuleString * key = RedisModule_LoadString(rdb);
        RedisModule_DictSet(redis_raft.locked_keys, key, NULL);
        RedisModule_FreeString(NULL, key);
    }
}

static int rdbLoadSnapshotInfo(RedisModuleIO *rdb, int encver, int when)
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

    /* Load locked_keys dict */
    LockedKeysRDBLoad(rdb);

    info->loaded = true;
    return REDISMODULE_OK;
}

void LockedKeysRDBSave(RedisModuleIO *rdb)
{
    RedisRaftCtx *rr = &redis_raft;
    RedisModuleDict * dict = rr->locked_keys;

    RedisModule_SaveUnsigned(rdb, RedisModule_DictSize(dict));

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);

    const char *key;
    size_t key_len;

    while ((key = RedisModule_DictNextC(iter, &key_len, NULL)) != NULL) {
        RedisModule_SaveStringBuffer(rdb, key, key_len);
    }
    RedisModule_DictIteratorStop(iter);
}

static void rdbSaveSnapshotInfo(RedisModuleIO *rdb, int when)
{
    RaftSnapshotInfo *info = &redis_raft.snapshot_info;

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

    /* Save LockedKeys dict */
    LockedKeysRDBSave(rdb);
}

/* Do nothing -- AOF should never be used with RedisRaft, but we have to specify
 * a callback. */
static void aofRewriteCallback(RedisModuleIO *io, RedisModuleString *key, void *value)
{
    UNUSED(io);
    UNUSED(key);
    UNUSED(value);
}

RedisModuleTypeMethods RedisRaftTypeMethods = {
    .version = REDISMODULE_TYPE_METHOD_VERSION,
    .aof_rewrite = aofRewriteCallback,
    .aux_load = rdbLoadSnapshotInfo,
    .aux_save = rdbSaveSnapshotInfo,
    .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB
};

static void handleSnapshotResponse(redisAsyncContext *c, void *r, void *privdata)
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

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 5 ||
        reply->element[0]->type != REDIS_REPLY_INTEGER ||
        reply->element[1]->type != REDIS_REPLY_INTEGER ||
        reply->element[2]->type != REDIS_REPLY_INTEGER ||
        reply->element[3]->type != REDIS_REPLY_INTEGER ||
        reply->element[4]->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_WARNING(node, "invalid RAFT.LOADSNAPSHOT reply");
        return;
    }

    raft_snapshot_resp_t response = {
        .term = reply->element[0]->integer,
        .msg_id = reply->element[1]->integer,
        .offset = reply->element[2]->integer,
        .success = reply->element[3]->integer,
        .last_chunk = reply->element[4]->integer
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);
    if (!raft_node) {
        NODE_LOG_DEBUG(node, "RAFT.SNAPSHOT stale reply.");
        return;
    }

    int ret;
    if ((ret = raft_recv_snapshot_response(rr->raft, raft_node, &response)) != 0) {
        LOG_DEBUG("raft_recv_snapshot_response failed, error %d", ret);
    }
}

int raftSendSnapshot(raft_server_t* raft,
                     void *user_data,
                     raft_node_t* raft_node,
                     raft_snapshot_req_t* msg)
{
    Node *node = raft_node_get_udata(raft_node);

    char target_node_id[32];
    snprintf(target_node_id, sizeof(target_node_id), "%d", node->id);

    char source_node_id[32];
    snprintf(source_node_id, sizeof(source_node_id), "%d", raft_get_nodeid(raft));

    char msgstr[256];
    snprintf(msgstr, sizeof(msgstr), "%lu:%d:%lu:%lu:%lu:%llu:%d",
             msg->term,
             msg->leader_id,
             msg->msg_id,
             msg->snapshot_index,
             msg->snapshot_term,
             msg->chunk.offset,
             msg->chunk.last_chunk);

    const char *args[] = {
        "RAFT.SNAPSHOT",
        target_node_id,
        source_node_id,
        msgstr,
        msg->chunk.data
    };

    size_t args_len[] = {
        strlen(args[0]),
        strlen(args[1]),
        strlen(args[2]),
        strlen(args[3]),
        msg->chunk.len,
    };

    if (!ConnIsConnected(node->conn)) {
        return -1;
    }

    if (redisAsyncCommandArgv(ConnGetRedisCtx(node->conn),
                handleSnapshotResponse, node, 5, args, args_len) != REDIS_OK) {
        return -1;
    }

    NodeAddPendingResponse(node, false);

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
