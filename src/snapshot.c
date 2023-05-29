/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "entrycache.h"
#include "log.h"
#include "redisraft.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>

/* ------------------------------------ Snapshot metadata ------------------------------------ */

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

    int fd = open(ctx->config.rdb_filename, O_RDONLY, mode);
    if (fd == -1) {
        PANIC("Cannot open rdb file at: %s \n", ctx->config.rdb_filename);
    }

    int rc = stat(ctx->config.rdb_filename, &st);
    if (rc != 0) {
        PANIC("stat failed: %s \n", strerror(errno));
    }

    void *p = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
        PANIC("mmap failed: %s \n", strerror(errno));
    }

    if (close(fd) != 0) {
        LOG_WARNING("close() failure for the file: %s, error: %s",
                    ctx->config.rdb_filename, strerror(errno));
    }

    ctx->outgoing_snapshot_file.mmap = p;
    ctx->outgoing_snapshot_file.len = st.st_size;
}

int raftGetSnapshotChunk(raft_server_t *raft, void *user_data,
                         raft_node_t *raft_node, raft_size_t offset,
                         raft_snapshot_chunk_t *chunk)
{
    RedisRaftCtx *rr = user_data;
    Node *node = raft_node_get_udata(raft_node);

    /* To apply some backpressure, we limit max message count on the fly */
    if (!ConnIsConnected(node->conn) ||
        node->pending_raft_response_num >= rr->config.snapshot_req_max_count) {
        return RAFT_ERR_DONE;
    }

    RedisModule_Assert(offset <= rr->outgoing_snapshot_file.len);

    const raft_size_t max_chunk_size = rr->config.snapshot_req_max_size;
    const raft_size_t remaining_bytes = rr->outgoing_snapshot_file.len - offset;

    chunk->len = MIN(max_chunk_size, remaining_bytes);
    if (chunk->len == 0) {
        /* All chunks are sent */
        return RAFT_ERR_DONE;
    }

    chunk->data = (char *) rr->outgoing_snapshot_file.mmap + offset;
    chunk->last_chunk = (offset + chunk->len == rr->outgoing_snapshot_file.len);

    return 0;
}

int raftStoreSnapshotChunk(raft_server_t *raft, void *user_data,
                           raft_index_t snapshot_index,
                           raft_size_t offset,
                           raft_snapshot_chunk_t *chunk)
{
    RedisRaftCtx *rr = user_data;

    if (offset == 0) {
        rr->incoming_snapshot_idx = snapshot_index;
    }

    if (rr->incoming_snapshot_idx != snapshot_index) {
        PANIC("Snapshot index was: %ld, received a chunk for %ld \n",
              rr->incoming_snapshot_idx, snapshot_index);
    }

    int flags = O_WRONLY | O_CREAT;
    if (offset == 0) {
        flags |= O_TRUNC;
    }

    int fd = open(rr->incoming_snapshot_file, flags, S_IWUSR | S_IRUSR);
    if (fd == -1) {
        LOG_WARNING("open() file: %s, error: %s \n", rr->incoming_snapshot_file,
                    strerror(errno));
        return -1;
    }

    off_t ret_offset = lseek(fd, (off_t) offset, SEEK_SET);
    if (ret_offset != (off_t) offset) {
        LOG_WARNING("lseek() file: %s, error: %s \n",
                    rr->incoming_snapshot_file, strerror(errno));
        close(fd);
        return -1;
    }

    ssize_t len = write(fd, chunk->data, chunk->len);
    if (len != (ssize_t) chunk->len) {
        LOG_WARNING("write(), written: %zd, chunk len: %llu, err: %s \n", len,
                    chunk->len, strerror(errno));
        close(fd);
        return -1;
    }

    if (close(fd) != 0) {
        LOG_WARNING("close() failure: %s, file: %s", strerror(errno),
                    rr->incoming_snapshot_file);
    }
    return 0;
}

int raftClearSnapshot(raft_server_t *raft, void *user_data)
{
    RedisRaftCtx *rr = user_data;

    int ret = unlink(rr->incoming_snapshot_file);
    if (ret != 0 && errno != ENOENT) {
        LOG_WARNING("Unlink file: %s, error: %s \n", rr->incoming_snapshot_file,
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
            na = &rr->config.addr;
        } else if (node != NULL) {
            na = &node->addr;
        } else {
            RedisModule_Assert(0);
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

static void resetSnapshotState(RedisRaftCtx *rr)
{
    rr->snapshot_in_progress = false;
    rr->curr_snapshot_last_term = -1;
    rr->curr_snapshot_last_idx = -1;
    rr->curr_snapshot_start_time = 0;
}

void cancelSnapshot(RedisRaftCtx *rr, SnapshotResult *sr)
{
    RedisModule_Assert(rr->snapshot_in_progress);

    raft_cancel_snapshot(rr->raft);
    resetSnapshotState(rr);

    if (sr != NULL) {
        if (sr->rdb_filename[0]) {
            unlink(sr->rdb_filename);
        }
    }
}

RRStatus finalizeSnapshot(RedisRaftCtx *rr, SnapshotResult *sr)
{
    RedisModule_Assert(rr->snapshot_in_progress);

    LOG_DEBUG("Finalizing snapshot.");

    /* We now have to rename the snapshot file first. This guarantees we lose no
     * data if we fail now before renaming the log -- all we'll have to do is
     * skip redundant log entries.
     */
    if (syncRename(sr->rdb_filename, rr->config.rdb_filename) != RR_OK) {
        LOG_WARNING("Failed to switch snapshot filename (%s to %s): %s",
                    sr->rdb_filename, rr->config.rdb_filename, strerror(errno));
        cancelSnapshot(rr, sr);
        return -1;
    }

    fsyncThreadWaitUntilCompleted(&rr->fsyncThread);
    createOutgoingSnapshotMmap(rr);

    /* Finalize snapshot. logImplPoll callback will be called and first log
     * page will be deleted. */
    raft_end_snapshot(rr->raft);

    LOG_NOTICE("Snapshot has been completed (snapshot idx=%lu).",
               raft_get_snapshot_last_idx(rr->raft));

    uint64_t took;

    took = RedisModule_MonotonicMicroseconds() - rr->curr_snapshot_start_time;
    rr->last_snapshot_time = took / 1000 / 1000;

    resetSnapshotState(rr);

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

    LOG_VERBOSE("Snapshot created successfully.");
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

    rr->curr_snapshot_last_idx = rr->snapshot_info.last_applied_idx;
    rr->curr_snapshot_last_term = rr->snapshot_info.last_applied_term;
    rr->snapshot_in_progress = true;
    rr->curr_snapshot_start_time = RedisModule_MonotonicMicroseconds();

    /* Create a snapshot of the nodes configuration */
    freeSnapshotCfgEntryList(rr->snapshot_info.cfg);
    rr->snapshot_info.cfg = generateSnapshotCfgEntryList(rr);

    /* Initiate a background child process that will:
     * 1. Create an RDB file that serves as a persistence snapshot.
     * 2. Create a new temporary log with old entries removed.
     * 3. Notify us back when it's done, so we can append any new log entries received
     *    since and rotate.
     */

    int snapshot_fds[2]; /* [0] our side, [1] child's side */
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

    fsyncThreadWaitUntilCompleted(&rr->fsyncThread);

    pid_t child = RedisModule_Fork(NULL, NULL);
    if (child < 0) {
        LOG_WARNING("Failed to fork snapshot child: %s", strerror(errno));
        cancelSnapshot(rr, NULL);
        return RR_ERROR;
    } else if (!child) {
        /* Report result */
        SnapshotResult sr = {
            .magic = SNAPSHOT_RESULT_MAGIC,
        };

        snprintf(sr.rdb_filename, sizeof(sr.rdb_filename) - 1, "%s.tmp.%d",
                 rr->config.rdb_filename, (int) getpid());

        if (rr->config.snapshot_delay) {
            sleep(rr->config.snapshot_delay);
        }

        if (rr->config.snapshot_fail) {
            strncpy(sr.err, "debug rdbSave() failed", sizeof(sr.err));
            sr.err[sizeof(sr.err) - 1] = '\0';
            goto exit;
        }

        RedisModuleRdbStream *s = RedisModule_RdbStreamCreateFromFile(sr.rdb_filename);

        if (RedisModule_RdbSave(rr->ctx, s, 0) != REDISMODULE_OK) {
            RedisModule_RdbStreamFree(s);
            snprintf(sr.err, sizeof(sr.err), "rdbSave(): %s", strerror(errno));
            goto exit;
        }

        RedisModule_RdbStreamFree(s);
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
            RedisModule_Assert(rn != NULL);
            raft_node_set_voting(rn, cfg->voting);
        } else {
            createNodeFromSnapshot(rr, cfg);
            added++;
        }
        cfg = cfg->next;
    }

    RedisModule_Assert(raft_get_num_nodes(rr->raft) == added + 1);
}

int raftLoadSnapshot(raft_server_t *raft, void *user_data, raft_term_t term,
                     raft_index_t index)
{
    RedisRaftCtx *rr = user_data;

    if (rr->snapshot_in_progress || rr->state == REDIS_RAFT_LOADING) {
        LOG_VERBOSE("Skipping loadsnapshot because of snapshot in progress.");
        return -1;
    }

    struct stat st;
    if (stat(rr->incoming_snapshot_file, &st) != 0) {
        LOG_WARNING("Failed to get file size: %s", rr->config.rdb_filename);
        return -1;
    }

    LOG_NOTICE("Received snapshot file, size: %lld", (long long) st.st_size);

    if (fsyncFileAt(rr->incoming_snapshot_file) != RR_OK) {
        return -1;
    }

    int rc = syncRename(rr->incoming_snapshot_file, rr->config.rdb_filename);
    if (rc != RR_OK) {
        return -1;
    }

    LOG_DEBUG("Beginning snapshot load, term=%lu, index=%lu", term, index);

    fsyncThreadWaitUntilCompleted(&rr->fsyncThread);

    rr->state = REDIS_RAFT_LOADING;
    rr->snapshot_info.loaded = false;

    if (rr->config.snapshot_disable_load) {
        return -1;
    }

    /* Destroy Node objects and connections to other nodes.
     *
     * `raft_begin_load_snapshot()` will free raft_node_t objects. So, we have
     * to free 'Node' objects associated with these nodes.
     * Later, configRaftFromSnapshotInfo() will call raft_add_node() for the
     * nodes in the snapshot, and we'll create Node objects for them in the
     * raftNotifyMembershipEvent() callback.
     */
    for (int i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *rn = raft_get_node_from_idx(rr->raft, i);
        Node *n = raft_node_get_udata(rn);
        if (n) {
            ConnAsyncTerminate(n->conn);
        }
    }

    int ret = raft_begin_load_snapshot(rr->raft, term, index);
    if (ret != 0) {
        PANIC("Cannot load snapshot: %lu, %lu", term, index);
    }

    RedisModuleRdbStream *s = RedisModule_RdbStreamCreateFromFile(rr->config.rdb_filename);
    if (RedisModule_RdbLoad(rr->ctx, s, 0) != REDISMODULE_OK ||
        !rr->snapshot_info.loaded) {
        PANIC("Failed to load snapshot, RM_RdbLoad(): %s", strerror(errno));
    }
    RedisModule_RdbStreamFree(s);

    configRaftFromSnapshotInfo(rr);
    raft_end_load_snapshot(rr->raft);

    EntryCacheDeleteHead(rr->logcache, raft_get_snapshot_last_idx(rr->raft) + 1);
    createOutgoingSnapshotMmap(rr);

    rr->state = REDIS_RAFT_UP;

    return 0;
}

/* ------------------------------------ Snapshot metadata type ------------------------------------ */

RedisModuleType *RedisRaftType = NULL;

static void lockedKeysRDBLoad(RedisModuleIO *rdb)
{
    RedisRaftCtx *rr = &redis_raft;
    size_t count = RedisModule_LoadUnsigned(rdb);

    MIGRATION_TRACE("Rebuilding locked_keys dict from RDB");

    if (rr->locked_keys) {
        RedisModule_FreeDict(rr->ctx, rr->locked_keys);
    }
    rr->locked_keys = RedisModule_CreateDict(rr->ctx);

    for (size_t i = 0; i < count; i++) {
        RedisModuleString *key = RedisModule_LoadString(rdb);

        size_t len;
        const char *str = RedisModule_StringPtrLen(key, &len);
        MIGRATION_TRACE("Loading key to locked_keys from RDB: %.*s ", (int) len, str);

        RedisModule_DictSet(rr->locked_keys, key, NULL);
        RedisModule_FreeString(NULL, key);
    }
}

static void clientSessionRDBLoad(RedisModuleIO *rdb)
{
    RedisRaftCtx *rr = &redis_raft;
    size_t count = RedisModule_LoadUnsigned(rdb);

    /* clear out client_session_dict, before loading */
    clearClientSessions(rr);

    for (size_t i = 0; i < count; i++) {
        ClientSession *client_session = RedisModule_Alloc(sizeof(ClientSession));
        unsigned long long id = RedisModule_LoadUnsigned(rdb);
        client_session->client_id = id;
        client_session->local = false;
        RedisModule_DictSetC(rr->client_session_dict, &id, sizeof(id), client_session);
    }
}

static int rdbLoadSnapshotInfo(RedisModuleIO *rdb, int encver, int when)
{
    size_t len;
    char *buf;

    RaftSnapshotInfo *info = &redis_raft.snapshot_info;

    /* dbid */
    buf = RedisModule_LoadStringBuffer(rdb, &len);
    RedisModule_Assert(len <= RAFT_DBID_LEN);
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

        RedisModule_Assert(len < sizeof(entry->addr.host));
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
    lockedKeysRDBLoad(rdb);

    /* Load client_session dict */
    clientSessionRDBLoad(rdb);

    /* load blocked command state */
    blockedCommandsLoad(rdb);

    info->loaded = true;
    return REDISMODULE_OK;
}

static void lockedKeysRDBSave(RedisModuleIO *rdb)
{
    RedisRaftCtx *rr = &redis_raft;
    RedisModuleDict *dict = rr->locked_keys;

    RedisModule_SaveUnsigned(rdb, RedisModule_DictSize(dict));

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);

    const char *key;
    size_t key_len;

    while ((key = RedisModule_DictNextC(iter, &key_len, NULL)) != NULL) {
        MIGRATION_TRACE("Saving locked key to RDB: %.*s", (int) key_len, key);
        RedisModule_SaveStringBuffer(rdb, key, key_len);
    }
    RedisModule_DictIteratorStop(iter);
}

static void clientSessionRDBSave(RedisModuleIO *rdb)
{
    RedisRaftCtx *rr = &redis_raft;
    RedisModuleDict *dict = rr->client_session_dict;

    RedisModule_SaveUnsigned(rdb, RedisModule_DictSize(dict));

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);

    ClientSession *client_session;
    while (RedisModule_DictNextC(iter, NULL, (void **) &client_session) != NULL) {
        RedisModule_SaveUnsigned(rdb, client_session->client_id);
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
    lockedKeysRDBSave(rdb);

    /* Save client_session dict */
    clientSessionRDBSave(rdb);

    /* save blocked command state */
    blockedCommandsSave(rdb);
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
    .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB,
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

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 6 ||
        reply->element[0]->type != REDIS_REPLY_INTEGER ||
        reply->element[1]->type != REDIS_REPLY_INTEGER ||
        reply->element[2]->type != REDIS_REPLY_INTEGER ||
        reply->element[3]->type != REDIS_REPLY_INTEGER ||
        reply->element[4]->type != REDIS_REPLY_INTEGER ||
        reply->element[5]->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_WARNING(node, "invalid RAFT.SNAPSHOT reply");
        return;
    }

    raft_snapshot_resp_t response = {
        .msg_id = reply->element[0]->integer,
        .snapshot_index = reply->element[1]->integer,
        .term = reply->element[2]->integer,
        .offset = reply->element[3]->integer,
        .success = reply->element[4]->integer,
        .last_chunk = reply->element[5]->integer,
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

int raftSendSnapshot(raft_server_t *raft,
                     void *user_data,
                     raft_node_t *raft_node,
                     raft_snapshot_req_t *msg)
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
        msg->chunk.data,
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

    int ret = redisAsyncCommandArgv(ConnGetRedisCtx(node->conn),
                                    handleSnapshotResponse, node, 5,
                                    args, args_len);
    if (ret != REDIS_OK) {
        return -1;
    }

    NodeAddPendingResponse(node, false);

    return 0;
}

void archiveSnapshot(RedisRaftCtx *rr)
{
    size_t bak_rdb_filename_maxlen = strlen(rr->config.rdb_filename);
    char bak_rdb_filename[bak_rdb_filename_maxlen];

    snprintf(bak_rdb_filename, bak_rdb_filename_maxlen - 1,
             "%s.bak.%d", rr->config.rdb_filename, raft_get_nodeid(rr->raft));
    rename(rr->config.rdb_filename, bak_rdb_filename);
}

void SnapshotInit(RedisRaftCtx *rr)
{
    rr->outgoing_snapshot_file = (SnapshotFile){0};

    /* Generate temp file name for incoming snapshots */
    snprintf(rr->incoming_snapshot_file, sizeof(rr->incoming_snapshot_file),
             "%s.tmp.recv", rr->config.rdb_filename);

    /* Delete if there is a partial snapshot file from the previous run */
    int ret = unlink(rr->incoming_snapshot_file);
    if (ret != 0 && errno != ENOENT) {
        PANIC("Unlink file: %s, error: %s \n", rr->incoming_snapshot_file,
              strerror(errno));
    }

    resetSnapshotState(rr);
}
