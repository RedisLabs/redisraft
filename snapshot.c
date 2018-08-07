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

long long int rewriteLog(RedisRaftCtx *rr, const char *filename)
{
    RaftLog *log = RaftLogCreate(filename);
    long long int num_entries = 0;

    /* Write a snapshot marker */
    if (RaftLogWriteSnapshotInfo(log, rr->snapshot_info.last_applied_term, rr->snapshot_info.last_applied_idx) < 0) {
        RaftLogClose(log);
        return -1;
    }

    raft_index_t i;
    for (i = raft_get_first_entry_idx(rr->raft) + 1; i <= raft_get_current_idx(rr->raft); i++) {
        num_entries++;
        raft_entry_t *ety = raft_get_entry_from_idx(rr->raft, i);
        if (RaftLogWriteEntry(log, ety) < 0) {
            RaftLogClose(log);
            return -1;
        }
    }

    if (!RaftLogSync(log)) {
        RaftLogClose(log);
        return -1;
    }

    RaftLogClose(log);
    return num_entries;
}


long long int appendLogEntries(RedisRaftCtx *rr, const char *filename, raft_index_t from_idx)
{
    long long int num_entries = 0;
    RaftLog *log = RaftLogOpen(filename);
    if (!log) {
        return -1;
    }

    raft_index_t i;
    for (i = from_idx; i <= raft_get_current_idx(rr->raft); i++) {
        num_entries++;
        raft_entry_t *ety = raft_get_entry_from_idx(rr->raft, i);
        if (RaftLogWriteEntry(log, ety) < 0) {
            RaftLogClose(log);
            return -1;
        }
    }

    if (!RaftLogSync(log)) {
        RaftLogClose(log);
        return -1;
    }

    return num_entries;
}


/* ------------------------------------ Generate snapshots ------------------------------------ */

const char *getTempLogFilename(RedisRaftCtx *rr)
{
    static char filename[PATH_MAX];

    if (!rr->config->raftlog) {
        return NULL;
    }

    snprintf(filename, sizeof(filename) - 1, "%s.templog", rr->config->raftlog);
    return filename;
}

const char *getTempDbFilename(RedisRaftCtx *rr)
{
    static char filename[PATH_MAX];

    if (!rr->config->raftlog) {
        return NULL;
    }

    snprintf(filename, sizeof(filename) - 1, "%s.temprdb", rr->config->raftlog);
    return filename;
}

#define SNAPSHOT_CHILD_MAGIC    0x70616e73  /* "snap" */
typedef struct SnapshotChildMsg {
    int magic;
    int success;
    long long int num_entries;
    char err[256];
} SnapshotChildMsg;

void cancelSnapshot(RedisRaftCtx *rr)
{
    assert(rr->snapshot_in_progress);

    raft_cancel_snapshot(rr->raft);
    rr->snapshot_in_progress = false;
}

RedisRaftResult finalizeSnapshot(RedisRaftCtx *rr)
{
    assert(rr->snapshot_in_progress);

    LOG_DEBUG("Finalizing snapshot.\n");

    /* If a persistent log is in use, we now have to append any new
     * entries to the temporary log and switch.
     */
    if (rr->config->persist) {
        long long int n = appendLogEntries(rr, getTempLogFilename(rr), rr->snapshot_rewrite_last_idx + 1);
        if (n < 0) {
            LOG_ERROR("Failed to append entries to rewritten log, aborting snapshot.\n");
            cancelSnapshot(rr);
            return -1;
        }

        LOG_INFO("Log rewrite complete, %lld entries appended (from idx %lu).\n", n,
                raft_get_snapshot_last_idx(rr->raft));

        RaftLog *new_log = RaftLogOpen(getTempLogFilename(rr));
        if (!new_log) {
            LOG_ERROR("Failed to open log after rewrite: %s\n", strerror(errno));
            cancelSnapshot(rr);
            return -1;
        }

        if (rename(getTempLogFilename(rr), rr->config->raftlog) < 0) {
            LOG_ERROR("Failed to switch logfiles (%s to %s): %s\n",
                    getTempLogFilename(rr), rr->config->raftlog, strerror(errno));
            RaftLogClose(new_log);
            cancelSnapshot(rr);
            return -1;
        }

        RaftLogClose(rr->log);
        rr->log = new_log;
    }

    raft_end_snapshot(rr->raft);
    rr->snapshot_in_progress = false;

    return RR_OK;
}

int pollSnapshotStatus(RedisRaftCtx *rr)
{
    SnapshotChildMsg msg;
    int ret = read(rr->snapshot_child_fd, &msg, sizeof(msg));
    if (ret == -1) {
        /* Not ready yet? */
        if (errno == EAGAIN) {
            return 0;
        }

        LOG_ERROR("Failed to read snapshot child status: %s\n", strerror(errno));
        goto exit;
    }

    if (msg.magic != SNAPSHOT_CHILD_MAGIC) {
        LOG_ERROR("Corrupted snapshot child status (magic=%08x)\n", msg.magic);
        goto exit;
    }

    if (!msg.success) {
        LOG_ERROR("Snapshot failed: %s", msg.err);
        goto exit;
    }

    LOG_INFO("Snapshot created, %lld log entries rewritten to log.\n", msg.num_entries);
    ret = 1;

exit:
    /* If this is a result of a RAFT.COMPACT request, we need to reply. */
    if (rr->compact_req) {
        if (ret == 1) {
            LOG_VERBOSE("RAFT.DEBUG COMPACT completed successfully.\n");
            RedisModule_ReplyWithSimpleString(rr->compact_req->ctx, "OK");
        } else {
            LOG_VERBOSE("RAFT.DEBUG COMPACT failed: %s\n", msg.err);
            RedisModule_ReplyWithError(rr->compact_req->ctx, msg.err);
        }
        RaftReqFree(rr->compact_req);
        rr->compact_req = NULL;
    }

    close(rr->snapshot_child_fd);
    rr->snapshot_child_fd = -1;

    return ret;
}

/* Create a snapshot.
 *
 * 1. raft_begin_snapshot() determines which part of the log can be compacted
 *    and applies any unapplied entry.
 * 2. storeSnapshotInfo() updates the metadata which is part of the snapshot.
 * 3. raft_end_snapshot() does the actual compaction of the log.
 *
 * TODO: We currently don't properly deal with snapshot persistence.  We need to either
 * (a) BGSAVE; or (b) make sure we're covered by AOF.  In the case of RDB, a better
 * approach may be to trigger snapshot generation on BGSAVE, but it requires better
 * synchronization so we can determine how far the log should be compacted.
 */

RedisRaftResult initiateSnapshot(RedisRaftCtx *rr)
{
    if (rr->snapshot_in_progress) {
       return RR_ERROR;
    }

    LOG_DEBUG("Initiating snapshot%s.\n", rr->compact_req ? ", trigerred by COMPACT" : "");

    if (raft_begin_snapshot(rr->raft) < 0) {
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

    /* If we are not persistent we're basically done.  The raft_end_snapshot() call will
     * take care of removing log entries that have been applied.
     */
    if (!rr->config->persist) {
        return finalizeSnapshot(rr);
    }

    /* Persistence is enabled, so we need to initiate a background process which will:
     * 1. Create an RDB file that serves as a persistence snapshot.
     * 2. Create a new temporary log with old entries removed.
     * 3. Notify us back when it's done, so we can append any new log entries received
     *    since and rotate.
     */

    int snapshot_fds[2];    /* [0] our side, [1] child's side */
    if (pipe2(snapshot_fds, O_NONBLOCK) < 0) {
        LOG_ERROR("Failed to create snapshot child pipe: %s\n", strerror(errno));
        cancelSnapshot(rr);
        return RR_ERROR;
    }

    /* Flush stdio files to avoid leaks from child */
    fflush(redis_raft_logfile);
    RaftLogSync(rr->log);

    rr->snapshot_child_fd = snapshot_fds[0];
    pid_t child = fork();
    if (child < 0) {
        LOG_ERROR("Failed to fork snapshot child: %s\n", strerror(errno));
        cancelSnapshot(rr);
        return RR_ERROR;
    } else if (!child) {
        /* Report result */
        SnapshotChildMsg msg = {
            .magic = SNAPSHOT_CHILD_MAGIC,
            .success = 0,
        };

        redis_raft_logfile = NULL;

        /* Handle compact delay, used for strictly as a debugging tool for testing */
        if (rr->compact_req && rr->config->compact_delay) {
            sleep(rr->config->compact_delay);
        }

        RedisModuleCallReply *reply;
#if 0
        /* Configure rdb filename */
        RedisModuleCallReply *reply = RedisModule_Call(rr->ctx, "CONFIG", "ccc", "SET", "dbfilename", getTempDbFilename(rr));
        if (!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
            snprintf(msg.err, sizeof(msg.err) - 1, "%s", "CONFIG SET dbfilename failed");
            goto exit;
        }
        RedisModule_FreeCallReply(reply);
#endif

        /* Save */
        reply = RedisModule_Call(rr->ctx, "SAVE", "");
        if (!reply || RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_STRING) {
            snprintf(msg.err, sizeof(msg.err) - 1, "%s", "SAVE failed");
            goto exit;
        }

        size_t len;
        const char *s = RedisModule_CallReplyStringPtr(reply, &len);
        if (len != 2 && memcmp(s, "OK", 2)) {
            snprintf(msg.err, sizeof(msg.err) - 1, "SAVE failed: %.*s", (int) len, s);
            goto exit;
        }
        RedisModule_FreeCallReply(reply);

        /* Now create a compact log file */
        msg.num_entries = rewriteLog(rr, getTempLogFilename(rr));
        if (msg.num_entries < 0) {
            snprintf(msg.err, sizeof(msg.err) - 1, "%s", "Log rewrite failed");
            goto exit;
        }
        msg.success = 1;

exit:
        write(snapshot_fds[1], &msg, sizeof(msg));

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
        /* Skip myself */
        if (cfg->id == raft_get_nodeid(rr->raft)) {
            continue;
        }

        /* Set up new node */
        raft_node_t *rn;
        Node *n = NodeInit(cfg->id, &cfg->addr);
        if (cfg->voting) {
            rn = raft_add_node(rr->raft, n, cfg->id, 0);
        } else {
            rn = raft_add_non_voting_node(rr->raft, n, cfg->id, 0);
        }

        assert(rn != NULL);
        raft_node_set_active(rn, cfg->active);
        cfg = cfg->next;
    }

}

void configRaftFromSnapshotInfo(RedisRaftCtx *rr)
{
    /* Load node configuration */
    removeAllNodes(rr);
    loadSnapshotNodes(rr, rr->snapshot_info.cfg);

    LOG_INFO("Snapshot configuration loaded. Raft state:\n");
    int i;
    for (i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *rnode = raft_get_node_from_idx(rr->raft, i);
        Node *node = raft_node_get_udata(rnode);

        if (!node) {
            LOG_INFO("  node <unknown?>\n", i);
        } else {
            LOG_INFO("  node id=%d,addr=%s,port=%d\n",
                    node->id, node->addr.host, node->addr.port);
        }
    }
}

/* After a snapshot is received (becomes the Redis dataset), load it into the Raft
 * library:
 * 1. Configure index/term/etc.
 * 2. Reconfigure nodes based on the snapshot metadata configuration.
 */
static void loadSnapshot(RedisRaftCtx *rr)
{
    if (!rr->snapshot_info.loaded) {
        LOG_ERROR("No snapshot metadata received, aborting.\n");
        return;
    }

    LOG_INFO("Begining snapshot load, term=%lu, last_included_index=%lu\n",
            rr->snapshot_info.last_applied_term,
            rr->snapshot_info.last_applied_idx);

    int ret;
    if ((ret = raft_begin_load_snapshot(rr->raft, rr->snapshot_info.last_applied_term,
                rr->snapshot_info.last_applied_idx)) != 0) {
        LOG_ERROR("Cannot load snapshot: already loaded?\n");
        return;
    }

    configRaftFromSnapshotInfo(rr);

    raft_end_load_snapshot(rr->raft);
}

/* Monitor Redis Replication progress when loading a snapshot.  If completed,
 * reconfigure Raft with the metadata from the new snapshot.
 */
void checkLoadSnapshotProgress(RedisRaftCtx *rr)
{
    RedisModule_ThreadSafeContextLock(rr->ctx);
    RedisModuleCallReply *reply = RedisModule_Call(rr->ctx, "INFO", "c", "replication");
    RedisModule_ThreadSafeContextUnlock(rr->ctx);
    assert(reply != NULL);

    size_t info_len;
    const char *info = RedisModule_CallReplyProto(reply, &info_len);
    const char *key, *val;
    size_t keylen, vallen;
    int ret;

    static const char _master_link_status[] = "master_link_status";
    static const char _master_sync_in_progress[] = "master_sync_in_progress";

    bool link_status_up = false;
    bool sync_in_progress = true;

    while ((ret = RedisInfoIterate(&info, &info_len, &key, &keylen, &val, &vallen))) {
        if (ret == -1) {
            LOG_ERROR("Failed to parse INFO reply");
            goto exit;
        }

        if (keylen == sizeof(_master_link_status)-1 &&
                !memcmp(_master_link_status, key, keylen) &&
            vallen == 2 && !memcmp(val, "up", 2)) {
            link_status_up = true;
        } else if (keylen == sizeof(_master_sync_in_progress)-1 &&
                !memcmp(_master_sync_in_progress, key, keylen) &&
                vallen == 1 && *val == '0') {
            sync_in_progress = false;
        }
    }


exit:
    RedisModule_FreeCallReply(reply);

    if (link_status_up && !sync_in_progress) {
        RedisModule_ThreadSafeContextLock(rr->ctx);
        reply = RedisModule_Call(rr->ctx, "SLAVEOF", "cc", "NO", "ONE");
        RedisModule_ThreadSafeContextUnlock(rr->ctx);
        assert(reply != NULL);

        RedisModule_FreeCallReply(reply);

        loadSnapshot(rr);
        rr->loading_snapshot = false;
    }
}

void handleLoadSnapshot(RedisRaftCtx *rr, RaftReq *req)
{
    RedisModule_ThreadSafeContextLock(rr->ctx);
    RedisModuleCallReply *reply = RedisModule_Call(
            rr->ctx, "SLAVEOF", "cl",
            req->r.loadsnapshot.addr.host,
            (long long) req->r.loadsnapshot.addr.port);
    RedisModule_ThreadSafeContextUnlock(rr->ctx);

    if (!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        /* No errors because we don't use a blocking client for this type
         * of requests.
         */
    } else {
        rr->snapshot_info.loaded = false;
        rr->loading_snapshot = true;
    }

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

    if (!rr->config->persist) {
        rr->compact_req = NULL;
        RedisModule_ReplyWithSimpleString(req->ctx, "OK");
        RaftReqFree(req);
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
    RaftSnapshotInfo *info = &redis_raft.snapshot_info;

    /* Load term/index */
    info->last_applied_term = RedisModule_LoadUnsigned(rdb);
    info->last_applied_idx = RedisModule_LoadUnsigned(rdb);

    /* Load configuration */
    freeSnapshotCfgEntryList(info->cfg);
    info->cfg = NULL;
    SnapshotCfgEntry **ep = &info->cfg;

    do {
        unsigned long _id = RedisModule_LoadUnsigned(rdb);
        char *buf;
        size_t len;

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


RedisModuleTypeMethods RedisRaftTypeMethods = {
    .version = 1,
    .rdb_load = rdbLoadSnapshotInfo,
    .rdb_save = rdbSaveSnapshotInfo,
};

