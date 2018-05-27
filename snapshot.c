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

RedisRaftResult performSnapshot(RedisRaftCtx *rr)
{
    if (raft_begin_snapshot(rr->raft) < 0) {
        return RR_ERROR;
    }

    /* Create a snapshot of the nodes configuration */
    freeSnapshotCfgEntryList(rr->snapshot_info.cfg);
    rr->snapshot_info.cfg = generateSnapshotCfgEntryList(rr);

    raft_end_snapshot(rr->raft);

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

    /* Load node configuration */
    removeAllNodes(rr);
    loadSnapshotNodes(rr, rr->snapshot_info.cfg);

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
    if (performSnapshot(rr) != RR_OK) {
        LOG_VERBOSE("RAFT.DEBUG COMPACT requested but failed.\n");
        RedisModule_ReplyWithError(req->ctx, "ERR operation failed, nothing to compact?");
    } else {
        LOG_VERBOSE("RAFT.DEBUG COMPACT completed successfully, index=%ld, committed=%ld, entries=%ld\n",
                raft_get_current_idx(rr->raft),
                raft_get_commit_idx(rr->raft),
                raft_get_log_count(rr->raft));
        RedisModule_ReplyWithSimpleString(req->ctx, "OK");
    }

    RaftReqFree(req);
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

