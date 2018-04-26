#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "redisraft.h"

/* ------------------------------------ Snapshot metadata ------------------------------------ */

static char *generateCfgString(RedisRaftCtx *rr)
{
    size_t buf_len = 1024;
    char *buf = RedisModule_Alloc(buf_len);
    int i;

    buf[0] = '\0';
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

        buf = catsnprintf(buf, &buf_len, "%s%u,%u,%u,%s:%u",
                buf[0] == '\0' ? "" : ";",
                raft_node_get_id(rnode),
                raft_node_is_active(rnode),
                raft_node_is_voting_committed(rnode),
                na->host,
                na->port);
    }

    return buf;
}

static const char __raft_snapshot[] = "__raft_snapshot__";
static const char __last_included_term[] = "last_included_term";
static const char __last_included_index[] = "last_included_index";
static const char __cfg[] = "cfg";

static void storeSnapshotInfo(RedisRaftCtx *rr)
{
    /* Persist data into the snapshot.  This will move out of the keyspace when
     * Redis Module API permits that. */
    char *cfg = generateCfgString(rr);
    unsigned int term = raft_get_current_term(rr->raft);
    unsigned int index = raft_get_last_applied_idx(rr->raft);

    LOG_DEBUG("storeSnapshotInfo: last included term %u, index %u\n", term, index);

    RedisModule_ThreadSafeContextLock(rr->ctx);
    RedisModuleCallReply *r = RedisModule_Call(rr->ctx, "HMSET", "cclclcc",
            __raft_snapshot,
            __last_included_term,
            term,
            __last_included_index,
            index,
            __cfg, cfg
            );
    RedisModule_ThreadSafeContextUnlock(rr->ctx);
    assert(r != NULL);

    RedisModule_FreeCallReply(r);
    RedisModule_Free(cfg);
}

typedef struct SnapshotCfgEntry {
    uint32_t    id;
    int         active;
    int         voting;
    NodeAddr    addr;
    struct SnapshotCfgEntry *next;
} SnapshotCfgEntry;

typedef struct SnapshotMetadata {
    uint32_t    last_included_term;
    uint32_t    last_included_index;
    SnapshotCfgEntry *cfg_entry;
} SnapshotMetadata;

static void freeSnapshotCfgEntryList(SnapshotCfgEntry *head)
{
    while (head != NULL) {
        SnapshotCfgEntry *next = head->next;
        RedisModule_Free(head);
        head = next;
    }
}


static SnapshotCfgEntry *parseCfgString(char *str)
{
    SnapshotCfgEntry *result = NULL;
    SnapshotCfgEntry **prev = &result;

    char *tmp;
    char *p = strtok_r(str, ";", &tmp);
    while (p != NULL) {
        SnapshotCfgEntry *r = RedisModule_Calloc(1, sizeof(SnapshotCfgEntry));
        *prev = r;
        char *addr;

        if (sscanf(p, "%u,%u,%u,%ms", &r->id, &r->active, &r->voting, &addr) != 4) {
            freeSnapshotCfgEntryList(result);
            return NULL;
        }

        bool parsed = NodeAddrParse(addr, strlen(addr), &r->addr);
        free(addr);

        if (!parsed) {
            freeSnapshotCfgEntryList(result);
            return NULL;
        }

        prev = &r->next;
        p = strtok_r(NULL, ";", &tmp);
    }

    return result;
}

static RedisRaftResult loadSnapshotInfo(RedisRaftCtx *rr, SnapshotMetadata *result)
{
    RedisRaftResult ret = RR_OK;

    RedisModule_ThreadSafeContextLock(rr->ctx);
    RedisModuleCallReply *r = RedisModule_Call(rr->ctx, "HGETALL", "c", __raft_snapshot);
    RedisModule_ThreadSafeContextUnlock(rr->ctx);

    assert(r != NULL);
    if (RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ARRAY) {
        ret = RR_ERROR;
        LOG_ERROR("loadSnapshotInfo failed, corrupt snapshot metadata\n");
        goto exit;
    }

    int i = 0;
    for (i = 0; i < RedisModule_CallReplyLength(r); i++) {
        RedisModuleCallReply *name = RedisModule_CallReplyArrayElement(r, i);
        RedisModuleCallReply *value = RedisModule_CallReplyArrayElement(r, ++i);

        size_t namelen;
        const char *namestr = RedisModule_CallReplyStringPtr(name, &namelen);

        size_t valuelen;
        const char *valuestr = RedisModule_CallReplyStringPtr(value, &valuelen);

        char namebuf[namelen + 1];
        memcpy(namebuf, namestr, namelen);
        namebuf[namelen] = '\0';

        char valuebuf[valuelen + 1];
        memcpy(valuebuf, valuestr, valuelen);
        valuebuf[valuelen] = '\0';

        char *endptr;

        if (!strcmp(__last_included_term, namebuf)) {
            result->last_included_term = strtoul(valuebuf, &endptr, 10);
            if (*endptr) {
                LOG_ERROR("Invalid last_included_term value\n");
                goto exit;
            }
        } else if (!strcmp(__last_included_index, namebuf)) {
            result->last_included_index = strtoul(valuebuf, &endptr, 10);
            if (*endptr) {
                LOG_ERROR("Invalid last_included_index value\n");
                goto exit;
            }
        } else if (namelen == strlen(__cfg) && !memcmp(namestr, __cfg, namelen)) {
            result->cfg_entry = parseCfgString(valuebuf);
            if (!result->cfg_entry) {
                ret = RR_ERROR;
                LOG_ERROR("Invalid cfg value\n");
                goto exit;
            }
        }
    }

exit:
    RedisModule_FreeCallReply(r);
    return ret;
}

/* ------------------------------------ Generate snapshots ------------------------------------ */

RedisRaftResult performSnapshot(RedisRaftCtx *rr)
{
    if (raft_begin_snapshot(rr->raft) < 0) {
        return RR_ERROR;
    }

    /* TODO: Persistence */
    storeSnapshotInfo(rr);
    raft_end_snapshot(rr->raft);

    return RR_OK;
}

/* ------------------------------------ Load snapshots ------------------------------------ */

static void loadSnapshot(RedisRaftCtx *rr)
{
    SnapshotMetadata metadata = { 0 };
    if (loadSnapshotInfo(rr, &metadata) != RR_OK ||
        !metadata.cfg_entry) {
            LOG_ERROR("Failed to load snapshot metadata, aborting.\n");
            return;
    }

    LOG_INFO("Begining snapshot load, term=%u, last_included_index=%u\n",
            metadata.last_included_term,
            metadata.last_included_index);

    int ret;
    if ((ret = raft_begin_load_snapshot(rr->raft, metadata.last_included_term,
                metadata.last_included_index)) != 0) {
        LOG_ERROR("Cannot load snapshot: already loaded?\n");
        goto exit;
    }

    SnapshotCfgEntry *cfg = metadata.cfg_entry;
    while (cfg != NULL) {
        if (cfg->id == raft_get_nodeid(rr->raft)) {
            continue;
        }

        /* TODO -- handle deletion properly here */
        raft_node_t *rn = raft_get_node(rr->raft, cfg->id);
        if (rn != NULL) {
            raft_remove_node(rr->raft, rn);
        }

        Node *n = NodeInit(cfg->id, &cfg->addr);
        if (cfg->voting) {
            rn = raft_add_node(rr->raft, n, cfg->id, 0);
        } else {
            rn = raft_add_non_voting_node(rr->raft, n, cfg->id, 0);
        }

        raft_node_set_active(rn, cfg->active);

        cfg = cfg->next;
    }

    raft_end_load_snapshot(rr->raft);

exit:
    freeSnapshotCfgEntryList(metadata.cfg_entry);
}

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

int handleLoadSnapshot(RedisRaftCtx *rr, RaftReq *req)
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
        rr->loading_snapshot = true;
    }

    if (reply) {
        RedisModule_FreeCallReply(reply);
    }

    return REDISMODULE_OK;
}

int handleCompact(RedisRaftCtx *rr, RaftReq *req)
{
    if (performSnapshot(rr) != RR_OK) {
        LOG_VERBOSE("RAFT.DEBUG COMPACT requested but failed.\n");
        RedisModule_ReplyWithError(req->ctx, "ERR operation failed, nothing to compact?");
    } else {
        LOG_VERBOSE("RAFT.DEBUG COMPACT completed successfully, index=%d, committed=%d, entries=%d\n",
                raft_get_current_idx(rr->raft),
                raft_get_commit_idx(rr->raft),
                raft_get_log_count(rr->raft));
        RedisModule_ReplyWithSimpleString(req->ctx, "OK");
    }

    RedisModule_UnblockClient(req->client, NULL);
    return REDISMODULE_OK;
}
