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

        NodeAddr *na = NULL;
        if (raft_node_get_id(rnode) == raft_get_nodeid(rr->raft)) {
            na = &rr->config->addr;
        } else if (node != NULL) {
            na = &node->addr;
        } else {
            assert(0);
        }

        buf = catsnprintf(buf, &buf_len, "%s%u,%u,%u,%u,%s:%u",
                buf[0] == '\0' ? "" : ";",
                raft_node_get_id(rnode),
                raft_node_is_active(rnode),
                raft_node_is_voting_committed(rnode),
                raft_node_is_addition_committed(rnode),
                na->host,
                na->port);
    }

    return buf;
}


static void storeSnapshotInfo(RedisRaftCtx *rr)
{
    /* Persist data into the snapshot.  This will move out of the keyspace when
     * Redis Module API permits that. */
    char *cfg = generateCfgString(rr);

    RedisModule_ThreadSafeContextLock(rr->ctx);
    RedisModuleCallReply *r = RedisModule_Call(rr->ctx, "HMSET", "cclclcc",
            "__raft_snapshot__",
            "last_included_term",
            raft_get_current_term(rr->raft),
            "last_included_index",
            raft_get_current_idx(rr->raft),
            "cfg", cfg
            );
    RedisModule_ThreadSafeContextUnlock(rr->ctx);
    assert(r != NULL);

    RedisModule_Free(cfg);
}

/* ------------------------------------ Generate snapshots ------------------------------------ */

void performSnapshot(RedisRaftCtx *rr)
{
    if (raft_begin_snapshot(rr->raft) < 0) {
        return;
    }

    storeSnapshotInfo(rr);
    raft_end_snapshot(rr->raft);
}

/* ------------------------------------ Load snapshots ------------------------------------ */

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
        rr->loading_snapshot = false;

        RedisModule_ThreadSafeContextLock(rr->ctx);
        reply = RedisModule_Call(rr->ctx, "SLAVEOF", "cc", "NO", "ONE");
        RedisModule_ThreadSafeContextUnlock(rr->ctx);
        assert(reply != NULL);

        RedisModule_FreeCallReply(reply);
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
        RedisModule_ReplyWithError(req->ctx, "ERR failed to initiate loading");
        RedisModule_UnblockClient(req->client, NULL);
    } else {
        rr->loading_snapshot = true;
    }

    return REDISMODULE_OK;
}


