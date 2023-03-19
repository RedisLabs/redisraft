/*
 * Copyright Redis Ltd. 2023 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"

#include <string.h>

/* A list of all current blocked commands */

BlockedCommand *addBlockedCommand(raft_index_t idx, raft_session_t session, const char *data, size_t data_len, RaftReq *req, RedisModuleCallReply *reply)
{
    BlockedCommand *bc = RedisModule_Calloc(1, sizeof(BlockedCommand));
    sc_list_init(&bc->blocked_list);
    sc_list_add_tail(&redis_raft.blocked_command_list, &bc->blocked_list);

    bc->idx = idx;
    bc->session = session;
    bc->data = RedisModule_Alloc(data_len);
    memcpy(bc->data, data, data_len);
    bc->data_len = data_len;
    bc->req = req;
    bc->reply = reply;

    RedisModule_DictSetC(redis_raft.blocked_command_dict, &bc->idx, sizeof(bc->idx), bc);

    return bc;
}

void deleteBlockedCommandFromLinkMap(raft_index_t idx)
{
    BlockedCommand *blocked = NULL;
    RedisModule_DictDelC(redis_raft.blocked_command_dict, &idx, sizeof(idx), &blocked);
    if (blocked == NULL) {
        return;
    }

    sc_list_del(&redis_raft.blocked_command_list, &blocked->blocked_list);
}

BlockedCommand *getBlockedCommand(raft_index_t idx)
{
    return RedisModule_DictGetC(redis_raft.blocked_command_dict, &idx, sizeof(idx), NULL);
}

void freeBlockedCommand(BlockedCommand *bc)
{
    if (bc->data) {
        RedisModule_Free(bc->data);
        bc->data = NULL;
    }
    if (bc->reply) {
        RedisModule_FreeCallReply(bc->reply);
        bc->reply = NULL;
    }
    RedisModule_Free(bc);
}

void clearAllBlockCommands()
{
    struct sc_list *elem;
    while ((elem = sc_list_pop_head(&redis_raft.blocked_command_list)) != NULL) {
        BlockedCommand *bc = sc_list_entry(elem, BlockedCommand, blocked_list);
        if (RedisModule_CallReplyPromiseAbort(bc->reply, NULL) != REDISMODULE_OK) {
            /* shouldn't happen with normal redis commands */
            LOG_WARNING("timeoutBlockedCommand: failed to abort %lu", bc->idx);
            return;
        }
        freeBlockedCommand(bc);
    }
    if (redis_raft.blocked_command_dict != NULL) {
        RedisModule_FreeDict(redis_raft.ctx, redis_raft.blocked_command_dict);
    }
    redis_raft.blocked_command_dict = RedisModule_CreateDict(redis_raft.ctx);
}

void blockedCommandsSave(RedisModuleIO *rdb)
{
    RaftSnapshotInfo *info = &redis_raft.snapshot_info;
    struct sc_list *it;
    int count = 0;

    sc_list_foreach (&redis_raft.blocked_command_list, it) {
        BlockedCommand *bc = sc_list_entry(it, BlockedCommand, blocked_list);
        if (bc->idx <= info->last_applied_idx) {
            count++;
        } else {
            break;
        }
    }

    RedisModule_SaveUnsigned(rdb, count);
    sc_list_foreach (&redis_raft.blocked_command_list, it) {
        BlockedCommand *bc = sc_list_entry(it, BlockedCommand, blocked_list);
        if (bc->idx <= info->last_applied_idx) {
            RedisModule_SaveUnsigned(rdb, bc->idx);
            RedisModule_SaveUnsigned(rdb, bc->session);
            RedisModule_SaveStringBuffer(rdb, bc->data, bc->data_len);
        } else {
            break;
        }
    }
}

void blockedCommandsLoad(RedisModuleIO *rdb)
{
    RedisRaftCtx *rr = &redis_raft;

    if (rr->blocked_command_dict) {
        RedisModule_FreeDict(rr->ctx, rr->blocked_command_dict);
    }
    rr->blocked_command_dict = RedisModule_CreateDict(rr->ctx);
    struct sc_list *elem;
    while ((elem = sc_list_pop_head(&rr->blocked_command_list)) != NULL) {
        RedisModule_Free(sc_list_entry(elem, BlockedCommand, blocked_list));
    }

    size_t command_count = RedisModule_LoadUnsigned(rdb);
    for (size_t i = 0; i < command_count; i++) {
        BlockedCommand *bc = RedisModule_Calloc(1, sizeof(BlockedCommand));
        sc_list_init(&bc->blocked_list);
        sc_list_add_tail(&redis_raft.blocked_command_list, &bc->blocked_list);

        bc->idx = RedisModule_LoadUnsigned(rdb);
        bc->session = RedisModule_LoadUnsigned(rdb);
        bc->data = RedisModule_LoadStringBuffer(rdb, &bc->data_len);

        RedisModule_DictSetC(redis_raft.blocked_command_dict, &bc->idx, sizeof(bc->idx), bc);

        /* execute blocking command again */
        RaftRedisCommandArray tmp = {0};
        if (RaftRedisCommandArrayDeserialize(&tmp,
                                             bc->data,
                                             bc->data_len) != RR_OK) {
            PANIC("Invalid Raft entry");
        }
        tmp.client_id = bc->session;
        bc->reply = RaftExecuteCommandArray(rr, NULL, &tmp);
        RedisModule_Assert(bc->reply != NULL);
        RedisModule_CallReplyPromiseSetUnblockHandler(bc->reply, handleUnblock, bc);
        RaftRedisCommandArrayFree(&tmp);
    }
}
