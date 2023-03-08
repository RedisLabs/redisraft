/*
 * Copyright Redis Ltd. 2023 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <string.h>

#include "redisraft.h"
#include "sc_list.h"

/* A list of all current blocked commands */

BlockedCommand *addBlockedCommand(raft_index_t idx, unsigned long long session, const char *data, size_t data_len, RaftReq *req, RedisModuleCallReply *reply)
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

    RedisModule_DictSetC(redis_raft.blocked_command_dict, &bc->idx, sizeof(bc->idx), bc);

    return bc;
}

void freeBlockedCommand(BlockedCommand *bc)
{
    if (bc->data) {
        RedisModule_Free(bc->data);
        bc->data = NULL;
    }
    RedisModule_Free(bc);
}

BlockedCommand *getAndDeleteBlockedCommand(raft_index_t idx)
{
    BlockedCommand *blocked = NULL;
    RedisModule_DictDelC(redis_raft.blocked_command_dict, &idx, sizeof(idx), &blocked);
    if (blocked == NULL) {
        return NULL;
    }

    sc_list_del(&redis_raft.blocked_command_list, &blocked->blocked_list);

    return blocked;
}

int timeoutBlockedCommand(raft_index_t idx)
{
    BlockedCommand *bc = getAndDeleteBlockedCommand(idx);
    if (bc == NULL) {
        return 0;
    }
    /* TODO: abort bc->reply */
    freeBlockedCommand(bc);
    return 0;
}

void blockedCommandsSave(RedisModuleIO *rdb)
{
    RaftSnapshotInfo *info = &redis_raft.snapshot_info;
    struct sc_list *it;
    int count = 0;

    sc_list_foreach (&redis_raft.blocked_command_list, it)
    {
        BlockedCommand *bc = sc_list_entry(it, BlockedCommand, blocked_list);
        if (bc->idx <= info->last_applied_idx) {
            count++;
        } else {
            break;
        }
    }

    RedisModule_SaveUnsigned(rdb, count);
    sc_list_foreach (&redis_raft.blocked_command_list, it)
    {
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
        bc->reply = RaftExecuteCommandArray(rr, rr->ctx, NULL, &tmp);
        RedisModule_Assert(bc->reply != NULL);
        RedisModule_CallReplyPromiseSetUnblockHandler(bc->reply, handleUnblock, bc);
        RaftRedisCommandArrayFree(&tmp);
    }
}
