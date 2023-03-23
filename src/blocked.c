/*
 * Copyright Redis Ltd. 2023 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"

#include <math.h>
#include <string.h>
#include <strings.h>

/* A list of all current blocked commands */

BlockedCommand *allocBlockedCommand(const char *cmd_name, raft_index_t idx, raft_session_t session, const char *data, size_t data_len, RaftReq *req, RedisModuleCallReply *reply)
{
    BlockedCommand *bc = RedisModule_Calloc(1, sizeof(BlockedCommand));
    bc->command = RedisModule_Strdup(cmd_name);
    bc->idx = idx;
    bc->session = session;
    bc->data = RedisModule_Alloc(data_len);
    memcpy(bc->data, data, data_len);
    bc->data_len = data_len;
    bc->req = req;
    bc->reply = reply;

    sc_list_init(&bc->blocked_list);

    return bc;
}

void addBlockedCommand(BlockedCommand *bc)
{
    sc_list_add_tail(&redis_raft.blocked_command_list, &bc->blocked_list);
    RedisModule_DictSetC(redis_raft.blocked_command_dict, &bc->idx, sizeof(bc->idx), bc);
}

void deleteBlockedCommand(raft_index_t idx)
{
    BlockedCommand *blocked = NULL;

    RedisModule_DictDelC(redis_raft.blocked_command_dict, &idx, sizeof(idx), &blocked);
    if (blocked == NULL) {
        return;
    }

    sc_list_del(&redis_raft.blocked_command_list, &blocked->blocked_list);
}

void freeBlockedCommand(BlockedCommand *bc)
{
    if (bc->reply) {
        RedisModule_FreeCallReply(bc->reply);
    }

    RedisModule_Free(bc->command);
    RedisModule_Free(bc->data);
    RedisModule_Free(bc);
}

BlockedCommand *getBlockedCommand(raft_index_t idx)
{
    return RedisModule_DictGetC(redis_raft.blocked_command_dict, &idx, sizeof(idx), NULL);
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

    clearAllBlockCommands();

    size_t command_count = RedisModule_LoadUnsigned(rdb);
    for (size_t i = 0; i < command_count; i++) {
        raft_index_t idx = RedisModule_LoadUnsigned(rdb);
        raft_session_t session = RedisModule_LoadUnsigned(rdb);
        size_t data_len;
        char *data = RedisModule_LoadStringBuffer(rdb, &data_len);

        /* execute blocking command again */
        RaftRedisCommandArray tmp = {
            .client_id = session,
        };
        if (RaftRedisCommandArrayDeserialize(&tmp, data, data_len) != RR_OK) {
            PANIC("Invalid Raft entry");
        }
        RedisModuleCallReply *reply = RaftExecuteCommandArray(rr, NULL, &tmp);
        RedisModule_Assert(reply != NULL);
        RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_PROMISE);

        /* save blocked command info */
        size_t cmdstr_len;
        const char *cmdstr = RedisModule_StringPtrLen(tmp.commands[0]->argv[0], &cmdstr_len);
        BlockedCommand *bc = allocBlockedCommand(cmdstr, idx, session, data, data_len, NULL, reply);
        addBlockedCommand(bc);

        /* setup handler */
        RedisModule_CallReplyPromiseSetUnblockHandler(reply, handleUnblock, bc);
        RedisModule_Free(data);
        RaftRedisCommandArrayFree(&tmp);
    }
}

static int findTimeoutIndex(RaftRedisCommand *cmd)
{
    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[0], &cmd_len);

    if (strncasecmp(cmd_str, "brpop", 5) == 0 ||
        strncasecmp(cmd_str, "blpop", 5) == 0 ||
        strncasecmp(cmd_str, "bzpopmin", 8) == 0 ||
        strncasecmp(cmd_str, "bzpopmax", 8) == 0 ||
        strncasecmp(cmd_str, "blmove", 6) == 0 ||
        strncasecmp(cmd_str, "brpoplpush", 10) == 0) {
        return cmd->argc - 1;
    } else if (strncasecmp(cmd_str, "blmpop", 6) == 0 ||
               strncasecmp(cmd_str, "bzmpop", 6) == 0) {
        return 1;
    } else {
        return -1;
    }
}

int extractBlockingTimeout(RedisModuleCtx *ctx, RaftRedisCommandArray *cmds, long long *timeout)
{
    long double tmp;
    RedisModule_Assert(cmds->len == 1);

    RaftRedisCommand *cmd = cmds->commands[0];
    if (cmd->argc < 2) {
        RedisModule_ReplyWithError(ctx, "ERR timeout is not a float or out of range");
        return RR_ERROR;
    }

    int index = findTimeoutIndex(cmd);
    if (index == -1) {
        RedisModule_ReplyWithError(ctx, "ERR timeout is not a float or out of range");
        return RR_ERROR;
    }

    if (RedisModule_StringToLongDouble(cmd->argv[index], &tmp) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "ERR timeout is not a float or out of range");
        return RR_ERROR;
    }

    tmp *= 1000.0;
    if (tmp > LLONG_MAX) {
        RedisModule_ReplyWithError(ctx, "ERR timeout is out of range");
        return RR_ERROR;
    }

    *timeout = (long long) ceill(tmp);

    if (*timeout < 0) {
        RedisModule_ReplyWithError(ctx, "ERR timeout is negative");
        return RR_ERROR;
    }

    if (*timeout > (LLONG_MAX - RedisModule_Milliseconds())) {
        RedisModule_ReplyWithError(ctx, "ERR timeout is out of range");
        return RR_ERROR;
    }

    return RR_OK;
}

static RedisModuleString *zero = NULL;

void replaceBlockingTimeout(RaftRedisCommandArray *cmds)
{
    RedisModule_Assert(cmds->len == 1);

    RaftRedisCommand *cmd = cmds->commands[0];
    RedisModule_Assert(cmd->argc >= 2);

    int index = findTimeoutIndex(cmd);
    RedisModule_Assert(index != -1);

    if (zero == NULL) {
        zero = RedisModule_CreateString(NULL, "0", 1);
    }

    RedisModule_FreeString(NULL, cmd->argv[index]);
    cmd->argv[index] = RedisModule_HoldString(NULL, zero);
}
