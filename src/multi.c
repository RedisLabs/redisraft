/* Handle MULTI/EXEC transactions here.
 *
 * If this logic was applied, the return value is true, indicating no further
 * processing is required.
 *
 * 1) On MULTI, we create a RaftRedisCommandArray which will store all
 *    user commands as they are queued.
 * 2) On EXEC, we remove the RaftRedisCommandArray with all queued commands
 *    from multi_client_state, place it in the RaftReq and let the rest of the
 *    code handle it.
 * 3) On DISCARD we simply remove the queued commands array.
 *
 * Important notes:
 * 1) Although as a module we don't need to pass MULTI to Redis, we still keep
 *    it in the array, because when processing the array we want to distinguish
 *    between a MULTI with a single command and a non-MULTI scenario.
 * 2) If our RaftReq contains multiple commands, we assume it was received as
 *    a RAFT.ENTRY in which case we need to process it as an EXEC.  That means
 *    we don't need to reply with +OK and multiple +QUEUED, but just process
 *    the commands atomically.  This is common when a follower proxies a batch
 *    of commands to a leader: the follower handles the user interaction and
 *    the leader only handles the execution (when the user issued the final
 *    EXEC).
 *
 * Error handling rules (derived from Redis):
 * 1) MULTI and DISCARD should always succeed.
 * 2) If we encounter errors inside a MULTI context, we need to flag that
 *    transaction as failed but keep going until EXEC/DISCARD.
 * 3) RAFT related state checks can be postponed and evaluated only at the
 *    time of EXEC.
 */

#include <strings.h>

#include "redisraft.h"


typedef struct MultiState {
    RaftRedisCommandArray cmds;
    bool error;
} MultiState;

void MultiInitClientState(RedisRaftCtx *rr)
{
    rr->multi_client_state = RedisModule_CreateDict(NULL);
}

uint64_t MultiClientStateCount(RedisRaftCtx *rr)
{
    return RedisModule_DictSize(rr->multi_client_state);
}

void MultiFreeClientState(RedisRaftCtx *rr, unsigned long long client_id)
{
    MultiState *state = NULL;

    if (RedisModule_DictDelC(rr->multi_client_state, &client_id,
                             sizeof(client_id), &state) == REDISMODULE_OK) {
        if (state) {
            RaftRedisCommandArrayFree(&state->cmds);
            RedisModule_Free(state);
        }
    }
}

bool MultiHandleCommand(RedisRaftCtx *rr,
                        RedisModuleCtx *ctx, RaftRedisCommandArray *cmds)
{
    unsigned long long client_id;
    MultiState *multiState;

    /* MULTI/EXEC bundling takes place only if we have a single command. If we
     * have multiple commands we've received this as a RAFT.ENTRY input and
     * bundling, probably through a proxy, and bundling was done before.
     */
    if (cmds->len != 1) {
        return false;
    }

    client_id = RedisModule_GetClientId(ctx);
    multiState = RedisModule_DictGetC(rr->multi_client_state,
                                      &client_id, sizeof(client_id), NULL);

    /* Is this a MULTI command? */
    RaftRedisCommand *cmd = cmds->commands[0];
    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[0], &cmd_len);

    if (cmd_len == 5 && !strncasecmp(cmd_str, "MULTI", 5)) {
        if (multiState) {
            RedisModule_ReplyWithError(ctx, "ERR MULTI calls can not be nested");
        } else {
            multiState = RedisModule_Calloc(sizeof(MultiState), 1);
            RedisModule_DictSetC(rr->multi_client_state, &client_id, sizeof(client_id), multiState);

            /* We put the MULTI as the first command in the array, as we still
             * need to distinguish single-MULTI array from a single command.
             */
            RaftRedisCommandArrayMove(&multiState->cmds, cmds);
            RedisModule_ReplyWithSimpleString(ctx, "OK");
        }

        return true;
    } else if (cmd_len == 4 && !strncasecmp(cmd_str, "EXEC", 4)) {
        if (!multiState) {
            RedisModule_ReplyWithError(ctx, "ERR EXEC without MULTI");
            return true;
        }

        if (multiState->error) {
            RedisModule_ReplyWithError(ctx, "EXECABORT Transaction discarded because of previous errors.");
            return true;
        }

        /* Just swap our commands with the EXEC command and proceed. */
        RaftRedisCommandArrayFree(cmds);
        RaftRedisCommandArrayMove(cmds, &multiState->cmds);
        MultiFreeClientState(rr, client_id);

        return false;
    } else if (cmd_len == 7 && !strncasecmp(cmd_str, "DISCARD", 7)) {
        if (!multiState) {
            RedisModule_ReplyWithError(ctx, "ERR DISCARD without MULTI");
            return true;
        }

        MultiFreeClientState(rr, client_id);
        RedisModule_ReplyWithSimpleString(ctx, "OK");

        return true;
    }

    /* Are we in MULTI? */
    if (multiState) {
        /* We have to detect commands that are unsupported or must not be
         * intercepted and reject the transaction.
         */
        unsigned int cmd_flags = CommandSpecGetAggregateFlags(cmds, 0);

        if (cmd_flags & CMD_SPEC_UNSUPPORTED) {
            RedisModule_ReplyWithError(ctx, "ERR not supported by RedisRaft");
            multiState->error = 1;
        } else if (cmd_flags & CMD_SPEC_DONT_INTERCEPT) {
            RedisModule_ReplyWithError(ctx, "ERR not supported by RedisRaft inside MULTI/EXEC");
            multiState->error = 1;
        } else {
            RaftRedisCommandArrayMove(&multiState->cmds, cmds);
            RedisModule_ReplyWithSimpleString(ctx, "QUEUED");
        }

        return true;
    }

    return false;
}
