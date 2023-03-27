/*
 * Copyright Redis Ltd. 2022 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

/* Handle MULTI/EXEC transactions here.
 *
 * We want to make sure that the commands are executed atomically across all
 * cluster nodes. To do this, we need to pack them as a single Raft log entry.
 *
 * If this logic was applied, the return value is true, indicating no further
 * processing is required.
 *
 * 1) On MULTI, we create a RaftRedisCommandArray which will store all
 *    user commands as they are queued.
 * 2) On EXEC, we move all queued commands from multi_client_state and place
 *    them in the command array and let the rest of the code handle it.
 * 3) On DISCARD we simply remove the queued commands array.
 *
 * Important notes:
 * 1) Although as a module we don't need to pass MULTI to Redis, we still keep
 *    it in the array, because when processing the array we want to distinguish
 *    between a MULTI with a single command and a non-MULTI scenario.
 * 2) If our command array contains multiple commands, we assume it was received
 *    as a RAFT.ENTRY in which case we need to process it as an EXEC. That means
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

#include "redisraft.h"

#include <strings.h>

bool MultiHandleCommand(RedisRaftCtx *rr,
                        RedisModuleCtx *ctx, RaftRedisCommandArray *cmds)
{
    ClientState *clientState = ClientStateGet(rr, ctx);
    MultiState *multiState = &clientState->multi_state;

    /* Is this a MULTI command? */
    RaftRedisCommand *cmd = cmds->commands[0];
    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[0], &cmd_len);

    if (cmd_len == 5 && !strncasecmp(cmd_str, "MULTI", 5)) {
        if (multiState->active) {
            RedisModule_ReplyWithError(ctx, "ERR MULTI calls can not be nested");
        } else {
            multiState->active = 1;

            /* We put the MULTI as the first command in the array, as we still
             * need to distinguish single-MULTI array from a single command.
             */
            RaftRedisCommandArrayMove(&multiState->cmds, cmds);
            RedisModule_ReplyWithSimpleString(ctx, "OK");
        }

        return true;
    } else if (cmd_len == 4 && !strncasecmp(cmd_str, "EXEC", 4)) {
        if (!multiState->active) {
            RedisModule_ReplyWithError(ctx, "ERR EXEC without MULTI");
            return true;
        }

        if (multiState->error) {
            MultiStateReset(multiState);
            RedisModule_ReplyWithError(ctx, "EXECABORT Transaction discarded because of previous errors.");
            return true;
        }

        /* TODO: should we check ACL for EXEC?
         * Currently we check ACL for MULTI as part of dry run processing,
         * but as EXEC is not added to CommandArray, it won't be tested
         */
        /* Just swap our commands with the EXEC command and proceed. */
        RaftRedisCommandArrayFree(cmds);
        RaftRedisCommandArrayMove(cmds, &multiState->cmds);
        MultiStateReset(multiState);

        return false;
    } else if (cmd_len == 7 && !strncasecmp(cmd_str, "DISCARD", 7)) {
        if (!multiState->active) {
            RedisModule_ReplyWithError(ctx, "ERR DISCARD without MULTI");
            return true;
        }

        MultiStateReset(multiState);
        RedisModule_ReplyWithSimpleString(ctx, "OK");

        return true;
    }

    /* Are we in MULTI? */
    if (multiState->active) {
        /* can't call WATCH within a MULTI, but doesn't error out MULTI */
        if (cmd_len == 5 && !strncasecmp(cmd_str, "WATCH", 5)) {
            RedisModule_ReplyWithError(ctx, "ERR WATCH inside MULTI is not allowed");
            return true;
        }

        /* We have to detect commands that are unsupported or must not be
         * intercepted and reject the transaction.
         */
        unsigned int cmd_flags = CommandSpecTableGetAggregateFlags(rr->commands_spec_table, rr->subcommand_spec_tables, cmds, 0);

        if (cmd_flags & CMD_SPEC_UNSUPPORTED) {
            RedisModule_ReplyWithError(ctx, "ERR not supported by RedisRaft");
            multiState->error = true;
            return true;
        }

        if (cmd_flags & CMD_SPEC_DONT_INTERCEPT) {
            RedisModule_ReplyWithError(ctx, "ERR not supported by RedisRaft inside MULTI/EXEC");
            multiState->error = true;
            return true;
        }

        if (RedisModule_GetUsedMemoryRatio() > 1.0) {
            /* Can only enqueue if not in an OOM situation, even for non deny oom commands */
            RedisModule_ReplyWithError(ctx, "OOM command not allowed when used memory > 'maxmemory'.");
            multiState->error = true;
            return true;
        }

        /* "Multi Dry Run" - only check for ACL, not for OOM, as OOM is checked above */
        enterRedisModuleCall();
        RedisModuleCallReply *reply = RedisModule_Call(ctx, cmd_str, "DCEv", cmd->argv + 1, cmd->argc - 1);
        exitRedisModuleCall();
        if (reply != NULL) {
            RedisModule_ReplyWithCallReply(ctx, reply);
            RedisModule_FreeCallReply(reply);

            multiState->error = true;
            return true;
        }

        RaftRedisCommandArrayMove(&multiState->cmds, cmds);
        RedisModule_ReplyWithSimpleString(ctx, "QUEUED");

        return true;
    }

    return false;
}
