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

void MultiClientStateReset(ClientState * clientState)
{
    RaftRedisCommandArrayFree(&clientState->multi_state.cmds);
    clientState->multi_state.active = false;
    clientState->multi_state.error = false;
}

bool MultiHandleCommand(RedisRaftCtx *rr,
                        RedisModuleCtx *ctx, RaftRedisCommandArray *cmds)
{
    ClientState *clientState = ClientStateGet(rr, ctx);

    /* Is this a MULTI command? */
    RaftRedisCommand *cmd = cmds->commands[0];
    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[0], &cmd_len);

    if (cmd_len == 5 && !strncasecmp(cmd_str, "MULTI", 5)) {
        if (clientState->multi_state.active) {
            RedisModule_ReplyWithError(ctx, "ERR MULTI calls can not be nested");
        } else {
            clientState->multi_state.active = 1;

            /* We put the MULTI as the first command in the array, as we still
             * need to distinguish single-MULTI array from a single command.
             */
            RaftRedisCommandArrayMove(&clientState->multi_state.cmds, cmds);
            RedisModule_ReplyWithSimpleString(ctx, "OK");
        }

        return true;
    } else if (cmd_len == 4 && !strncasecmp(cmd_str, "EXEC", 4)) {
        if (!clientState->multi_state.active) {
            RedisModule_ReplyWithError(ctx, "ERR EXEC without MULTI");
            return true;
        }

        if (clientState->multi_state.error) {
            MultiClientStateReset(clientState);
            RedisModule_ReplyWithError(ctx, "EXECABORT Transaction discarded because of previous errors.");
            return true;
        }

        /* Just swap our commands with the EXEC command and proceed. */
        RaftRedisCommandArrayFree(cmds);
        RaftRedisCommandArrayMove(cmds, &clientState->multi_state.cmds);
        MultiClientStateReset(clientState);

        return false;
    } else if (cmd_len == 7 && !strncasecmp(cmd_str, "DISCARD", 7)) {
        if (!clientState->multi_state.active) {
            RedisModule_ReplyWithError(ctx, "ERR DISCARD without MULTI");
            return true;
        }

        MultiClientStateReset(clientState);
        RedisModule_ReplyWithSimpleString(ctx, "OK");

        return true;
    }

    /* Are we in MULTI? */
    if (clientState->multi_state.active) {
        /* We have to detect commands that are unsupported or must not be
         * intercepted and reject the transaction.
         */
        unsigned int cmd_flags = CommandSpecGetAggregateFlags(cmds, 0);

        if (cmd_flags & CMD_SPEC_UNSUPPORTED) {
            RedisModule_ReplyWithError(ctx, "ERR not supported by RedisRaft");
            clientState->multi_state.error = true;
        } else if (cmd_flags & CMD_SPEC_DONT_INTERCEPT) {
            RedisModule_ReplyWithError(ctx, "ERR not supported by RedisRaft inside MULTI/EXEC");
            clientState->multi_state.error = true;
        } else {
            RaftRedisCommandArrayMove(&clientState->multi_state.cmds, cmds);
            RedisModule_ReplyWithSimpleString(ctx, "QUEUED");
        }

        return true;
    }

    return false;
}
