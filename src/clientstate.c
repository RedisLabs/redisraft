/*
 * Copyright Redis Ltd. 2022 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"

ClientState *ClientStateGetById(RedisRaftCtx *rr, unsigned long long client_id)
{
    return RedisModule_DictGetC(rr->client_state, &client_id, sizeof(client_id), NULL);
}

ClientState *ClientStateGet(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    return ClientStateGetById(rr, client_id);
}

void ClientStateAlloc(RedisRaftCtx *rr, unsigned long long client_id)
{
    ClientState *clientState = RedisModule_Calloc(sizeof(ClientState), 1);
    int ret = RedisModule_DictSetC(rr->client_state, &client_id, sizeof(client_id), clientState);
    RedisModule_Assert(ret == REDISMODULE_OK);
}

void ClientStateFree(RedisRaftCtx *rr, unsigned long long client_id)
{
    ClientState *state = NULL;

    RedisModule_DictDelC(rr->client_state, &client_id, sizeof(client_id), &state);

    if (state != NULL) {
        ClientStateReset(state);
        RedisModule_Free(state);
    }
}

void ClientStateSetBlockedReq(RedisRaftCtx *rr, raft_session_t client_id, RaftReq *req)
{
    ClientState *cs = RedisModule_DictGetC(rr->client_state, &client_id, sizeof(client_id), NULL);
    cs->blocked_req = req;
}

void BlockedReqResetById(RedisRaftCtx *rr, raft_session_t client_id)
{
    ClientState *cs = RedisModule_DictGetC(rr->client_state, &client_id, sizeof(client_id), NULL);
    if (cs) {
        cs->blocked_req = NULL;
    }
}

void MultiStateReset(MultiState *multi_state)
{
    RaftRedisCommandArrayFree(&multi_state->cmds);
    multi_state->active = false;
    multi_state->error = false;
}

void ClientStateReset(ClientState *client_state)
{
    MultiStateReset(&client_state->multi_state);
}
