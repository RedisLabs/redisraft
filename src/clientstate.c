/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2022 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include "redisraft.h"

ClientState *ClientStateGet(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    return RedisModule_DictGetC(rr->client_state, &client_id, sizeof(client_id), NULL);
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
        ClientStateResetMulti(state);
        RedisModule_Free(state);
    }
}

void ClientStateResetMulti(ClientState *client_state)
{
    RaftRedisCommandArrayFree(&client_state->multi_state.cmds);
    client_state->multi_state.active = false;
    client_state->multi_state.error = false;
}
