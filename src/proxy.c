/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"

static RRStatus hiredisReplyToModule(redisReply *reply, RedisModuleCtx *ctx)
{
    switch (reply->type) {
        case REDIS_REPLY_STRING:
            RedisModule_ReplyWithStringBuffer(ctx, reply->str, reply->len);
            break;
        case REDIS_REPLY_ARRAY:
            RedisModule_ReplyWithArray(ctx, reply->elements);
            for (size_t i = 0; i < reply->elements; i++) {
                if (hiredisReplyToModule(reply->element[i], ctx) != RR_OK) {
                    RedisModule_ReplyWithError(ctx, "ERR bad reply from leader");
                }
            }
            break;
        case REDIS_REPLY_INTEGER:
            RedisModule_ReplyWithLongLong(ctx, reply->integer);
            break;
        case REDIS_REPLY_NIL:
            RedisModule_ReplyWithNull(ctx);
            break;
        case REDIS_REPLY_STATUS:
            RedisModule_ReplyWithSimpleString(ctx, reply->str);
            break;
        case REDIS_REPLY_ERROR:
            RedisModule_ReplyWithError(ctx, reply->str);
            break;
        default:
            return RR_ERROR;
    }

    return RR_OK;
}

static void handleProxiedCommandResponse(redisAsyncContext *c, void *r, void *privdata)
{
    RaftReq *req = privdata;
    redisReply *reply = r;

    redis_raft.proxy_outstanding_reqs--;
    NodeDismissPendingResponse(req->r.redis.proxy_node);

    if (!reply) {
        /* Connection have dropped.  The state of the request is unknown at this point
         * and this must be reflected to the user.
         *
         * Ideally the connection should be dropped but Module API does not provide for that.
         */
        ConnMarkDisconnected(req->r.redis.proxy_node->conn);
        RedisModule_ReplyWithError(req->ctx, "TIMEOUT no reply from leader");
        redis_raft.proxy_failed_responses++;
        goto exit;
    }

    if (RedisModule_BlockedClientDisconnected(req->ctx)) {
        goto exit;
    }

    if (hiredisReplyToModule(reply, req->ctx) != RR_OK) {
        RedisModule_ReplyWithError(req->ctx, "ERR bad reply from leader");
    }

exit:
    RaftReqFree(req);
}

RRStatus ProxyCommand(RedisRaftCtx *rr, RedisModuleCtx *ctx,
                      RaftRedisCommandArray *cmds, Node *leader)
{
    redisAsyncContext *rc;

    if (!ConnIsConnected(leader->conn) || !(rc = ConnGetRedisCtx(leader->conn))) {
        rr->proxy_failed_reqs++;
        return RR_ERROR;
    }

    RaftReq *req = RaftReqInit(ctx, RR_GENERIC);
    req->r.redis.proxy_node = leader;

    raft_entry_t *entry = RaftRedisCommandArraySerialize(cmds);
    int ret = redisAsyncCommand(rc, handleProxiedCommandResponse, req,
                                "RAFT.ENTRY %b", entry->data, entry->data_len);
    raft_entry_release(entry);

    if (ret != REDIS_OK) {
        RaftReqFree(req);
        rr->proxy_failed_reqs++;
        return RR_ERROR;
    }

    NodeAddPendingResponse(leader, true);
    rr->proxy_reqs++;
    rr->proxy_outstanding_reqs++;

    return RR_OK;
}
