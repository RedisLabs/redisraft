#include "redisraft.h"

static RRStatus hiredisReplyToModule(redisReply *reply, RedisModuleCtx *ctx)
{
    int i;

    switch (reply->type) {
        case REDIS_REPLY_STRING:
            RedisModule_ReplyWithStringBuffer(ctx, reply->str, reply->len);
            break;
        case REDIS_REPLY_ARRAY:
            RedisModule_ReplyWithArray(ctx, reply->elements);
            for (i = 0 ; i < reply->elements; i++) {
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

    if (!reply) {
        /* Connection have dropped */
        RedisModule_ReplyWithError(req->ctx, "NOLEADER lost leader connection");
        goto exit;
    }

    if (hiredisReplyToModule(reply, req->ctx) != RR_OK) {
        RedisModule_ReplyWithError(req->ctx, "ERR bad reply from leader");
    }

exit:
    RaftReqFree(req);
}

RRStatus ProxyCommand(RedisRaftCtx *rr, RaftReq *req, Node *leader)
{
    int argc = req->r.redis.cmd.argc + 1;
    const char *argv[argc];
    size_t argvlen[argc];
    int i;

    argv[0] = "RAFT";
    argvlen[0] = 4;

    for (i = 1; i < argc; i++) {
        argv[i] = RedisModule_StringPtrLen(req->r.redis.cmd.argv[i-1], &argvlen[i]);
    }

    if (redisAsyncCommandArgv(leader->rc, handleProxiedCommandResponse,
                req, req->r.redis.cmd.argc + 1, argv, argvlen) != REDIS_OK) {
        return RR_ERROR;
    }

    return RR_OK;
}

