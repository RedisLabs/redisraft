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
        /* Connection have dropped.  The state of the request is unknown at this point
         * and this must be reflected to the user.
         *
         * Ideally the connection should be dropped but Module API does not provide for that.
         */
        RedisModule_ReplyWithError(req->ctx, "TIMEOUT no reply from leader");
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
    /* TODO: This is just a quick hack to proxy a single command.
     * We need to proxy all commands we get inside MULTI/EXEC,
     * and somehow fail if we have a WATCH.
     */
    RaftRedisCommand *c = req->r.redis.cmds.commands[0];

    int argc = c->argc + 1;
    const char *argv[argc];
    size_t argvlen[argc];
    int i;

    if (!leader->rc || leader->state != NODE_CONNECTED) {
        return RR_ERROR;
    }

    argv[0] = "RAFT";
    argvlen[0] = 4;

    for (i = 1; i < argc; i++) {
        argv[i] = RedisModule_StringPtrLen(c->argv[i-1], &argvlen[i]);
    }

    if (redisAsyncCommandArgv(leader->rc, handleProxiedCommandResponse,
                req, c->argc + 1, argv, argvlen) != REDIS_OK) {
        return RR_ERROR;
    }

    return RR_OK;
}

