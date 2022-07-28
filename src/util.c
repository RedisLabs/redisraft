/*
* This file is part of RedisRaft.
*
* Copyright (c) 2020-2021 Redis Ltd.
*
* RedisRaft is licensed under the Redis Source Available License (RSAL).
*/

#include "redisraft.h"

#include <string.h>
#include <strings.h>

const char *getStateStr(RedisRaftCtx *rr)
{
   switch (rr->state) {
       case REDIS_RAFT_UNINITIALIZED:
           return "uninitialized";
       case REDIS_RAFT_UP:
           return "up";
       case REDIS_RAFT_JOINING:
           return "joining";
       default:
           return "<invalid>";
   }
}

/* Convert a Raft library error code to an error reply.
*/
void replyRaftError(RedisModuleCtx *ctx, int error)
{
   char buf[128];

   switch (error) {
       case RAFT_ERR_NOT_LEADER:
           RedisModule_ReplyWithError(ctx, "ERR not leader");
           break;
       case RAFT_ERR_SHUTDOWN:
           LOG_WARNING("Raft requires immediate shutdown!");
           RedisModule_Call(ctx, "SHUTDOWN", "");
           break;
       case RAFT_ERR_ONE_VOTING_CHANGE_ONLY:
           RedisModule_ReplyWithError(ctx, "ERR a voting change is already in progress");
           break;
       case RAFT_ERR_NOMEM:
           RedisModule_ReplyWithError(ctx, "OOM Raft out of memory");
           break;
       case RAFT_ERR_LEADER_TRANSFER_IN_PROGRESS:
           RedisModule_ReplyWithError(ctx, "ERR transfer already in progress");
           break;
       case RAFT_ERR_INVALID_NODEID:
           RedisModule_ReplyWithError(ctx, "ERR invalid node id");
           break;
       default:
           snprintf(buf, sizeof(buf), "ERR Raft error %d", error);
           RedisModule_ReplyWithError(ctx, buf);
           break;
   }
}

/* Create a -MOVED reply. */
void replyRedirect(RedisModuleCtx *ctx, unsigned int slot, NodeAddr *addr)
{
   char buf[sizeof(addr->host) + 256];

   snprintf(buf, sizeof(buf), "MOVED %u %s:%u", slot, addr->host, addr->port);
   RedisModule_ReplyWithError(ctx, buf);
}

void replyClusterDown(RedisModuleCtx *ctx)
{
   RedisModule_ReplyWithError(ctx, "CLUSTERDOWN No raft leader");
}

/* Returns the leader node (Node), or reply a -CLUSTERDOWN error
* and return NULL.
*
* Note: it's possible to have an elected but unknown leader node, in which
* case NULL will also be returned.
*/
RRStatus checkLeader(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    Node *node = NULL;

    if (raft_is_leader(rr->raft)) {
        return RR_OK;
    }

    raft_node_t *raft_node = raft_get_leader_node(rr->raft);
    if (raft_node) {
        node = raft_node_get_udata(raft_node);
    }

    if (!node) {
        replyClusterDown(ctx);
        return RR_ERROR;
    }

    replyRedirect(ctx, 0, &node->addr);
    return RR_ERROR;
}

RRStatus checkRaftUninitialized(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
   if (rr->state != REDIS_RAFT_UNINITIALIZED) {
       RedisModule_ReplyWithError(ctx, "ERR Already cluster member");
       return RR_ERROR;
   }
   return RR_OK;
}

/* Check that we're in a REDIS_RAFT_UP state.  If not, reply with an appropriate
* error code and return an error.
*/
RRStatus checkRaftState(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
   switch (rr->state) {
       case REDIS_RAFT_UNINITIALIZED:
           RedisModule_ReplyWithError(ctx, "NOCLUSTER No Raft Cluster");
           return RR_ERROR;
       case REDIS_RAFT_JOINING:
           RedisModule_ReplyWithError(ctx, "NOCLUSTER No Raft Cluster (joining now)");
           return RR_ERROR;
       case REDIS_RAFT_UP:
           break;
   }
   return RR_OK;
}

/* Parse a -MOVED reply and update the returned address in addr.
* Both standard Redis Cluster reply (with the hash slot) or the simplified
* RedisRaft reply are supported.
*/
bool parseMovedReply(const char *str, NodeAddr *addr)
{
   /* -MOVED 0 1.1.1.1:1 or -MOVED 1.1.1.1:1 */
   if (strlen(str) < 15 || strncmp(str, "MOVED ", 6) != 0) {
       return false;
   }

   /* Handle current or cluster-style -MOVED replies. */
   const char *p = strrchr(str, ' ') + 1;
   return NodeAddrParse(p, strlen(p), addr);
}

int RedisModuleStringToInt(RedisModuleString *str, int *value)
{
   long long tmpll;

   if (RedisModule_StringToLongLong(str, &tmpll) != REDISMODULE_OK) {
       return REDISMODULE_ERR;
   }

   if (tmpll < INT32_MIN || tmpll > INT32_MAX) {
       return REDISMODULE_ERR;
   }

   *value = (int) tmpll;
   return REDISMODULE_OK;
}

char *catsnprintf(char *strbuf, size_t *strbuf_len, const char *fmt, ...)
{
   va_list ap;
   size_t len;
   size_t used = strlen(strbuf);
   size_t avail = *strbuf_len - used;

   va_start(ap, fmt);
   len = vsnprintf(strbuf + used, avail, fmt, ap);

   if (len >= avail) {
       if (len - avail > 4096) {
           *strbuf_len += (len + 1);
       } else {
           *strbuf_len += 4096;
       }

       va_end(ap);
       va_start(ap, fmt);

       strbuf = RedisModule_Realloc(strbuf, *strbuf_len);
       vsnprintf(strbuf + used, *strbuf_len - used, fmt, ap);
   }
   va_end(ap);

   return strbuf;
}
