#include "redisraft.h"



const char *getStateStr(RedisRaftCtx *rr);
RRStatus checkLeader(RedisRaftCtx *rr, RedisModuleCtx *ctx);
RRStatus checkRaftUninitialized(RedisRaftCtx *rr, RedisModuleCtx *ctx);
RRStatus checkRaftState(RedisRaftCtx *rr, RedisModuleCtx *ctx);
bool parseMovedReply(const char *str, NodeAddr *addr);

void replyRaftError(RedisModuleCtx *ctx, int error);
void replyRedirect(RedisModuleCtx *ctx, unsigned int slot, NodeAddr *addr);
void replyClusterDown(RedisModuleCtx *ctx);

char *catsnprintf(char *strbuf, size_t *strbuf_len, const char *fmt, ...);
int RedisModuleStringToInt(RedisModuleString *str, int *value);