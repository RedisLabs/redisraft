#ifndef REDISRAFT_SERIALIZATION_H
#define REDISRAFT_SERIALIZATION_H

#include "raft.h"
#include "redismodule.h"

#include <stdbool.h>
#include <stddef.h>

typedef struct {
    int argc;
    RedisModuleString **argv;
} Command;

raft_entry_t *CommandEncode(Command *cmd);
size_t CommandDecode(Command *target, const void *buf, size_t buf_size);
void CommandFree(Command *r);

#endif
