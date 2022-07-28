/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2022 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include "serialization.h"

#include "redisraft.h"

#include <string.h>

/* Return expected length of a serialized integer value as decimal digits + 2 byte overhead */
int calcIntSerializedLen(size_t val)
{
    if (val < 10) {
        return 3;
    } else if (val < 100) {
        return 4;
    } else if (val < 1000) {
        return 5;
    } else if (val < 10000) {
        return 6;
    } else if (val < 100000) {
        return 7;
    } else if (val < 1000000) {
        return 8;
    } else if (val < 10000000) {
        return 9;
    } else {
        return 22;
    }
}

/* decodes a serialized integer stored at buffer denoted by ptr
 * sz = remaining buffer size in bytes
 * expected_prefix = character value we expected to denote this serialization
 * val, pointer we return integer into
 *
 * return the amount of bytes consumed from the buffer or -1 upon error
 */
int decodeInteger(const char *ptr, size_t sz, char expect_prefix, size_t *val)
{
    size_t tmp = 0;
    int len = 1;

    if (sz < 3 || *ptr != expect_prefix) {
        return -1;
    }

    ptr++;
    sz--;
    while (*ptr != '\n') {
        if (*ptr < '0' || *ptr > '9') {
            return -1;
        }
        tmp *= 10;
        tmp += (*ptr - '0');

        ptr++;
        sz--;
        len++;

        if (!sz) {
            return -1;
        }
    }

    sz--;
    ptr++;
    *val = tmp;

    return len + 1;
}

/* encodes an integer val into a buffer at ptr
 * prefix = character value we denote this serialization as
 * sz = remaining bytes in buffer
 *
 * returns the number of bytes consumed in the buffer by the serialization
 * or -1 if buffer wasn't large enough
 */
int encodeInteger(char prefix, char *ptr, size_t sz, unsigned long val)
{
    int n = snprintf(ptr, sz, "%c%lu\n", prefix, val);

    if (n >= (int) sz) {
        return -1;
    }
    return n;
}
/* Free a RaftRedisCommand */
void CommandFree(Command *r)
{
    int i;

    if (r->argv) {
        for (i = 0; i < r->argc; i++) {
            RedisModule_FreeString(NULL, r->argv[i]);
        }
        RedisModule_Free(r->argv);
    }
    r->argc = 0;
    r->argv = NULL;
}


static size_t calcSerializedSize(Command *cmd)
{
    size_t sz = calcIntSerializedLen(cmd->argc + 1);
    int i;

    for (i = 0; i < cmd->argc; i++) {
        size_t len;
        RedisModule_StringPtrLen(cmd->argv[i], &len);
        sz += calcIntSerializedLen(len) + len + 1;
    }

    return sz;
}

/* Serialize a number of RaftRedisCommand into a Raft entry */
raft_entry_t *CommandEncode(Command *cmd)
{
    size_t sz = calcSerializedSize(cmd);
    size_t len;
    int n;
    char *p;


    /* Prepare entry */
    raft_entry_t *ety = raft_entry_new(sz);
    p = ety->data;

    n = encodeInteger('*', p, sz, cmd->argc);
    RedisModule_Assert(n != -1);
    p += n;
    sz -= n;

    for (int i = 0; i < cmd->argc; i++) {
        const char *e = RedisModule_StringPtrLen(cmd->argv[i], &len);
        RedisModule_Assert(sz > len);

        n = encodeInteger('$', p, sz, len);
        RedisModule_Assert(n != -1);
        p += n;
        sz -= n;

        memcpy(p, e, len);
        p += len;
        *p = '\n';
        p++;
        sz -= (len + 1);
    }

    return ety;
}

size_t CommandDecode(Command *cmd, const void *buf, size_t buf_size)
{
    const char *p = buf;
    int i, n;
    size_t len;

    /* Read argc */
    if ((n = decodeInteger(p, buf_size, '*', &len)) < 0 || !len) {
        return 0;
    }
    p += n;
    buf_size -= n;
    cmd->argc = len;
    cmd->argv = RedisModule_Calloc(len, sizeof(RedisModuleString *));

    /* Read args */
    for (i = 0; i < cmd->argc; i++) {
        if ((n = decodeInteger(p, buf_size, '$', &len)) < 0) {
            goto error;
        }
        p += n;
        buf_size -= n;
        if (buf_size <= len) {
            goto error;
        }

        cmd->argv[i] = RedisModule_CreateString(NULL, p, len);
        p += len + 1;
        buf_size -= (len + 1);
    }

    return p - (char *) buf;

error:
    CommandFree(cmd);
    return 0;
}
