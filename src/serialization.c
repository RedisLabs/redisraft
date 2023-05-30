/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"

#include <string.h>

/* RaftRedisCommand represents a single Redis command to execute.  Every Raft log entry
 * contains a serialized list of commands in Redis multi-bulk compatible encoding (using \n
 * rather than \r\n termination).
 * For example:
 *
 * *2\n*3\n:0\n:0\n$-1\n*1\n*3\n$3\nSET\n$3\nkey\n$5\nvalue\n
 *     |--------------| | -----------------------------------|
 *           |                           |
 * Metadata(asking, flags, acl)    Redis Command (set key value)
 */

#define MULTIBULK_ARRAY_COUNT 2
#define METADATA_ELEM_COUNT   3

RaftRedisCommand *RaftRedisCommandArrayExtend(RaftRedisCommandArray *target)
{
    if (target->size == target->len) {
        target->size++;
        target->commands = RedisModule_Realloc(target->commands, target->size * sizeof(RaftRedisCommand *));
    }

    target->commands[target->len] = RedisModule_Calloc(1, sizeof(RaftRedisCommand));
    target->len++;
    return target->commands[target->len - 1];
}

/* Concatenate the commands in the source array to the target array.  The source array
 * will remain empty when this operation is complete!
 */
void RaftRedisCommandArrayMove(RaftRedisCommandArray *target, RaftRedisCommandArray *source)
{
    int i;

    if (target->len + source->len > target->size) {
        target->size = target->len + source->len;
        target->commands = RedisModule_Realloc(target->commands, target->size * sizeof(RaftRedisCommand *));
    }

    for (i = 0; i < source->len; i++) {
        target->commands[target->len] = source->commands[i];
        target->len++;
    }

    source->len = 0;
    memset(source->commands, 0, sizeof(RaftRedisCommand *) * source->size);

    if (source->acl) {
        target->acl = source->acl;
        source->acl = NULL;
    }

    target->asking |= source->asking;
    target->client_id = source->client_id;
    target->cmd_flags |= source->cmd_flags;
}

/* Free a RaftRedisCommand */
void RaftRedisCommandFree(RaftRedisCommand *r)
{
    int i;

    if (r->argv) {
        for (i = 0; i < r->argc; i++) {
            RedisModule_FreeString(NULL, r->argv[i]);
        }
        RedisModule_Free(r->argv);
    }
    r->argc = 0;
}

void RaftRedisCommandArrayFree(RaftRedisCommandArray *array)
{
    int i;

    if (!array) {
        return;
    }

    if (array->commands) {
        for (i = 0; i < array->len; i++) {
            if (!array->commands[i]) {
                continue;
            }
            RaftRedisCommandFree(array->commands[i]);
            RedisModule_Free(array->commands[i]);
            array->commands[i] = NULL;
        }
        RedisModule_Free(array->commands);
        array->commands = NULL;
    }
    array->size = array->len = 0;
    if (array->acl) {
        RedisModule_FreeString(NULL, array->acl);
    }
    array->asking = false;
}

static size_t calcSerializedSize(RaftRedisCommand *cmd)
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
raft_entry_t *RaftRedisCommandArraySerialize(const RaftRedisCommandArray *source)
{
    int i, j;
    ssize_t n;
    char *p;
    size_t len, sz = 0;

    sz += calcIntSerializedLen(MULTIBULK_ARRAY_COUNT);
    sz += calcIntSerializedLen(METADATA_ELEM_COUNT);
    sz += calcIntSerializedLen(source->asking);
    sz += calcIntSerializedLen(source->cmd_flags);
    sz += calcIntSerializedLen(source->len);

    /* Compute sizes */
    for (i = 0; i < source->len; i++) {
        sz += calcSerializedSize(source->commands[i]);
    }
    sz += calcSerializeStringSize(source->acl);

    /* Prepare entry */
    raft_entry_t *ety = raft_entry_new(sz);
    p = ety->data;

    /* Encode array count */
    n = encodeInteger('*', p, sz, MULTIBULK_ARRAY_COUNT);
    RedisModule_Assert(n != -1);
    p += n;
    sz -= n;

    /* Encode metadata element count */
    n = encodeInteger('*', p, sz, METADATA_ELEM_COUNT);
    RedisModule_Assert(n != -1);
    p += n;
    sz -= n;

    /* Encode Asking */
    n = encodeInteger(':', p, sz, source->asking);
    RedisModule_Assert(n != -1);
    p += n;
    sz -= n;

    /* Encode cmd_flags */
    n = encodeInteger(':', p, sz, source->cmd_flags);
    RedisModule_Assert(n != -1);
    p += n;
    sz -= n;

    /* Encode ACL */
    n = encodeString(p, sz, source->acl);
    p += n;
    sz -= n;

    /* Encode count */
    n = encodeInteger('*', p, sz, source->len);
    RedisModule_Assert(n != -1);
    p += n;
    sz -= n;

    /* Encode entries */
    for (i = 0; i < source->len; i++) {
        RaftRedisCommand *src = source->commands[i];

        n = encodeInteger('*', p, sz, src->argc);
        RedisModule_Assert(n != -1);
        p += n;
        sz -= n;

        for (j = 0; j < src->argc; j++) {
            const char *e = RedisModule_StringPtrLen(src->argv[j], &len);
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
    }

    return ety;
}

size_t RaftRedisCommandDeserialize(RaftRedisCommand *target, const void *buf, size_t buf_size)
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
    target->argc = len;
    target->argv = RedisModule_Calloc(len, sizeof(RedisModuleString *));

    /* Read args */
    for (i = 0; i < target->argc; i++) {
        if ((n = decodeInteger(p, buf_size, '$', &len)) < 0) {
            goto error;
        }
        p += n;
        buf_size -= n;
        if (buf_size <= len) {
            goto error;
        }

        target->argv[i] = RedisModule_CreateString(NULL, p, len);
        p += len + 1;
        buf_size -= (len + 1);
    }

    return p - (char *) buf;

error:
    RaftRedisCommandFree(target);
    return 0;
}

RRStatus RaftRedisCommandArrayDeserialize(RaftRedisCommandArray *target, const void *buf, size_t buf_size)
{
    const char *p = buf;
    size_t commands_num, val;
    ssize_t n;

    if (target->len) {
        RaftRedisCommandArrayFree(target);
    }

    n = decodeInteger(p, buf_size, '*', &val);
    if (n < 0 || val != MULTIBULK_ARRAY_COUNT) {
        return RR_ERROR;
    }
    p += n;
    buf_size -= n;

    n = decodeInteger(p, buf_size, '*', &val);
    if (n < 0 || val != METADATA_ELEM_COUNT) {
        return RR_ERROR;
    }
    p += n;
    buf_size -= n;

    /* Read asking */
    size_t asking;
    if ((n = decodeInteger(p, buf_size, ':', &asking)) < 0) {
        return RR_ERROR;
    }
    p += n;
    buf_size -= n;
    target->asking = asking;

    /* Read cmd_flags */
    size_t cmd_flags;
    if ((n = decodeInteger(p, buf_size, ':', &cmd_flags)) < 0) {
        return RR_ERROR;
    }
    p += n;
    buf_size -= n;
    target->cmd_flags = cmd_flags;

    /* Read ACL */
    RedisModuleString *acl;
    if ((n = decodeString(p, buf_size, &acl)) < 0) {
        return RR_ERROR;
    }
    p += n;
    buf_size -= n;

    size_t acl_len;
    RedisModule_StringPtrLen(acl, &acl_len);
    if (acl_len == 0) {
        RedisModule_FreeString(NULL, acl);
        acl = NULL;
    }
    target->acl = acl;

    /* Read command count */
    if ((n = decodeInteger(p, buf_size, '*', &commands_num)) < 0 ||
        !commands_num) {
        return RR_ERROR;
    }
    p += n;
    buf_size -= n;

    /* Allocate array */
    target->len = target->size = commands_num;
    target->commands = RedisModule_Calloc(commands_num, sizeof(RaftRedisCommand *));
    for (size_t i = 0; i < commands_num; i++) {
        target->commands[i] = RedisModule_Calloc(1, sizeof(RaftRedisCommand));
        size_t len = RaftRedisCommandDeserialize(target->commands[i], p, buf_size);
        if (!len) {
            /* Error */
            RaftRedisCommandArrayFree(target);
            return RR_ERROR;
        }

        p += len;
        buf_size -= len;
    }

    return RR_OK;
}

RRStatus RaftRedisDeserializeImport(ImportKeys *target, const void *buf, size_t buf_size)
{
    const char *p = buf;
    ssize_t n;

    FreeImportKeys(target);

    /* Read term */
    size_t term;
    if ((n = decodeInteger(p, buf_size, '*', &term)) < 0 || !term) {
        return RR_ERROR;
    }
    p += n;
    buf_size -= n;
    target->term = term;

    /* Read migration_session_key */
    size_t migration_session_key;
    if ((n = decodeInteger(p, buf_size, '*', &migration_session_key)) < 0) {
        return RR_ERROR;
    }
    p += n;
    buf_size -= n;
    target->migration_session_key = (unsigned long long) migration_session_key;

    /* Read number of keys serialized in import entry */
    size_t num_keys;
    if ((n = decodeInteger(p, buf_size, '*', &num_keys)) < 0 || !num_keys) {
        return RR_ERROR;
    }
    p += n;
    buf_size -= n;
    target->num_keys = num_keys;

    target->key_names = RedisModule_Calloc(num_keys, sizeof(RedisModuleString *));
    target->key_serialized = RedisModule_Calloc(num_keys, sizeof(RedisModuleString *));

    for (size_t i = 0; i < num_keys; i++) {
        n = decodeString(p, buf_size, &target->key_names[i]);
        if (n < 0 || !target->key_names[i]) {
            return RR_ERROR;
        }
        p += n;
        buf_size -= n;
        n = decodeString(p, buf_size, &target->key_serialized[i]);
        if (n < 0 || !target->key_serialized[i]) {
            return RR_ERROR;
        }
        p += n;
        buf_size -= n;
    }

    return RR_OK;
}

raft_entry_t *RaftRedisSerializeImport(const ImportKeys *import_keys)
{
    ssize_t n;

    size_t sz = calcIntSerializedLen(import_keys->term);
    sz += calcIntSerializedLen(import_keys->migration_session_key);
    sz += calcIntSerializedLen(import_keys->num_keys);
    for (size_t i = 0; i < import_keys->num_keys; i++) {
        sz += calcSerializeStringSize(import_keys->key_names[i]);
        sz += calcSerializeStringSize(import_keys->key_serialized[i]);
    }

    /* Prepare entry */
    raft_entry_t *ety = raft_entry_new(sz);
    char *p = ety->data;

    /* Encode term */
    n = encodeInteger('*', p, sz, import_keys->term);
    RedisModule_Assert(n != -1);
    p += n;
    sz -= n;

    /* encode migration_session_key */
    n = encodeInteger('*', p, sz, import_keys->migration_session_key);
    RedisModule_Assert(n != -1);
    p += n;
    sz -= n;

    /* encode num_keys */
    n = encodeInteger('*', p, sz, import_keys->num_keys);
    RedisModule_Assert(n != -1);
    p += n;
    sz -= n;

    for (size_t i = 0; i < import_keys->num_keys; i++) {
        n = encodeString(p, sz, import_keys->key_names[i]);
        RedisModule_Assert(n != -1);
        p += n;
        sz -= n;
        n = encodeString(p, sz, import_keys->key_serialized[i]);
        RedisModule_Assert(n != -1);
        p += n;
        sz -= n;
    }

    return ety;
}

/* serialize out keys in a RaftRedisCommand for locking */
raft_entry_t *RaftRedisLockKeysSerialize(RedisModuleString **argv, size_t argc)
{
    RedisModuleDict *keys = RedisModule_CreateDict(NULL);
    size_t total_key_size = 0;
    int num_keys = 0;

    for (size_t idx = 0; idx < argc; idx++) {
        size_t str_len;
        RedisModule_StringPtrLen(argv[idx], &str_len);

        if (RedisModule_DictSet(keys, argv[idx], NULL) == REDISMODULE_OK) {
            total_key_size += str_len + 1;
            num_keys++;
        }
    }

    size_t data_len = calcIntSerializedLen(num_keys) + total_key_size;
    raft_entry_t *ety = raft_entry_new(data_len);
    char *p = ety->data;

    /* Encode number of keys */
    int n = encodeInteger('*', p, total_key_size, num_keys);
    RedisModule_Assert(n != -1);
    p += n;
    data_len -= n;

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(keys, "^", NULL, 0);
    char *key;
    size_t key_len;
    while ((key = RedisModule_DictNextC(iter, &key_len, NULL)) != NULL) {
        RedisModule_Assert(total_key_size >= (key_len + 1));
        memcpy(p, key, key_len);
        p += key_len;
        *p = '\0';
        p++;
        data_len -= (key_len + 1);
    }
    RedisModule_DictIteratorStop(iter);

    RedisModule_Assert(data_len == 0);
    RedisModule_FreeDict(NULL, keys);

    return ety;
}

RedisModuleString **RaftRedisLockKeysDeserialize(const void *buf, size_t buf_size, size_t *num_keys)
{
    RedisModuleString **ret;

    const char *p = buf;
    ssize_t n;
    /* Read number of keys */
    n = decodeInteger(p, buf_size, '*', num_keys);
    if (n < 0 || num_keys == 0) {
        return NULL;
    }
    p += n;

    ret = RedisModule_Alloc(sizeof(RedisModuleString *) * (*num_keys));
    for (size_t i = 0; i < *num_keys; i++) {
        size_t str_len = strlen(p);
        ret[i] = RedisModule_CreateString(redis_raft.ctx, p, str_len);
        p += str_len + 1;
    }

    return ret;
}

raft_entry_t *RaftRedisSerializeTimeout(raft_index_t idx, bool error)
{
    ssize_t n;
    int err_val = error ? 1 : 0;

    size_t sz = calcIntSerializedLen(idx); /* idx we are timing out */
    sz += calcIntSerializedLen(err_val);   /* encoding the error bool */
    sz++;

    raft_entry_t *ety = raft_entry_new(sz);
    ety->type = RAFT_LOGTYPE_TIMEOUT_BLOCKED;

    char *p = ety->data;

    /* Encode idx of entry we are timing out */
    n = encodeInteger('*', p, sz, idx);
    RedisModule_Assert(n != -1);
    p += n;
    sz -= n;

    /* Encode if regular timeout or error */
    n = encodeInteger('*', p, sz, err_val);
    RedisModule_Assert(n != -1);
    p += n;
    sz -= n;

    return ety;
}

RRStatus RaftRedisDeserializeTimeout(const void *buf, size_t buf_size, raft_index_t *idx, bool *error)
{
    const char *p = buf;
    ssize_t n;

    size_t tmp;

    /* Read idx */
    if ((n = decodeInteger(p, buf_size, '*', &tmp)) < 0 || !idx) {
        return RR_ERROR;
    }
    p += n;
    buf_size -= n;
    *idx = tmp;

    /* read error */
    if ((n = decodeInteger(p, buf_size, '*', &tmp)) < 0) {
        return RR_ERROR;
    }
    p += n;
    buf_size -= n;
    *error = (tmp == 1);

    RedisModule_Assert(buf_size == 1); /* should only have the final '\0' at the end of the data */

    return RR_OK;
}