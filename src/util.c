/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "log.h"
#include "redisraft.h"

#include "common/crc16.h"

#include <ctype.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

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

        /* "Rewind" va_arg(); Apparently this is required by older versions (rhel6) */
        va_end(ap);
        va_start(ap, fmt);

        strbuf = RedisModule_Realloc(strbuf, *strbuf_len);
        len = vsnprintf(strbuf + used, *strbuf_len - used, fmt, ap);
    }
    va_end(ap);

    return strbuf;
}

/* Checks return value of snprintf and panic on truncation or error. */
int safesnprintf(void *buf, size_t size, const char *fmt, ...)
{
    va_list va;

    va_start(va, fmt);
    int ret = vsnprintf(buf, size, fmt, va);
    va_end(va);

    if (ret < 0 || ret >= (int) size) {
        PANIC("vsnprintf(): ret=%d, size=%zu, errno=%d", ret, size, errno);
    }

    return ret;
}

/* Calculate output length */
int lensnprintf(const char *fmt, ...)
{
    va_list va;

    va_start(va, fmt);
    int ret = vsnprintf(NULL, 0, fmt, va);
    va_end(va);

    return ret;
}

/* This function assumes that the rr->config->slot_config has already been validated as valid */
ShardGroup *CreateAndFillShard(RedisRaftCtx *rr)
{
    ShardGroup *sg = ShardGroupCreate();

    if (!strcmp(rr->config.slot_config, "")) {
        goto exit;
    }

    char *str = RedisModule_Strdup(rr->config.slot_config);
    sg->slot_ranges_num = 1;
    char *pos = str;
    while ((pos = strchr(pos + 1, ','))) {
        sg->slot_ranges_num++;
    }
    sg->slot_ranges = RedisModule_Calloc(sg->slot_ranges_num, sizeof(ShardGroupSlotRange));

    char *saveptr = NULL;
    char *token = strtok_r(str, ",", &saveptr);
    for (unsigned int i = 0; i < sg->slot_ranges_num; i++) {
        unsigned long val;
        if ((pos = strchr(token, ':'))) {
            *pos = '\0';
            val = strtoul(token, NULL, 10);
            sg->slot_ranges[i].start_slot = val;
            val = strtoul(pos + 1, NULL, 10);
            sg->slot_ranges[i].end_slot = val;
        } else {
            val = strtoul(token, NULL, 10);
            sg->slot_ranges[i].start_slot = val;
            sg->slot_ranges[i].end_slot = val;
        }
        sg->slot_ranges[i].type = SLOTRANGE_TYPE_STABLE;

        token = strtok_r(NULL, ",", &saveptr);
    }

    RedisModule_Free(str);

exit:
    return sg;
}

void AddBasicLocalShardGroup(RedisRaftCtx *rr)
{
    ShardGroup *sg = CreateAndFillShard(rr);
    RedisModule_Assert(sg != NULL);

    sg->local = true;
    memcpy(sg->id, rr->meta.dbid, RAFT_DBID_LEN);
    sg->id[RAFT_DBID_LEN] = '\0';

    RRStatus ret = ShardingInfoAddShardGroup(rr, sg);
    RedisModule_Assert(ret == RR_OK);
}

void FreeImportKeys(ImportKeys *target)
{
    if (target->num_keys) {
        if (target->key_names) {
            for (size_t i = 0; i < target->num_keys; i++) {
                if (target->key_names[i]) {
                    RedisModule_FreeString(NULL, target->key_names[i]);
                }
            }
            RedisModule_Free(target->key_names);
            target->key_names = NULL;
        }
        if (target->key_serialized) {
            for (size_t i = 0; i < target->num_keys; i++) {
                if (target->key_serialized[i]) {
                    RedisModule_FreeString(NULL, target->key_serialized[i]);
                }
            }
            RedisModule_Free(target->key_serialized);
            target->key_serialized = NULL;
        }
    }
}

/* -----------------------------------------------------------------------------
 * Hashing code - copied directly from Redis.
 * -------------------------------------------------------------------------- */

/* We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress). */
unsigned int keyHashSlot(const char *key, size_t keylen)
{
    size_t s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++) {
        if (key[s] == '{') {
            break;
        }
    }

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) {
        return crc16_ccitt(key, keylen) & 0x3FFF;
    }

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s + 1; e < keylen; e++) {
        if (key[e] == '}') {
            break;
        }
    }

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (e == keylen || e == s + 1) {
        return crc16_ccitt(key, keylen) & 0x3FFF;
    }

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return crc16_ccitt(key + s + 1, e - s - 1) & 0x3FFF;
}

unsigned int keyHashSlotRedisString(RedisModuleString *str)
{
    size_t keylen;
    const char *key = RedisModule_StringPtrLen(str, &keylen);

    return keyHashSlot(key, keylen);
}

RRStatus parseHashSlots(char *slots, char *string)
{
    string = RedisModule_Strdup(string);
    RRStatus ret = RR_OK;
    char *saveptr = NULL;
    char *tok = strtok_r(string, ",", &saveptr);
    while (tok != NULL) {
        char *dash = strchr(tok, '-');
        if (dash == NULL) {
            char *endptr;
            unsigned int slot = strtoul(tok, &endptr, 10);
            if (*endptr != '\0' || slot > REDIS_RAFT_HASH_MAX_SLOT) {
                ret = RR_ERROR;
                goto exit;
            }
            slots[slot] = 1;
        } else {
            *dash = '\0';
            char *endptr;
            unsigned int start = strtoul(tok, &endptr, 10);
            if (*endptr != '\0' || start > REDIS_RAFT_HASH_MAX_SLOT) {
                ret = RR_ERROR;
                goto exit;
            }
            tok = dash + 1;
            unsigned int end = strtoul(tok, &endptr, 10);
            if (*endptr != '\0' || end > REDIS_RAFT_HASH_MAX_SLOT || end < start) {
                ret = RR_ERROR;
                goto exit;
            }
            for (unsigned int i = start; i <= end; i++) {
                slots[i] = 1;
            }
        }
        tok = strtok_r(NULL, ",", &saveptr);
    }

exit:
    RedisModule_Free(string);
    return ret;
}

bool parseLongLong(const char *str, char **end, long long *val)
{
    char *endp;

    errno = 0;
    *val = strtoll(str, &endp, 0);

    if (end) {
        *end = endp;
    }

    if (endp == str ||
        ((*val == LLONG_MIN || *val == LLONG_MAX) && errno == ERANGE)) {
        return false;
    }

    return true;
}

bool parseLong(const char *str, char **end, long *val)
{
    long long num;

    bool ret = parseLongLong(str, end, &num);
    if (!ret || num < LONG_MIN || num > LONG_MAX) {
        return false;
    }

    *val = (long) num;
    return true;
}

bool parseInt(const char *str, char **end, int *val)
{
    long long num;

    bool ret = parseLongLong(str, end, &num);
    if (!ret || num < INT_MIN || num > INT_MAX) {
        return false;
    }

    *val = (int) num;
    return true;
}

bool multibulkReadLen(File *fp, char type, int *length)
{
    char *end;
    char buf[64] = {0};

    ssize_t ret = FileGets(fp, buf, sizeof(buf) - strlen("\r\n"));
    if (ret <= 0 || buf[0] != type) {
        return false;
    }

    if (!parseInt(buf + 1, &end, length) ||
        *end != '\r' || *(end + 1) != '\n') {
        return false;
    }

    return true;
}

bool multibulkReadInt(File *fp, int *value)
{
    char buf[64] = {0};

    if (!multibulkReadStr(fp, buf, sizeof(buf)) ||
        !parseInt(buf, NULL, value)) {
        return false;
    }

    return true;
}

bool multibulkReadLong(File *fp, long *value)
{
    char buf[64] = {0};

    if (!multibulkReadStr(fp, buf, sizeof(buf)) ||
        !parseLong(buf, NULL, value)) {
        return false;
    }

    return true;
}

bool multibulkReadStr(File *fp, char *buf, size_t size)
{
    int len;
    char crlf[2];

    if (!multibulkReadLen(fp, '$', &len) ||
        len > (int) size) {
        return false;
    }

    if (FileRead(fp, buf, len) != len ||
        FileRead(fp, crlf, 2) != 2 ||
        crlf[0] != '\r' || crlf[1] != '\n') {
        return false;
    }

    buf[size - 1] = '\0';
    return true;
}

int multibulkWriteLen(void *buf, size_t cap, char prefix, int len)
{
    return safesnprintf(buf, cap, "%c%d\r\n", prefix, len);
}

int multibulkWriteInt(void *buf, size_t cap, int val)
{
    return multibulkWriteLong(buf, cap, val);
}

int multibulkWriteLong(void *buf, size_t cap, long val)
{
    int len = lensnprintf("%ld", val);
    return safesnprintf(buf, cap, "$%d\r\n%ld\r\n", len, val);
}

int multibulkWriteStr(void *buf, size_t cap, const char *val)
{
    int len = lensnprintf("%s", val);
    return safesnprintf(buf, cap, "$%d\r\n%s\r\n", len, val);
}
