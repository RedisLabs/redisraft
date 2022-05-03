/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>
#include "redisraft.h"

int RedisModuleStringToInt(RedisModuleString *str, int *value)
{
    long long tmpll;
    if (RedisModule_StringToLongLong(str, &tmpll) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    if (tmpll < INT32_MIN || tmpll > INT32_MAX) {
        return REDISMODULE_ERR;
    }

    *value = tmpll;
    return REDISMODULE_OK;
}

char *StrCreate(const void *buf, size_t len)
{
    char *p;

    p = RedisModule_Alloc(len + 1);
    memcpy(p, buf, len);
    p[len] = '\0';

    return p;
}

char *StrCreateFromString(RedisModuleString *str)
{
    size_t len;
    const char *p = RedisModule_StringPtrLen(str, &len);

    return StrCreate(p, len);
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

/* Glob-style pattern matching. */
int stringmatchlen(const char *pattern, int patternLen, const char *string, int stringLen, int nocase)
{
    while (patternLen && stringLen) {
        switch (pattern[0]) {
        case '*':
            while (patternLen && pattern[1] == '*') {
                pattern++;
                patternLen--;
            }

            if (patternLen == 1) {
                return 1;    /* match */
            }

            while (stringLen) {
                if (stringmatchlen(pattern + 1, patternLen - 1,
                            string, stringLen, nocase)) {
                    return 1;    /* match */
                }

                string++;
                stringLen--;
            }

            return 0; /* no match */
            break;

        case '?':
            if (stringLen == 0) {
                return 0;    /* no match */
            }

            string++;
            stringLen--;
            break;

        case '[': {
            int not, match;

            pattern++;
            patternLen--;
            not = pattern[0] == '^';

            if (not) {
                pattern++;
                patternLen--;
            }

            match = 0;

            while (1) {
                if (pattern[0] == '\\' && patternLen >= 2) {
                    pattern++;
                    patternLen--;

                    if (pattern[0] == string[0]) {
                        match = 1;
                    }
                } else if (pattern[0] == ']') {
                    break;
                } else if (patternLen == 0) {
                    pattern--;
                    patternLen++;
                    break;
                } else if (patternLen >= 3 && pattern[1] == '-') {
                    int start = pattern[0];
                    int end = pattern[2];
                    int c = string[0];

                    if (start > end) {
                        int t = start;
                        start = end;
                        end = t;
                    }

                    if (nocase) {
                        start = tolower(start);
                        end = tolower(end);
                        c = tolower(c);
                    }

                    pattern += 2;
                    patternLen -= 2;

                    if (c >= start && c <= end) {
                        match = 1;
                    }
                } else {
                    if (!nocase) {
                        if (pattern[0] == string[0]) {
                            match = 1;
                        }
                    } else {
                        if (tolower((int)pattern[0]) == tolower((int)string[0])) {
                            match = 1;
                        }
                    }
                }

                pattern++;
                patternLen--;
            }

            if (not) {
                match = !match;
            }

            if (!match) {
                return 0;    /* no match */
            }

            string++;
            stringLen--;
            break;
        }

        case '\\':
            if (patternLen >= 2) {
                pattern++;
                patternLen--;
            }

            /* fall through */
        default:
            if (!nocase) {
                if (pattern[0] != string[0]) {
                    return 0;    /* no match */
                }
            } else {
                if (tolower((int)pattern[0]) != tolower((int)string[0])) {
                    return 0;    /* no match */
                }
            }

            string++;
            stringLen--;
            break;
        }

        pattern++;
        patternLen--;

        if (stringLen == 0) {
            while (*pattern == '*') {
                pattern++;
                patternLen--;
            }

            break;
        }
    }

    if (patternLen == 0 && stringLen == 0) {
        return 1;
    }

    return 0;
}

int stringmatch(const char *pattern, const char *string, int nocase)
{
    return stringmatchlen(pattern, strlen(pattern), string, strlen(string), nocase);
}

RRStatus parseMemorySize(const char *value, unsigned long *result)
{
    unsigned long val;
    char *eptr;

    val = strtoul(value, &eptr, 10);
    if (!val && eptr == value) {
        return RR_ERROR;
    }

    if (!*eptr) {
        /* No prefix */
    } else if (!strcasecmp(eptr, "kb")) {
        val *= 1000;
    } else if (!strcasecmp(eptr, "kib")) {
        val *= 1024;
    } else if (!strcasecmp(eptr, "mb")) {
        val *= 1000*1000;
    } else if (!strcasecmp(eptr, "mib")) {
        val *= 1024*1024;
    } else if (!strcasecmp(eptr, "gb")) {
        val *= 1000L*1000*1000;
    } else if (!strcasecmp(eptr, "gib")) {
        val *= 1024L*1024*1024;
    } else {
        return RR_ERROR;
    }

    *result = val;
    return RR_OK;
}

RRStatus formatExactMemorySize(unsigned long value, char *buf, size_t size)
{
    char suffix[4];

    if (!(value % (1000L*1000*1000))) {
        value /= 1000L*1000*1000;
        strcpy(suffix, "GB");
    } else if (!(value % (1024L*1024*1024))) {
        value /= 1024L*1024*1024;
        strcpy(suffix, "GiB");
    } else if (!(value % (1000L*1000))) {
        value /= 1000L*1000;
        strcpy(suffix, "MB");
    } else if (!(value % (1024L*1024))) {
        value /= 1024L*1024;
        strcpy(suffix, "MiB");
    } else if (!(value % 1000)) {
        value /= 1000;
        strcpy(suffix, "KB");
    } else if (!(value % 1024)) {
        value /= 1024;
        strcpy(suffix, "KiB");
    } else {
        suffix[0] = '\0';
    }

    if (snprintf(buf, size - 1, "%lu%s", value, suffix) == (int) (size - 1)) {
        /* Truncated... */
        return RR_ERROR;
    }

    return RR_OK;
}

void handleRMCallError(RedisModuleCtx *ctx, int ret_errno, const char *cmd, size_t cmd_len) {
    /* Try to produce an error message which is similar to Redis */
    int trunc_cmdlen = cmd_len > 256 ? 256 : (int) cmd_len;
    size_t errmsg_len = 128 + trunc_cmdlen;   /* Big enough for msg + cmd */
    char *errmsg = RedisModule_Alloc(errmsg_len);

    switch (ret_errno) {
        case ENOENT:
            snprintf(errmsg, errmsg_len, "ERR unknown command `%.*s`", trunc_cmdlen, cmd);
            break;
        case EINVAL:
            snprintf(errmsg, errmsg_len, "ERR wrong number of arguments for '%.*s' command",
                     trunc_cmdlen, cmd);
            break;
        default:
            snprintf(errmsg, errmsg_len, "ERR failed to execute command '%.*s'",
                     trunc_cmdlen, cmd);
    }
    RedisModule_ReplyWithError(ctx, errmsg);
    RedisModule_Free(errmsg);
}

/* This function assumes that the rr->config->slot_config has already been validated as valid */
ShardGroup * CreateAndFillShard(RedisRaftCtx *rr)
{
    ShardGroup *sg = ShardGroupCreate();

    if (!strcmp(rr->config->slot_config, "")) {
        goto exit;
    }

    char *str = RedisModule_Strdup(rr->config->slot_config);
    sg->slot_ranges_num = 1;
    char *pos = str;
    while ((pos = strchr(pos+1, ','))) {
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
            val = strtoul(pos+1, NULL, 10);
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

void AddBasicLocalShardGroup(RedisRaftCtx *rr) {
    ShardGroup *sg = CreateAndFillShard(rr);
    RedisModule_Assert(sg != NULL);

    sg->local = true;
    memcpy(sg->id, rr->log->dbid, RAFT_DBID_LEN);
    sg->id[RAFT_DBID_LEN] = 0;

    RRStatus ret = ShardingInfoAddShardGroup(rr, sg);
    RedisModule_Assert(ret == RR_OK);
}