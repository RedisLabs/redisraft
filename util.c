/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
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
    while (patternLen) {
        switch (pattern[0]) {
        case '*':
            while (pattern[1] == '*') {
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
                if (pattern[0] == '\\') {
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
                } else if (pattern[1] == '-' && patternLen >= 3) {
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

int RedisInfoIterate(const char **info_ptr, size_t *info_len,
        const char **key, size_t *keylen, const char **value, size_t *valuelen)
{
    while (*info_len > 0) {
        bool exit_loop = false;

        /* Find end of line */
        const char *eol = memchr(*info_ptr, '\n', *info_len);
        if (!eol) {
            return -1;
        }

        /* Line length is without CR/LF */
        int nl_offset = eol - *info_ptr;
        int line_len = nl_offset;
        if (line_len > 0 && (*info_ptr)[line_len-1] == '\r') {
            line_len--;
        }

        /* Skip empty line */
        if (!line_len) {
            goto next;
        }

        /* Find colon, skip lines without it */
        const char *colon = memchr(*info_ptr, ':', line_len);
        if (!colon) {
            goto next;
            continue;
        }

        /* Have it! */
        *key = *info_ptr;
        *keylen = colon - *key;
        *value = colon + 1;
        *valuelen = line_len - *keylen - 1;
        exit_loop = true;

next:
        *info_ptr += nl_offset + 1;
        *info_len -= nl_offset + 1;
        if (exit_loop) {
            return 1;
        }
    }
    return 0;
}

char *RedisInfoGetParam(RedisRaftCtx *rr, const char *section, const char *param)
{
    RedisModule_ThreadSafeContextLock(rr->ctx);
    RedisModuleCallReply *reply = RedisModule_Call(rr->ctx, "INFO", "c", section);
    RedisModule_ThreadSafeContextUnlock(rr->ctx);
    assert(reply != NULL);

    size_t info_len;
    const char *info = RedisModule_CallReplyProto(reply, &info_len);
    const char *key, *val;
    size_t keylen, vallen;
    int r;
    char *ret = NULL;

    while ((r = RedisInfoIterate(&info, &info_len, &key, &keylen, &val, &vallen))) {
        if (r == -1) {
            LOG_ERROR("Failed to parse INFO reply");
            goto exit;
        }

        if (keylen == strlen(param) && !memcmp(param, key, keylen)) {
            ret = RedisModule_Alloc(vallen + 1);
            memcpy(ret, val, vallen);
            ret[vallen] = '\0';

            break;
        }
    }

exit:
    RedisModule_FreeCallReply(reply);
    return ret;
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

RRStatus formatExactMemorySize(unsigned long value, char *buf, size_t buf_size)
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

    if (snprintf(buf, buf_size - 1, "%lu%s", value, suffix) == buf_size - 1) {
        /* Truncated... */
        return RR_ERROR;
    }

    return RR_OK;
}
