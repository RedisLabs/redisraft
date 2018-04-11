#include <string.h>
#include <ctype.h>
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
